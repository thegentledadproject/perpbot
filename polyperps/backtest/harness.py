"""Event-driven hourly backtest (spec 5.2), following the live router's rules (Phase 2b Part A §6).

Point-in-time: the strategy receives bars[:t+1]. Fills happen at the 1-minute close latency_s
after the NEXT bar opens; no minute candle -> the hourly open (counted). At each bar, in order:
  1. the 15 % stop fires intrabar when the bar's high/low crosses it, at the stop price;
  2. funding is paid on the position's value at the bar close;
  3. a bar that is not complete exits to flat at its close and never enters (a next bar with no
     price at all - a hole in stored data - is flattened before, as it always was);
  4. the liquidation-distance and funding-cost exits (risk.liquidation_guard) run at the close;
  5. the strategy decides; a flip exits this bar and re-enters next bar only if the strategy
     still wants the other side.
Every trade that ends flat calls strategy.on_flatten(), as the router's fill handler does.
Costs are costs.fill_cost on the traded value - the same call SimExecutor makes. Fixed notional;
the guards use the router's 3x liquidation price.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import ROUND_DOWN, Decimal
from typing import Literal

from polyperps.backtest.bars import Bar, floor_minute
from polyperps.backtest.costs import fill_cost
from polyperps.backtest.strategy import Strategy, clamp_target
from polyperps.execution.types import PositionView
from polyperps.risk.liquidation_guard import LIMITS, check_open, funding_exit_due, liquidation_price, stop_price
from polyperps.signal.sufficiency import BAR

Kind = Literal["funding", "fill", "fill_unavailable", "gap_flatten", "mark", "stop", "guard_exit"]
_Q = Decimal("0.00000001")


@dataclass(frozen=True, slots=True, kw_only=True)
class LedgerRow:
    ts: datetime
    kind: Kind
    position: Decimal
    price: Decimal | None
    cash_delta: Decimal
    equity: Decimal
    note: str = ""


@dataclass(slots=True)
class BacktestResult:
    ledger: list[LedgerRow] = field(default_factory=list)
    equity: list[tuple[datetime, Decimal]] = field(default_factory=list)
    returns: list[Decimal] = field(default_factory=list)
    trade_pnls: list[Decimal] = field(default_factory=list)
    fill_notionals: list[Decimal] = field(default_factory=list)
    trades: list[tuple[datetime, str, Decimal]] = field(default_factory=list)  # (ts, side, quantity) per fill
    params: dict[str, object] = field(default_factory=dict)  # harness params + {"strategy_params": {...}}
    bars_total: int = 0
    bars_complete: int = 0
    bars_constant_spread: int = 0  # bars whose spread came from the constant fallback, not the book
    fills: int = 0
    fills_unavailable: int = 0
    fills_at_hourly_open: int = 0


def _sign(x: Decimal) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def _units(fraction: Decimal, notional: Decimal, price: Decimal) -> Decimal:
    """Contracts for a leg worth |fraction| x notional at `price`: the router's quantity rule."""
    return (abs(fraction) * notional / price).quantize(_Q, rounding=ROUND_DOWN)


class _Book:
    """Mutable position state for one run."""

    def __init__(self, notional: Decimal) -> None:
        self.notional = notional
        self.cash = Decimal(0)
        self.position = Decimal(0)
        self.entry = Decimal(0)
        self.funding = Decimal(0)   # cumulative since entry; negative = paid (PositionView convention)

    def unrealised(self, price: Decimal) -> Decimal:
        if self.position == 0:
            return Decimal(0)
        return self.position * self.notional * (price / self.entry - 1)

    def equity(self, price: Decimal | None) -> Decimal:
        return self.cash + (self.unrealised(price) if price is not None else Decimal(0))

    def view(self, instrument_id: int, price: Decimal) -> PositionView:
        """The book as the router's guards see a venue position."""
        size = self.position * self.notional / self.entry
        return PositionView(instrument_id=instrument_id, size=size, entry_price=self.entry,
                            notional=abs(size) * price, leverage=LIMITS.max_leverage,
                            liquidation_price=liquidation_price(size, self.entry),
                            unrealised_pnl=size * (price - self.entry), cumulative_funding=self.funding)


def run_backtest(
    bars: Sequence[Bar],
    strategy: Strategy,
    *,
    minute_closes: Mapping[datetime, Decimal],
    taker_fee_rate: Decimal,
    warmup: int,
    latency_s: int = BAR.latency_s,
    impact_bps: Decimal = BAR.impact_bps,
    notional: Decimal = BAR.notional_usd,
) -> BacktestResult:
    res = BacktestResult(
        params={"taker_fee_rate": str(taker_fee_rate), "latency_s": str(latency_s),
                "impact_bps": str(impact_bps), "notional": str(notional), "warmup": str(warmup),
                "strategy": strategy.name,
                "strategy_params": {k: str(v) for k, v in strategy.params.items()}},
        bars_total=len(bars),
        bars_complete=sum(1 for b in bars if b.complete),
        bars_constant_spread=sum(1 for b in bars if b.spread_source == "constant"),
    )
    book = _Book(notional)
    latency = timedelta(seconds=latency_s)
    last_equity = Decimal(0)  # equity starts at 0, so the first mark's return includes entry costs

    def log(ts: datetime, kind: Kind, price: Decimal | None, cash_delta: Decimal, note: str = "") -> None:
        res.ledger.append(LedgerRow(ts=ts, kind=kind, position=book.position, price=price,
                                    cash_delta=cash_delta, equity=book.equity(price), note=note))

    def trade_to(target: Decimal, price: Decimal, spread_bps: Decimal, ts: datetime, kind: Kind,
                 note: str = "") -> None:
        delta = target - book.position
        if delta == 0:
            return
        old_position, old_entry = book.position, book.entry
        # Flips never reach here: the decision step turns a flip into an exit (router rule).
        reducing = old_position != 0 and abs(target) < abs(old_position)
        if old_position != 0 and target == 0:
            realised = old_position * notional * (price / old_entry - 1)
            book.cash += realised
            res.trade_pnls.append(realised)
            new_entry = Decimal(0)
        elif reducing:
            # Same-direction reduction: realise only the closed portion; the retained leg keeps
            # its original cost basis.
            closed = abs(old_position) - abs(target)
            realised = closed * _sign(old_position) * notional * (price / old_entry - 1)
            book.cash += realised
            res.trade_pnls.append(realised)
            new_entry = old_entry
        else:
            # Opening from flat, or a same-direction increase: nothing realised; entry becomes
            # the size-weighted average of the retained and added notional.
            new_entry = (abs(old_position) * old_entry + abs(delta) * price) / abs(target)
        if reducing:
            # A closing leg trades at its current value - what the venue charges fees on.
            value = abs(delta) * notional * price / old_entry
            qty = _units(delta, notional, old_entry)
        else:
            value = abs(delta) * notional
            qty = _units(delta, notional, price)
        cost = fill_cost(notional_delta=value, notional=notional, spread_bps=spread_bps,
                         taker_fee_rate=taker_fee_rate, impact_bps=impact_bps)
        book.cash -= cost
        book.position = target
        book.entry = new_entry
        res.fill_notionals.append(value)
        res.trades.append((ts, "buy" if delta > 0 else "sell", qty))
        res.fills += 1
        log(ts, kind, price, -cost, note)
        if target == 0:
            book.funding = Decimal(0)
            hook = getattr(strategy, "on_flatten", None)   # simple test strategies may not have one
            if callable(hook):
                hook()

    def mark(ts: datetime, price: Decimal | None) -> None:
        nonlocal last_equity
        eq = book.equity(price)
        res.equity.append((ts, eq))
        res.returns.append((eq - last_equity) / notional)
        last_equity = eq
        log(ts, "mark", price, Decimal(0))

    for t in range(warmup, len(bars) - 1):
        bar, nxt = bars[t], bars[t + 1]

        # 1. Intrabar stop.
        # ponytail: stop and guards are bar-granular vs the router's 20 s loop; move to 1m bars once the 1m backfill exists
        if book.position != 0 and bar.high is not None and bar.low is not None:
            long = book.position > 0
            trigger = stop_price(side="long" if long else "short", entry=book.entry)
            if (bar.low <= trigger) if long else (bar.high >= trigger):
                trade_to(Decimal(0), trigger, bar.spread_bps, bar.open_ts, "stop")

        # 2. Funding on the position's value at the close.
        if book.position != 0 and bar.funding_rate is not None and bar.close is not None:
            paid = -book.position * notional * (bar.close / book.entry) * bar.funding_rate
            book.cash += paid
            book.funding += paid
            log(bar.open_ts, "funding", bar.close, paid)

        # 3. Gaps. Invariant: a position can only be non-zero here if the previous iteration saw
        # this bar priced (nxt.close not None), so bar.close is never None when book.position != 0.
        if not bar.complete or nxt.close is None:
            if book.position != 0 and bar.close is not None:
                trade_to(Decimal(0), bar.close, bar.spread_bps, nxt.open_ts, "gap_flatten")
            mark(nxt.open_ts, nxt.close)
            continue

        # 4. The router's protective exits, at the close.
        if book.position != 0:
            view = book.view(bar.instrument_id, bar.close)
            if check_open(view, mark=bar.close) == "flatten" or funding_exit_due(view):
                trade_to(Decimal(0), bar.close, bar.spread_bps, nxt.open_ts, "guard_exit")

        # 5. Decide. A flip exits this bar; re-entry is next bar's decision.
        target = clamp_target(strategy.target(bars[: t + 1]))
        if book.position != 0 and target != 0 and _sign(target) != _sign(book.position):
            target = Decimal(0)
        if target != book.position:
            fill_ts = nxt.open_ts + latency
            price = minute_closes.get(floor_minute(fill_ts))
            if price is not None:
                trade_to(target, price, bar.spread_bps, nxt.open_ts, "fill")
            elif nxt.open is not None:
                # Spec amendment (Task 5): proxy 1m candles exist for ~3.5 days only.
                # Fall back to the hourly open and COUNT it so records show the reliance.
                res.fills_at_hourly_open += 1
                trade_to(target, nxt.open, bar.spread_bps, nxt.open_ts, "fill",
                         note="fill_source=hourly_open")
            else:
                res.fills_unavailable += 1
                log(nxt.open_ts, "fill_unavailable", None, Decimal(0), f"no price at {fill_ts.isoformat()}")

        mark(nxt.open_ts, nxt.close)

    return res
