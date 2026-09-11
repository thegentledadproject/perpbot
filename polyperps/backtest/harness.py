"""Event-driven hourly backtest (spec 5.2).

Point-in-time: the strategy receives bars[:t+1]. Fills happen at the 1-minute
close latency_s after the NEXT bar opens; no minute candle -> no fill.
Gaps: flatten before an incomplete bar, no re-entry until a complete one.
Fixed notional, 1x, no liquidation modelling (Phase 2 owns sizing/leverage).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal

from polyperps.backtest.bars import Bar, floor_minute
from polyperps.backtest.costs import fill_cost
from polyperps.backtest.strategy import Strategy, clamp_target
from polyperps.signal.sufficiency import BAR

Kind = Literal["funding", "fill", "fill_unavailable", "gap_flatten", "mark"]


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
    params: dict[str, str] = field(default_factory=dict)
    bars_total: int = 0
    bars_complete: int = 0
    fills: int = 0
    fills_unavailable: int = 0
    fills_at_hourly_open: int = 0


def _sign(x: Decimal) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


class _Book:
    """Mutable position state for one run."""

    def __init__(self, notional: Decimal) -> None:
        self.notional = notional
        self.cash = Decimal(0)
        self.position = Decimal(0)
        self.entry = Decimal(0)

    def unrealised(self, price: Decimal) -> Decimal:
        if self.position == 0:
            return Decimal(0)
        return self.position * self.notional * (price / self.entry - 1)

    def equity(self, price: Decimal | None) -> Decimal:
        return self.cash + (self.unrealised(price) if price is not None else Decimal(0))


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
                "strategy": strategy.name, **{k: str(v) for k, v in strategy.params.items()}},
        bars_total=len(bars),
        bars_complete=sum(1 for b in bars if b.complete),
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
        if old_position != 0 and (target == 0 or _sign(target) != _sign(old_position)):
            # Close or flip: realise the full old leg at the fill price; the new leg (if any)
            # starts fresh at this fill.
            realised = old_position * notional * (price / old_entry - 1)
            book.cash += realised
            res.trade_pnls.append(realised)
            new_entry = price if target != 0 else Decimal(0)
        elif old_position != 0 and abs(target) < abs(old_position):
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
        notional_delta = abs(delta) * notional
        cost = fill_cost(notional_delta=notional_delta, notional=notional, spread_bps=spread_bps,
                         taker_fee_rate=taker_fee_rate, impact_bps=impact_bps)
        book.cash -= cost
        book.position = target
        book.entry = new_entry
        res.fill_notionals.append(notional_delta)
        res.fills += 1
        log(ts, kind, price, -cost, note)

    def mark(ts: datetime, price: Decimal | None) -> None:
        nonlocal last_equity
        eq = book.equity(price)
        res.equity.append((ts, eq))
        res.returns.append((eq - last_equity) / notional)
        last_equity = eq
        log(ts, "mark", price, Decimal(0))

    for t in range(warmup, len(bars) - 1):
        bar, nxt = bars[t], bars[t + 1]

        if book.position != 0 and bar.funding_rate is not None:
            paid = -book.position * notional * bar.funding_rate
            book.cash += paid
            log(bar.open_ts, "funding", bar.close, paid)

        if not nxt.complete or not bar.complete:
            # Invariant: a position can only be non-zero here if the previous iteration's
            # completeness check passed for this same bar, so bar.close is never None when
            # book.position != 0 — the guard below is defensive, not a live path.
            if book.position != 0 and bar.close is not None:
                trade_to(Decimal(0), bar.close, bar.spread_bps, bar.open_ts, "gap_flatten")
                # Simple test strategies may not implement on_flatten; only real ones need to
                # reset internal position/hold state after a forced flatten.
                hook = getattr(strategy, "on_flatten", None)
                if callable(hook):
                    hook()
            mark(nxt.open_ts, nxt.close)
            continue

        target = clamp_target(strategy.target(bars[: t + 1]))
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
