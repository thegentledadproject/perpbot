from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.backtest.bars import Bar
from polyperps.backtest.harness import run_backtest
from polyperps.backtest.strategy import clamp_target
from polyperps.exchange.types import SourceType

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 0, 0, tzinfo=UTC)
H = timedelta(hours=1)
FEE = Decimal("0.0005")


def bar(i, close="100", funding="0", complete=True):
    ts = T0 + i * H
    c = Decimal(close) if complete else None
    return Bar(instrument_id=6, source_type=SourceType.POLYMARKET_REST, open_ts=ts, open=c, high=c, low=c,
               close=c, index_close=None, funding_rate=Decimal(funding) if complete else None,
               spread_bps=Decimal("10"), complete=complete)


def minutes(bars, price_by_hour=None):
    """1m closes for every minute of every bar = that bar's close (or an override)."""
    out = {}
    for b in bars:
        px = (price_by_hour or {}).get(b.open_ts, b.close)
        if px is None:
            continue
        for m in range(60):
            out[b.open_ts + timedelta(minutes=m)] = px
    return out


class Const:
    name = "const"
    params = {}

    def __init__(self, x):
        self.x = Decimal(x)

    def target(self, history):
        return self.x


class Recorder:
    name = "rec"
    params = {}

    def __init__(self):
        self.seen = []

    def target(self, history):
        self.seen.append((len(history), history[-1].open_ts))
        return Decimal(0)


def test_clamp():
    assert clamp_target(Decimal("1.7")) == 1 and clamp_target(Decimal("-3")) == -1 and clamp_target(Decimal("0.2")) == Decimal("0.2")


def test_strategy_sees_exactly_bars_up_to_t():
    bars = [bar(i) for i in range(6)]
    rec = Recorder()
    run_backtest(bars, rec, minute_closes=minutes(bars), taker_fee_rate=FEE, warmup=2)
    assert rec.seen == [(3, T0 + 2 * H), (4, T0 + 3 * H), (5, T0 + 4 * H)]


def test_long_pays_positive_funding_exactly():
    bars = [bar(0), bar(1, funding="0.001"), bar(2, funding="0.001"), bar(3)]
    res = run_backtest(bars, Const(1), minute_closes=minutes(bars), taker_fee_rate=Decimal(0), warmup=0)
    # enters at bar1 open (fill at t0+1h+2s), pays funding on bars 1 and 2 -> 2 * 100 * 0.001 = 0.2
    funding_rows = [r for r in res.ledger if r.kind == "funding"]
    assert sum(r.cash_delta for r in funding_rows) == Decimal("-0.2")
    assert res.fills == 1


def test_fill_pays_fee_spread_and_impact():
    bars = [bar(0), bar(1)]
    res = run_backtest(bars, Const(1), minute_closes=minutes(bars), taker_fee_rate=FEE, warmup=0)
    (fill,) = [r for r in res.ledger if r.kind == "fill"]
    # delta 100: fee .05 + half-spread 10bps/2*100 = .05 + impact 5bps*1*100 = .05 -> 0.15
    assert fill.cash_delta == Decimal("-0.15")
    assert res.fill_notionals == [Decimal("100")]


def test_fill_uses_minute_close_after_latency():
    bars = [bar(0, close="100"), bar(1, close="100")]
    mc = minutes(bars)
    mc[T0 + H] = Decimal("101")  # the minute containing open+2s
    res = run_backtest(bars, Const(1), minute_closes=mc, taker_fee_rate=Decimal(0), warmup=0, impact_bps=Decimal(0))
    (fill,) = [r for r in res.ledger if r.kind == "fill"]
    assert fill.price == Decimal("101")


def test_missing_minute_candle_falls_back_to_hourly_open_and_counts_it():
    bars = [bar(0), bar(1, close="103"), bar(2)]
    res = run_backtest(bars, Const(1), minute_closes={}, taker_fee_rate=FEE, warmup=0)
    (fill,) = [r for r in res.ledger if r.kind == "fill"]
    assert fill.price == Decimal("103") and fill.note == "fill_source=hourly_open"
    assert res.fills == 1 and res.fills_at_hourly_open == 1 and res.fills_unavailable == 0


def test_minute_candle_preferred_over_hourly_open():
    bars = [bar(0), bar(1, close="103")]
    mc = {T0 + H: Decimal("101")}
    res = run_backtest(bars, Const(1), minute_closes=mc, taker_fee_rate=FEE, warmup=0)
    (fill,) = [r for r in res.ledger if r.kind == "fill"]
    assert fill.price == Decimal("101") and fill.note == "" and res.fills_at_hourly_open == 0


def test_gap_forces_flatten_and_blocks_reentry_until_complete():
    bars = [bar(0), bar(1), bar(2), bar(3, complete=False), bar(4), bar(5)]
    res = run_backtest(bars, Const(1), minute_closes=minutes(bars), taker_fee_rate=Decimal(0), warmup=0)
    kinds = [(r.ts, r.kind) for r in res.ledger if r.kind in ("fill", "gap_flatten")]
    assert (T0 + 2 * H, "gap_flatten") in kinds          # flattened at bar 2 close before the gap at bar 3
    assert not any(ts == T0 + 3 * H and k == "fill" for ts, k in kinds)
    assert (T0 + 5 * H, "fill") in kinds                  # re-enters after the next complete bar
    assert res.bars_complete == 5


def test_equity_and_returns_track_price():
    bars = [bar(0, "100"), bar(1, "100"), bar(2, "110"), bar(3, "110")]
    res = run_backtest(bars, Const(1), minute_closes=minutes(bars), taker_fee_rate=Decimal(0), warmup=0,
                       impact_bps=Decimal(0))
    # enter at 100 (bar1 open), cost half-spread 10bps/2 * 100 = 0.05; mark at bar2 close 110 -> +10
    final = res.equity[-1][1]
    assert final == Decimal("9.95")
    assert sum(res.returns) == Decimal("0.0995")


def zero_spread(bars):
    """Drop spread cost so cash only moves from realised PnL, for clean hand-checkable numbers."""
    return [replace(b, spread_bps=Decimal(0)) for b in bars]


def test_increase_does_not_realise_and_averages_entry():
    bars = zero_spread([bar(0, "100"), bar(1, "100"), bar(2, "110"), bar(3, "110")])

    class Increase:
        name = "inc"; params = {}
        def target(self, history):
            return Decimal("0.5") if len(history) < 2 else Decimal("1.0")

    res = run_backtest(bars, Increase(), minute_closes=minutes(bars), taker_fee_rate=Decimal(0), warmup=0,
                       impact_bps=Decimal(0))
    # 0.5 opened at 100 (bar1 fill), increased to 1.0 at 110 (bar2 fill). Spread/fee/impact are all
    # 0, so cash never moves and nothing is realised on either fill: entry becomes the size-weighted
    # average (0.5*100 + 0.5*110) / 1.0 = 105.
    assert res.trade_pnls == []
    fill_rows = [r for r in res.ledger if r.kind == "fill"]
    assert len(fill_rows) == 2
    # cash == 0 throughout, so the final mark's equity IS the unrealised PnL at entry 105.
    final_equity = res.equity[-1][1]
    assert final_equity == Decimal("1.0") * Decimal("100") * (Decimal("110") / Decimal("105") - 1)


def test_partial_reduction_realises_only_closed_portion():
    bars = zero_spread([bar(0, "100"), bar(1, "100"), bar(2, "110"), bar(3, "110")])

    class Reduce:
        name = "red"; params = {}
        def target(self, history):
            return Decimal("1.0") if len(history) < 2 else Decimal("0.5")

    res = run_backtest(bars, Reduce(), minute_closes=minutes(bars), taker_fee_rate=Decimal(0), warmup=0,
                       impact_bps=Decimal(0))
    # 1.0 opened at 100 (bar1 fill), reduced to 0.5 at 110 (bar2 fill):
    # closed = 1.0 - 0.5 = 0.5; realised = 0.5 * 100 * (110/100 - 1) = 5; entry stays 100 (unchanged).
    assert res.trade_pnls == [Decimal("5")]
    # cash = realised 5 (zero costs); final equity = cash + unrealised(0.5 @ entry 100, price 110)
    #      = 5 + 0.5*100*(110/100 - 1) = 5 + 5 = 10.
    final_equity = res.equity[-1][1]
    assert final_equity == Decimal("10")


def test_flip_realises_trade_pnl():
    bars = [bar(0, "100"), bar(1, "100"), bar(2, "105"), bar(3, "105"), bar(4, "105")]

    class Flip:
        name = "flip"; params = {}
        def target(self, history):
            return Decimal(1) if len(history) < 3 else Decimal(-1)

    res = run_backtest(bars, Flip(), minute_closes=minutes(bars), taker_fee_rate=Decimal(0), warmup=0,
                       impact_bps=Decimal(0))
    assert res.trade_pnls == [Decimal("5")]  # long from 100, flipped at 105
    assert res.fill_notionals == [Decimal("100"), Decimal("200")]
