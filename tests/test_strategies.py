from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from polyperps.backtest.bars import Bar
from polyperps.backtest.harness import run_backtest
from polyperps.exchange.types import SourceType
from polyperps.strategies import GRIDS, build_strategy
from polyperps.strategies._zscore import zscore
from polyperps.strategies.basis import Basis
from polyperps.strategies.funding_reversion import FundingReversion
from polyperps.strategies.index_lag import IndexLag

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 0, 0, tzinfo=UTC)
H = timedelta(hours=1)


def bar(i, close="100", funding="0", index=None, st=SourceType.POLYMARKET_REST):
    ts = T0 + i * H
    return Bar(instrument_id=6, source_type=st, open_ts=ts, open=Decimal(close), high=Decimal(close),
               low=Decimal(close), close=Decimal(close), index_close=Decimal(index) if index else None,
               funding_rate=Decimal(funding), spread_bps=Decimal("5"), complete=True)


def minutes(bars):
    return {b.open_ts + timedelta(minutes=m): b.close for b in bars for m in range(60)}


def test_zscore():
    assert zscore([Decimal(1)] * 5) is None
    assert zscore([Decimal(1), Decimal(2)]) is None
    z = zscore([Decimal(0)] * 9 + [Decimal(3)])
    assert z > 2


def test_grids_are_pre_registered():
    assert GRIDS["h1"] == [
        {"lookback": 48, "entry_z": Decimal("1.5"), "exit_z": Decimal("0.5")},
        {"lookback": 48, "entry_z": Decimal("2.0"), "exit_z": Decimal("0.5")},
        {"lookback": 168, "entry_z": Decimal("1.5"), "exit_z": Decimal("0.5")},
        {"lookback": 168, "entry_z": Decimal("2.0"), "exit_z": Decimal("0.5")},
    ]
    assert GRIDS["h2"] == [
        {"lookback": 24, "entry_z": Decimal("2.0")}, {"lookback": 24, "entry_z": Decimal("3.0")},
        {"lookback": 72, "entry_z": Decimal("2.0")}, {"lookback": 72, "entry_z": Decimal("3.0")},
    ]
    assert GRIDS["h3"] == [
        {"entry_bps": Decimal("10"), "hold_bars": 1}, {"entry_bps": Decimal("10"), "hold_bars": 3},
        {"entry_bps": Decimal("25"), "hold_bars": 1}, {"entry_bps": Decimal("25"), "hold_bars": 3},
    ]


def test_h1_shorts_extreme_positive_funding_and_profits_on_synthetic():
    # 47 calm hours, then funding spikes to +1%/h for 6 hours at flat price: a short collects 6% of notional.
    bars = [bar(i, funding="0.0001") for i in range(47)] + [bar(47 + j, funding="0.01") for j in range(8)]
    s = FundingReversion(lookback=48, entry_z=Decimal("1.5"), exit_z=Decimal("0.5"))
    assert s.warmup == 48
    res = run_backtest(bars, s, minute_closes=minutes(bars), taker_fee_rate=Decimal("0.0005"), warmup=s.warmup)
    assert res.fills >= 1
    (first_fill,) = [r for r in res.ledger if r.kind == "fill"][:1]
    assert first_fill.position == Decimal(-1)
    assert res.equity[-1][1] > Decimal("3")   # several 1%-of-notional funding receipts net of ~0.1 costs


def test_h1_never_trades_on_constant_funding():
    bars = [bar(i, funding="0.0001") for i in range(60)]
    s = FundingReversion(lookback=48, entry_z=Decimal("1.5"), exit_z=Decimal("0.5"))
    res = run_backtest(bars, s, minute_closes=minutes(bars), taker_fee_rate=Decimal("0.0005"), warmup=s.warmup)
    assert res.fills == 0


def test_h2_fades_rich_polymarket_basis():
    # PM flat at 100, HL flat at 100 for 24h, then PM jumps to 103 while HL stays -> basis z spikes -> short PM
    bars = [bar(i) for i in range(24)] + [bar(24 + j, close="103") for j in range(4)]
    hl = {b.open_ts: Decimal("100") for b in bars}
    s = Basis(lookback=24, entry_z=Decimal("2.0"), proxy_close_by_hour=hl)
    res = run_backtest(bars, s, minute_closes=minutes(bars), taker_fee_rate=Decimal("0.0005"), warmup=s.warmup)
    first = [r for r in res.ledger if r.kind == "fill"][0]
    assert first.position == Decimal(-1)


def test_h2_is_flat_when_proxy_hour_missing():
    bars = [bar(i) for i in range(30)]
    s = Basis(lookback=24, entry_z=Decimal("2.0"), proxy_close_by_hour={})
    assert s.target(bars[:25]) == 0


def test_h3_trades_toward_index_and_holds_then_exits():
    # mark 100 vs index 100.5 -> premium -50bps -> mark should catch up -> long; hold 1 bar then flat
    bars = [bar(0, index="100"), bar(1, index="100.5"), bar(2, index="100.5"), bar(3, index="100.5"), bar(4, index="100.5")]
    s = IndexLag(entry_bps=Decimal("25"), hold_bars=1)
    assert s.warmup == 1
    assert s.target(bars[:2]) == Decimal(1)
    # strategies carry position state between calls: use a fresh instance for the harness run
    res = run_backtest(bars, IndexLag(entry_bps=Decimal("25"), hold_bars=1),
                       minute_closes=minutes(bars), taker_fee_rate=Decimal("0.0005"), warmup=1)
    positions = [r.position for r in res.ledger if r.kind == "fill"]
    assert positions[:2] == [Decimal(1), Decimal(0)]


def test_h3_returns_zero_on_proxy_bars():
    bars = [bar(i, st=SourceType.PROXY_HYPERLIQUID) for i in range(3)]
    s = IndexLag(entry_bps=Decimal("10"), hold_bars=1)
    assert s.target(bars) == 0


def test_h1_on_flatten_resets_stale_position():
    bars = [bar(i, funding="0.0001") for i in range(47)] + [bar(47 + j, funding="0.01") for j in range(2)]
    s = FundingReversion(lookback=48, entry_z=Decimal("1.5"), exit_z=Decimal("0.5"))
    assert s.target(bars) == Decimal(-1)  # entered short on the funding spike
    s.on_flatten()
    neutral = [bar(i, funding="0.0001") for i in range(48)]  # constant funding -> z is None -> returns _position
    assert s.target(neutral) == Decimal(0)  # without the reset this would still be -1


def test_h2_on_flatten_resets_stale_position():
    bars = [bar(i) for i in range(24)] + [bar(24 + j, close="103") for j in range(2)]
    hl = {b.open_ts: Decimal("100") for b in bars}
    s = Basis(lookback=24, entry_z=Decimal("2.0"), proxy_close_by_hour=hl)
    assert s.target(bars) == Decimal(-1)  # entered short on the rich basis
    s.on_flatten()
    neutral = [bar(i) for i in range(24)]  # flat basis throughout -> z is None -> returns _position
    assert s.target(neutral) == Decimal(0)  # without the reset this would still be -1


def test_h2_early_return_resets_state_on_missing_proxy_hour():
    bars = [bar(i) for i in range(24)] + [bar(24 + j, close="103") for j in range(2)]
    hl = {b.open_ts: Decimal("100") for b in bars}
    s = Basis(lookback=24, entry_z=Decimal("2.0"), proxy_close_by_hour=hl)
    assert s.target(bars) == Decimal(-1)  # entered short
    missing_hour = bars + [bar(26)]  # bar 26's open_ts is not in hl
    assert s.target(missing_hour) == Decimal(0)  # early return
    neutral = [bar(i) for i in range(24)]
    assert s.target(neutral) == Decimal(0)  # a following neutral call must not see the stale -1


def test_h3_on_flatten_resets_stale_position_and_hold_counter():
    bars = [bar(0, index="100"), bar(1, index="100.5")]
    s = IndexLag(entry_bps=Decimal("25"), hold_bars=3)
    assert s.target(bars[:2]) == Decimal(1)  # entered long
    s.on_flatten()
    neutral = [bar(2, close="100", index="100")]  # premium 0bps, below entry_bps -> no re-entry
    assert s.target(neutral) == Decimal(0)  # without the reset this would still be 1 (mid-hold)


def test_build_strategy():
    assert build_strategy("h1", GRIDS["h1"][0]).name == "h1_funding_reversion"
    assert build_strategy("h3", GRIDS["h3"][0]).name == "h3_index_lag"
    assert build_strategy("h2", GRIDS["h2"][0], proxy_close_by_hour={}).name == "h2_basis"
    with pytest.raises(ValueError):
        build_strategy("h2", GRIDS["h2"][0])
    with pytest.raises(ValueError):
        build_strategy("h9", {})
