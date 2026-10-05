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
from polyperps.strategies.lead_lag import LeadLag

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 0, 0, tzinfo=UTC)
H = timedelta(hours=1)


def bar(i, close="100", funding="0", index=None, st=SourceType.POLYMARKET_REST):
    ts = T0 + i * H
    return Bar(instrument_id=6, source_type=st, open_ts=ts, open=Decimal(close), high=Decimal(close),
               low=Decimal(close), close=Decimal(close), index_close=Decimal(index) if index else None,
               funding_rate=Decimal(funding), spread_bps=Decimal("5"), spread_source="constant", complete=True)


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
    assert GRIDS["h4"] == [
        {"gap_bps": Decimal("25"), "hold_bars": 1}, {"gap_bps": Decimal("25"), "hold_bars": 3},
        {"gap_bps": Decimal("50"), "hold_bars": 1}, {"gap_bps": Decimal("50"), "hold_bars": 3},
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


def test_h2_window_is_last_lookback_aligned_pairs_and_skips_gaps_inside():
    # Spec 6: the window is the last `lookback` ALIGNED pairs, not the last `lookback` hours.
    # 27 hours: proxy missing at hours 3, 10 and 17 (three gaps inside the window), PM jumps to
    # 103 at the last hour. Only 24 aligned pairs exist over 27 hours -> the window is full
    # exactly at the last bar and the rich basis is faded (short PM).
    bars = [bar(i) for i in range(26)] + [bar(26, close="103")]
    hl = {b.open_ts: Decimal("100") for b in bars if b.open_ts not in {bars[3].open_ts, bars[10].open_ts, bars[17].open_ts}}
    s = Basis(lookback=24, entry_z=Decimal("2.0"), proxy_close_by_hour=hl)
    assert s.target(bars) == Decimal(-1)
    # Same data, one hour shorter: 26 hours present but only 23 aligned pairs -> flat.
    s2 = Basis(lookback=24, entry_z=Decimal("2.0"), proxy_close_by_hour=hl)
    assert s2.target(bars[:26]) == Decimal(0)


def test_h2_uses_older_pairs_to_fill_window_past_gaps():
    # 40 hours of history, proxy missing at 5 of the last 24 hours: the old rule found only 19
    # pairs in bars[-24:] and returned 0; the new rule reaches back to hour 11 for 24 pairs.
    bars = [bar(i) for i in range(39)] + [bar(39, close="103")]
    missing = {bars[i].open_ts for i in (20, 25, 30, 33, 37)}
    hl = {b.open_ts: Decimal("100") for b in bars if b.open_ts not in missing}
    s = Basis(lookback=24, entry_z=Decimal("2.0"), proxy_close_by_hour=hl)
    assert s.target(bars) == Decimal(-1)


def test_h2_is_flat_when_current_bar_has_no_close():
    bars = [bar(i) for i in range(24)]
    hl = {b.open_ts: Decimal("100") for b in bars}
    s = Basis(lookback=24, entry_z=Decimal("2.0"), proxy_close_by_hour=hl)
    from dataclasses import replace
    incomplete_last = bars[:-1] + [replace(bars[-1], close=None, complete=False)]
    assert s.target(incomplete_last) == Decimal(0)


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
    assert build_strategy("h4", GRIDS["h4"][0], proxy_close_by_hour={}).name == "h4_lead_lag"
    with pytest.raises(ValueError, match="h4 needs proxy_close_by_hour"):
        build_strategy("h4", GRIDS["h4"][0])
    with pytest.raises(ValueError):
        build_strategy("h2", GRIDS["h2"][0])
    with pytest.raises(ValueError):
        build_strategy("h9", {})


# --- final review I4: strategy position state survives a restart ---------------------------


def test_h1_on_recover_restores_position_through_a_neutral_bar():
    s = FundingReversion(lookback=48, entry_z=Decimal("1.5"), exit_z=Decimal("0.5"))
    s.on_recover(1)
    assert s.target([bar(i, funding="0.0001") for i in range(48)]) == Decimal(1)   # flat funding: z None -> hold
    s.on_recover(-1)
    assert s.target([bar(i, funding="0.0001") for i in range(48)]) == Decimal(-1)
    s.on_recover(0)
    assert s.target([bar(i, funding="0.0001") for i in range(48)]) == Decimal(0)


def test_h2_on_recover_restores_position_through_a_neutral_bar():
    bars = [bar(i) for i in range(3)]
    s = Basis(lookback=3, entry_z=Decimal("2.0"), proxy_close_by_hour={b.open_ts: Decimal(100) for b in bars})
    s.on_recover(1)
    assert s.target(bars) == Decimal(1)                # basis 0 everywhere: z None -> hold what we had


def test_h3_on_recover_restores_position_through_a_neutral_bar():
    s = IndexLag(entry_bps=Decimal("10"), hold_bars=3)
    s.on_recover(1)
    assert s.target([bar(0, index="100")]) == Decimal(1)   # held 1 of 3 bars


# --- H4: Lead-Lag ---


def nbar(i, close):
    """Native bar at T0 + i h whose close may be None (bar() always sets a close)."""
    ts = T0 + i * H
    c = Decimal(close) if close is not None else None
    return Bar(instrument_id=6, source_type=SourceType.POLYMARKET_REST, open_ts=ts, open=c, high=c, low=c,
               close=c, index_close=None, funding_rate=Decimal("0"), spread_bps=Decimal("5"),
               spread_source="constant", complete=True)


def h4(gap="25", hold=1, proxy=None):
    return LeadLag(gap_bps=Decimal(gap), hold_bars=hold, proxy_close_by_hour=proxy or {})


def hl(*pairs):
    return {T0 + i * H: Decimal(v) for i, v in pairs}


def test_h4_follows_the_leader_up_and_down():
    bars = [bar(0, "100"), bar(1, "100")]
    assert h4(proxy=hl((0, "100"), (1, "100.5"))).target(bars) == Decimal(1)    # HL +50 bps, PM flat
    assert h4(proxy=hl((0, "100"), (1, "99.5"))).target(bars) == Decimal(-1)    # HL -50 bps, PM flat
    assert h4().warmup == 2


def test_h4_no_entry_when_polymarket_already_matched_or_overshot():
    proxy = hl((0, "100"), (1, "100.5"))
    assert h4(proxy=proxy).target([bar(0, "100"), bar(1, "100.5")]) == 0   # PM moved as much: lag 0
    assert h4(proxy=proxy).target([bar(0, "100"), bar(1, "100.6")]) == 0   # PM moved further: lag opposite


def test_h4_no_entry_when_leader_moved_less_than_gap():
    # HL +20 bps, PM -10 bps: lag 30 bps >= 25 but the leader itself moved < 25 bps
    s = h4(proxy=hl((0, "100"), (1, "100.2")))
    assert s.target([bar(0, "100"), bar(1, "99.9")]) == 0


def test_h4_no_entry_on_missing_or_zero_data_or_non_adjacent_bars():
    proxy = hl((0, "100"), (1, "100.5"), (2, "100.5"))
    assert h4(proxy=hl((1, "100.5"))).target([bar(0, "100"), bar(1, "100")]) == 0        # no proxy at p
    assert h4(proxy=hl((0, "100"))).target([bar(0, "100"), bar(1, "100")]) == 0          # no proxy at c
    assert h4(proxy=hl((0, "0"), (1, "100.5"))).target([bar(0, "100"), bar(1, "100")]) == 0  # zero proxy
    assert h4(proxy=proxy).target([nbar(0, None), bar(1, "100")]) == 0                   # no native close at p
    assert h4(proxy=proxy).target([bar(0, "100"), nbar(1, None)]) == 0                   # no native close at c
    assert h4(proxy=proxy).target([bar(0, "100"), bar(2, "100")]) == 0                   # hour 1 missing
    assert h4(proxy=proxy).target([bar(1, "100")]) == 0                                  # one bar only


def test_h4_holds_exactly_hold_bars_and_does_not_reenter_on_the_exit_bar():
    # every hour HL jumps +50 bps while PM stays flat, so every bar is a trigger
    proxy = {T0 + i * H: Decimal(100) * Decimal("1.005") ** i for i in range(8)}
    bars = [bar(i, "100") for i in range(8)]
    s = h4(hold=3, proxy=proxy)
    assert s.target(bars[:2]) == Decimal(1)      # entry
    assert s.target(bars[:3]) == Decimal(1)      # held 1
    assert s.target(bars[:4]) == Decimal(1)      # held 2
    assert s.target(bars[:5]) == 0               # held 3 -> flat, trigger ignored on the exit bar
    assert s.target(bars[:6]) == Decimal(1)      # fresh entry on the next bar


def test_h4_keeps_counting_through_missing_data_while_open():
    proxy = hl((0, "100"), (1, "100.5"))         # no proxy after hour 1
    bars = [bar(0, "100"), bar(1, "100"), nbar(2, None), bar(3, "100")]
    s = h4(hold=2, proxy=proxy)
    assert s.target(bars[:2]) == Decimal(1)
    assert s.target(bars[:3]) == Decimal(1)      # missing close: still held
    assert s.target(bars[:4]) == 0               # exits on schedule


class _StrictProxy(dict):
    """Proxy closes that fail the test if the strategy reads an hour not yet in history."""

    def __init__(self, data, allowed):
        super().__init__(data)
        self.allowed = allowed

    def get(self, key, default=None):
        assert key in self.allowed, f"look-ahead: read proxy hour {key} not in history"
        return super().get(key, default)

    def __getitem__(self, key):
        assert key in self.allowed, f"look-ahead: read proxy hour {key} not in history"
        return super().__getitem__(key)


def test_h4_never_reads_a_proxy_hour_not_in_history():
    bars = [bar(i, "100") for i in range(3)]
    data = {T0 + i * H: Decimal(100) for i in range(6)}   # proxy has future hours 3..5
    s = h4(proxy=_StrictProxy(data, allowed={b.open_ts for b in bars}))
    for n in range(1, 4):
        s.target(bars[:n])


def test_h4_on_flatten_resets_position_and_hold_counter():
    proxy = {T0 + i * H: Decimal(100) * Decimal("1.005") ** i for i in range(4)}
    bars = [bar(i, "100") for i in range(4)]
    s = h4(hold=3, proxy=proxy)
    assert s.target(bars[:2]) == Decimal(1)
    s.target(bars[:3])                            # held 1
    s.on_flatten()
    assert s.target(bars[:4]) == Decimal(1)      # fresh entry: the counter restarted
    assert s._held == 0


def test_h4_on_recover_holds_the_recovered_side_for_hold_bars():
    s = h4(hold=2)
    s.on_recover(-1)
    assert s.target([bar(0, "100")]) == Decimal(-1)              # held 1 of 2
    assert s.target([bar(0, "100"), bar(1, "100")]) == 0         # held 2 -> flat
