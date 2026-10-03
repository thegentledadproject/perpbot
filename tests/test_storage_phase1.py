import dataclasses
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.exchange.types import (
    BookLevel, BookSnapshot, Candle, FeeSchedule, FundingObservation, SourceType, Tick,
)
from polyperps.storage.db import (
    connect, insert_book, insert_candle, insert_fee, insert_funding, insert_tick,
    latest_fee, query_book_spread_bps, query_book_spread_bps_by_hour, query_candles, query_funding,
    query_last_index_by_hour, query_ticks, rollup_hours,
)

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)
H = timedelta(hours=1)


def funding(ts, st, rate="0.0001"):
    return FundingObservation(instrument_id=6, funding_rate=Decimal(rate), exchange_ts=ts,
                              received_ts=ts, source_type=st)


def candle(ts, st, interval="1h", close="100"):
    return Candle(instrument_id=6, interval=interval, open_ts=ts, open=Decimal("99"), high=Decimal("101"),
                  low=Decimal("98"), close=Decimal(close), volume=Decimal("1"), trades=1,
                  received_ts=ts, source_type=st)


def test_fee_schedule_round_trip_and_latest():
    conn = connect(":memory:")
    older = FeeSchedule(category="crypto", taker_fee_rate=Decimal("0.0005"), maker_fee_rate=Decimal("0.0002"), fetched_at=T0)
    newer = FeeSchedule(category="crypto", taker_fee_rate=Decimal("0.0006"), maker_fee_rate=Decimal("0.0002"), fetched_at=T0 + H)
    assert insert_fee(conn, older) is True
    assert insert_fee(conn, newer) is True
    assert insert_fee(conn, newer) is False
    assert latest_fee(conn, "crypto") == newer
    assert latest_fee(conn, "equity") is None


def test_query_funding_filters_by_source_type():
    conn = connect(":memory:")
    insert_funding(conn, funding(T0, SourceType.POLYMARKET_REST, "0.0001"))
    insert_funding(conn, funding(T0, SourceType.PROXY_HYPERLIQUID, "0.0009"))
    both = query_funding(conn, 6, start=T0, end=T0)
    native = query_funding(conn, 6, start=T0, end=T0, source_type=SourceType.POLYMARKET_REST)
    proxy = query_funding(conn, 6, start=T0, end=T0, source_type=SourceType.PROXY_HYPERLIQUID)
    assert len(both) == 2
    assert [f.funding_rate for f in native] == [Decimal("0.0001")]
    assert [f.funding_rate for f in proxy] == [Decimal("0.0009")]


def test_query_ticks_filters_by_source_type():
    conn = connect(":memory:")
    for st in (SourceType.POLYMARKET_WS, SourceType.POLYMARKET_REST):
        insert_tick(conn, Tick(instrument_id=6, mark_price=Decimal("100"), index_price=Decimal("100"),
                               last_price=Decimal("100"), funding_rate=Decimal("0"), next_funding=T0,
                               exchange_ts=T0, received_ts=T0, source_type=st, sequence=1))
    assert len(query_ticks(conn, 6, start=T0, end=T0)) == 2
    assert len(query_ticks(conn, 6, start=T0, end=T0, source_type=SourceType.POLYMARKET_WS)) == 1


def test_query_candles_by_interval_and_source_ordered():
    conn = connect(":memory:")
    insert_candle(conn, candle(T0 + H, SourceType.POLYMARKET_REST, close="102"))
    insert_candle(conn, candle(T0, SourceType.POLYMARKET_REST, close="101"))
    insert_candle(conn, candle(T0, SourceType.POLYMARKET_REST, interval="1m"))
    insert_candle(conn, candle(T0, SourceType.PROXY_HYPERLIQUID, close="999"))
    rows = query_candles(conn, 6, interval="1h", source_type=SourceType.POLYMARKET_REST, start=T0, end=T0 + H)
    assert [c.close for c in rows] == [Decimal("101"), Decimal("102")]


def test_query_book_spread_bps():
    conn = connect(":memory:")
    snap = BookSnapshot(instrument_id=6,
                        bids=(BookLevel(price=Decimal("99.5"), quantity=Decimal("1")),
                              BookLevel(price=Decimal("99.0"), quantity=Decimal("5"))),
                        asks=(BookLevel(price=Decimal("100.5"), quantity=Decimal("1")),),
                        exchange_ts=T0, received_ts=T0, source_type=SourceType.POLYMARKET_REST)
    empty = BookSnapshot(instrument_id=6, bids=(), asks=(), exchange_ts=T0 + H, received_ts=T0 + H,
                         source_type=SourceType.POLYMARKET_REST)
    insert_book(conn, snap)
    insert_book(conn, empty)
    rows = query_book_spread_bps(conn, 6, start=T0, end=T0 + H)
    assert rows == [(T0, Decimal("100"))]  # (100.5-99.5)/100 * 1e4 = 100 bps


def _tick(ts, index, st=SourceType.POLYMARKET_WS, seq=None):
    return Tick(instrument_id=6, mark_price=Decimal("100"), index_price=Decimal(index), last_price=Decimal("100"),
                funding_rate=Decimal("0"), next_funding=ts, exchange_ts=ts, received_ts=ts, source_type=st,
                sequence=seq if seq is not None else int(ts.timestamp()))


def test_query_last_index_by_hour_last_per_hour_wins_native_only():
    conn = connect(":memory:")
    insert_tick(conn, _tick(T0 + timedelta(minutes=10), "100.1"))
    insert_tick(conn, _tick(T0 + timedelta(minutes=50), "100.9"))                # last in hour 0
    insert_tick(conn, _tick(T0 + timedelta(minutes=55), "777", st=SourceType.PROXY_HYPERLIQUID))  # ignored
    insert_tick(conn, _tick(T0 + H + timedelta(minutes=1), "101.0", st=SourceType.POLYMARKET_REST))
    insert_tick(conn, _tick(T0 + 2 * H + timedelta(minutes=30), "102.0"))     # outside [start, end]
    insert_tick(conn, _tick(T0 - timedelta(minutes=1), "99.0"))                 # before start
    out = query_last_index_by_hour(conn, 6, start=T0, end=T0 + 2 * H - timedelta(microseconds=1))
    assert out == {T0: Decimal("100.9"), T0 + H: Decimal("101.0")}
    assert query_last_index_by_hour(conn, 7, start=T0, end=T0 + 2 * H) == {}


def test_query_last_index_by_hour_matches_native_sources_constant():
    from polyperps.signal.sufficiency import NATIVE_SOURCES
    from polyperps.storage.db import _NATIVE_TICK_SOURCES
    assert set(_NATIVE_TICK_SOURCES) == {s.value for s in NATIVE_SOURCES}


def _book(ts, bid, ask):
    return BookSnapshot(instrument_id=6, bids=(BookLevel(price=Decimal(bid), quantity=Decimal(1)),),
                        asks=(BookLevel(price=Decimal(ask), quantity=Decimal(1)),),
                        exchange_ts=ts, received_ts=ts, source_type=SourceType.POLYMARKET_REST)


def test_query_book_spread_bps_by_hour_groups_and_skips_one_sided_books():
    conn = connect(":memory:")
    insert_book(conn, _book(T0 + timedelta(minutes=5), "99.5", "100.5"))    # 100 bps, hour 0
    insert_book(conn, _book(T0 + timedelta(minutes=35), "99.9", "100.1"))   # 20 bps, hour 0
    insert_book(conn, BookSnapshot(instrument_id=6, bids=(), asks=(BookLevel(price=Decimal(1), quantity=Decimal(1)),),
                                   exchange_ts=T0 + timedelta(minutes=40), received_ts=T0,
                                   source_type=SourceType.POLYMARKET_REST))  # one-sided: skipped
    insert_book(conn, _book(T0 + H + timedelta(minutes=1), "99", "101"))    # 200 bps, hour 1
    insert_book(conn, _book(T0 + 3 * H, "99", "101"))                        # outside range
    out = query_book_spread_bps_by_hour(conn, 6, start=T0, end=T0 + 2 * H - timedelta(microseconds=1))
    assert out == {T0: [Decimal("100"), Decimal("20")], T0 + H: [Decimal("200")]}
    # the per-snapshot query and the grouped query agree
    flat = query_book_spread_bps(conn, 6, start=T0, end=T0 + 2 * H - timedelta(microseconds=1))
    assert [bps for _, bps in flat] == [Decimal("100"), Decimal("20"), Decimal("200")]


def test_query_last_index_by_hour_same_timestamp_tie_is_one_value():
    """Two native ticks (WS and REST) share the hour's last exchange_ts: one value for that hour, no crash."""
    conn = connect(":memory:")
    ts = T0 + timedelta(minutes=30)
    insert_tick(conn, _tick(ts, "100", seq=1))
    insert_tick(conn, _tick(ts, "101", st=SourceType.POLYMARKET_REST, seq=2))
    out = query_last_index_by_hour(conn, 6, start=T0, end=T0 + H - timedelta(microseconds=1))
    assert list(out) == [T0] and out[T0] in (Decimal("100"), Decimal("101"))


def test_query_last_index_by_hour_uses_one_indexed_seek_per_hour():
    """One ticks_by_time seek per hour (LIMIT 1), not a read of every tick in the window."""
    import inspect

    import polyperps.storage.db as dbmod
    src = inspect.getsource(dbmod.query_last_index_by_hour)
    assert "INDEXED BY ticks_by_time" in src and "LIMIT 1" in src


def test_query_last_index_by_hour_window_starting_and_ending_mid_hour():
    conn = connect(":memory:")
    for ts, px in [(T0 + timedelta(minutes=10), "100"), (T0 + timedelta(minutes=50), "101"),
                   (T0 + H + timedelta(minutes=5), "102"), (T0 + H + timedelta(minutes=40), "103")]:
        insert_tick(conn, _tick(ts, px))
    out = query_last_index_by_hour(conn, 6, start=T0 + timedelta(minutes=20), end=T0 + H + timedelta(minutes=30))
    assert out == {T0: Decimal("101"), T0 + H: Decimal("102")}


# ---- hourly_rollup ----

def _seed_three_hours(conn):
    for h in range(3):
        insert_tick(conn, _tick(T0 + h * H + timedelta(minutes=10), f"10{h}.1"))
        insert_tick(conn, _tick(T0 + h * H + timedelta(minutes=50), f"10{h}.9"))
        insert_book(conn, _book(T0 + h * H + timedelta(minutes=5), "99.5", "100.5"))   # 100 bps
        insert_book(conn, _book(T0 + h * H + timedelta(minutes=15), "99", "101"))      # ~200 bps
        insert_book(conn, _book(T0 + h * H + timedelta(minutes=25), "99.9", "100.1"))  # ~20 bps
    conn.commit()


def _median_spreads(d):
    return {h: sorted(v)[len(v) // 2] for h, v in d.items()}


def test_fill_latency_constant_matches_bar():
    from polyperps.signal.sufficiency import BAR
    from polyperps.storage.db import _FILL_LATENCY
    assert _FILL_LATENCY == timedelta(seconds=BAR.latency_s)


def test_rollup_round_trip_survives_raw_deletion():
    conn = connect(":memory:")
    _seed_three_hours(conn)
    kw = dict(start=T0, end=T0 + 3 * H - timedelta(microseconds=1))
    idx, spr = query_last_index_by_hour(conn, 6, **kw), query_book_spread_bps_by_hour(conn, 6, **kw)
    assert len(idx) == 3 and len(spr) == 3
    assert rollup_hours(conn, now=T0 + 3 * H) == 3
    conn.execute("DELETE FROM ticks")
    conn.execute("DELETE FROM book_snapshots")
    assert query_last_index_by_hour(conn, 6, **kw) == idx
    got = query_book_spread_bps_by_hour(conn, 6, **kw)
    assert _median_spreads(got) == _median_spreads(spr)


def test_rollup_skips_the_open_hour():
    conn = connect(":memory:")
    insert_tick(conn, _tick(T0 + timedelta(minutes=1), "100"))
    insert_tick(conn, _tick(T0 + H + timedelta(minutes=1), "101"))
    assert rollup_hours(conn, now=T0 + H + timedelta(minutes=30)) == 1
    assert [r[0] for r in conn.execute("SELECT hour_ts FROM hourly_rollup")] == [T0.isoformat()]


def test_rollup_is_idempotent_and_resumes_from_max_hour():
    conn = connect(":memory:")
    insert_tick(conn, _tick(T0 + timedelta(minutes=1), "100"))
    assert rollup_hours(conn, now=T0 + H) == 1
    assert rollup_hours(conn, now=T0 + H) == 0
    insert_tick(conn, _tick(T0 + H + timedelta(minutes=1), "101"))
    assert rollup_hours(conn, now=T0 + 2 * H) == 1
    assert conn.execute("SELECT COUNT(*) FROM hourly_rollup").fetchone()[0] == 2


def test_rollup_open_mark_is_last_native_tick_within_fill_latency():
    conn = connect(":memory:")
    def mk(ts, mark, st=SourceType.POLYMARKET_WS):
        t = _tick(ts, "100", st)
        return dataclasses.replace(t, mark_price=Decimal(mark))
    insert_tick(conn, mk(T0 - timedelta(minutes=30), "90"))
    insert_tick(conn, mk(T0 + timedelta(seconds=1), "91"))
    insert_tick(conn, mk(T0 + timedelta(seconds=1, milliseconds=500), "93", SourceType.PROXY_HYPERLIQUID))  # ignored
    insert_tick(conn, mk(T0 + timedelta(seconds=3), "94"))                                                 # after T0+2s
    insert_tick(conn, mk(T0 + H + timedelta(seconds=2), "95"))                                             # boundary is inclusive
    conn.commit()
    rollup_hours(conn, now=T0 + 2 * H)
    rows = dict(conn.execute("SELECT hour_ts, open_mark FROM hourly_rollup"))
    assert rows[T0.isoformat()] == "91"
    assert rows[(T0 + H).isoformat()] == "95"


def test_rollup_open_mark_null_without_native_tick_in_window():
    conn = connect(":memory:")
    insert_tick(conn, _tick(T0 - 2 * H, "100"))                          # too old for hour T0
    insert_tick(conn, _tick(T0 + timedelta(seconds=3), "101"))           # too new for hour T0
    rollup_hours(conn, now=T0 + H)
    row = conn.execute("SELECT index_close, open_mark FROM hourly_rollup WHERE hour_ts=?", (T0.isoformat(),)).fetchone()
    assert row == ("101", None)


def test_reader_mixes_rollup_hours_with_raw_hours():
    conn = connect(":memory:")
    insert_tick(conn, _tick(T0 + timedelta(minutes=10), "100"))
    insert_book(conn, _book(T0 + timedelta(minutes=5), "99.5", "100.5"))
    conn.commit()
    rollup_hours(conn, now=T0 + H)
    conn.execute("DELETE FROM ticks")
    conn.execute("DELETE FROM book_snapshots")
    insert_tick(conn, _tick(T0 + H + timedelta(minutes=10), "105"))
    insert_book(conn, _book(T0 + H + timedelta(minutes=5), "99", "101"))
    kw = dict(start=T0, end=T0 + 2 * H - timedelta(microseconds=1))
    assert query_last_index_by_hour(conn, 6, **kw) == {T0: Decimal("100"), T0 + H: Decimal("105")}
    spr = query_book_spread_bps_by_hour(conn, 6, **kw)
    assert set(spr) == {T0, T0 + H} and len(spr[T0]) == 1


def test_reader_mid_hour_window_ignores_rollup_row():
    conn = connect(":memory:")
    insert_tick(conn, _tick(T0 + timedelta(minutes=10), "100"))
    insert_tick(conn, _tick(T0 + timedelta(minutes=50), "101"))
    insert_book(conn, _book(T0 + timedelta(minutes=5), "99.5", "100.5"))
    conn.commit()
    rollup_hours(conn, now=T0 + H)
    out = query_last_index_by_hour(conn, 6, start=T0 + timedelta(minutes=5), end=T0 + timedelta(minutes=30))
    assert out == {T0: Decimal("100")}                # not the rollup's 101
    assert query_book_spread_bps_by_hour(conn, 6, start=T0 + timedelta(minutes=10), end=T0 + timedelta(minutes=30)) == {}
