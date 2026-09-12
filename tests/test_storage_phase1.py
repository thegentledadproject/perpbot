import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.exchange.types import (
    BookLevel, BookSnapshot, Candle, FeeSchedule, FundingObservation, SourceType, Tick,
)
from polyperps.storage.db import (
    connect, insert_book, insert_candle, insert_fee, insert_funding, insert_tick,
    latest_fee, query_book_spread_bps, query_book_spread_bps_by_hour, query_candles, query_funding,
    query_last_index_by_hour, query_ticks,
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
