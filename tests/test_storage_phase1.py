import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.exchange.types import (
    BookLevel, BookSnapshot, Candle, FeeSchedule, FundingObservation, SourceType, Tick,
)
from polyperps.storage.db import (
    connect, insert_book, insert_candle, insert_fee, insert_funding, insert_tick,
    latest_fee, query_book_spread_bps, query_candles, query_funding, query_ticks,
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
