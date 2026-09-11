from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.exchange.types import (
    BookLevel, BookSnapshot, Candle, FundingObservation, SourceType, Tick,
)
from polyperps.storage.db import (
    connect, count_rejections, insert_book, insert_candle, insert_funding,
    insert_rejection, insert_tick, query_funding, query_ticks,
)

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


def tick(ts=T0, seq=1, mark="100.123456789"):
    return Tick(instrument_id=1, mark_price=Decimal(mark), index_price=Decimal("100"),
                last_price=Decimal("100"), funding_rate=Decimal("0.0001"), next_funding=T0,
                exchange_ts=ts, received_ts=ts, source_type=SourceType.POLYMARKET_WS, sequence=seq)


def test_schema_created_and_tick_round_trips_decimal_exactly():
    conn = connect(":memory:")
    assert insert_tick(conn, tick()) is True
    rows = query_ticks(conn, 1, start=T0 - timedelta(minutes=1), end=T0 + timedelta(minutes=1))
    assert rows == [tick()]
    assert rows[0].mark_price == Decimal("100.123456789")
    assert rows[0].exchange_ts.tzinfo is not None


def test_insert_tick_is_idempotent():
    conn = connect(":memory:")
    assert insert_tick(conn, tick()) is True
    assert insert_tick(conn, tick()) is False


def test_query_ticks_respects_range_and_order():
    conn = connect(":memory:")
    for i in (3, 1, 2):
        insert_tick(conn, tick(ts=T0 + timedelta(seconds=i), seq=i))
    rows = query_ticks(conn, 1, start=T0 + timedelta(seconds=2), end=T0 + timedelta(seconds=3))
    assert [r.sequence for r in rows] == [2, 3]


def test_funding_round_trip():
    conn = connect(":memory:")
    obs = FundingObservation(instrument_id=1, funding_rate=Decimal("-0.00025"), exchange_ts=T0,
                             received_ts=T0, source_type=SourceType.POLYMARKET_REST)
    assert insert_funding(conn, obs) is True
    assert query_funding(conn, 1, start=T0, end=T0) == [obs]


def test_book_stores_top_levels_only():
    conn = connect(":memory:")
    levels = tuple(BookLevel(price=Decimal(100 - i), quantity=Decimal(1)) for i in range(20))
    snap = BookSnapshot(instrument_id=1, bids=levels, asks=levels, exchange_ts=T0, received_ts=T0,
                        source_type=SourceType.POLYMARKET_REST, sequence=9)
    assert insert_book(conn, snap, max_levels=5) is True
    (bids_json,) = conn.execute("SELECT bids_json FROM book_snapshots").fetchone()
    assert bids_json.count('"price"') == 5


def test_candle_insert_idempotent():
    conn = connect(":memory:")
    c = Candle(instrument_id=1, interval="1m", open_ts=T0, open=Decimal(1), high=Decimal(2),
               low=Decimal(1), close=Decimal(2), volume=Decimal(3), trades=1, received_ts=T0,
               source_type=SourceType.POLYMARKET_REST)
    assert insert_candle(conn, c) is True
    assert insert_candle(conn, c) is False


def test_rejections_counted_by_reason():
    conn = connect(":memory:")
    insert_rejection(conn, instrument_id=1, reason="stale", detail="x", at=T0)
    insert_rejection(conn, instrument_id=1, reason="stale", detail="y", at=T0 + timedelta(seconds=1))
    insert_rejection(conn, instrument_id=1, reason="price_jump", detail="z", at=T0)
    assert count_rejections(conn, 1) == {"stale": 2, "price_jump": 1}
