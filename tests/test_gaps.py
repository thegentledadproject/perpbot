from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.exchange.types import FundingObservation, SourceType, Tick
from polyperps.storage.db import connect, insert_funding, insert_tick
from polyperps.storage.gaps import find_gaps

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)
S = timedelta(seconds=1)


def tick(ts, seq):
    return Tick(instrument_id=1, mark_price=Decimal(1), index_price=Decimal(1), last_price=Decimal(1),
                funding_rate=Decimal(0), next_funding=T0, exchange_ts=ts, received_ts=ts,
                source_type=SourceType.POLYMARKET_WS, sequence=seq)


def test_no_gaps_when_dense():
    conn = connect(":memory:")
    for i in range(5):
        insert_tick(conn, tick(T0 + i * S, i))
    assert find_gaps(conn, 1, table="ticks", max_gap=2 * S, start=T0, end=T0 + 4 * S) == []


def test_internal_gap_detected():
    conn = connect(":memory:")
    for i in (0, 1, 2, 10, 11):
        insert_tick(conn, tick(T0 + i * S, i))
    gaps = find_gaps(conn, 1, table="ticks", max_gap=2 * S, start=T0, end=T0 + 11 * S)
    assert gaps == [(T0 + 2 * S, T0 + 10 * S)]


def test_leading_and_trailing_gaps_against_range():
    conn = connect(":memory:")
    insert_tick(conn, tick(T0 + 5 * S, 1))
    gaps = find_gaps(conn, 1, table="ticks", max_gap=2 * S, start=T0, end=T0 + 10 * S)
    assert gaps == [(T0, T0 + 5 * S), (T0 + 5 * S, T0 + 10 * S)]


def test_empty_table_is_one_whole_gap():
    conn = connect(":memory:")
    assert find_gaps(conn, 1, table="ticks", max_gap=S, start=T0, end=T0 + 10 * S) == [(T0, T0 + 10 * S)]


def test_funding_table_supported():
    conn = connect(":memory:")
    for i in (0, 1):
        insert_funding(conn, FundingObservation(instrument_id=1, funding_rate=Decimal(0),
                                                exchange_ts=T0 + i * timedelta(hours=1),
                                                received_ts=T0, source_type=SourceType.POLYMARKET_REST))
    assert find_gaps(conn, 1, table="funding_rates", max_gap=timedelta(hours=1, minutes=5),
                     start=T0, end=T0 + timedelta(hours=1)) == []
