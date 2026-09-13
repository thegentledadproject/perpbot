from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.execution.live_bars import LiveBarBuilder
from polyperps.exchange.types import SourceType, Tick

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


def tick(minutes, mark, index="100", funding="0.0001", iid=6):
    ts = T0 + timedelta(minutes=minutes)
    return Tick(instrument_id=iid, mark_price=Decimal(mark), index_price=Decimal(index), last_price=Decimal(mark),
                funding_rate=Decimal(funding), next_funding=ts, exchange_ts=ts, received_ts=ts,
                source_type=SourceType.POLYMARKET_WS, sequence=minutes)


def test_bar_closes_when_hour_advances():
    b = LiveBarBuilder()
    assert b.on_tick(tick(1, "100")) is None
    assert b.on_tick(tick(30, "105", index="104")) is None
    assert b.on_tick(tick(59, "98", funding="0.0002")) is None
    closed = b.on_tick(tick(61, "99"))
    assert closed is not None and closed.open_ts == T0
    assert (closed.open, closed.high, closed.low, closed.close) == (Decimal(100), Decimal(105), Decimal(98), Decimal(98))
    assert closed.index_close == Decimal(100) and closed.funding_rate == Decimal("0.0002")
    assert closed.complete and closed.spread_source == "constant"
    assert b.history(6) == [closed]


def test_instruments_are_independent_and_history_capped():
    b = LiveBarBuilder(max_history=2)
    for h in range(4):
        b.on_tick(tick(60 * h + 1, "100"))
        b.on_tick(tick(60 * h + 2, "100", iid=7))
    assert len(b.history(6)) == 2 and len(b.history(7)) == 2
    assert b.history(6)[-1].open_ts == T0 + timedelta(hours=2)


def test_close_all_on_shutdown():
    b = LiveBarBuilder()
    b.on_tick(tick(5, "100"))
    (bar,) = b.close_all(T0 + timedelta(minutes=10))
    assert bar.close == Decimal(100) and b.history(6) == [bar]
