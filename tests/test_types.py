from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from polyperps.exchange.types import (
    BookLevel,
    BookSnapshot,
    Candle,
    FundingObservation,
    SourceType,
    Tick,
)

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


def make_tick(**over):
    base = dict(
        instrument_id=1,
        mark_price=Decimal("100.5"),
        index_price=Decimal("100.4"),
        last_price=Decimal("100.6"),
        funding_rate=Decimal("0.0001"),
        next_funding=T0,
        exchange_ts=T0,
        received_ts=T0,
        source_type=SourceType.POLYMARKET_WS,
    )
    base.update(over)
    return Tick(**base)


def test_tick_is_frozen_and_keyword_only():
    t = make_tick()
    with pytest.raises(AttributeError):
        t.mark_price = Decimal("1")  # type: ignore[misc]
    with pytest.raises(TypeError):
        Tick(1)  # positional not allowed


def test_naive_datetime_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        make_tick(exchange_ts=datetime(2026, 9, 11, 12, 0))


def test_non_utc_offset_rejected():
    with pytest.raises(ValueError, match="UTC"):
        make_tick(exchange_ts=datetime(2026, 9, 11, 12, 0, tzinfo=timezone(timedelta(hours=5))))


def test_source_type_is_required_and_enum():
    with pytest.raises(TypeError):
        make_tick(source_type=None)
    assert SourceType.POLYMARKET_WS.value == "polymarket_ws"
    assert SourceType.PROXY_HYPERLIQUID.value == "proxy_hyperliquid"


def test_book_snapshot_levels_are_tuples():
    snap = BookSnapshot(
        instrument_id=1,
        bids=(BookLevel(price=Decimal("99"), quantity=Decimal("1")),),
        asks=(BookLevel(price=Decimal("101"), quantity=Decimal("2")),),
        exchange_ts=T0,
        received_ts=T0,
        source_type=SourceType.POLYMARKET_REST,
    )
    assert snap.bids[0].price == Decimal("99")
    assert snap.sequence is None


def test_funding_and_candle_construct():
    f = FundingObservation(
        instrument_id=1,
        funding_rate=Decimal("-0.0002"),
        exchange_ts=T0,
        received_ts=T0,
        source_type=SourceType.POLYMARKET_REST,
    )
    c = Candle(
        instrument_id=1,
        interval="1m",
        open_ts=T0,
        open=Decimal("1"),
        high=Decimal("2"),
        low=Decimal("0.5"),
        close=Decimal("1.5"),
        volume=Decimal("10"),
        trades=3,
        received_ts=T0,
        source_type=SourceType.POLYMARKET_REST,
    )
    assert f.funding_rate < 0
    assert c.close == Decimal("1.5")
