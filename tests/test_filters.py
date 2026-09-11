from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.data_ingest.filters import DEFAULT_BOUNDS, SanityBounds, check_tick
from polyperps.exchange.types import SourceType, Tick

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


def tick(**over):
    base = dict(
        instrument_id=1, mark_price=Decimal("100"), index_price=Decimal("100"),
        last_price=Decimal("100"), funding_rate=Decimal("0.0001"), next_funding=T0,
        exchange_ts=T0, received_ts=T0, source_type=SourceType.POLYMARKET_WS, sequence=1,
    )
    base.update(over)
    return Tick(**base)


def test_clean_tick_accepted():
    assert check_tick(tick(), bounds=DEFAULT_BOUNDS, previous=None) is None


def test_stale_tick_rejected():
    r = check_tick(tick(received_ts=T0 + timedelta(seconds=6)), bounds=DEFAULT_BOUNDS, previous=None)
    assert r is not None and r.reason == "stale"


def test_non_positive_price_rejected():
    r = check_tick(tick(mark_price=Decimal("0")), bounds=DEFAULT_BOUNDS, previous=None)
    assert r is not None and r.reason == "non_positive_price"


def test_mark_index_divergence_rejected():
    r = check_tick(tick(mark_price=Decimal("110")), bounds=DEFAULT_BOUNDS, previous=None)
    assert r is not None and r.reason == "mark_index_divergence"


def test_funding_out_of_bounds_rejected():
    r = check_tick(tick(funding_rate=Decimal("0.5")), bounds=DEFAULT_BOUNDS, previous=None)
    assert r is not None and r.reason == "funding_out_of_bounds"


def test_price_jump_vs_previous_rejected():
    prev = tick(sequence=1)
    nxt = tick(mark_price=Decimal("120"), index_price=Decimal("120"), sequence=2,
               exchange_ts=T0 + timedelta(seconds=1), received_ts=T0 + timedelta(seconds=1))
    r = check_tick(nxt, bounds=DEFAULT_BOUNDS, previous=prev)
    assert r is not None and r.reason == "price_jump"


def test_out_of_order_sequence_rejected():
    prev = tick(sequence=5)
    r = check_tick(tick(sequence=4), bounds=DEFAULT_BOUNDS, previous=prev)
    assert r is not None and r.reason == "out_of_order"


def test_sequence_not_compared_across_sources():
    prev = tick(sequence=5)
    rest = tick(sequence=None, source_type=SourceType.POLYMARKET_REST)
    assert check_tick(rest, bounds=DEFAULT_BOUNDS, previous=prev) is None


def test_custom_bounds_respected():
    loose = SanityBounds(max_staleness=timedelta(seconds=60), max_abs_funding_rate=Decimal("1"),
                         max_mark_index_divergence=Decimal("1"), max_jump=Decimal("1"))
    assert check_tick(tick(received_ts=T0 + timedelta(seconds=30)), bounds=loose, previous=None) is None
