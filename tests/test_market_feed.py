from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from polyperps.config import load_settings
from polyperps.data_ingest.filters import DEFAULT_BOUNDS
from polyperps.data_ingest.market_feed import MarketFeed
from polyperps.exchange.types import SourceType, Tick

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


def tick(i, seq, ts=T0, mark="100", instrument_id=1, received=None):
    return Tick(instrument_id=instrument_id, mark_price=Decimal(mark), index_price=Decimal("100"),
                last_price=Decimal("100"), funding_rate=Decimal("0.0001"), next_funding=T0,
                exchange_ts=ts,
                received_ts=received if received is not None else ts + timedelta(milliseconds=i),
                source_type=SourceType.POLYMARKET_WS, sequence=seq)


async def gen(items):
    for it in items:
        yield it


async def test_accepts_clean_ticks_and_tracks_health():
    accepted = []
    feed = MarketFeed(ticks=gen([tick(0, 1), tick(1, 2)]), bounds=DEFAULT_BOUNDS,
                      on_accept=accepted.append)
    health = await feed.run()
    assert [t.sequence for t in accepted] == [1, 2]
    assert health.received == 2 and health.accepted == 2 and health.rejected == {}
    assert health.last_accepted[1] == T0


async def test_rejects_and_counts_by_reason_without_updating_previous():
    accepted, rejected = [], []
    # exchange said T0-10s, we received it at T0 -> 10s old, over the 5s bound
    stale = tick(0, 2, ts=T0 - timedelta(seconds=10), received=T0)
    ticks = [tick(0, 1), stale, tick(2, 3)]
    feed = MarketFeed(ticks=gen(ticks), bounds=DEFAULT_BOUNDS, on_accept=accepted.append,
                      on_reject=lambda t, r: rejected.append((t.sequence, r.reason)))
    health = await feed.run()
    assert [t.sequence for t in accepted] == [1, 3]
    assert rejected == [(2, "stale")]
    assert health.rejected == {"stale": 1}


async def test_previous_is_tracked_per_instrument():
    accepted = []
    ticks = [tick(0, 5, instrument_id=1), tick(1, 1, instrument_id=2), tick(2, 6, instrument_id=1)]
    feed = MarketFeed(ticks=gen(ticks), bounds=DEFAULT_BOUNDS, on_accept=accepted.append)
    health = await feed.run()
    assert health.accepted == 3  # instrument 2's seq 1 is not "out of order" vs instrument 1's 5


async def test_max_events_stops_early():
    feed = MarketFeed(ticks=gen([tick(i, i + 1) for i in range(10)]), bounds=DEFAULT_BOUNDS,
                      on_accept=lambda t: None)
    health = await feed.run(max_events=3)
    assert health.received == 3


def test_load_settings_parses_env(tmp_path):
    s = load_settings({
        "POLYPERPS_DB_PATH": str(tmp_path / "x.sqlite3"),
        "POLYPERPS_INSTRUMENT_IDS": "7, 9",
        "POLYPERPS_MAX_STALENESS_S": "2.5",
    })
    assert s.instrument_ids == (7, 9)
    assert s.bounds.max_staleness == timedelta(seconds=2.5)
    assert s.book_snapshot_interval_s == 5.0
    assert s.rest_rate_per_sec == 2.0 and s.rest_burst == 4


def test_load_settings_requires_instruments():
    with pytest.raises(ValueError, match="POLYPERPS_INSTRUMENT_IDS"):
        load_settings({})


async def test_market_feed_does_not_close_the_ticks_generator_on_error():
    # MarketFeed.run() must propagate an error from a callback without
    # closing the underlying ticks generator itself - closing it is the
    # caller's job (see scripts/run_feed.py's `await ticks.aclose()` in its
    # `finally`). This test documents that contract at the MarketFeed level.
    state = {"closed": False}

    async def gen_with_cleanup():
        try:
            yield tick(0, 1)
            yield tick(1, 2)
        finally:
            state["closed"] = True

    g = gen_with_cleanup()

    def on_accept(t):
        raise RuntimeError("boom")

    feed = MarketFeed(ticks=g, bounds=DEFAULT_BOUNDS, on_accept=on_accept)

    with pytest.raises(RuntimeError, match="boom"):
        await feed.run()

    assert state["closed"] is False

    await g.aclose()

    assert state["closed"] is True
