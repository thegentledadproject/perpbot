from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from polyperps.exchange.client import (
    PolymarketPerpsClient,
    book_from_rest,
    candle_from_rest,
    funding_from_rest,
    instrument_from_rest,
    tick_from_event,
    tick_from_rest,
)
from polyperps.exchange.rate_limiter import TokenBucket
from polyperps.exchange.types import SourceType

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)
RX = T0 + timedelta(milliseconds=50)


def rest_ticker(ts=T0):
    return SimpleNamespace(
        instrument_id=7, symbol="BTC-PERP", index_price=Decimal("100"), mark_price=Decimal("101"),
        last_price=Decimal("100.5"), mid_price=Decimal("100.5"), open_interest=Decimal("5"),
        funding_rate=Decimal("0.0001"), next_funding=T0 + timedelta(hours=1), timestamp=ts,
    )


def ws_event(seq=10):
    payload = SimpleNamespace(
        instrument_id=7, index_price=Decimal("100"), mark_price=Decimal("101"),
        last_price=Decimal("100.5"), mid_price=Decimal("100.5"), open_interest=Decimal("5"),
        funding_rate=Decimal("0.0001"), next_funding=T0 + timedelta(hours=1),
    )
    return SimpleNamespace(topic="perps.tickers", type="ticker", channel="c", timestamp=T0,
                           sequence=seq, payload=payload)


def test_tick_from_rest_uses_exchange_timestamp():
    t = tick_from_rest(rest_ticker(), RX)
    assert t.instrument_id == 7
    assert t.exchange_ts == T0 and t.received_ts == RX
    assert t.source_type is SourceType.POLYMARKET_REST
    assert t.sequence is None


def test_tick_from_rest_without_timestamp_falls_back_to_received():
    t = tick_from_rest(rest_ticker(ts=None), RX)
    assert t.exchange_ts == RX


def test_tick_from_event_uses_envelope_timestamp_and_sequence():
    t = tick_from_event(ws_event(seq=42), RX)
    assert t.exchange_ts == T0 and t.sequence == 42
    assert t.source_type is SourceType.POLYMARKET_WS
    assert t.mark_price == Decimal("101")


def test_book_from_rest():
    b = SimpleNamespace(
        instrument_id=7,
        bids=(SimpleNamespace(price=Decimal("99"), quantity=Decimal("1")),),
        asks=(SimpleNamespace(price=Decimal("102"), quantity=Decimal("3")),),
        timestamp=T0, sequence=5,
    )
    snap = book_from_rest(b, RX)
    assert snap.bids[0].quantity == Decimal("1") and snap.asks[0].price == Decimal("102")
    assert snap.sequence == 5 and snap.source_type is SourceType.POLYMARKET_REST


def test_funding_and_candle_and_instrument_from_rest():
    f = funding_from_rest(7, SimpleNamespace(funding_rate=Decimal("-0.0003"), timestamp=T0), RX)
    assert f.instrument_id == 7 and f.funding_rate == Decimal("-0.0003")
    c = candle_from_rest(7, "1m", SimpleNamespace(
        timestamp=T0, open=Decimal("1"), high=Decimal("2"), low=Decimal("0.5"),
        close=Decimal("1.5"), volume=Decimal("9"), trades=4), RX)
    assert c.interval == "1m" and c.open_ts == T0 and c.trades == 4
    i = instrument_from_rest(SimpleNamespace(
        id=7, symbol="BTC-PERP", category="crypto", funding_interval="1h", max_leverage=20,
        price_decimals=2, quantity_decimals=4, min_notional=Decimal("10"), isolated_only=False))
    assert i.instrument_id == 7 and i.max_leverage == 20


class FakePaginator:
    def __init__(self, items):
        self._items = items

    async def iter_items(self):
        for it in self._items:
            yield it


class FakeHandle:
    def __init__(self, events):
        self._events = events
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.closed = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._events:
            raise StopAsyncIteration
        return self._events.pop(0)


class FakeSdk:
    def __init__(self):
        self.calls = []
        self.handle = FakeHandle([ws_event(1), SimpleNamespace(topic="perps.bbo"), ws_event(2)])
        self.closed = False

    async def fetch_perps_instruments(self):
        self.calls.append("instruments")
        return (SimpleNamespace(id=7, symbol="BTC-PERP", category="crypto", funding_interval="1h",
                                max_leverage=20, price_decimals=2, quantity_decimals=4,
                                min_notional=Decimal("10"), isolated_only=False),)

    async def fetch_perps_ticker(self, *, instrument_id):
        self.calls.append(("ticker", instrument_id))
        return rest_ticker()

    async def fetch_perps_book(self, *, instrument_id, depth):
        self.calls.append(("book", instrument_id, depth))
        return SimpleNamespace(instrument_id=instrument_id, bids=(), asks=(), timestamp=T0, sequence=1)

    def list_perps_funding_history(self, *, instrument_id, start, end):
        self.calls.append(("funding", instrument_id, start, end))
        return FakePaginator([SimpleNamespace(funding_rate=Decimal("0.0001"), timestamp=T0)])

    def list_perps_candles(self, *, instrument_id, interval, start, end):
        self.calls.append(("candles", instrument_id, interval))
        return FakePaginator([SimpleNamespace(timestamp=T0, open=Decimal("1"), high=Decimal("1"),
                                              low=Decimal("1"), close=Decimal("1"),
                                              volume=Decimal("0"), trades=0)])

    async def subscribe(self, specs):
        self.calls.append(("subscribe", [s.instrument_id for s in specs]))
        return self.handle

    async def close(self):
        self.closed = True


class CountingBucket(TokenBucket):
    def __init__(self):
        super().__init__(rate_per_sec=1000, burst=1000)
        self.acquired = 0

    async def acquire(self):
        self.acquired += 1
        return 0.0


@pytest.fixture
def client():
    sdk = FakeSdk()
    bucket = CountingBucket()
    return PolymarketPerpsClient(sdk, limiter=bucket, clock=lambda: RX), sdk, bucket


async def test_rest_calls_go_through_limiter_and_convert(client):
    c, sdk, bucket = client
    inst = await c.fetch_instruments()
    tick = await c.fetch_ticker(7)
    book = await c.fetch_book(7, depth=10)
    fund = await c.fetch_funding_history(7, start=T0 - timedelta(days=1), end=T0)
    candles = await c.fetch_candles(7, interval="1m", start=T0 - timedelta(hours=1), end=T0)
    assert inst[0].symbol == "BTC-PERP"
    assert tick.received_ts == RX
    assert book.instrument_id == 7 and ("book", 7, 10) in sdk.calls
    assert fund[0].source_type is SourceType.POLYMARKET_REST
    assert candles[0].interval == "1m"
    assert bucket.acquired == 5


async def test_stream_ticks_filters_to_ticker_events_and_closes(client):
    c, sdk, _ = client
    ticks = [t async for t in c.stream_ticks([7])]
    assert [t.sequence for t in ticks] == [1, 2]
    assert ("subscribe", [7]) in sdk.calls
    assert sdk.handle.closed is True


async def test_close_closes_sdk(client):
    c, sdk, _ = client
    await c.close()
    assert sdk.closed is True


def test_no_trading_methods_exist_in_phase0():
    for name in ("place_order", "cancel_order", "update_leverage", "withdraw"):
        assert not hasattr(PolymarketPerpsClient, name)
