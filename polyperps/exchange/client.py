"""Read-only adapter over polymarket-client's AsyncPublicClient.

Clean interface boundary: nothing outside this module imports `polymarket`.
Swapping the venue (or a future NautilusTrader adapter, spec 2.0) means
re-implementing ExchangeClient here and nowhere else.

Phase 0 is read-only by construction: there are no order/cancel/leverage/
withdraw methods on this class. Phase 2 adds a separate authenticated
trading client whose every write path calls polyperps.gates first.

SDK surface used (verified against Polymarket/py-sdk main, 2026-09-10; all
Perps APIs are marked experimental - re-verify on every version bump):
  AsyncPublicClient.fetch_perps_instruments / fetch_perps_ticker /
  fetch_perps_book / list_perps_funding_history / list_perps_candles /
  subscribe(PerpsTickersSpec)
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Sequence
from datetime import datetime, timezone
from typing import Any, Protocol

from polyperps.exchange.rate_limiter import TokenBucket
from polyperps.exchange.types import (
    BookLevel,
    BookSnapshot,
    Candle,
    FundingObservation,
    Instrument,
    SourceType,
    Tick,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# --- pure converters (SDK model -> our types) -------------------------------


def instrument_from_rest(i: Any) -> Instrument:
    return Instrument(
        instrument_id=int(i.id),
        symbol=i.symbol,
        category=str(i.category),
        funding_interval=i.funding_interval,
        max_leverage=int(i.max_leverage),
        price_decimals=int(i.price_decimals),
        quantity_decimals=int(i.quantity_decimals),
        min_notional=i.min_notional,
        isolated_only=bool(i.isolated_only),
    )


def tick_from_rest(t: Any, received_ts: datetime) -> Tick:
    # PerpsTicker.timestamp is Optional. If absent we record received_ts;
    # source_type=POLYMARKET_REST tells consumers the exchange_ts may be local.
    return Tick(
        instrument_id=int(t.instrument_id),
        mark_price=t.mark_price,
        index_price=t.index_price,
        last_price=t.last_price,
        funding_rate=t.funding_rate,
        next_funding=t.next_funding,
        exchange_ts=t.timestamp if t.timestamp is not None else received_ts,
        received_ts=received_ts,
        source_type=SourceType.POLYMARKET_REST,
        sequence=None,
    )


def tick_from_event(event: Any, received_ts: datetime) -> Tick:
    # PerpsTickerUpdate (payload) has no timestamp; the envelope does.
    p = event.payload
    return Tick(
        instrument_id=int(p.instrument_id),
        mark_price=p.mark_price,
        index_price=p.index_price,
        last_price=p.last_price,
        funding_rate=p.funding_rate,
        next_funding=p.next_funding,
        exchange_ts=event.timestamp,
        received_ts=received_ts,
        source_type=SourceType.POLYMARKET_WS,
        sequence=int(event.sequence),
    )


def book_from_rest(b: Any, received_ts: datetime) -> BookSnapshot:
    return BookSnapshot(
        instrument_id=int(b.instrument_id),
        bids=tuple(BookLevel(price=lvl.price, quantity=lvl.quantity) for lvl in b.bids),
        asks=tuple(BookLevel(price=lvl.price, quantity=lvl.quantity) for lvl in b.asks),
        exchange_ts=b.timestamp,
        received_ts=received_ts,
        source_type=SourceType.POLYMARKET_REST,
        sequence=int(b.sequence) if b.sequence is not None else None,
    )


def funding_from_rest(instrument_id: int, fr: Any, received_ts: datetime) -> FundingObservation:
    return FundingObservation(
        instrument_id=instrument_id,
        funding_rate=fr.funding_rate,
        exchange_ts=fr.timestamp,
        received_ts=received_ts,
        source_type=SourceType.POLYMARKET_REST,
    )


def candle_from_rest(instrument_id: int, interval: str, c: Any, received_ts: datetime) -> Candle:
    return Candle(
        instrument_id=instrument_id,
        interval=interval,
        open_ts=c.timestamp,
        open=c.open,
        high=c.high,
        low=c.low,
        close=c.close,
        volume=c.volume,
        trades=int(c.trades),
        received_ts=received_ts,
        source_type=SourceType.POLYMARKET_REST,
    )


# --- interface ---------------------------------------------------------------


class ExchangeClient(Protocol):
    async def fetch_instruments(self) -> tuple[Instrument, ...]: ...
    async def fetch_ticker(self, instrument_id: int) -> Tick: ...
    async def fetch_book(self, instrument_id: int, *, depth: int = 100) -> BookSnapshot: ...
    async def fetch_funding_history(
        self, instrument_id: int, *, start: datetime, end: datetime
    ) -> list[FundingObservation]: ...
    async def fetch_candles(
        self, instrument_id: int, *, interval: str, start: datetime, end: datetime
    ) -> list[Candle]: ...
    def stream_ticks(self, instrument_ids: Sequence[int]) -> AsyncIterator[Tick]: ...
    async def close(self) -> None: ...


# --- Polymarket implementation ----------------------------------------------


class PolymarketPerpsClient:
    def __init__(
        self,
        sdk: Any,
        *,
        limiter: TokenBucket,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._sdk = sdk
        self._limiter = limiter
        self._clock = clock

    @classmethod
    def create_public(cls, *, rate_per_sec: float = 5.0, burst: int = 10) -> PolymarketPerpsClient:
        from polymarket import AsyncPublicClient

        return cls(AsyncPublicClient(), limiter=TokenBucket(rate_per_sec=rate_per_sec, burst=burst))

    async def fetch_instruments(self) -> tuple[Instrument, ...]:
        await self._limiter.acquire()
        raw = await self._sdk.fetch_perps_instruments()
        return tuple(instrument_from_rest(i) for i in raw)

    async def fetch_ticker(self, instrument_id: int) -> Tick:
        await self._limiter.acquire()
        raw = await self._sdk.fetch_perps_ticker(instrument_id=instrument_id)
        return tick_from_rest(raw, self._clock())

    async def fetch_book(self, instrument_id: int, *, depth: int = 100) -> BookSnapshot:
        await self._limiter.acquire()
        raw = await self._sdk.fetch_perps_book(instrument_id=instrument_id, depth=depth)
        return book_from_rest(raw, self._clock())

    async def fetch_funding_history(
        self, instrument_id: int, *, start: datetime, end: datetime
    ) -> list[FundingObservation]:
        await self._limiter.acquire()
        pager = self._sdk.list_perps_funding_history(instrument_id=instrument_id, start=start, end=end)
        now = self._clock()
        return [funding_from_rest(instrument_id, fr, now) async for fr in pager.iter_items()]

    async def fetch_candles(
        self, instrument_id: int, *, interval: str, start: datetime, end: datetime
    ) -> list[Candle]:
        await self._limiter.acquire()
        pager = self._sdk.list_perps_candles(
            instrument_id=instrument_id, interval=interval, start=start, end=end
        )
        now = self._clock()
        return [candle_from_rest(instrument_id, interval, c, now) async for c in pager.iter_items()]

    async def stream_ticks(self, instrument_ids: Sequence[int]) -> AsyncIterator[Tick]:
        from polymarket.streams import PerpsTickersSpec

        specs = [PerpsTickersSpec(instrument_id=i) for i in instrument_ids]
        handle = await self._sdk.subscribe(specs)
        async with handle:
            async for event in handle:
                if getattr(event, "topic", None) != "perps.tickers":
                    continue
                yield tick_from_event(event, self._clock())

    async def close(self) -> None:
        await self._sdk.close()
