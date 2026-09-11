"""Consume a Tick stream, filter it, and hand accepted ticks downstream.

Rejected ticks never update `previous` - a bad tick must not become the
baseline for the next jump check.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone

from polyperps.data_ingest.filters import Rejection, SanityBounds, check_tick
from polyperps.exchange.types import Tick


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class FeedHealth:
    received: int = 0
    accepted: int = 0
    rejected: Counter[str] = field(default_factory=Counter)
    last_accepted: dict[int, datetime] = field(default_factory=dict)
    last_event_wallclock: datetime | None = None


class MarketFeed:
    def __init__(
        self,
        *,
        ticks: AsyncIterator[Tick],
        bounds: SanityBounds,
        on_accept: Callable[[Tick], None],
        on_reject: Callable[[Tick, Rejection], None] | None = None,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._ticks = ticks
        self._bounds = bounds
        self._on_accept = on_accept
        self._on_reject = on_reject
        self._clock = clock
        self._previous: dict[int, Tick] = {}
        self._health = FeedHealth()

    @property
    def health(self) -> FeedHealth:
        return self._health

    async def run(self, *, max_events: int | None = None) -> FeedHealth:
        h = self._health
        async for tick in self._ticks:
            h.received += 1
            h.last_event_wallclock = self._clock()
            rejection = check_tick(
                tick, bounds=self._bounds, previous=self._previous.get(tick.instrument_id)
            )
            if rejection is None:
                self._previous[tick.instrument_id] = tick
                h.accepted += 1
                h.last_accepted[tick.instrument_id] = tick.exchange_ts
                self._on_accept(tick)
            else:
                h.rejected[rejection.reason] += 1
                if self._on_reject is not None:
                    self._on_reject(tick, rejection)
            if max_events is not None and h.received >= max_events:
                break
        return h
