"""Client-side token bucket.

polymarket-client does not throttle requests; it only surfaces server
Poly-RateLimit-* headers on order/cancel responses (polymarket.rate_limit).
This bucket paces *all* our calls so sustained polling never trips the server.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable


class TokenBucket:
    def __init__(
        self,
        *,
        rate_per_sec: float,
        burst: int,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if rate_per_sec <= 0:
            raise ValueError("rate_per_sec must be > 0")
        if burst < 1:
            raise ValueError("burst must be >= 1")
        self._rate = float(rate_per_sec)
        self._burst = float(burst)
        self._clock = clock
        self._sleep = sleep
        self._tokens = self._burst
        self._last = clock()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self._clock()
        self._tokens = min(self._burst, self._tokens + (now - self._last) * self._rate)
        self._last = now

    async def acquire(self) -> float:
        """Take one token, sleeping if none is available. Returns seconds waited."""
        async with self._lock:
            self._refill()
            waited = 0.0
            if self._tokens < 1.0:
                waited = (1.0 - self._tokens) / self._rate
                await self._sleep(waited)
                self._refill()
            self._tokens -= 1.0
            return waited
