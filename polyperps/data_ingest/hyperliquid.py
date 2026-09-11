"""Hyperliquid public market-data client (proxy source, spec 1.1).

Proxy data is for SCREENING hypotheses only. Rows are tagged
SourceType.PROXY_HYPERLIQUID and stored under the Polymarket instrument id of
the same asset; polyperps.signal.sufficiency refuses to let a proxy dataset
meet the bar.

Shapes confirmed by one live call in Task 5 Step 7 (POST {base_url}/info):
  {"type": "fundingHistory", "coin": "BTC", "startTime": ms, "endTime": ms}
    -> [{"coin","fundingRate","premium","time"}]
  {"type": "candleSnapshot", "req": {"coin","interval","startTime","endTime"}}
    -> [{"t","T","s","i","o","c","h","l","v","n"}]
If the live shapes differ, fix the parsers and the fixtures in
tests/test_hyperliquid.py together; do not special-case in callers.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime

import httpx

from polyperps.exchange.rate_limiter import TokenBucket
from polyperps.exchange.types import Candle, FundingObservation, SourceType

DEFAULT_BASE_URL = "https://api.hyperliquid.xyz"
_TIMEOUT_S = 30.0


class TransientProxyError(RuntimeError):
    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _retry_after_seconds(value: str | None) -> float | None:
    """Parse a Retry-After header value.

    RFC 7231 allows two forms: delta-seconds ("7") or an HTTP-date
    ("Wed, 21 Oct 2026 07:28:00 GMT"). Delta-seconds parses directly; the
    date form is converted to seconds-from-now (clamped at 0.0 so a date in
    the past never yields a negative sleep). Anything unparseable, or a
    missing header, returns None.
    """
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


def parse_funding_history(
    items: list[dict], *, instrument_id: int, received_ts: datetime
) -> list[FundingObservation]:
    return [
        FundingObservation(
            instrument_id=instrument_id,
            funding_rate=Decimal(str(item["fundingRate"])),
            exchange_ts=_from_ms(int(item["time"])),
            received_ts=received_ts,
            source_type=SourceType.PROXY_HYPERLIQUID,
        )
        for item in items
    ]


def parse_candles(
    items: list[dict], *, instrument_id: int, interval: str, received_ts: datetime
) -> list[Candle]:
    return [
        Candle(
            instrument_id=instrument_id,
            interval=interval,
            open_ts=_from_ms(int(item["t"])),
            open=Decimal(str(item["o"])),
            high=Decimal(str(item["h"])),
            low=Decimal(str(item["l"])),
            close=Decimal(str(item["c"])),
            volume=Decimal(str(item["v"])),
            trades=int(item["n"]),
            received_ts=received_ts,
            source_type=SourceType.PROXY_HYPERLIQUID,
        )
        for item in items
    ]


class HyperliquidClient:
    def __init__(
        self,
        *,
        limiter: TokenBucket,
        base_url: str = DEFAULT_BASE_URL,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._limiter = limiter
        self._clock = clock
        self._http = httpx.AsyncClient(base_url=base_url, timeout=_TIMEOUT_S, transport=transport)

    async def _info(self, body: dict) -> list[dict]:
        await self._limiter.acquire()
        try:
            resp = await self._http.post("/info", json=body)
        except httpx.TransportError as exc:
            raise TransientProxyError(f"transport error: {type(exc).__name__}") from exc
        if resp.status_code == 429 or resp.status_code >= 500:
            ra = resp.headers.get("Retry-After")
            raise TransientProxyError(
                f"HTTP {resp.status_code}", retry_after=_retry_after_seconds(ra)
            )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            raise ValueError(f"expected a JSON list from /info, got {type(data).__name__}")
        return data

    async def funding_history(
        self, coin: str, *, start: datetime, end: datetime, instrument_id: int
    ) -> list[FundingObservation]:
        items = await self._info(
            {"type": "fundingHistory", "coin": coin, "startTime": _ms(start), "endTime": _ms(end)}
        )
        return parse_funding_history(items, instrument_id=instrument_id, received_ts=self._clock())

    async def candles(
        self, coin: str, *, interval: str, start: datetime, end: datetime, instrument_id: int
    ) -> list[Candle]:
        items = await self._info(
            {"type": "candleSnapshot",
             "req": {"coin": coin, "interval": interval, "startTime": _ms(start), "endTime": _ms(end)}}
        )
        return parse_candles(items, instrument_id=instrument_id, interval=interval, received_ts=self._clock())

    async def close(self) -> None:
        await self._http.aclose()
