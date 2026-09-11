import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from email.utils import format_datetime

import httpx
import pytest

from polyperps.data_ingest.hyperliquid import (
    HyperliquidClient, TransientProxyError, _retry_after_seconds, parse_candles,
    parse_funding_history,
)
from polyperps.exchange.rate_limiter import TokenBucket
from polyperps.exchange.types import SourceType

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 0, 0, tzinfo=UTC)
T0_MS = int(T0.timestamp() * 1000)
RX = T0 + timedelta(hours=5)

FUNDING_ITEMS = [
    {"coin": "BTC", "fundingRate": "0.0000125", "premium": "0.0001", "time": T0_MS},
    {"coin": "BTC", "fundingRate": "-0.00002", "premium": "-0.0001", "time": T0_MS + 3_600_000},
]
CANDLE_ITEMS = [
    {"t": T0_MS, "T": T0_MS + 3_599_999, "s": "BTC", "i": "1h", "o": "100.0", "c": "101.5",
     "h": "102", "l": "99", "v": "12.5", "n": 40},
]


def test_parse_funding_history_tags_proxy_and_utc():
    out = parse_funding_history(FUNDING_ITEMS, instrument_id=6, received_ts=RX)
    assert [f.funding_rate for f in out] == [Decimal("0.0000125"), Decimal("-0.00002")]
    assert out[0].exchange_ts == T0 and out[1].exchange_ts == T0 + timedelta(hours=1)
    assert all(f.source_type is SourceType.PROXY_HYPERLIQUID and f.instrument_id == 6 for f in out)
    assert out[0].received_ts == RX


def test_parse_candles():
    (c,) = parse_candles(CANDLE_ITEMS, instrument_id=6, interval="1h", received_ts=RX)
    assert c.open_ts == T0 and c.close == Decimal("101.5") and c.trades == 40
    assert c.interval == "1h" and c.source_type is SourceType.PROXY_HYPERLIQUID


def test_parse_rejects_unexpected_shape():
    with pytest.raises(KeyError):
        parse_funding_history([{"coin": "BTC", "rate": "0.1"}], instrument_id=6, received_ts=RX)


class CountingBucket(TokenBucket):
    def __init__(self):
        super().__init__(rate_per_sec=1000, burst=1000)
        self.acquired = 0

    async def acquire(self):
        self.acquired += 1
        return 0.0


def make_client(handler, bucket=None):
    transport = httpx.MockTransport(handler)
    return HyperliquidClient(limiter=bucket or CountingBucket(), transport=transport, clock=lambda: RX)


async def test_funding_history_posts_expected_body_and_parses():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json=FUNDING_ITEMS)

    bucket = CountingBucket()
    c = make_client(handler, bucket)
    out = await c.funding_history("BTC", start=T0, end=T0 + timedelta(hours=2), instrument_id=6)
    await c.close()
    assert seen["url"] == "https://api.hyperliquid.xyz/info"
    assert seen["body"] == {"type": "fundingHistory", "coin": "BTC", "startTime": T0_MS,
                            "endTime": T0_MS + 7_200_000}
    assert len(out) == 2 and bucket.acquired == 1


async def test_candles_posts_expected_body():
    seen = {}

    def handler(request):
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json=CANDLE_ITEMS)

    c = make_client(handler)
    out = await c.candles("BTC", interval="1h", start=T0, end=T0 + timedelta(hours=1), instrument_id=6)
    await c.close()
    assert seen["body"] == {"type": "candleSnapshot", "req": {"coin": "BTC", "interval": "1h",
                                                               "startTime": T0_MS, "endTime": T0_MS + 3_600_000}}
    assert len(out) == 1


async def test_429_raises_transient_with_retry_after():
    def handler(request):
        return httpx.Response(429, headers={"Retry-After": "7"}, json={"error": "rate limited"})

    c = make_client(handler)
    with pytest.raises(TransientProxyError) as exc:
        await c.funding_history("BTC", start=T0, end=T0 + timedelta(hours=1), instrument_id=6)
    await c.close()
    assert exc.value.retry_after == 7.0


async def test_400_is_not_transient():
    def handler(request):
        return httpx.Response(400, json={"error": "bad coin"})

    c = make_client(handler)
    with pytest.raises(httpx.HTTPStatusError):
        await c.funding_history("XXX", start=T0, end=T0 + timedelta(hours=1), instrument_id=6)
    await c.close()


def test_retry_after_seconds_delta_and_invalid():
    assert _retry_after_seconds("7") == 7.0
    assert _retry_after_seconds("garbage") is None
    assert _retry_after_seconds(None) is None


async def test_429_with_http_date_retry_after_does_not_crash():
    future = datetime.now(UTC) + timedelta(seconds=30)

    def handler(request):
        return httpx.Response(
            429, headers={"Retry-After": format_datetime(future, usegmt=True)}, json={"error": "rate limited"}
        )

    c = make_client(handler)
    with pytest.raises(TransientProxyError) as exc:
        await c.funding_history("BTC", start=T0, end=T0 + timedelta(hours=1), instrument_id=6)
    await c.close()
    assert exc.value.retry_after is not None
    assert 0 <= exc.value.retry_after <= 60
