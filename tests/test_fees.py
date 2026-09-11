from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from polyperps.exchange.client import PolymarketPerpsClient, fee_from_rest
from polyperps.exchange.rate_limiter import TokenBucket
from polyperps.exchange.types import FeeSchedule

T0 = datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc)


def test_fee_from_rest():
    entry = SimpleNamespace(category="crypto", taker_fee_rate=Decimal("0.0005"),
                            maker_fee_rate=Decimal("0.0002"), tiers=())
    assert fee_from_rest(entry, T0) == FeeSchedule(category="crypto", taker_fee_rate=Decimal("0.0005"),
                                                  maker_fee_rate=Decimal("0.0002"), fetched_at=T0)


class CountingBucket(TokenBucket):
    def __init__(self):
        super().__init__(rate_per_sec=1000, burst=1000)
        self.acquired = 0

    async def acquire(self):
        self.acquired += 1
        return 0.0


class FakeSdk:
    async def fetch_perps_fees(self):
        return (SimpleNamespace(category="crypto", taker_fee_rate=Decimal("0.0005"),
                                maker_fee_rate=Decimal("0.0002"), tiers=()),
                SimpleNamespace(category="equity", taker_fee_rate=Decimal("0.001"),
                                maker_fee_rate=Decimal("0.0005"), tiers=()))


async def test_fetch_fees_goes_through_limiter():
    bucket = CountingBucket()
    c = PolymarketPerpsClient(FakeSdk(), limiter=bucket, clock=lambda: T0)
    fees = await c.fetch_fees()
    assert [f.category for f in fees] == ["crypto", "equity"]
    assert all(f.fetched_at == T0 for f in fees)
    assert bucket.acquired == 1
