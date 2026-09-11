import pytest

from polyperps.exchange.rate_limiter import TokenBucket


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def make_sleep(clock, log):
    async def _sleep(seconds):
        log.append(seconds)
        clock.t += seconds

    return _sleep


async def test_burst_is_free():
    clock, log = FakeClock(), []
    b = TokenBucket(rate_per_sec=1.0, burst=2, clock=clock, sleep=make_sleep(clock, log))
    assert await b.acquire() == 0.0
    assert await b.acquire() == 0.0
    assert log == []


async def test_third_call_waits_for_refill():
    clock, log = FakeClock(), []
    b = TokenBucket(rate_per_sec=1.0, burst=2, clock=clock, sleep=make_sleep(clock, log))
    await b.acquire()
    await b.acquire()
    waited = await b.acquire()
    assert waited == pytest.approx(1.0)
    assert log == [pytest.approx(1.0)]


async def test_tokens_refill_with_time():
    clock, log = FakeClock(), []
    b = TokenBucket(rate_per_sec=2.0, burst=1, clock=clock, sleep=make_sleep(clock, log))
    await b.acquire()
    clock.t += 0.5  # one token refilled at 2/s
    assert await b.acquire() == 0.0


def test_rejects_bad_config():
    with pytest.raises(ValueError):
        TokenBucket(rate_per_sec=0, burst=1)
    with pytest.raises(ValueError):
        TokenBucket(rate_per_sec=1, burst=0)
