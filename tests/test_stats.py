import random
from decimal import Decimal

import pytest

from polyperps.backtest.stats import (
    block_bootstrap_ci, chronological_split, hit_rate, max_drawdown, sharpe, turnover,
)


def test_sharpe_constant_positive_returns_is_zero_stdev_guard():
    assert sharpe([Decimal("0.001")] * 10) == 0.0


def test_sharpe_alternating_returns():
    r = [Decimal("0.01"), Decimal("-0.005")] * 50
    s = sharpe(r)
    assert 20 < s < 40  # mean .0025, stdev ~.0075 -> .33 * sqrt(8760) ~ 31


def test_max_drawdown():
    eq = [Decimal(x) for x in (100, 110, 105, 120, 90, 95)]
    assert max_drawdown(eq) == Decimal("30")
    assert max_drawdown([Decimal(1), Decimal(2), Decimal(3)]) == Decimal("0")


def test_hit_rate_and_turnover():
    assert hit_rate([Decimal(1), Decimal(-1), Decimal(2), Decimal(0)]) == 0.5
    assert hit_rate([]) == 0.0
    assert turnover([Decimal(50), Decimal(100)], notional=Decimal(100)) == Decimal("1.5")


def test_bootstrap_iid_noise_contains_zero():
    rng = random.Random(1)
    r = [Decimal(str(round(rng.gauss(0, 0.01), 6))) for _ in range(500)]
    lo, hi = block_bootstrap_ci(r, block_len=24, resamples=500, ci=0.95, seed=7)
    assert lo < 0 < hi


def test_bootstrap_positive_drift_excludes_zero():
    rng = random.Random(2)
    r = [Decimal(str(round(0.005 + rng.gauss(0, 0.002), 6))) for _ in range(500)]
    lo, hi = block_bootstrap_ci(r, block_len=24, resamples=500, ci=0.95, seed=7)
    assert lo > 0


def test_bootstrap_is_deterministic_for_seed():
    r = [Decimal(str(i % 7 - 3)) for i in range(200)]
    a = block_bootstrap_ci(r, block_len=24, resamples=100, ci=0.95, seed=3)
    b = block_bootstrap_ci(r, block_len=24, resamples=100, ci=0.95, seed=3)
    assert a == b


def test_bootstrap_too_short_raises():
    with pytest.raises(ValueError, match="insufficient"):
        block_bootstrap_ci([Decimal(1)] * 40, block_len=24, resamples=10, ci=0.95, seed=1)


def test_chronological_split():
    train, hold = chronological_split(list(range(10)), holdout_fraction=Decimal("0.30"))
    assert train == [0, 1, 2, 3, 4, 5, 6] and hold == [7, 8, 9]
