"""Performance statistics (spec 5.4). Floats are fine here - nothing is money."""

from __future__ import annotations

import math
import random
from collections.abc import Sequence
from decimal import ROUND_CEILING, Decimal
from statistics import mean, stdev
from typing import TypeVar

T = TypeVar("T")


def sharpe(returns: Sequence[Decimal], *, periods_per_year: int = 24 * 365) -> float:
    if len(returns) < 2:
        return 0.0
    xs = [float(r) for r in returns]
    sd = stdev(xs)
    if sd == 0.0:
        return 0.0
    return mean(xs) / sd * math.sqrt(periods_per_year)


def max_drawdown(equity: Sequence[Decimal]) -> Decimal:
    peak = None
    worst = Decimal(0)
    for e in equity:
        if peak is None or e > peak:
            peak = e
        dd = peak - e
        if dd > worst:
            worst = dd
    return worst


def hit_rate(trade_pnls: Sequence[Decimal]) -> float:
    if not trade_pnls:
        return 0.0
    return sum(1 for p in trade_pnls if p > 0) / len(trade_pnls)


def turnover(fill_notionals: Sequence[Decimal], *, notional: Decimal) -> Decimal:
    return sum(fill_notionals, Decimal(0)) / notional


def block_bootstrap_ci(
    returns: Sequence[Decimal],
    *,
    block_len: int,
    resamples: int,
    ci: float,
    seed: int,
) -> tuple[float, float]:
    """Circular block bootstrap CI for the mean return (funding is autocorrelated)."""
    n = len(returns)
    if n < 2 * block_len:
        raise ValueError(f"insufficient for block bootstrap: {n} returns < 2 x block_len {block_len}")
    xs = [float(r) for r in returns]
    rng = random.Random(seed)
    n_blocks = math.ceil(n / block_len)
    means: list[float] = []
    for _ in range(resamples):
        sample: list[float] = []
        for _ in range(n_blocks):
            start = rng.randrange(n)
            sample.extend(xs[(start + k) % n] for k in range(block_len))
        means.append(mean(sample[:n]))
    means.sort()
    alpha = (1.0 - ci) / 2.0
    lo = means[int(alpha * (resamples - 1))]
    hi = means[int((1.0 - alpha) * (resamples - 1))]
    return lo, hi


def chronological_split(items: Sequence[T], *, holdout_fraction: Decimal) -> tuple[list[T], list[T]]:
    n = len(items)
    k = int((Decimal(n) * holdout_fraction).to_integral_value(rounding=ROUND_CEILING))
    return list(items[: n - k]), list(items[n - k :])
