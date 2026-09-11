from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from statistics import mean, stdev


def zscore(values: Sequence[Decimal]) -> Decimal | None:
    """z of the LAST value against the whole window. None if the window is too short or flat."""
    if len(values) < 3:
        return None
    xs = [float(v) for v in values]
    sd = stdev(xs)
    if sd == 0.0:
        return None
    return Decimal(str((xs[-1] - mean(xs)) / sd))
