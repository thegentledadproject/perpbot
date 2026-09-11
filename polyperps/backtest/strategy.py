"""Strategy interface. The harness passes bars[:t+1] and nothing else."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Protocol

from polyperps.backtest.bars import Bar

ONE = Decimal(1)


class Strategy(Protocol):
    name: str
    params: Mapping[str, Decimal | int]

    def target(self, history: Sequence[Bar]) -> Decimal:
        """Desired position as a fraction of notional in [-1, +1], decided after history[-1] closed."""
        ...


def clamp_target(x: Decimal) -> Decimal:
    return max(-ONE, min(ONE, x))
