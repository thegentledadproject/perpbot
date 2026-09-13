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

    def on_flatten(self) -> None:
        """Called by the harness after it force-flattens the book on a data gap; reset any
        internal position/hold state."""
        ...

    def on_recover(self, position_sign: int) -> None:
        """OPTIONAL (the harness never calls it; the live router checks with getattr). Called
        once after a restart when state recovery found an open position: align internal side
        state with position_sign (-1 / 0 / +1) instead of restarting at flat."""
        ...


def clamp_target(x: Decimal) -> Decimal:
    return max(-ONE, min(ONE, x))
