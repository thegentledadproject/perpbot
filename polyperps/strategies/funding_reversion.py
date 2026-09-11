"""H1: funding-rate mean reversion. Extreme positive funding -> short (collect it);
extreme negative -> long. Flat once |z| falls under exit_z."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from polyperps.backtest.bars import Bar
from polyperps.strategies._zscore import zscore


class FundingReversion:
    name = "h1_funding_reversion"

    def __init__(self, *, lookback: int, entry_z: Decimal, exit_z: Decimal) -> None:
        self.lookback = lookback
        self.entry_z = entry_z
        self.exit_z = exit_z
        self.params = {"lookback": lookback, "entry_z": entry_z, "exit_z": exit_z}
        self.warmup = lookback
        self._position = Decimal(0)

    def target(self, history: Sequence[Bar]) -> Decimal:
        window = [b.funding_rate for b in history[-self.lookback:] if b.funding_rate is not None]
        z = zscore(window)
        if z is None:
            return self._position
        if z >= self.entry_z:
            self._position = Decimal(-1)
        elif z <= -self.entry_z:
            self._position = Decimal(1)
        elif abs(z) < self.exit_z:
            self._position = Decimal(0)
        return self._position

    def on_flatten(self) -> None:
        self._position = Decimal(0)
