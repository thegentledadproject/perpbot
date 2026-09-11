"""H3: mark-vs-index lag. If the mark sits far from its own index, bet it catches up;
hold hold_bars bars, then flat. Native-only: returns 0 when index_close is None."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from polyperps.backtest.bars import Bar

_BPS = Decimal(10_000)


class IndexLag:
    name = "h3_index_lag"

    def __init__(self, *, entry_bps: Decimal, hold_bars: int) -> None:
        self.entry_bps = entry_bps
        self.hold_bars = hold_bars
        self.params = {"entry_bps": entry_bps, "hold_bars": hold_bars}
        self.warmup = 1
        self._position = Decimal(0)
        self._held = 0

    def target(self, history: Sequence[Bar]) -> Decimal:
        last = history[-1]
        if last.index_close is None or last.close is None or last.index_close == 0:
            self._position = Decimal(0)
            self._held = 0
            return self._position
        if self._position != 0:
            self._held += 1
            if self._held >= self.hold_bars:
                self._position = Decimal(0)
                self._held = 0
            return self._position
        premium_bps = (last.close / last.index_close - 1) * _BPS
        if premium_bps <= -self.entry_bps:
            self._position = Decimal(1)   # mark below index: expect it to rise
        elif premium_bps >= self.entry_bps:
            self._position = Decimal(-1)  # mark above index: expect it to fall
        self._held = 0
        return self._position
