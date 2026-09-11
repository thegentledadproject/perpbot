"""H2: cross-venue basis. basis = pm_close / hl_close - 1. Fade a stretched basis by
trading the Polymarket leg. Proxy closes are looked up ONLY for open_ts values present in
history, so the strategy cannot see a proxy hour the harness has not yet reached."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal

from polyperps.backtest.bars import Bar
from polyperps.strategies._zscore import zscore

_EXIT_Z = Decimal("0.5")


class Basis:
    name = "h2_basis"

    def __init__(self, *, lookback: int, entry_z: Decimal, proxy_close_by_hour: Mapping[datetime, Decimal]) -> None:
        self.lookback = lookback
        self.entry_z = entry_z
        self.params = {"lookback": lookback, "entry_z": entry_z}
        self.warmup = lookback
        self._proxy = proxy_close_by_hour
        self._position = Decimal(0)

    def target(self, history: Sequence[Bar]) -> Decimal:
        window: list[Decimal] = []
        for b in history[-self.lookback:]:
            hl = self._proxy.get(b.open_ts)
            if b.close is None or hl is None or hl == 0:
                continue
            window.append(b.close / hl - 1)
        if len(window) < self.lookback or self._proxy.get(history[-1].open_ts) is None:
            self._position = Decimal(0)
            return self._position
        z = zscore(window)
        if z is None:
            return self._position
        if z >= self.entry_z:
            self._position = Decimal(-1)
        elif z <= -self.entry_z:
            self._position = Decimal(1)
        elif abs(z) < _EXIT_Z:
            self._position = Decimal(0)
        return self._position

    def on_flatten(self) -> None:
        self._position = Decimal(0)
