"""H2: cross-venue basis. basis = pm_close / hl_close - 1. Fade a stretched basis by
trading the Polymarket leg. Proxy closes are looked up ONLY for open_ts values present in
history, so the strategy cannot see a proxy hour the harness has not yet reached.
The z-score window is the last `lookback` aligned (PM close, HL close) pairs walking
back through history; hours missing either leg are skipped, not fatal (spec 6)."""

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

    def _pair(self, b: Bar) -> Decimal | None:
        hl = self._proxy.get(b.open_ts)
        if b.close is None or hl is None or hl == 0:
            return None
        return b.close / hl - 1

    def target(self, history: Sequence[Bar]) -> Decimal:
        # Spec 6: the window is the last `lookback` ALIGNED pairs, not the last `lookback`
        # hours. Walk backwards, skipping hours with no candle or no proxy close; gaps
        # inside the window are skipped, not fatal. The current hour itself must be a
        # valid pair, since the z-score is of the LAST value.
        if not history or self._pair(history[-1]) is None:
            self._position = Decimal(0)
            return self._position
        window: list[Decimal] = []
        for b in reversed(history):
            basis = self._pair(b)
            if basis is not None:
                window.append(basis)
                if len(window) == self.lookback:
                    break
        if len(window) < self.lookback:
            self._position = Decimal(0)
            return self._position
        window.reverse()  # chronological; window[-1] is the current hour
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

    def on_recover(self, position_sign: int) -> None:
        """Live restart (spec 2.4b): the router recovered a position from the exchange; align the
        internal side with it (-1/0/+1) so the next target() holds instead of restarting at flat."""
        self._position = Decimal(position_sign)
