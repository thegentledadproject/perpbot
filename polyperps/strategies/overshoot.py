"""H5: overshoot. Fade a one-hour Polymarket move that is unusually large for the past week:
z of the current hourly return against the last 168 valid returns (current included) >= entry_z
and the move itself >= 30 bps; hold hold_bars bars, then flat (spec 2026-10-03-h4-h5 2.2).
Returns need two closes exactly one hour apart; invalid pairs are skipped, not fatal."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from decimal import Decimal

from polyperps.backtest.bars import Bar
from polyperps.strategies._zscore import zscore

_BPS = Decimal(10_000)
_HOUR = timedelta(hours=1)
_LOOKBACK = 168
_MIN_MOVE_BPS = Decimal(30)


def _ret(prev: Bar, cur: Bar) -> Decimal | None:
    if cur.open_ts - prev.open_ts != _HOUR or not prev.close or not cur.close:
        return None
    return cur.close / prev.close - 1


class Overshoot:
    name = "h5_overshoot"

    def __init__(self, *, entry_z: Decimal, hold_bars: int) -> None:
        self.entry_z = entry_z
        self.hold_bars = hold_bars
        self.params = {"entry_z": entry_z, "hold_bars": hold_bars}
        self.warmup = _LOOKBACK + 1
        self._position = Decimal(0)
        self._held = 0

    def target(self, history: Sequence[Bar]) -> Decimal:
        if self._position != 0:
            self._held += 1
            if self._held >= self.hold_bars:
                self._position = Decimal(0)
                self._held = 0
            return self._position
        if len(history) < 2:
            return self._position
        r_t = _ret(history[-2], history[-1])
        if r_t is None:
            return self._position
        window: list[Decimal] = []
        for i in range(len(history) - 1, 0, -1):
            r = _ret(history[i - 1], history[i])
            if r is not None:
                window.append(r)
                if len(window) == _LOOKBACK:
                    break
        if len(window) < _LOOKBACK:
            return self._position
        window.reverse()  # chronological; window[-1] is r_t
        z = zscore(window)
        if z is None:
            return self._position
        if abs(z) >= self.entry_z and abs(r_t) * _BPS >= _MIN_MOVE_BPS:
            self._position = Decimal(-1) if r_t > 0 else Decimal(1)
            self._held = 0
        return self._position

    def on_flatten(self) -> None:
        self._position = Decimal(0)
        self._held = 0

    def on_recover(self, position_sign: int) -> None:
        """Live restart (spec 2.4b): the router recovered a position from the exchange; align the
        internal side with it (-1/0/+1) so the next target() holds instead of restarting at flat."""
        self._position = Decimal(position_sign)
