"""H6: funding carry (spec 2026-10-11-h6). Hold the side that is paid while the mean of the last
smooth_hours non-missing funding rates is large: f >= entry -> short, f <= -entry -> long; exit when
f falls back inside entry/2 (or flips). Fewer than smooth_hours values: hold."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from itertools import islice

from polyperps.backtest.bars import Bar

_BPS = Decimal(10_000)


class FundingCarry:
    name = "h6_funding_carry"

    def __init__(self, *, entry_bps_h: Decimal, smooth_hours: int) -> None:
        self.entry_bps_h = entry_bps_h
        self.smooth_hours = smooth_hours
        self.params = {"entry_bps_h": entry_bps_h, "smooth_hours": smooth_hours}
        self.warmup = smooth_hours
        self._position = Decimal(0)

    def target(self, history: Sequence[Bar]) -> Decimal:
        rates = list(islice((b.funding_rate for b in reversed(history) if b.funding_rate is not None),
                            self.smooth_hours))
        if len(rates) < self.smooth_hours:
            return self._position
        f = sum(rates) / len(rates) * _BPS
        half = self.entry_bps_h / 2
        if f >= self.entry_bps_h:
            self._position = Decimal(-1)
        elif f <= -self.entry_bps_h:
            self._position = Decimal(1)
        elif (self._position < 0 and f < half) or (self._position > 0 and f > -half):
            self._position = Decimal(0)
        return self._position

    def on_flatten(self) -> None:
        self._position = Decimal(0)

    def on_recover(self, position_sign: int) -> None:
        self._position = Decimal(position_sign)
