"""H4: lead-lag. If Hyperliquid moved at least gap_bps in the last hour and Polymarket moved at
least gap_bps less in the same direction, bet Polymarket catches up; hold hold_bars bars, then
flat (spec 2026-10-03-h4-h5 2.1). Proxy closes are looked up ONLY for open_ts values present in
history, as in H2, so the strategy cannot see a proxy hour the harness has not reached."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from decimal import Decimal

from polyperps.backtest.bars import Bar

_BPS = Decimal(10_000)
_HOUR = timedelta(hours=1)


class LeadLag:
    name = "h4_lead_lag"

    def __init__(self, *, gap_bps: Decimal, hold_bars: int, proxy_close_by_hour: Mapping[datetime, Decimal]) -> None:
        self.gap_bps = gap_bps
        self.hold_bars = hold_bars
        self.params = {"gap_bps": gap_bps, "hold_bars": hold_bars}
        self.warmup = 2
        self._proxy = proxy_close_by_hour
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
        p, c = history[-2], history[-1]
        if c.open_ts - p.open_ts != _HOUR or not p.close or not c.close:
            return self._position
        hl_p, hl_c = self._proxy.get(p.open_ts), self._proxy.get(c.open_ts)
        if not hl_p or not hl_c:
            return self._position
        r_hl = hl_c / hl_p - 1
        lag = r_hl - (c.close / p.close - 1)
        gap = self.gap_bps / _BPS
        if abs(r_hl) >= gap and lag * r_hl > 0 and abs(lag) >= gap:
            self._position = Decimal(1) if r_hl > 0 else Decimal(-1)
            self._held = 0
        return self._position

    def on_flatten(self) -> None:
        self._position = Decimal(0)
        self._held = 0

    def on_recover(self, position_sign: int) -> None:
        """Live restart (spec 2.4b): the router recovered a position from the exchange; align the
        internal side with it (-1/0/+1) so the next target() holds instead of restarting at flat."""
        self._position = Decimal(position_sign)
