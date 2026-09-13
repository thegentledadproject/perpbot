"""Turn accepted live ticks into closed hourly Bars for the router (spec section 9).
funding_rate on a bar is the last tick's predicted rate for the settlement at
open_ts + 1h - the same convention bars.build_bars uses (see Phase 1 spec section 4.2 TODO)."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from decimal import Decimal

from polyperps.backtest.bars import Bar, floor_hour
from polyperps.exchange.types import Tick
from polyperps.signal.sufficiency import BAR


class _Acc:
    __slots__ = ("open_ts", "o", "h", "l", "c", "index", "funding", "source")

    def __init__(self, t: Tick) -> None:
        self.open_ts = floor_hour(t.exchange_ts)
        self.o = self.h = self.l = self.c = t.mark_price
        self.index, self.funding, self.source = t.index_price, t.funding_rate, t.source_type

    def add(self, t: Tick) -> None:
        self.h, self.l, self.c = max(self.h, t.mark_price), min(self.l, t.mark_price), t.mark_price
        self.index, self.funding = t.index_price, t.funding_rate


class LiveBarBuilder:
    def __init__(self, *, spread_bps: Decimal = BAR.proxy_spread_bps, max_history: int = 500) -> None:
        self._spread = spread_bps
        self._max = max_history
        self._acc: dict[int, _Acc] = {}
        self._hist: dict[int, list[Bar]] = defaultdict(list)

    def _close(self, iid: int) -> Bar:
        a = self._acc.pop(iid)
        bar = Bar(instrument_id=iid, source_type=a.source, open_ts=a.open_ts, open=a.o, high=a.h, low=a.l, close=a.c,
                  index_close=a.index, funding_rate=a.funding, spread_bps=self._spread, spread_source="constant",
                  complete=True)
        h = self._hist[iid]
        h.append(bar)
        del h[:-self._max]
        return bar

    def on_tick(self, tick: Tick) -> Bar | None:
        iid = tick.instrument_id
        acc = self._acc.get(iid)
        if acc is None:
            self._acc[iid] = _Acc(tick)
            return None
        if floor_hour(tick.exchange_ts) > acc.open_ts:
            closed = self._close(iid)
            self._acc[iid] = _Acc(tick)
            return closed
        acc.add(tick)
        return None

    def history(self, instrument_id: int) -> list[Bar]:
        return list(self._hist[instrument_id])

    def close_all(self, now: datetime) -> list[Bar]:
        """Force-close every open accumulator, stamping each partial hour complete=True.

        Not called by run_paper.py on shutdown - the partial hour would be delivered
        to the router as a complete bar it isn't, poisoning strategy history. Kept for
        tests/tools that want a deterministic flush (e.g. an offline replay harness).
        """
        return [self._close(iid) for iid in list(self._acc)]
