"""Paper executor (spec section 4.3): in-memory account, cost-model fills at the
live mark, self-firing stops, JSON persistence so a restart exercises recovery.
Liquidation price uses MAINTENANCE_RATE (a documented assumption; live uses the
exchange's own number)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timezone
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Literal

from polyperps.execution.executor import ExecutorTimeout
from polyperps.execution.types import (
    AccountSnapshot, FillUpdate, OrderAck, OrderRequest, OrderUpdate, PositionView, StopAck,
)
from polyperps.risk.liquidation_guard import LIMITS
from polyperps.signal.sufficiency import BAR

MAINTENANCE_RATE = LIMITS.maintenance_rate
_BPS = Decimal(10_000)
_P = Decimal("0.01")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class _Pos:
    __slots__ = ("size", "entry", "funding")

    def __init__(self, size: Decimal = Decimal(0), entry: Decimal = Decimal(0), funding: Decimal = Decimal(0)) -> None:
        self.size, self.entry, self.funding = size, entry, funding


class SimExecutor:
    name = "sim"

    def __init__(
        self,
        run_id: str,
        *,
        equity: Decimal,
        taker_fee_rate: Decimal,
        spread_bps: Decimal = BAR.proxy_spread_bps,
        impact_bps: Decimal = BAR.impact_bps,
        leverage: int = LIMITS.max_leverage,
        clock: Callable[[], datetime] = _utcnow,
        persist: Callable[[str], None] | None = None,
    ) -> None:
        self.run_id = run_id
        self._cash = equity
        self._fee = taker_fee_rate
        self._slip = (spread_bps / 2 + impact_bps) / _BPS
        self._lev = leverage
        self._clock = clock
        self._persist = persist
        self._marks: dict[int, Decimal] = {}
        self._pos: dict[int, _Pos] = {}
        self._stops: dict[int, Decimal] = {}
        self._queue: asyncio.Queue[OrderUpdate | FillUpdate] = asyncio.Queue()
        self._n = 0
        self.fail_next: Literal["timeout", "reject"] | None = None
        self.heartbeat_count = 0

    # --- market data in ---------------------------------------------------
    def update_mark(self, instrument_id: int, mark: Decimal) -> None:
        self._marks[instrument_id] = mark

    def apply_funding(self, instrument_id: int, rate: Decimal) -> None:
        p = self._pos.get(instrument_id)
        if p is None or p.size == 0:
            return
        paid = -p.size * self._marks[instrument_id] * rate  # longs pay positive funding
        p.funding += paid
        self._cash += paid
        self._save()

    # --- executor protocol ------------------------------------------------
    async def submit(self, order: OrderRequest) -> OrderAck:
        now = self._clock()
        mode, self.fail_next = self.fail_next, None
        if mode == "reject":
            return OrderAck(client_order_id=order.client_order_id, exchange_order_id=None, status="rejected",
                            reason="sim: injected reject", ts=now)
        p = self._pos.setdefault(order.instrument_id, _Pos())
        if order.reduce_only and (p.size == 0 or (p.size > 0) == (order.side == "buy")):
            return OrderAck(client_order_id=order.client_order_id, exchange_order_id=None, status="rejected",
                            reason="reduce_only order would open or increase a position", ts=now)
        fill_qty = min(order.quantity, abs(p.size)) if order.reduce_only else order.quantity
        self._n += 1
        xid = f"sim-{self._n}"
        fill = self._fill(order, now, fill_qty)
        self._queue.put_nowait(OrderUpdate(client_order_id=order.client_order_id, status="filled",
                                           filled_quantity=fill_qty, ts=now))
        self._queue.put_nowait(fill)
        self._save()
        if mode == "timeout":
            raise ExecutorTimeout(f"sim: injected timeout for {order.client_order_id}")
        return OrderAck(client_order_id=order.client_order_id, exchange_order_id=xid, status="accepted", reason="", ts=now)

    def _fill(self, order: OrderRequest, now: datetime, quantity: Decimal | None = None) -> FillUpdate:
        qty = order.quantity if quantity is None else quantity
        mark = self._marks[order.instrument_id]
        s = Decimal(1) if order.side == "buy" else Decimal(-1)
        price = (mark * (1 + s * self._slip)).quantize(_P, rounding=ROUND_HALF_EVEN)
        fee = qty * price * self._fee
        self._cash -= fee
        self._apply_position(order.instrument_id, s * qty, price)
        return FillUpdate(client_order_id=order.client_order_id, instrument_id=order.instrument_id, side=order.side,
                          quantity=qty, price=price, fee=fee, ts=now)

    def _apply_position(self, iid: int, delta: Decimal, price: Decimal) -> None:
        p = self._pos.setdefault(iid, _Pos())
        old, new = p.size, p.size + delta
        if old == 0:
            p.size, p.entry = new, price
        elif (old > 0) == (new > 0) and abs(new) > abs(old):        # increase: average cost
            p.entry = (abs(old) * p.entry + abs(delta) * price) / abs(new)
            p.size = new
        elif (old > 0) == (new > 0) and new != 0:                   # reduce: realise closed part
            closed = abs(old) - abs(new)
            self._cash += closed * (Decimal(1) if old > 0 else Decimal(-1)) * (price - p.entry)
            p.size = new
        else:                                                       # close or flip
            self._cash += old * (price - p.entry)
            p.size, p.entry = new, (price if new != 0 else Decimal(0))
        if p.size == 0:
            p.funding = Decimal(0)

    async def cancel(self, client_order_id: str) -> None:
        return None  # IOC fills instantly; nothing rests in the sim

    async def place_stop(self, instrument_id: int, trigger_price: Decimal) -> StopAck:
        self._stops[instrument_id] = trigger_price
        self._save()
        return StopAck(instrument_id=instrument_id, trigger_price=trigger_price,
                       exchange_order_id=f"sim-stop-{instrument_id}", ts=self._clock())

    def check_triggers(self) -> list[FillUpdate]:
        """Fire stops against current marks. Callable with the router stopped."""
        fired: list[FillUpdate] = []
        for iid, trig in list(self._stops.items()):
            p = self._pos.get(iid)
            mark = self._marks.get(iid)
            if p is None or p.size == 0 or mark is None:
                continue
            if (p.size > 0 and mark <= trig) or (p.size < 0 and mark >= trig):
                self._n += 1
                side = "sell" if p.size > 0 else "buy"
                req = OrderRequest(client_order_id=f"sim-stop-{iid}-{self._n}", instrument_id=iid, side=side,
                                   quantity=abs(p.size), reduce_only=True, ts=self._clock())
                self._marks[iid] = trig  # stops fill at the trigger (plus slippage)
                fill = self._fill(req, self._clock())
                self._marks[iid] = mark
                self._queue.put_nowait(fill)
                fired.append(fill)
                del self._stops[iid]
        if fired:
            self._save()
        return fired

    async def heartbeat(self) -> None:
        self.heartbeat_count += 1

    async def snapshot(self) -> AccountSnapshot:
        views: list[PositionView] = []
        unreal = Decimal(0)
        for iid, p in self._pos.items():
            if p.size == 0:
                continue
            mark = self._marks[iid]
            u = p.size * (mark - p.entry)
            unreal += u
            if p.size > 0:
                liq = p.entry * (1 - Decimal(1) / self._lev + MAINTENANCE_RATE)
            else:
                liq = p.entry * (1 + Decimal(1) / self._lev - MAINTENANCE_RATE)
            views.append(PositionView(instrument_id=iid, size=p.size, entry_price=p.entry, notional=abs(p.size) * mark,
                                      leverage=self._lev, liquidation_price=liq.quantize(_P), unrealised_pnl=u,
                                      cumulative_funding=p.funding))
        return AccountSnapshot(equity=self._cash + unreal, positions=tuple(views), open_orders=(),
                               stops=dict(self._stops), in_liquidation=False, ts=self._clock())

    async def events(self) -> AsyncIterator[OrderUpdate | FillUpdate]:
        while True:
            yield await self._queue.get()

    def drain_events(self) -> list[OrderUpdate | FillUpdate]:
        out = []
        while not self._queue.empty():
            out.append(self._queue.get_nowait())
        return out

    async def close(self) -> None:
        return None

    # --- persistence ------------------------------------------------------
    def to_json(self) -> str:
        return json.dumps({
            "cash": str(self._cash), "n": self._n,
            "positions": {str(i): {"size": str(p.size), "entry": str(p.entry), "funding": str(p.funding)}
                          for i, p in self._pos.items() if p.size != 0},
            "stops": {str(i): str(t) for i, t in self._stops.items()},
            "marks": {str(i): str(m) for i, m in self._marks.items()},
        })

    @classmethod
    def from_json(cls, run_id: str, text: str, **kw) -> "SimExecutor":
        d = json.loads(text)
        ex = cls(run_id, equity=Decimal(d["cash"]), **kw)
        ex._n = d["n"]
        for i, p in d["positions"].items():
            ex._pos[int(i)] = _Pos(Decimal(p["size"]), Decimal(p["entry"]), Decimal(p["funding"]))
        ex._stops = {int(i): Decimal(t) for i, t in d["stops"].items()}
        ex._marks = {int(i): Decimal(m) for i, m in d.get("marks", {}).items()}
        return ex

    def _save(self) -> None:
        if self._persist is not None:
            self._persist(self.to_json())
