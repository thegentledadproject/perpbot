"""Live executor over polymarket-client's PerpsSession (spec section 4.4).

The ONLY execution module that imports the SDK. Never run in Phase 2a: the
constructor calls the three-lock gate for every instrument and raises
GateClosed unless all are open, and scripts/run_paper.py refuses --executor live.

SDK verification (2026-09-12, polymarket-client==0.10.0 installed in .venv;
verified by reading source, never by calling the network):

  PerpsOrderPlacement.order.id                 verified unchanged
      .venv/Lib/site-packages/polymarket/models/perps/results.py:28-34
      (PerpsOrderPlacement.order: PerpsOrder), orders.py:59 (PerpsOrder.id).
  PerpsPlacedTpSlOrders.stop_loss.order_id     verified unchanged
      .venv/Lib/site-packages/polymarket/models/perps/results.py:10-24.
  PerpsTpSlOrderFields.kind / .trigger_price   verified unchanged
      .venv/Lib/site-packages/polymarket/models/perps/orders.py:40-48
      (kind: Literal["tp", "sl"]; trigger_price: Decimal).
  PerpsFill.side                               DIFFERS from the brief.
      .venv/Lib/site-packages/polymarket/models/perps/types.py:26 defines
      PerpsSide = Literal["long", "short"], not "buy"/"sell". events() below
      maps long->buy, short->sell to satisfy FillUpdate.side (decision:
      FillUpdate.side is "buy"/"sell"), with an identity fallback for
      already-lowercase buy/sell values (as used by this module's tests).
  place_order(side=...)                        NOTE, outside the brief's
      listed verification scope, not changed: OrderSide is
      Literal["BUY", "SELL"] (polymarket/models/types.py:6), uppercase. This
      module forwards OrderRequest.side ("buy"/"sell") unchanged, matching
      the verbatim brief test. Wiring this module to a real PerpsSession will
      need an uppercase mapping before any live use; flagged here, not fixed,
      since no test in this phase covers it and Phase 2a never calls the
      real SDK.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from polymarket.errors import RequestRejectedError
from polymarket.models.perps.events import PerpsResyncEvent
from polymarket.models.perps.requests import PerpsPositionTpSlTrigger

from polyperps.execution.executor import GateClosed
from polyperps.execution.types import (
    AccountSnapshot, FillUpdate, OrderAck, OrderRequest, OrderUpdate, PositionView, ReconcileNow, StopAck,
)
from polyperps.gates import ExecutionMode, GateDecision, live_orders_allowed

_FILL_SIDE_MAP = {"long": "buy", "short": "sell", "buy": "buy", "sell": "sell"}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class LiveExecutor:
    name = "live"

    def __init__(
        self,
        session: Any,
        *,
        instrument_ids: Sequence[int],
        modes: Mapping[int, ExecutionMode],
        gate: Callable[[int], GateDecision] | None = None,
        clock: Callable[[], datetime] = _utcnow,
        dead_man_s: int = 60,
    ) -> None:
        check = gate if gate is not None else (lambda iid: live_orders_allowed(iid, modes=modes))
        for iid in instrument_ids:
            d = check(iid)
            if not d.allowed:
                raise GateClosed(f"instrument {iid}: {d.reason}")
        self._s = session
        self._clock = clock
        self._dead_man = timedelta(seconds=dead_man_s)
        self._stop_ids: dict[int, int] = {}

    async def submit(self, order: OrderRequest) -> OrderAck:
        try:
            placement = await self._s.place_order(
                instrument_id=order.instrument_id, side=order.side, quantity=order.quantity,
                time_in_force="ioc", reduce_only=order.reduce_only, client_order_id=order.client_order_id)
        except RequestRejectedError as exc:
            return OrderAck(client_order_id=order.client_order_id, exchange_order_id=None, status="rejected",
                            reason=str(exc), ts=self._clock())
        xid = getattr(getattr(placement, "order", None), "id", None)
        return OrderAck(client_order_id=order.client_order_id, exchange_order_id=str(xid) if xid is not None else None,
                        status="accepted", reason="", ts=self._clock())

    async def cancel(self, client_order_id: str) -> None:
        await self._s.cancel_order(client_order_id=client_order_id)

    async def place_stop(self, instrument_id: int, trigger_price: Decimal) -> StopAck:
        placed = await self._s.place_position_tp_sl(
            instrument_id=instrument_id, stop_loss=PerpsPositionTpSlTrigger(trigger_price=trigger_price))
        oid = getattr(getattr(placed, "stop_loss", None), "order_id", None)
        if oid is not None:
            self._stop_ids[instrument_id] = int(oid)
        return StopAck(instrument_id=instrument_id, trigger_price=trigger_price,
                       exchange_order_id=str(oid) if oid is not None else None, ts=self._clock())

    async def cancel_stop(self, instrument_id: int) -> None:
        oid = self._stop_ids.pop(instrument_id, None)
        if oid is not None:
            await self._s.cancel_order(order_id=oid)

    async def heartbeat(self) -> None:
        await self._s.arm_auto_cancel(cancel_at=self._clock() + self._dead_man)

    async def snapshot(self) -> AccountSnapshot:
        pf = await self._s.fetch_portfolio()
        orders = await self._s.fetch_open_orders()
        positions = tuple(
            PositionView(instrument_id=int(p.instrument_id), size=p.size, entry_price=p.entry_price,
                         notional=abs(p.position_value), leverage=int(p.leverage),
                         liquidation_price=p.liquidation_price, unrealised_pnl=p.unrealized_pnl,
                         cumulative_funding=p.cumulative_funding)
            for p in pf.positions if p.size != 0)
        open_ids = tuple(o.client_order_id for o in orders if o.client_order_id and o.tp_sl is None)
        stops: dict[int, Decimal] = {}
        for o in orders:
            tp_sl = getattr(o, "tp_sl", None)
            if tp_sl is not None and getattr(tp_sl, "kind", None) == "sl":
                stops[int(o.instrument_id)] = tp_sl.trigger_price
                self._stop_ids[int(o.instrument_id)] = int(o.id)
        return AccountSnapshot(equity=pf.margin.total_account_value, positions=positions, open_orders=open_ids,
                               stops=stops, in_liquidation=bool(pf.in_liquidation), ts=self._clock())

    async def events(self) -> AsyncIterator[OrderUpdate | FillUpdate | ReconcileNow]:
        async for ev in self._s:
            if isinstance(ev, PerpsResyncEvent):
                yield ReconcileNow("resync")
                continue
            kind = getattr(ev, "type", None)
            if kind == "order":
                p = ev.payload
                if p.client_order_id:
                    yield OrderUpdate(client_order_id=p.client_order_id, status=p.status,
                                      filled_quantity=p.filled_quantity, ts=ev.timestamp)
            elif kind == "fill":
                for f in ev.payload:
                    if f.client_order_id:
                        yield FillUpdate(client_order_id=f.client_order_id, instrument_id=int(f.instrument_id),
                                         side=_FILL_SIDE_MAP.get(f.side, f.side), quantity=f.quantity,
                                         price=f.price, fee=f.fee, ts=ev.timestamp)

    async def close(self) -> None:
        await self._s.close()
