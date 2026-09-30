"""Live venue access over polymarket-client's PerpsSession (spec section 4.4; Part A §3.3).

The ONLY execution module that imports the SDK. Three classes:

  LiveReader      the read side (snapshot, events, close). No gate: it cannot place anything.
  LiveExecutor    LiveReader + the write methods. Its constructor calls the three-lock gate for
                  every instrument and raises GateClosed unless all are open;
                  scripts/run_trader.py exits 2 while any lock is closed.
  ShadowExecutor  LiveReader whose writes raise ShadowRefused: the strategy runs against the
                  real account and every would-be order is recorded, nothing is sent.

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
  place_order(side=...)                        DIFFERS from the brief.
      .venv/Lib/site-packages/polymarket/models/types.py:6 defines
      OrderSide = Literal["BUY", "SELL"], uppercase. Our own OrderRequest.side
      is "buy"/"sell" (polyperps/execution/types.py). submit() below maps
      "buy"->"BUY", "sell"->"SELL" before calling session.place_order so the
      payload matches what the installed SDK actually accepts.
  PerpsOrderStatus (order acks/updates)        review finding, fixed.
      .venv/Lib/site-packages/polymarket/models/perps/types.py:42-67 defines a
      much larger PerpsOrderStatus than our own OrderStatus (spec section
      4.1): e.g. an IOC order that finds no liquidity comes back with status
      "ioc_no_fill", not a plain rejection. _SDK_ORDER_STATUS below maps every
      SDK status onto our OrderStatus (rejection-shaped statuses ->
      "rejected") or to None for TP/SL trigger-lifecycle statuses that never
      apply to a plain order ack/update. submit() now inspects
      placement.order.status through this table instead of always acking
      "accepted"; events() applies the same table to order-update payloads,
      skipping TP/SL lifecycle statuses and logging+skipping anything
      genuinely unrecognised.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from polymarket.errors import RequestRejectedError
from polymarket.models.perps.events import PerpsResyncEvent
from polymarket.models.perps.requests import PerpsPositionTpSlTrigger

from polyperps.execution.executor import GateClosed, ShadowRefused
from polyperps.execution.types import (
    AccountSnapshot, FillUpdate, OrderAck, OrderRequest, OrderStatus, OrderUpdate, PositionView, ReconcileNow,
    StopAck,
)
from polyperps.gates import ExecutionMode, GateDecision, live_orders_allowed

if TYPE_CHECKING:
    from polyperps.backtest.bars import Bar
    from polyperps.exchange.types import Tick

_log = logging.getLogger(__name__)

_FILL_SIDE_MAP = {"long": "buy", "short": "sell", "buy": "buy", "sell": "sell"}
_ORDER_SIDE_MAP = {"buy": "BUY", "sell": "SELL"}

# Maps the SDK's PerpsOrderStatus (polymarket/models/perps/types.py:42-67) onto
# our own OrderStatus (polyperps/execution/types.py). Statuses that mean "the
# order did not/will not rest or fill" collapse to "rejected"; the TP/SL
# trigger-lifecycle statuses map to None since they never apply to a plain
# order ack/update and are skipped by callers below.
_SDK_ORDER_STATUS: dict[str, OrderStatus | None] = {
    "accepted": "accepted",
    "open": "open",
    "partial": "partial",
    "filled": "filled",
    "cancelled": "cancelled",
    "auto_cancelled": "auto_cancelled",
    "post_only_rejected": "rejected",
    "fok_unfilled": "rejected",
    "ioc_no_fill": "rejected",
    "ioc_expired": "rejected",
    "stp_cancelled": "rejected",
    "zero_quantity": "rejected",
    "duplicate_order": "rejected",
    "order_not_found": "rejected",
    "reduce_only_invalid": "rejected",
    "reduce_only_expired": "rejected",
    "order_expired": "rejected",
    "expired": "rejected",
    "untriggered": None,
    "armed": None,
    "triggered": None,
    "parent_cancelled": None,
    "position_closed": None,
    "position_flipped": None,
    "reduce_only_invalid_at_trigger": None,
}
_UNMAPPED = object()  # sentinel: status string not in _SDK_ORDER_STATUS at all


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def open_session(label: str) -> tuple[Any, Any]:
    """(sdk client, perps session) for the wallet in POLYMARKET_PRIVATE_KEY; the caller closes
    both. Signs with the real wallet, so no test ever calls it."""
    from polymarket import AsyncSecureClient

    from polyperps.security.key_management import load_secret

    client = await AsyncSecureClient.create(private_key=load_secret("POLYMARKET_PRIVATE_KEY"))
    return client, await client.open_perps_session(label=label)


class LiveReader:
    """Read side of the live account. No gate: nothing here can place, cancel or arm anything."""

    name = "live-reader"

    def __init__(self, session: Any, *, clock: Callable[[], datetime] = _utcnow) -> None:
        self._s = session
        self._clock = clock
        self._stop_ids: dict[int, int] = {}
        self.start_equity: Decimal | None = None   # loss-limit baseline; set by the runner

    # --- runner hooks: the venue owns marks, funding and stop triggers -------
    def on_tick(self, tick: Tick) -> None:
        return None

    def on_bar(self, bar: Bar) -> None:
        return None

    def poll_fills(self) -> list[FillUpdate]:
        return []

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
                mapped = _SDK_ORDER_STATUS.get(p.status, _UNMAPPED)
                if mapped is _UNMAPPED:
                    _log.warning("live: unmapped order status %s", p.status)
                elif mapped is not None and p.client_order_id:
                    yield OrderUpdate(client_order_id=p.client_order_id, status=mapped,
                                      filled_quantity=p.filled_quantity, ts=ev.timestamp)
            elif kind == "fill":
                for f in ev.payload:
                    if f.client_order_id:
                        yield FillUpdate(client_order_id=f.client_order_id, instrument_id=int(f.instrument_id),
                                         side=_FILL_SIDE_MAP.get(f.side, f.side), quantity=f.quantity,
                                         price=f.price, fee=f.fee, ts=ev.timestamp)

    async def close(self) -> None:
        await self._s.close()


class LiveExecutor(LiveReader):
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
        if not instrument_ids:
            raise GateClosed("no instruments to gate")
        check = gate if gate is not None else (lambda iid: live_orders_allowed(iid, modes=modes))
        for iid in instrument_ids:
            d = check(iid)
            if not d.allowed:
                raise GateClosed(f"instrument {iid}: {d.reason}")
        super().__init__(session, clock=clock)
        self._dead_man = timedelta(seconds=dead_man_s)

    async def submit(self, order: OrderRequest) -> OrderAck:
        try:
            placement = await self._s.place_order(
                instrument_id=order.instrument_id, side=_ORDER_SIDE_MAP[order.side], quantity=order.quantity,
                time_in_force="ioc", reduce_only=order.reduce_only, client_order_id=order.client_order_id)
        except RequestRejectedError as exc:
            return OrderAck(client_order_id=order.client_order_id, exchange_order_id=None, status="rejected",
                            reason=str(exc), ts=self._clock())
        placed = getattr(placement, "order", None)
        xid = getattr(placed, "id", None)
        exchange_order_id = str(xid) if xid is not None else None
        raw_status = getattr(placed, "status", None)
        mapped = _SDK_ORDER_STATUS.get(raw_status, _UNMAPPED)
        if mapped is _UNMAPPED:
            return OrderAck(client_order_id=order.client_order_id, exchange_order_id=exchange_order_id,
                            status="rejected", reason=f"unknown status {raw_status}", ts=self._clock())
        if mapped in ("rejected", "cancelled", "auto_cancelled"):
            return OrderAck(client_order_id=order.client_order_id, exchange_order_id=exchange_order_id,
                            status="rejected", reason=str(raw_status), ts=self._clock())
        return OrderAck(client_order_id=order.client_order_id, exchange_order_id=exchange_order_id,
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


class ShadowExecutor(LiveReader):
    """Part A §3.3: the real account, read-only. Every write raises ShadowRefused (the router
    records the order as shadow_refused and halts the instrument); heartbeat is a no-op because
    shadow never has an order to protect."""

    name = "shadow"

    async def submit(self, order: OrderRequest) -> OrderAck:
        raise ShadowRefused(f"shadow: submit {order.client_order_id} refused")

    async def cancel(self, client_order_id: str) -> None:
        raise ShadowRefused(f"shadow: cancel {client_order_id} refused")

    async def place_stop(self, instrument_id: int, trigger_price: Decimal) -> StopAck:
        raise ShadowRefused(f"shadow: stop for {instrument_id} at {trigger_price} refused")

    async def cancel_stop(self, instrument_id: int) -> None:
        raise ShadowRefused(f"shadow: cancel stop for {instrument_id} refused")

    async def heartbeat(self) -> None:
        return None
