"""Spec 2.4b: rebuild router state from the executor (exchange = truth) before any strategy runs."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal

from polyperps.execution.executor import Executor
from polyperps.execution.order_router import InstrumentRouter
from polyperps.execution.types import State
from polyperps.monitor.alerts import Alert, Alerter
from polyperps.storage import db


class RecoveryHalt(RuntimeError):
    pass


@dataclass
class RecoveryReport:
    adopted: list[str] = field(default_factory=list)
    abandoned: list[str] = field(default_factory=list)
    cancelled: list[str] = field(default_factory=list)
    stops_replaced: list[int] = field(default_factory=list)
    unknown_positions: list[int] = field(default_factory=list)
    states: dict[int, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"adopted": self.adopted, "abandoned": self.abandoned, "cancelled": self.cancelled,
                "stops_replaced": self.stops_replaced, "unknown_positions": self.unknown_positions,
                "states": self.states}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def recover(
    *,
    conn,
    run_id: str,
    executor: Executor,
    routers: Mapping[int, InstrumentRouter],
    alerter: Alerter,
    clock: Callable[[], datetime] = _utcnow,
) -> RecoveryReport:
    try:
        snap = await executor.snapshot()
    except Exception as exc:
        raise RecoveryHalt(f"executor snapshot unavailable: {type(exc).__name__}") from exc

    local = db.get_positions_local(conn, run_id)
    rep = RecoveryReport()

    for iid, router in routers.items():
        row = local.get(iid)
        if row is not None:
            router.load_local(row)
        if row is not None and row.state is State.HALTED:
            rep.states[iid] = State.HALTED.value
            continue
        pos = snap.position(iid)
        if pos is not None and pos.size != 0:
            router.size, router.entry, router.cumulative_funding = pos.size, pos.entry_price, pos.cumulative_funding
            router.state = State.OPEN
            router._persist()
            if iid not in snap.stops:
                await router.replace_stop()
                rep.stops_replaced.append(iid)
            else:
                router.stop_trigger = snap.stops[iid]
                router._persist()
        else:
            router.size, router.entry, router.stop_trigger = Decimal(0), None, None
            router.state = State.FLAT
            router._persist()
        rep.states[iid] = router.state.value

    for pos in snap.positions:
        if pos.size != 0 and pos.instrument_id not in routers:
            rep.unknown_positions.append(pos.instrument_id)
            alerter.emit(Alert(level="CRITICAL", kind="unknown_position", instrument_id=pos.instrument_id,
                               detail={"size": str(pos.size)}, ts=clock()))

    prefix = f"{run_id}-"
    for o in db.list_orders(conn, run_id):
        if o.status not in ("submitting", "accepted"):
            continue
        status = "adopted" if o.client_order_id in snap.open_orders else "abandoned"
        db.upsert_order(conn, replace(o, status=status, updated_at=clock()))
        (rep.adopted if status == "adopted" else rep.abandoned).append(o.client_order_id)
    for oid in snap.open_orders:
        if not oid.startswith(prefix):
            await executor.cancel(oid)
            rep.cancelled.append(oid)

    db.insert_recovery(conn, run_id=run_id, ts=clock(), findings_json=json.dumps(rep.to_dict()))
    alerter.emit(Alert(level="INFO", kind="recovery", instrument_id=None,
                       detail={"states": rep.states, "abandoned": len(rep.abandoned)}, ts=clock()))
    return rep
