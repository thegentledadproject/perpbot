"""Spec 2.4: local rows vs the executor's view. Pure diff; responses live in Portfolio.reconcile_now().

Amendment vs spec section 7.1: `liq_price_drift` is replaced by `stop_drift` (we do not
store a local liquidation price; we do store our stop trigger).

Controller ruling: a local row in ENTRY_PENDING/EXIT_PENDING has an order in flight, so its
persisted size (and stop) is transiently stale by design - the fill handler, not reconciliation,
owns that row's transition once the fill lands. diff() therefore skips the size and stop
comparisons for such rows entirely (no size, missing_stop, or stop_drift mismatch is raised for
them); unknown_order detection is unaffected since it does not consult local row state."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from polyperps.execution.types import AccountSnapshot, PositionLocalRow, State

Kind = Literal["size", "unknown_order", "missing_stop", "stop_without_position", "stop_drift"]


@dataclass(frozen=True, slots=True, kw_only=True)
class Mismatch:
    kind: Kind
    instrument_id: int | None
    local: str
    remote: str


def diff(
    *,
    local: Mapping[int, PositionLocalRow],
    remote: AccountSnapshot,
    run_id: str,
    known_orders: set[str],
    stop_drift_tolerance: Decimal = Decimal("0.05"),
) -> list[Mismatch]:
    out: list[Mismatch] = []
    remote_pos = {p.instrument_id: p for p in remote.positions}
    for iid in sorted(set(local) | set(remote_pos)):
        lrow = local.get(iid)
        if lrow is not None and lrow.state in (State.ENTRY_PENDING, State.EXIT_PENDING):
            continue  # order in flight; fill handler owns this row's size/stop transition
        lsize = lrow.size if lrow is not None else Decimal(0)
        rsize = remote_pos[iid].size if iid in remote_pos else Decimal(0)
        if lsize != rsize:
            out.append(Mismatch(kind="size", instrument_id=iid, local=str(lsize), remote=str(rsize)))
            continue
        if rsize != 0 and lrow is not None and lrow.state is State.OPEN:
            rstop = remote.stops.get(iid)
            lstop = lrow.stop_trigger
            if rstop is None:
                out.append(Mismatch(kind="missing_stop", instrument_id=iid, local=str(lstop), remote="none"))
            elif lstop is not None and lstop != 0 and abs(rstop - lstop) / lstop > stop_drift_tolerance:
                out.append(Mismatch(kind="stop_drift", instrument_id=iid, local=str(lstop), remote=str(rstop)))
    for iid, trig in remote.stops.items():
        if iid not in remote_pos or remote_pos[iid].size == 0:
            out.append(Mismatch(kind="stop_without_position", instrument_id=iid, local="none", remote=str(trig)))
    prefix = f"{run_id}-"
    for oid in remote.open_orders:
        if not oid.startswith(prefix) or oid not in known_orders:
            out.append(Mismatch(kind="unknown_order", instrument_id=None, local="none", remote=oid))
    return out
