"""Spec 2.5: a trade's full story from SQLite alone."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime

from polyperps.storage import db

_ORDER = {"recovery": 3, "decision": 0, "order": 1, "alert": 2}


@dataclass(frozen=True, slots=True, kw_only=True)
class TrailEvent:
    ts: datetime
    kind: str
    instrument_id: int | None
    summary: str
    data: dict


def reconstruct(conn: sqlite3.Connection, run_id: str, instrument_id: int) -> list[TrailEvent]:
    events: list[TrailEvent] = []
    for d in db.list_decisions(conn, run_id, instrument_id):
        events.append(TrailEvent(ts=d.ts, kind="decision", instrument_id=instrument_id,
                                 summary=f"seq {d.seq} {d.state_before.value} target={d.target} "
                                         f"verdicts={d.verdicts} order={d.client_order_id}",
                                 data={"seq": d.seq, "verdicts": d.verdicts, "client_order_id": d.client_order_id}))
    for o in db.list_orders(conn, run_id):
        if o.instrument_id != instrument_id:
            continue
        events.append(TrailEvent(ts=o.updated_at, kind="order", instrument_id=instrument_id,
                                 summary=f"{o.client_order_id} {o.side} {o.quantity} {o.status} @ {o.avg_price} ({o.reason})",
                                 data={"client_order_id": o.client_order_id, "status": o.status}))
    for ts, level, kind, iid, detail in db.list_alerts(conn, run_id):
        if iid not in (instrument_id, None):
            continue
        events.append(TrailEvent(ts=ts, kind="alert", instrument_id=iid, summary=f"{level} {kind} {detail}",
                                 data={"level": level, "alert_kind": kind, "detail": detail}))
    for ts, findings in db.list_recovery(conn, run_id):
        events.append(TrailEvent(ts=ts, kind="recovery", instrument_id=None, summary=f"recovery {findings}",
                                 data=findings))
    events.sort(key=lambda e: (e.ts, _ORDER[e.kind]))
    return events
