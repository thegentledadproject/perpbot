"""Build the read-only dashboard state from the trading DB.

Pure: reads rows through polyperps.storage.db, never writes, no network.
Every number the page shows is computed here so it can be pinned by tests.
"""
from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping, Protocol

from polyperps.execution.types import State
from polyperps.risk.liquidation_guard import LIMITS
from polyperps.risk.portfolio_exposure import EXPOSURE, cluster_of
from polyperps.storage import db


class InstrumentInfo(Protocol):
    symbol: str
    category: str


_CENT = Decimal("0.01")
_ZERO = Decimal(0)


def _dec(x: Any) -> Decimal:
    return Decimal(str(x))


def _s(d: Decimal) -> str:
    """Decimal -> plain string without exponent ("9997", "68666.67")."""
    return format(d.normalize(), "f") if d == d.to_integral() else format(d, "f")


def _f(d: Decimal) -> float:
    return float(d)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def liq_price(size: Decimal, entry: Decimal) -> Decimal:
    """Same formula SimExecutor.snapshot() uses (sim_executor.py), pinned by a test."""
    lev = Decimal(LIMITS.max_leverage)
    if size > 0:
        liq = entry * (1 - Decimal(1) / lev + LIMITS.maintenance_rate)
    else:
        liq = entry * (1 + Decimal(1) / lev - LIMITS.maintenance_rate)
    return liq.quantize(_CENT)


def _name(iid: int, instruments: Mapping[int, InstrumentInfo] | None) -> str:
    inst = instruments.get(iid) if instruments else None
    return inst.symbol if inst is not None else f"inst {iid}"


def _opened_at(orders, iid: int) -> str | None:
    fills = [o for o in orders if o.instrument_id == iid and not o.reduce_only]
    return _iso(fills[-1].submitted_at) if fills else None


def account_and_positions(
    conn, *, run_id: str, instruments: Mapping[int, InstrumentInfo] | None,
) -> tuple[dict | None, list[dict]]:
    text = db.load_sim_account(conn, run_id)
    if text is None:
        return None, []
    blob = json.loads(text)
    cash = _dec(blob["cash"])
    start_equity = _dec(blob.get("start_equity", blob["cash"]))
    marks = {int(k): _dec(v) for k, v in blob.get("marks", {}).items()}
    stops = {int(k): _dec(v) for k, v in blob.get("stops", {}).items()}
    local = db.get_positions_local(conn, run_id)
    fills = db.list_orders(conn, run_id, status="filled")

    positions: list[dict] = []
    unreal = _ZERO
    gross = _ZERO
    net_by_cluster: dict[str, Decimal] = {}
    all_known = instruments is not None
    for key, p in blob.get("positions", {}).items():
        iid = int(key)
        size, entry, funding = _dec(p["size"]), _dec(p["entry"]), _dec(p["funding"])
        if size == 0:
            continue
        mark = marks.get(iid, entry)
        pnl = size * (mark - entry)
        unreal += pnl
        notional = abs(size) * mark
        gross += notional
        liq = liq_price(size, entry)
        liq_distance = abs(mark - liq) / mark if mark else _ZERO
        move = (mark - entry) / entry if entry else _ZERO
        adverse = max(_ZERO, -move if size > 0 else move)
        base = abs(size) * entry
        funding_paid = funding / base if base else _ZERO
        inst = instruments.get(iid) if instruments else None
        if inst is None:
            all_known = False
        else:
            cl = cluster_of(inst.category)
            net_by_cluster[cl] = net_by_cluster.get(cl, _ZERO) + (notional if size > 0 else -notional)
        row = local.get(iid)
        positions.append({
            "instrument_id": iid,
            "name": _name(iid, instruments),
            "state": str(row.state) if row is not None else str(State.OPEN),
            "side": "LONG" if size > 0 else "SHORT",
            "size": _s(size),
            "entry_price": _s(entry),
            "mark": _s(mark),
            "pnl": _s(pnl),
            "liq_price": _s(liq),
            "liq_distance": _f(liq_distance),
            "adverse_move": _f(adverse),
            "funding_paid": _f(funding_paid),
            "stop_trigger": _s(stops[iid]) if iid in stops else None,
            "opened_at": _opened_at(fills, iid),
        })

    equity = cash + unreal
    cluster_net: float | None = None
    if all_known:
        worst = max((abs(v) for v in net_by_cluster.values()), default=_ZERO)
        cluster_net = _f(worst / equity) if equity else 0.0
    account = {
        "equity": _s(equity),
        "start_equity": _s(start_equity),
        "pnl_since_start": _s(equity - start_equity),
        "unrealized": _s(unreal),
        "gross_exposure": _f(gross / equity) if equity else 0.0,
        "gross_limit": _f(EXPOSURE.gross),
        "cluster_net": cluster_net,
        "cluster_limit": _f(EXPOSURE.cluster_net),
        "leverage": LIMITS.max_leverage,
        "kill_switch": "unarmed",   # Phase 2a: thresholds are None (risk/kill_switch.py)
    }
    return account, positions


_HALT_STATES = (State.HALTED, State.LIQUIDATED)


def _halted(conn, run_id: str) -> list[int]:
    local = db.get_positions_local(conn, run_id)
    return sorted(iid for iid, row in local.items() if row.state in _HALT_STATES)


def guards(conn, *, run_id: str, account: dict | None, positions: list[dict]) -> dict:
    margin = "ok"
    for ts, level, kind, _iid, _detail in reversed(db.list_alerts(conn, run_id)):
        if kind == "margin_ratio":
            margin = level
            break
    liquidation = "ok"
    if any(p["liq_distance"] < _f(LIMITS.min_liq_distance) for p in positions):
        liquidation = "breach"
    exposure = "ok"
    if account is not None:
        if account["gross_exposure"] > _f(EXPOSURE.gross):
            exposure = "breach"
        cn = account["cluster_net"]
        if cn is not None and cn > _f(EXPOSURE.cluster_net):
            exposure = "breach"
    recovery = db.list_recovery(conn, run_id)
    reconciliation = None
    if recovery:
        ts, findings = recovery[-1]
        reconciliation = {"findings": len(findings), "at": _iso(ts)}
    return {
        "margin": margin,
        "liquidation": liquidation,
        "exposure": exposure,
        "halted": _halted(conn, run_id),
        "reconciliation": reconciliation,
    }


def recent_decisions(conn, *, run_id: str, instrument_ids, limit: int = 50) -> list[dict]:
    rows = []
    for iid in instrument_ids:
        rows.extend(db.list_decisions(conn, run_id, iid))
    rows.sort(key=lambda r: r.ts, reverse=True)
    return [
        {
            "ts": _iso(r.ts),
            "instrument_id": r.instrument_id,
            "note": r.note,
            "state_before": str(r.state_before),
            "target": _s(r.target) if r.target is not None else None,
            "verdicts": dict(r.verdicts),
            "client_order_id": r.client_order_id,
        }
        for r in rows[:limit]
    ]


def recent_alerts(conn, *, run_id: str, limit: int = 50) -> list[dict]:
    rows = list(reversed(db.list_alerts(conn, run_id)))[:limit]
    return [
        {"ts": _iso(ts), "level": level, "kind": kind, "instrument_id": iid, "detail": detail}
        for ts, level, kind, iid, detail in rows
    ]


def is_clean(conn, *, run_id: str) -> bool:
    if _halted(conn, run_id):
        return False
    return not any(level == "CRITICAL" for _ts, level, _k, _i, _d in db.list_alerts(conn, run_id))
