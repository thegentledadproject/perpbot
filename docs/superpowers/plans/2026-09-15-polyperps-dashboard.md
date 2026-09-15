# polyperps Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A read-only web page served from the EC2 box on port 80 that shows the paper run's account, positions with guard distances, decisions, alerts, feed health, live locks and road-to-live counters, refreshed every 10 s from the trading SQLite DB.

**Architecture:** `polyperps/dashboard/state.py` is a pure function `build_state(...)` that turns DB rows into one JSON-able dict (all logic and all unit tests live here). `polyperps/dashboard/server.py` is a stdlib `ThreadingHTTPServer` with two routes (`/` → `static/index.html`, `/api/state` → JSON) that opens a fresh **read-only** SQLite connection per request. `scripts/run_dashboard.py` wires env → server and fetches instrument names/categories best-effort in a background thread. A third systemd unit runs it as the `polyperps` user with `CAP_NET_BIND_SERVICE`.

**Tech Stack:** Python ≥ 3.11 stdlib only (`http.server`, `sqlite3`, `json`, `threading`, `importlib.resources`); vanilla HTML/JS page; pytest. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-09-15-polyperps-dashboard-design.md`

## Global Constraints

- No new runtime dependencies (`pyproject.toml` deps stay `polymarket-client==0.10.0`, `keyring>=25`, `httpx>=0.27,<1`).
- The dashboard never writes the trading DB: every request connection is opened with `?mode=ro` (URI form).
- Nothing in `deploy/` may contain `--executor live` or the string `POLYMARKET_LIVE_TRADING` (enforced by `tests/test_deploy_files.py`).
- Deploy files are LF-only, shell scripts start with `#!/usr/bin/env bash` and carry `set -euo pipefail`.
- Timestamps in the JSON are ISO-8601 UTC strings; Decimals are serialized as strings; ratios/percentages as floats in [0, 1].
- Run tests with `.venv/Scripts/python -m pytest` (Windows dev box) — the repo's README convention.
- Commit after every task with the attribution line `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.

---

## File map

| File | Responsibility |
|---|---|
| `polyperps/dashboard/__init__.py` | empty package marker |
| `polyperps/dashboard/state.py` | `build_state(...)` and its helpers — pure, deterministic under `now` |
| `polyperps/dashboard/server.py` | `DashboardServer`: routing, read-only DB connection per request, error mapping |
| `polyperps/dashboard/static/index.html` | the page (approved ops-console look), polls `/api/state` |
| `scripts/run_dashboard.py` | env → settings, instrument cache thread, serve, SIGTERM |
| `deploy/polyperps-dashboard.service` | third unit, port 80 via ambient capability |
| `deploy/env.example`, `bootstrap.sh`, `update.sh`, `deploy.ps1` | know about the third unit |
| `tests/test_dashboard_state.py` | numbers the page shows, pinned |
| `tests/test_dashboard_server.py` | routes, read-only, 503/404 |
| `tests/test_deploy_files.py` | extended for the new unit |
| `pyproject.toml` | package-data for `dashboard/static/*.html` |
| `README.md` | "Dashboard" subsection under Deploy |

---

### Task 1: `state.py` — account and positions from `sim_account` + `positions_local`

**Files:**
- Create: `polyperps/dashboard/__init__.py` (empty)
- Create: `polyperps/dashboard/state.py`
- Test: `tests/test_dashboard_state.py`

**Interfaces:**
- Consumes: `polyperps.storage.db.load_sim_account(conn, run_id) -> str | None`, `db.get_positions_local(conn, run_id) -> dict[int, PositionLocalRow]` (fields `state: State`, `size`, `entry_price`, `stop_trigger`, `cumulative_funding`), `db.list_orders(conn, run_id, *, status=None) -> list[OrderRow]` (fields `instrument_id`, `reduce_only`, `submitted_at: datetime`, `status`), `polyperps.risk.liquidation_guard.LIMITS` (`max_leverage=3`, `maintenance_rate=Decimal("0.02")`, `min_liq_distance=Decimal("0.25")`), `polyperps.risk.portfolio_exposure.EXPOSURE` (`gross=1.0`, `cluster_net=0.6`) and `cluster_of(category) -> str`.
- Produces: `account_and_positions(conn, *, run_id, instruments) -> tuple[dict | None, list[dict]]` and the module-level helpers `_dec(x) -> Decimal`, `_s(d: Decimal) -> str`, `_f(d: Decimal) -> float`, `_iso(dt) -> str`, `liq_price(size, entry) -> Decimal`. Task 2 and 3 add more functions to this same module and Task 3 adds `build_state`.

- [ ] **Step 1: Write the failing test**

`tests/test_dashboard_state.py`:

```python
"""Numbers the dashboard shows, pinned against hand-computed values."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from polyperps.dashboard import state as st
from polyperps.exchange.types import Instrument
from polyperps.execution.sim_executor import SimExecutor
from polyperps.execution.types import OrderRow, PositionLocalRow, State
from polyperps.storage.db import (
    connect,
    get_positions_local,
    save_sim_account,
    upsert_order,
    upsert_position_local,
)

RUN = "paper-test"
T0 = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)

BLOB = {
    "cash": "10000",
    "start_equity": "10000",
    "n": 2,
    "positions": {
        "6": {"size": "0.001", "entry": "100000", "funding": "0.3"},
        "7": {"size": "-0.1", "entry": "4000", "funding": "0.5"},
    },
    "stops": {"6": "85000", "7": "4600"},
    "marks": {"6": "101000", "7": "4040"},
}


def instrument(iid: int, symbol: str, category: str = "crypto") -> Instrument:
    return Instrument(
        instrument_id=iid, symbol=symbol, category=category, funding_interval="1h",
        max_leverage=20, price_decimals=2, quantity_decimals=4,
        min_notional=Decimal("1"), isolated_only=True,
    )


INSTRUMENTS = {6: instrument(6, "BTC"), 7: instrument(7, "ETH")}


def seeded_conn():
    conn = connect(":memory:")
    save_sim_account(conn, RUN, json.dumps(BLOB))
    upsert_position_local(conn, PositionLocalRow(
        run_id=RUN, instrument_id=6, state=State.OPEN, size=Decimal("0.001"),
        entry_price=Decimal("100000"), stop_trigger=Decimal("85000"), stop_order_id=None,
        cumulative_funding=Decimal("0.3"), updated_at=T0))
    upsert_position_local(conn, PositionLocalRow(
        run_id=RUN, instrument_id=7, state=State.OPEN, size=Decimal("-0.1"),
        entry_price=Decimal("4000"), stop_trigger=Decimal("4600"), stop_order_id=None,
        cumulative_funding=Decimal("0.5"), updated_at=T0))
    upsert_order(conn, OrderRow(
        client_order_id="6-open", run_id=RUN, instrument_id=6, side="BUY",
        quantity=Decimal("0.001"), reduce_only=False, status="filled", exchange_order_id=None,
        filled_quantity=Decimal("0.001"), avg_price=Decimal("100000"),
        submitted_at=T0 - timedelta(hours=2), updated_at=T0 - timedelta(hours=2), reason="strategy"))
    upsert_order(conn, OrderRow(
        client_order_id="7-open", run_id=RUN, instrument_id=7, side="SELL",
        quantity=Decimal("0.1"), reduce_only=False, status="filled", exchange_order_id=None,
        filled_quantity=Decimal("0.1"), avg_price=Decimal("4000"),
        submitted_at=T0 - timedelta(hours=1), updated_at=T0 - timedelta(hours=1), reason="strategy"))
    return conn


def test_account_numbers():
    conn = seeded_conn()
    account, positions = st.account_and_positions(conn, run_id=RUN, instruments=INSTRUMENTS)
    # equity = cash + 0.001*(101000-100000) + (-0.1)*(4040-4000) = 10000 + 1 - 4
    assert account["equity"] == "9997"
    assert account["start_equity"] == "10000"
    assert account["pnl_since_start"] == "-3"
    assert account["unrealized"] == "-3"
    # gross = 0.001*101000 + 0.1*4040 = 101 + 404 = 505
    assert account["gross_exposure"] == pytest.approx(505 / 9997)
    assert account["gross_limit"] == 1.0
    # both crypto: net = +101 - 404 = -303
    assert account["cluster_net"] == pytest.approx(303 / 9997)
    assert account["cluster_limit"] == 0.6
    assert account["leverage"] == 3
    assert account["kill_switch"] == "unarmed"
    assert len(positions) == 2


def test_position_numbers_long():
    conn = seeded_conn()
    _, positions = st.account_and_positions(conn, run_id=RUN, instruments=INSTRUMENTS)
    p = {x["instrument_id"]: x for x in positions}[6]
    assert p["name"] == "BTC"
    assert p["side"] == "LONG"
    assert p["state"] == "OPEN"
    assert p["size"] == "0.001"
    assert p["entry_price"] == "100000"
    assert p["mark"] == "101000"
    assert p["pnl"] == "1"
    # liq = 100000 * (1 - 1/3 + 0.02) = 68666.67
    assert p["liq_price"] == "68666.67"
    assert p["liq_distance"] == pytest.approx((101000 - 68666.67) / 101000)
    assert p["adverse_move"] == 0.0            # moved in our favour
    assert p["funding_paid"] == pytest.approx(0.3 / 100)
    assert p["stop_trigger"] == "85000"
    assert p["opened_at"] == (T0 - timedelta(hours=2)).isoformat()


def test_position_numbers_short():
    conn = seeded_conn()
    _, positions = st.account_and_positions(conn, run_id=RUN, instruments=INSTRUMENTS)
    p = {x["instrument_id"]: x for x in positions}[7]
    assert p["side"] == "SHORT"
    assert p["pnl"] == "-4"
    # liq = 4000 * (1 + 1/3 - 0.02) = 5253.33
    assert p["liq_price"] == "5253.33"
    assert p["liq_distance"] == pytest.approx((5253.33 - 4040) / 4040)
    assert p["adverse_move"] == pytest.approx(0.01)   # short, price rose 1 %
    assert p["funding_paid"] == pytest.approx(0.5 / 400)


async def test_liq_price_matches_sim_executor():
    ex = SimExecutor.from_json(RUN, json.dumps(BLOB), taker_fee_rate=Decimal("0"))
    snap = await ex.snapshot()
    by_id = {v.instrument_id: v for v in snap.positions}
    assert st.liq_price(Decimal("0.001"), Decimal("100000")) == by_id[6].liquidation_price
    assert st.liq_price(Decimal("-0.1"), Decimal("4000")) == by_id[7].liquidation_price
    assert Decimal(st.account_and_positions(seeded_conn(), run_id=RUN, instruments=INSTRUMENTS)[0]["equity"]) == snap.equity


def test_unknown_instruments_fall_back():
    conn = seeded_conn()
    account, positions = st.account_and_positions(conn, run_id=RUN, instruments=None)
    assert account["cluster_net"] is None
    assert {p["name"] for p in positions} == {"inst 6", "inst 7"}


def test_no_sim_account_yet():
    conn = connect(":memory:")
    assert st.account_and_positions(conn, run_id=RUN, instruments=INSTRUMENTS) == (None, [])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_dashboard_state.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.dashboard'`

- [ ] **Step 3: Write the implementation**

`polyperps/dashboard/__init__.py`: empty file.

`polyperps/dashboard/state.py`:

```python
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
```

Note on `_s`: `Decimal("0.001") * Decimal("1000")` is `Decimal("1.000")`; `normalize()` turns it into `"1"`. Non-integral values keep their digits (`"68666.67"`). If a test shows a stray exponent (e.g. `1E+1`), replace `_s` with `format(d.quantize(Decimal(1)) if d == d.to_integral() else d, "f")`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_dashboard_state.py -v`
Expected: 6 passed. If `PositionLocalRow`/`OrderRow` constructor kwargs differ from those used in the test, read `polyperps/execution/types.py:134-180` and fix the **test** to match the real dataclass — do not change the dataclasses.

- [ ] **Step 5: Commit**

```bash
git add polyperps/dashboard/__init__.py polyperps/dashboard/state.py tests/test_dashboard_state.py
git commit -m "feat(dashboard): account and position state from sim_account

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 2: `state.py` — guards, decisions, alerts, reconciliation, clean flag

**Files:**
- Modify: `polyperps/dashboard/state.py`
- Test: `tests/test_dashboard_state.py`

**Interfaces:**
- Consumes: `db.list_alerts(conn, run_id) -> list[tuple[datetime, level, kind, instrument_id, detail_dict]]` (ordered by ts asc), `db.list_recovery(conn, run_id) -> list[tuple[datetime, findings_dict]]`, `db.list_decisions(conn, run_id, instrument_id) -> list[DecisionRow]` (fields `ts`, `instrument_id`, `note`, `state_before: State`, `target: Decimal | None`, `verdicts: dict`, `client_order_id`), `db.get_positions_local`, `State.HALTED`, `State.LIQUIDATED`.
- Produces: `guards(conn, *, run_id, account, positions) -> dict`, `recent_decisions(conn, *, run_id, instrument_ids, limit=50) -> list[dict]`, `recent_alerts(conn, *, run_id, limit=50) -> list[dict]`, `is_clean(conn, *, run_id) -> bool`.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_dashboard_state.py`)

```python
from polyperps.execution.types import DecisionRow
from polyperps.storage.db import insert_alert, insert_decision, insert_recovery


def test_guards_ok_when_quiet():
    conn = seeded_conn()
    account, positions = st.account_and_positions(conn, run_id=RUN, instruments=INSTRUMENTS)
    g = st.guards(conn, run_id=RUN, account=account, positions=positions)
    assert g == {
        "margin": "ok", "liquidation": "ok", "exposure": "ok",
        "halted": [], "reconciliation": None,
    }


def test_guards_margin_follows_latest_alert():
    conn = seeded_conn()
    insert_alert(conn, run_id=RUN, level="WARN", kind="margin_ratio", instrument_id=6,
                 detail_json="{}", ts=T0 - timedelta(minutes=5))
    insert_alert(conn, run_id=RUN, level="INFO", kind="margin_ratio", instrument_id=6,
                 detail_json="{}", ts=T0 - timedelta(minutes=1))
    insert_alert(conn, run_id=RUN, level="CRITICAL", kind="pnl_drawdown", instrument_id=None,
                 detail_json="{}", ts=T0)
    account, positions = st.account_and_positions(conn, run_id=RUN, instruments=INSTRUMENTS)
    g = st.guards(conn, run_id=RUN, account=account, positions=positions)
    assert g["margin"] == "INFO"          # latest margin_ratio row, not the CRITICAL pnl one


def test_guards_halted_and_reconciliation():
    conn = seeded_conn()
    upsert_position_local(conn, PositionLocalRow(
        run_id=RUN, instrument_id=7, state=State.HALTED, size=Decimal("0"),
        entry_price=None, stop_trigger=None, stop_order_id=None,
        cumulative_funding=Decimal("0"), updated_at=T0))
    insert_recovery(conn, run_id=RUN, ts=T0, findings_json=json.dumps({"stop_missing": [6]}))
    account, positions = st.account_and_positions(conn, run_id=RUN, instruments=INSTRUMENTS)
    g = st.guards(conn, run_id=RUN, account=account, positions=positions)
    assert g["halted"] == [7]
    assert g["reconciliation"] == {"findings": 1, "at": T0.isoformat()}


def test_guards_breach_flags():
    conn = seeded_conn()
    account, positions = st.account_and_positions(conn, run_id=RUN, instruments=INSTRUMENTS)
    positions[0]["liq_distance"] = 0.20
    account["gross_exposure"] = 1.2
    g = st.guards(conn, run_id=RUN, account=account, positions=positions)
    assert g["liquidation"] == "breach"
    assert g["exposure"] == "breach"


def test_recent_decisions_newest_first_and_capped():
    conn = connect(":memory:")
    for seq in range(60):
        insert_decision(conn, DecisionRow(
            run_id=RUN, instrument_id=6, seq=seq, ts=T0 + timedelta(minutes=seq),
            state_before=State.FLAT, target=None, verdicts={"vet_entry": "allow"},
            intent=None, client_order_id=None, note="skip:warmup" if seq < 48 else "hold"))
    insert_decision(conn, DecisionRow(
        run_id=RUN, instrument_id=7, seq=0, ts=T0 + timedelta(minutes=100),
        state_before=State.OPEN, target=Decimal("1"), verdicts={}, intent=None,
        client_order_id="7-x", note="flip"))
    rows = st.recent_decisions(conn, run_id=RUN, instrument_ids=(6, 7))
    assert len(rows) == 50
    assert rows[0] == {
        "ts": (T0 + timedelta(minutes=100)).isoformat(), "instrument_id": 7, "note": "flip",
        "state_before": "OPEN", "target": "1", "verdicts": {}, "client_order_id": "7-x",
    }
    assert rows[1]["note"] == "hold" and rows[1]["instrument_id"] == 6


def test_recent_alerts_newest_first():
    conn = connect(":memory:")
    insert_alert(conn, run_id=RUN, level="WARN", kind="margin_ratio", instrument_id=6,
                 detail_json=json.dumps({"ratio": 0.33}), ts=T0)
    insert_alert(conn, run_id=RUN, level="INFO", kind="stop_placed", instrument_id=6,
                 detail_json="{}", ts=T0 + timedelta(minutes=1))
    rows = st.recent_alerts(conn, run_id=RUN)
    assert [r["kind"] for r in rows] == ["stop_placed", "margin_ratio"]
    assert rows[1] == {"ts": T0.isoformat(), "level": "WARN", "kind": "margin_ratio",
                       "instrument_id": 6, "detail": {"ratio": 0.33}}


def test_is_clean():
    conn = seeded_conn()
    assert st.is_clean(conn, run_id=RUN) is True
    insert_alert(conn, run_id=RUN, level="CRITICAL", kind="kill_switch", instrument_id=None,
                 detail_json="{}", ts=T0)
    assert st.is_clean(conn, run_id=RUN) is False


def test_is_clean_false_when_halted():
    conn = seeded_conn()
    upsert_position_local(conn, PositionLocalRow(
        run_id=RUN, instrument_id=6, state=State.LIQUIDATED, size=Decimal("0"),
        entry_price=None, stop_trigger=None, stop_order_id=None,
        cumulative_funding=Decimal("0"), updated_at=T0))
    assert st.is_clean(conn, run_id=RUN) is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_dashboard_state.py -v -k "guards or recent or clean"`
Expected: FAIL with `AttributeError: module 'polyperps.dashboard.state' has no attribute 'guards'`

- [ ] **Step 3: Write the implementation** (append to `polyperps/dashboard/state.py`)

```python
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
```

`str(State.OPEN)` is `"OPEN"` because `State` is a `StrEnum` whose values equal their names.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_dashboard_state.py -v`
Expected: 14 passed.

- [ ] **Step 5: Commit**

```bash
git add polyperps/dashboard/state.py tests/test_dashboard_state.py
git commit -m "feat(dashboard): guards, decision/alert trails, clean flag

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 3: `state.py` — run, feed, road, locks, and `build_state`

**Files:**
- Modify: `polyperps/dashboard/state.py`
- Test: `tests/test_dashboard_state.py`

**Interfaces:**
- Consumes: `polyperps.storage.gaps.find_gaps(conn, instrument_id, *, table, max_gap, start, end) -> list[tuple[datetime, datetime]]` (an empty table in the window returns one gap spanning the whole window), `db.insert_tick(conn, t: Tick)` (test only; `Tick` from `polyperps.exchange.types` — read its fields there), `polyperps.signal.sufficiency.check_dataset(conn, instrument_id, source_type, *, now) -> SufficiencyReport(met, days: Decimal, funding_periods: int, ...)`, `polyperps.signal.sufficiency.BAR.min_days` / `.min_funding_periods`, `polyperps.signal.sufficiency.NATIVE_SOURCES`, `polyperps.gates.LIVE_ENV_VAR`, `polyperps.signal.base.SIGNAL_VALIDATED`.
- Produces: `build_state(conn, *, run_id: str, instrument_ids: Sequence[int], instruments: Mapping[int, InstrumentInfo] | None, hypothesis: str, host: str, now: datetime, env: Mapping[str, str] | None = None, signal_validated: bool | None = None) -> dict` — the complete JSON dict from spec §4. Task 4's server calls exactly this.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_dashboard_state.py`)

```python
from polyperps.exchange.types import SourceType, Tick
from polyperps.storage.db import insert_tick


def tick(iid: int, ts: datetime, mark: str = "100") -> Tick:
    return Tick(
        instrument_id=iid, source_type=SourceType.POLYMARKET_WS, exchange_ts=ts, received_ts=ts,
        sequence=None, mark_price=Decimal(mark), index_price=Decimal(mark), last_price=Decimal(mark),
        funding_rate=Decimal("0.0000125"), next_funding=ts + timedelta(hours=1),
    )


def test_build_state_empty_run():
    conn = connect(":memory:")
    s = st.build_state(conn, run_id=RUN, instrument_ids=(6, 7), instruments=INSTRUMENTS,
                       hypothesis="h1", host="box", now=T0, env={}, signal_validated=False)
    assert s["generated_at"] == T0.isoformat()
    assert s["run"] == {"run_id": RUN, "executor": "sim", "hypothesis": "h1", "host": "box",
                        "started_at": None, "uptime_s": 0}
    assert s["account"] is None and s["positions"] == []
    assert s["guards"]["margin"] == "ok"
    assert s["decisions"] == [] and s["alerts"] == []
    assert s["locks"] == {"auto_mode": {"6": False, "7": False}, "live_env": False,
                          "signal_validated": False}
    assert s["road"]["clean"] is True
    assert s["road"]["paper_days"] == 0.0 and s["road"]["paper_days_target"] == 14
    assert s["road"]["native_days_required"] == 60
    assert s["road"]["funding_periods_required"] == 1000
    # no ticks at all: the whole 48 h window is one gap per instrument
    assert s["feed"]["tick_gaps_48h"] == 2
    assert s["feed"]["instruments"] == [
        {"instrument_id": 6, "last_tick_age_s": None, "last_funding_ts": None},
        {"instrument_id": 7, "last_tick_age_s": None, "last_funding_ts": None},
    ]
    json.dumps(s)   # must be serializable as-is


def test_build_state_run_and_feed():
    conn = seeded_conn()
    insert_alert(conn, run_id=RUN, level="INFO", kind="stop_placed", instrument_id=6,
                 detail_json="{}", ts=T0 - timedelta(hours=3))
    # continuous ticks for 6 every 10 s over the last 2 min, then one 90 s hole, then more
    t = T0 - timedelta(minutes=5)
    while t <= T0 - timedelta(minutes=3):
        insert_tick(conn, tick(6, t)); t += timedelta(seconds=10)
    t = T0 - timedelta(seconds=90)
    while t <= T0 - timedelta(seconds=2):
        insert_tick(conn, tick(6, t)); t += timedelta(seconds=10)
    s = st.build_state(conn, run_id=RUN, instrument_ids=(6,), instruments=INSTRUMENTS,
                       hypothesis="h1", host="box", now=T0,
                       env={"POLYMARKET_LIVE_TRADING": "true"}, signal_validated=True)
    assert s["run"]["started_at"] == (T0 - timedelta(hours=3)).isoformat()
    assert s["run"]["uptime_s"] == 3 * 3600
    assert s["road"]["paper_days"] == pytest.approx(3 / 24)
    assert s["locks"]["live_env"] is True and s["locks"]["signal_validated"] is True
    feed = s["feed"]
    assert feed["instruments"][0]["last_tick_age_s"] == pytest.approx(10.0)   # last tick at T0-10s
    # the 48 h window starts empty (one leading gap) and has the 90 s hole: >= 2 gaps
    assert feed["tick_gaps_48h"] >= 2
    assert feed["funding_gaps_48h"] >= 1
    assert feed["rejections_48h"] == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_dashboard_state.py -v -k build_state`
Expected: FAIL with `AttributeError: ... no attribute 'build_state'`

- [ ] **Step 3: Write the implementation** (append to `polyperps/dashboard/state.py`; add the imports at the top of the file)

Add to the imports:

```python
import os
from datetime import timedelta
from typing import Sequence

from polyperps.gates import LIVE_ENV_VAR
from polyperps.signal import base as signal_base
from polyperps.signal.sufficiency import BAR, NATIVE_SOURCES, check_dataset
from polyperps.storage.gaps import find_gaps
```

Append:

```python
TICK_GAP = timedelta(seconds=30)      # same defaults as scripts/gap_report.py
FUNDING_GAP = timedelta(hours=2)
FEED_WINDOW = timedelta(hours=48)
PAPER_DAYS_TARGET = 14


def _started_at(conn, run_id: str) -> datetime | None:
    stamps: list[datetime] = []
    for sql in (
        "SELECT MIN(ts) FROM decisions WHERE run_id=?",
        "SELECT MIN(submitted_at) FROM orders WHERE run_id=?",
        "SELECT MIN(ts) FROM alerts WHERE run_id=?",
    ):
        (v,) = conn.execute(sql, (run_id,)).fetchone()
        if v:
            stamps.append(datetime.fromisoformat(v))
    return min(stamps) if stamps else None


def run_info(conn, *, run_id: str, hypothesis: str, host: str, now: datetime) -> dict:
    started = _started_at(conn, run_id)
    uptime = int((now - started).total_seconds()) if started else 0
    return {
        "run_id": run_id, "executor": "sim", "hypothesis": hypothesis, "host": host,
        "started_at": _iso(started) if started else None, "uptime_s": max(0, uptime),
    }


def feed_health(conn, *, instrument_ids: Sequence[int], now: datetime) -> dict:
    start = now - FEED_WINDOW
    per: list[dict] = []
    tick_gaps = funding_gaps = rejections = 0
    for iid in instrument_ids:
        (last_tick,) = conn.execute(
            "SELECT MAX(exchange_ts) FROM ticks WHERE instrument_id=?", (iid,)).fetchone()
        (last_funding,) = conn.execute(
            "SELECT MAX(exchange_ts) FROM funding_rates WHERE instrument_id=?", (iid,)).fetchone()
        age = (now - datetime.fromisoformat(last_tick)).total_seconds() if last_tick else None
        per.append({"instrument_id": iid, "last_tick_age_s": age, "last_funding_ts": last_funding})
        tick_gaps += len(find_gaps(conn, iid, table="ticks", max_gap=TICK_GAP, start=start, end=now))
        funding_gaps += len(find_gaps(conn, iid, table="funding_rates", max_gap=FUNDING_GAP,
                                      start=start, end=now))
        (n,) = conn.execute(
            "SELECT COUNT(*) FROM rejections WHERE instrument_id=? AND at >= ?",
            (iid, _iso(start))).fetchone()
        rejections += n
    return {"instruments": per, "tick_gaps_48h": tick_gaps, "funding_gaps_48h": funding_gaps,
            "rejections_48h": rejections}


def road_to_live(conn, *, run_id: str, instrument_ids: Sequence[int], uptime_s: int,
                 now: datetime) -> dict:
    days = Decimal(0)
    periods = 0
    if instrument_ids:
        for source in NATIVE_SOURCES:      # backfill may have used either native source
            rep = check_dataset(conn, instrument_ids[0], source, now=now)
            days = max(days, rep.days)
            periods = max(periods, rep.funding_periods)
    return {
        "native_days": _f(days), "native_days_required": BAR.min_days,
        "funding_periods": periods, "funding_periods_required": BAR.min_funding_periods,
        "paper_days": uptime_s / 86400, "paper_days_target": PAPER_DAYS_TARGET,
        "clean": is_clean(conn, run_id=run_id),
    }


def locks(*, instrument_ids: Sequence[int], env: Mapping[str, str],
          signal_validated: bool) -> dict:
    # Phase 2a has no per-instrument mode store; gates.py defaults every
    # instrument to MANUAL_REVIEW, so AUTO is False for all of them.
    return {
        "auto_mode": {str(i): False for i in instrument_ids},
        "live_env": env.get(LIVE_ENV_VAR) == "true",
        "signal_validated": signal_validated,
    }


def build_state(
    conn, *, run_id: str, instrument_ids: Sequence[int],
    instruments: Mapping[int, InstrumentInfo] | None, hypothesis: str, host: str,
    now: datetime, env: Mapping[str, str] | None = None, signal_validated: bool | None = None,
) -> dict:
    env = os.environ if env is None else env
    validated = signal_base.SIGNAL_VALIDATED if signal_validated is None else signal_validated
    run = run_info(conn, run_id=run_id, hypothesis=hypothesis, host=host, now=now)
    account, positions = account_and_positions(conn, run_id=run_id, instruments=instruments)
    return {
        "generated_at": _iso(now),
        "run": run,
        "account": account,
        "positions": positions,
        "guards": guards(conn, run_id=run_id, account=account, positions=positions),
        "feed": feed_health(conn, instrument_ids=instrument_ids, now=now),
        "decisions": recent_decisions(conn, run_id=run_id, instrument_ids=instrument_ids),
        "alerts": recent_alerts(conn, run_id=run_id),
        "locks": locks(instrument_ids=instrument_ids, env=env, signal_validated=validated),
        "road": road_to_live(conn, run_id=run_id, instrument_ids=instrument_ids,
                             uptime_s=run["uptime_s"], now=now),
    }
```

If `check_dataset` raises on an empty `funding_rates` table, wrap that call: `try: rep = check_dataset(...) except Exception: continue` — then add a one-line comment saying why. Check `polyperps/signal/sufficiency.py:97-108` first; it may already return zeros.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_dashboard_state.py -v`
Expected: 16 passed. If `Tick`'s fields differ from the test helper, read `polyperps/exchange/types.py` and fix the test helper, not the code.

- [ ] **Step 5: Commit**

```bash
git add polyperps/dashboard/state.py tests/test_dashboard_state.py
git commit -m "feat(dashboard): build_state with run, feed health, locks and road-to-live

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 4: `server.py` — two routes, read-only connection per request

**Files:**
- Create: `polyperps/dashboard/server.py`
- Create: `polyperps/dashboard/static/index.html` (placeholder for this task; Task 5 replaces it)
- Modify: `pyproject.toml` (`[tool.setuptools.package-data]`)
- Test: `tests/test_dashboard_server.py`

**Interfaces:**
- Consumes: `build_state(...)` from Task 3 with exactly its keyword signature.
- Produces: `DashboardServer(*, bind: tuple[str, int], db_path: Path, run_id: str, instrument_ids: Sequence[int], hypothesis: str, instruments_provider: Callable[[], Mapping[int, InstrumentInfo] | None], host: str | None = None, clock: Callable[[], datetime] | None = None)` with `.port -> int`, `.serve_forever() -> None`, `.shutdown() -> None`, `.server_close() -> None`. Task 6's script uses these.

- [ ] **Step 1: Write the failing test**

`tests/test_dashboard_server.py`:

```python
"""Routes, read-only DB access, and error mapping of the dashboard server."""
from __future__ import annotations

import json
import sqlite3
import threading
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pytest

from polyperps.dashboard.server import DashboardServer
from polyperps.storage.db import connect

T0 = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    p = tmp_path / "polyperps.sqlite3"
    connect(p).close()          # creates the schema
    return p


@pytest.fixture
def server(db_path: Path):
    srv = DashboardServer(
        bind=("127.0.0.1", 0), db_path=db_path, run_id="paper-test", instrument_ids=(6, 7),
        hypothesis="h1", instruments_provider=lambda: None, host="testbox", clock=lambda: T0,
    )
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield srv
    srv.shutdown()
    srv.server_close()


def get(server: DashboardServer, path: str):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{server.port}{path}", timeout=5) as r:
            return r.status, r.headers, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.headers, e.read()


def test_index_is_html_that_polls_the_api(server):
    status, headers, body = get(server, "/")
    assert status == 200
    assert headers["Content-Type"].startswith("text/html")
    assert b"/api/state" in body


def test_state_is_json_with_top_level_keys(server):
    status, headers, body = get(server, "/api/state")
    assert status == 200
    assert headers["Content-Type"].startswith("application/json")
    assert headers["Cache-Control"] == "no-store"
    s = json.loads(body)
    assert set(s) == {"generated_at", "run", "account", "positions", "guards", "feed",
                      "decisions", "alerts", "locks", "road"}
    assert s["run"]["host"] == "testbox" and s["generated_at"] == T0.isoformat()


def test_unknown_path_is_404_json(server):
    status, _, body = get(server, "/nope")
    assert status == 404 and json.loads(body) == {"error": "not_found"}


def test_post_is_405(server):
    req = urllib.request.Request(f"http://127.0.0.1:{server.port}/api/state", data=b"x", method="POST")
    with pytest.raises(urllib.error.HTTPError) as ei:
        urllib.request.urlopen(req, timeout=5)
    assert ei.value.code == 405


def test_missing_db_is_503(server, db_path: Path):
    db_path.unlink()
    status, _, body = get(server, "/api/state")
    assert status == 503 and json.loads(body) == {"error": "db_unavailable"}


def test_connection_is_read_only(db_path: Path):
    conn = DashboardServer.open_readonly(db_path)
    with pytest.raises(sqlite3.OperationalError):
        conn.execute("INSERT INTO recovery VALUES ('x', 'y', '{}')")
    conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_dashboard_server.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.dashboard.server'`

- [ ] **Step 3: Write the implementation**

`polyperps/dashboard/static/index.html` (placeholder, replaced in Task 5):

```html
<!doctype html>
<html><head><meta charset="utf-8"><title>polyperps</title></head>
<body><pre id="out">loading /api/state ...</pre>
<script>
async function tick() {
  const r = await fetch('/api/state', {cache: 'no-store'});
  document.getElementById('out').textContent = JSON.stringify(await r.json(), null, 2);
}
tick(); setInterval(tick, 10000);
</script></body></html>
```

`polyperps/dashboard/server.py`:

```python
"""Stdlib HTTP server for the read-only dashboard.

Two routes: "/" (the page) and "/api/state" (JSON). Each state request opens
its own read-only SQLite connection, so this process can never write the
trading DB and never shares a connection between threads.
"""
from __future__ import annotations

import json
import logging
import socket
import sqlite3
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from pathlib import Path
from typing import Callable, Mapping, Sequence

from polyperps.dashboard.state import InstrumentInfo, build_state

log = logging.getLogger(__name__)


def _load_index() -> bytes:
    return (resources.files("polyperps.dashboard") / "static" / "index.html").read_bytes()


class DashboardServer:
    def __init__(
        self, *, bind: tuple[str, int], db_path: Path, run_id: str,
        instrument_ids: Sequence[int], hypothesis: str,
        instruments_provider: Callable[[], Mapping[int, InstrumentInfo] | None],
        host: str | None = None, clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._db_path = Path(db_path)
        self._run_id = run_id
        self._instrument_ids = tuple(instrument_ids)
        self._hypothesis = hypothesis
        self._instruments = instruments_provider
        self._host = host or socket.gethostname()
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._index = _load_index()
        self._httpd = ThreadingHTTPServer(bind, self._handler_class())
        self._httpd.daemon_threads = True

    @property
    def port(self) -> int:
        return self._httpd.server_address[1]

    def serve_forever(self) -> None:
        self._httpd.serve_forever()

    def shutdown(self) -> None:
        self._httpd.shutdown()

    def server_close(self) -> None:
        self._httpd.server_close()

    @staticmethod
    def open_readonly(db_path: Path) -> sqlite3.Connection:
        uri = Path(db_path).resolve().as_uri() + "?mode=ro"
        return sqlite3.connect(uri, uri=True)

    def state(self) -> dict:
        conn = self.open_readonly(self._db_path)
        try:
            return build_state(
                conn, run_id=self._run_id, instrument_ids=self._instrument_ids,
                instruments=self._instruments(), hypothesis=self._hypothesis,
                host=self._host, now=self._clock(),
            )
        finally:
            conn.close()

    def _handler_class(self):
        server = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "polyperps-dashboard"

            def log_message(self, fmt, *args):   # route stdlib access log to logging
                log.debug("%s " + fmt, self.address_string(), *args)

            def _send(self, status: HTTPStatus, body: bytes, ctype: str, *, head: bool = False) -> None:
                self.send_response(status)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                if ctype.startswith("application/json"):
                    self.send_header("Cache-Control", "no-store")
                self.end_headers()
                if not head:
                    self.wfile.write(body)

            def _json(self, status: HTTPStatus, payload: dict, *, head: bool = False) -> None:
                self._send(status, json.dumps(payload).encode(), "application/json; charset=utf-8", head=head)

            def _route(self, *, head: bool) -> None:
                path = self.path.split("?", 1)[0]
                if path == "/":
                    self._send(HTTPStatus.OK, server._index, "text/html; charset=utf-8", head=head)
                elif path == "/api/state":
                    try:
                        payload = server.state()
                    except sqlite3.OperationalError as e:
                        log.warning("dashboard: db unavailable: %s", e)
                        self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "db_unavailable"}, head=head)
                        return
                    except Exception:
                        log.exception("dashboard: state failed")
                        self._json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "internal"}, head=head)
                        return
                    self._json(HTTPStatus.OK, payload, head=head)
                else:
                    self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"}, head=head)

            def do_GET(self) -> None:
                self._route(head=False)

            def do_HEAD(self) -> None:
                self._route(head=True)

            def _reject(self) -> None:
                self._json(HTTPStatus.METHOD_NOT_ALLOWED, {"error": "method_not_allowed"})

            do_POST = do_PUT = do_DELETE = do_PATCH = _reject

        return Handler
```

`pyproject.toml` — change the package-data line to:

```toml
[tool.setuptools.package-data]
polyperps = ["signal/*.jsonl", "signal/*.json", "dashboard/static/*.html"]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_dashboard_server.py -v`
Expected: 6 passed. If `test_missing_db_is_503` fails because a `MIN(...)` query hits a missing table first, that is still `sqlite3.OperationalError` → 503, so it should pass; if the `unlink` itself fails on Windows with "file in use", the fixture's schema connection was not closed — check `connect(p).close()`.

If `test_connection_is_read_only` fails because `as_uri()` on Windows produces a path SQLite rejects, use `"file:" + Path(db_path).resolve().as_posix() + "?mode=ro"` instead and keep the test.

- [ ] **Step 5: Commit**

```bash
git add polyperps/dashboard/server.py polyperps/dashboard/static/index.html pyproject.toml tests/test_dashboard_server.py
git commit -m "feat(dashboard): stdlib server with / and /api/state, read-only DB per request

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 5: `static/index.html` — the approved ops-console page

**Files:**
- Modify: `polyperps/dashboard/static/index.html` (replace the placeholder)
- Test: `tests/test_dashboard_server.py` (one added assertion)

**Interfaces:**
- Consumes: the JSON from `/api/state` exactly as specified in spec §4 and produced by Task 3. Field names used by the page: `run.{run_id,hypothesis,host,started_at,uptime_s}`, `account.{equity,pnl_since_start,unrealized,gross_exposure,gross_limit,cluster_net,cluster_limit,leverage,kill_switch}`, `positions[].{instrument_id,name,side,state,entry_price,mark,pnl,liq_distance,adverse_move,funding_paid,stop_trigger,opened_at}`, `guards.{margin,liquidation,exposure,halted,reconciliation}`, `feed.{instruments[].{instrument_id,last_tick_age_s,last_funding_ts},tick_gaps_48h,funding_gaps_48h,rejections_48h}`, `decisions[].{ts,instrument_id,note,verdicts,client_order_id}`, `alerts[].{ts,level,kind,instrument_id,detail}`, `locks.{auto_mode,live_env,signal_validated}`, `road.{native_days,native_days_required,funding_periods,funding_periods_required,paper_days,paper_days_target,clean}`.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Add the failing assertion** (append to `tests/test_dashboard_server.py`)

```python
def test_index_has_the_panels(server):
    _, _, body = get(server, "/")
    text = body.decode()
    for marker in ("id=\"positions\"", "id=\"trail\"", "id=\"guards\"", "id=\"feed\"",
                   "id=\"locks\"", "id=\"road\"", "id=\"stale\"", "setInterval"):
        assert marker in text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_dashboard_server.py::test_index_has_the_panels -v`
Expected: FAIL on `id="positions"`.

- [ ] **Step 3: Write the page**

Replace `polyperps/dashboard/static/index.html` with:

```html
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>polyperps · paper soak</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&display=swap">
<style>
  :root { --bg:#141516; --panel:#1a1c1f; --line:#2a2d31; --line2:#202327; --ink:#d4d7db; --ink2:#b6bbc2;
          --mute:#8a9098; --dim:#6c737b; --hi:#f2f3f4; --accent:#4fb3a9; --track:#24403d;
          --good:#0ca30c; --warn:#fab219; --serious:#ec835a; --crit:#d03b3b; }
  * { box-sizing: border-box; }
  body { margin:0; background:var(--bg); color:var(--ink); font:12px/1.45 'JetBrains Mono', ui-monospace, Consolas, Menlo, monospace; }
  a { color:#7fcfc6; } a:hover { color:#a8e3dc; }
  button { font:inherit; cursor:pointer; color:inherit; background:transparent; border:0; }
  .page { min-height:100vh; padding:24px; display:flex; flex-direction:column; gap:16px; }
  .hdr { display:flex; align-items:center; gap:28px; padding-bottom:14px; border-bottom:1px solid var(--line); flex-wrap:wrap; }
  .brand { font-size:16px; font-weight:600; color:var(--hi); letter-spacing:.02em; }
  .sub { color:var(--mute); font-size:11px; }
  .chips { display:flex; gap:8px; flex-wrap:wrap; }
  .chip { border:1px solid var(--line); padding:5px 10px; border-radius:3px; color:var(--ink2); }
  .chip b { color:var(--hi); font-weight:500; }
  .right { margin-left:auto; display:flex; align-items:center; gap:20px; }
  .seg { display:flex; gap:2px; background:#1d1f22; border:1px solid var(--line); border-radius:4px; padding:2px; }
  .seg button { border-radius:3px; padding:6px 12px; min-height:28px; color:var(--mute); }
  .seg button.on { background:#2e3236; color:var(--hi); }
  .lamp { width:8px; height:8px; border-radius:50%; display:inline-block; flex-shrink:0; }
  .tiles { display:grid; grid-template-columns:repeat(6, minmax(0,1fr)); gap:12px; }
  .panel { background:var(--panel); border:1px solid var(--line); border-radius:4px; padding:14px 16px; display:flex; flex-direction:column; gap:8px; min-height:0; }
  .tile .k { color:var(--mute); font-size:11px; }
  .tile .v { font-size:28px; font-weight:500; color:var(--hi); line-height:1.1; }
  .tile .n { color:var(--ink2); }
  .meter { height:4px; border-radius:2px; background:var(--track); position:relative; }
  .meter > i { display:block; height:100%; border-radius:2px; background:var(--accent); }
  .meter.m6 { height:6px; background:var(--line); }
  .meter .floor { position:absolute; top:-3px; width:2px; height:12px; background:var(--hi); }
  .main { display:grid; grid-template-columns:5fr 4fr 3fr; gap:12px; flex-grow:1; }
  .h { color:var(--mute); font-size:11px; text-transform:uppercase; letter-spacing:.08em; }
  .hint { color:var(--dim); font-size:11px; }
  .row { display:grid; grid-template-columns:1.4fr 1fr 1.2fr 1.2fr 1fr 1fr 1fr; gap:8px; padding:8px 6px; margin:0 -6px; border-radius:3px; font-variant-numeric:tabular-nums; }
  .row.head { color:var(--mute); font-size:11px; border-bottom:1px solid var(--line); padding-bottom:6px; border-radius:0; }
  .row.body { cursor:pointer; } .row.body:hover { background:#1f2226; } .row.body.on { background:#24282c; }
  .tabs { display:flex; gap:16px; border-bottom:1px solid var(--line); }
  .tabs button { padding:0 0 8px; min-height:28px; color:var(--mute); border-bottom:2px solid transparent; font-size:11px; text-transform:uppercase; letter-spacing:.08em; }
  .tabs button.on { color:var(--hi); border-bottom-color:var(--accent); }
  .ev { display:grid; grid-template-columns:52px 56px 1fr; gap:10px; padding:5px 0; border-bottom:1px solid var(--line2); font-variant-numeric:tabular-nums; }
  .kv { display:flex; justify-content:space-between; align-items:center; font-variant-numeric:tabular-nums; }
  .kv span:first-child { color:var(--ink2); display:flex; align-items:center; gap:8px; }
  .kv span:last-child { color:var(--hi); }
  .foot { display:grid; grid-template-columns:3fr 4fr; gap:12px; border-top:1px solid var(--line); padding-top:16px; }
  .col { display:flex; flex-direction:column; gap:9px; }
  #stale { display:none; background:#3a2323; color:#f2c8c8; border:1px solid var(--crit); padding:8px 12px; border-radius:3px; }
  #stale.on { display:block; }
  .good { color:var(--good) } .warn { color:var(--warn) } .serious { color:var(--serious) } .crit { color:var(--crit) } .mute { color:var(--mute) }
  @media (max-width: 1100px) { .tiles { grid-template-columns:repeat(3, minmax(0,1fr)); } .main, .foot { grid-template-columns:1fr; } }
</style>
</head>
<body>
<div class="page">
  <div id="stale"></div>

  <div class="hdr">
    <div><div class="brand">polyperps</div><div class="sub" id="subtitle">paper soak</div></div>
    <div class="chips" id="chips"></div>
    <div class="right">
      <div class="seg" id="instseg"></div>
      <div class="kv" id="feedlamp"><span><i class="lamp" style="background:var(--mute)"></i>feed —</span></div>
    </div>
  </div>

  <div class="tiles" id="tiles"></div>

  <div class="main">
    <div class="panel">
      <div class="kv"><span class="h">Positions</span><span class="hint">click a row for its guards</span></div>
      <div class="row head"><div>inst</div><div>side</div><div>entry</div><div>mark</div><div>pnl</div><div>liq dist</div><div>funding</div></div>
      <div id="positions"></div>
      <div id="detail"></div>
    </div>
    <div class="panel">
      <div class="tabs" id="tabs"></div>
      <div id="trail"></div>
      <div class="hint" id="trailfoot" style="margin-top:auto"></div>
    </div>
    <div class="col">
      <div class="panel"><div class="h">Guards</div><div id="guards" class="col"></div>
        <div class="hint">WARN is expected at 3x: the 35 % line is crossed at entry.</div></div>
      <div class="panel" style="flex-grow:1"><div class="h">Feed health · 48 h</div><div id="feed" class="col"></div></div>
    </div>
  </div>

  <div class="foot">
    <div class="col"><div class="h" id="lockshead">Live locks</div><div id="locks" class="col"></div>
      <div class="hint">All three must open for any live order. None can be opened from this page.</div></div>
    <div class="col"><div class="h">Road to live</div><div id="road" class="col"></div>
      <div class="hint">re-check monthly with scripts/sufficiency.py</div></div>
  </div>
</div>

<script>
(function () {
  'use strict';
  var S = null, inst = 'all', tab = 'decisions', selected = null, lastOk = null;
  var GOOD = 'var(--good)', WARN = 'var(--warn)', SERIOUS = 'var(--serious)', CRIT = 'var(--crit)', MUTE = 'var(--mute)';

  function esc(x) { return String(x).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  function pct(x, d) { return x == null ? '—' : (100 * x).toFixed(d == null ? 1 : d) + ' %'; }
  function num(s) { if (s == null) return '—'; var n = Number(s); return isNaN(n) ? esc(s) : n.toLocaleString('en-US', { maximumFractionDigits: 2 }); }
  function signed(s) { if (s == null) return '—'; var n = Number(s); return (n > 0 ? '+' : '') + num(s); }
  function hhmm(iso) { return iso ? iso.slice(11, 16) + 'Z' : '—'; }
  function dur(sec) { var d = Math.floor(sec / 86400), h = Math.floor((sec % 86400) / 3600); return d + ' d ' + h + ' h'; }
  function lamp(color) { return '<i class="lamp" style="background:' + color + '"></i>'; }
  function levelColor(l) { return l === 'CRITICAL' ? CRIT : l === 'WARN' ? WARN : l === 'ok' || l === 'INFO' ? GOOD : MUTE; }
  function name(id) { var p = (S.positions || []).filter(function (x) { return x.instrument_id === id; })[0]; return p ? p.name : 'inst ' + id; }
  function ids() { return (S.feed.instruments || []).map(function (f) { return f.instrument_id; }); }
  function meter(p, floorPct, cls) { return '<div class="meter ' + (cls || '') + '"><i style="width:' + Math.max(0, Math.min(100, p)) + '%"></i>' + (floorPct == null ? '' : '<span class="floor" style="left:' + floorPct + '%"></span>') + '</div>'; }

  function render() {
    if (!S) return;
    var r = S.run, a = S.account, g = S.guards, f = S.feed, L = S.locks, rd = S.road;
    document.getElementById('subtitle').textContent = 'paper soak · day ' + Math.floor(rd.paper_days) + ' of ' + rd.paper_days_target + (rd.clean ? ' · clean' : ' · NOT clean');
    document.getElementById('chips').innerHTML =
      '<div class="chip">run <b>' + esc(r.run_id) + '</b></div><div class="chip">executor <b>SIM</b></div>' +
      '<div class="chip">hypothesis <b>' + esc(r.hypothesis) + '</b></div><div class="chip">box <b>' + esc(r.host) + '</b></div>' +
      '<div class="chip">up <b>' + (r.started_at ? dur(r.uptime_s) : '—') + '</b></div>';

    var seg = [{ k: 'all', l: 'All' }].concat(ids().map(function (id) { return { k: id, l: id + ' ' + name(id) }; }));
    document.getElementById('instseg').innerHTML = seg.map(function (t) { return '<button data-k="' + t.k + '" class="' + (String(t.k) === String(inst) ? 'on' : '') + '">' + esc(t.l) + '</button>'; }).join('');

    var ages = f.instruments.map(function (x) { return x.last_tick_age_s; }).filter(function (x) { return x != null; });
    var age = ages.length ? Math.max.apply(null, ages) : null;
    var feedColor = age == null ? MUTE : age < 30 ? GOOD : age < 300 ? WARN : CRIT;
    document.getElementById('feedlamp').innerHTML = '<span>' + lamp(feedColor) + 'feed ' + (age == null ? 'no ticks' : 'live · last tick ' + Math.round(age) + ' s') + '</span>';

    function tile(k, v, n) { return '<div class="panel tile"><div class="k">' + k + '</div><div class="v">' + v + '</div><div class="n">' + n + '</div></div>'; }
    document.getElementById('tiles').innerHTML = a ? [
      tile('Paper equity', num(a.equity), '<span class="' + (Number(a.pnl_since_start) >= 0 ? 'good' : 'serious') + '">' + signed(a.pnl_since_start) + '</span> since start'),
      tile('Unrealized', signed(a.unrealized), S.positions.length + ' open position' + (S.positions.length === 1 ? '' : 's')),
      tile('Gross exposure', a.gross_exposure.toFixed(2) + 'x', meter(100 * a.gross_exposure / a.gross_limit) + '<div>limit ' + a.gross_limit.toFixed(1) + 'x</div>'),
      tile('Cluster net', a.cluster_net == null ? 'n/a' : a.cluster_net.toFixed(2) + 'x', (a.cluster_net == null ? '' : meter(100 * a.cluster_net / a.cluster_limit)) + '<div>limit ' + a.cluster_limit.toFixed(1) + 'x</div>'),
      tile('Leverage', a.leverage + 'x', 'pre-registered, fixed'),
      tile('Kill switch', '<span class="mute">' + esc(a.kill_switch) + '</span>', 'thresholds None until 2b')
    ].join('') : [tile('Paper equity', '—', 'warming up'), tile('Unrealized', '—', ''), tile('Gross exposure', '—', ''), tile('Cluster net', '—', ''), tile('Leverage', '3x', 'pre-registered, fixed'), tile('Kill switch', '<span class="mute">unarmed</span>', 'thresholds None until 2b')].join('');

    var pos = S.positions.filter(function (p) { return inst === 'all' || String(p.instrument_id) === String(inst); });
    document.getElementById('positions').innerHTML = pos.length ? pos.map(function (p) {
      return '<div class="row body ' + (selected === p.instrument_id ? 'on' : '') + '" data-id="' + p.instrument_id + '">' +
        '<div style="color:var(--hi)">' + p.instrument_id + ' ' + esc(p.name) + '</div>' +
        '<div style="display:flex;align-items:center;gap:6px">' + lamp(p.side === 'LONG' ? GOOD : SERIOUS) + p.side + '</div>' +
        '<div>' + num(p.entry_price) + '</div><div>' + num(p.mark) + '</div>' +
        '<div class="' + (Number(p.pnl) >= 0 ? 'good' : 'serious') + '">' + signed(p.pnl) + '</div>' +
        '<div>' + pct(p.liq_distance, 0) + '</div><div>' + pct(p.funding_paid) + '</div></div>';
    }).join('') : '<div class="hint" style="padding:8px 0">no open positions</div>';

    var sel = pos.filter(function (p) { return p.instrument_id === selected; })[0];
    if (sel) {
      var adv = sel.adverse_move;
      function m(label, value, p, floor, fill, note) { return '<div class="col" style="gap:5px"><div class="kv"><span>' + label + '</span><span>' + value + '</span></div>' + meter(p, floor, 'm6').replace('<i style', '<i style="background:' + fill + ';" data-x') + '<div class="hint">' + note + '</div></div>'; }
      document.getElementById('detail').innerHTML = '<div class="col" style="margin-top:6px;border-top:1px solid var(--line);padding-top:12px;gap:12px">' +
        '<div class="kv"><span style="color:var(--hi)">' + sel.instrument_id + ' ' + esc(sel.name) + ' ' + sel.side + ' · guard distances</span><span class="hint">' + (sel.opened_at ? 'opened ' + esc(sel.opened_at.slice(0, 16)) + 'Z' : '') + '</span></div>' +
        m('liquidation distance', pct(sel.liq_distance, 0), 100 * sel.liq_distance, 25, sel.liq_distance > 0.30 ? 'var(--accent)' : sel.liq_distance > 0.25 ? WARN : CRIT, 'exit when it falls to the 25 % floor (marker)') +
        m('adverse move', pct(adv), 100 * adv / 0.15, 100 * 8 / 15, adv < 0.05 ? 'var(--accent)' : adv < 0.08 ? WARN : CRIT, 'check_open flattens near 8 % (marker); exchange stop at 15 %') +
        m('funding cost paid', pct(sel.funding_paid), 100 * sel.funding_paid / 0.02, 100, sel.funding_paid < 0.015 ? 'var(--accent)' : WARN, 'funding-cost exit at 2 % (marker)') + '</div>';
    } else {
      document.getElementById('detail').innerHTML = '<div class="hint" style="margin-top:auto">exit rules: liquidation-distance floor 25 % · exchange-side stop 15 % · funding-cost exit 2 % · check_open flattens near 8 % adverse</div>';
    }

    document.getElementById('tabs').innerHTML = ['decisions', 'alerts'].map(function (t) { return '<button data-t="' + t + '" class="' + (t === tab ? 'on' : '') + '">' + t + '</button>'; }).join('');
    var evs = (tab === 'decisions' ? S.decisions : S.alerts).filter(function (e) { return inst === 'all' || e.instrument_id == null || String(e.instrument_id) === String(inst); }).slice(0, 12);
    document.getElementById('trail').innerHTML = evs.length ? evs.map(function (e) {
      var head, note, color;
      if (tab === 'decisions') {
        head = e.note; color = /^enter|strategy|flip/.test(e.note) ? GOOD : /liq|funding|kill|reject|halt/.test(e.note) ? SERIOUS : MUTE;
        note = Object.keys(e.verdicts || {}).map(function (k) { return k + ' ' + e.verdicts[k]; }).join(' · ') + (e.client_order_id ? ' · ' + esc(e.client_order_id) : '');
      } else {
        head = e.level + ' ' + e.kind; color = levelColor(e.level);
        note = Object.keys(e.detail || {}).slice(0, 4).map(function (k) { return k + ' ' + esc(JSON.stringify(e.detail[k])); }).join(' · ');
      }
      return '<div class="ev"><div class="mute">' + hhmm(e.ts) + '</div><div style="color:var(--ink2)">' + (e.instrument_id == null ? '—' : e.instrument_id) + '</div>' +
        '<div><div style="display:flex;align-items:center;gap:6px">' + lamp(color) + '<span style="color:var(--hi)">' + esc(head) + '</span></div><div class="hint">' + note + '</div></div></div>';
    }).join('') : '<div class="hint" style="padding:8px 0">nothing yet</div>';
    document.getElementById('trailfoot').textContent = tab === 'decisions' ? 'newest first · one row per router tick' : 'level changes only · CRITICAL also goes to Telegram when configured';

    document.getElementById('guards').innerHTML = [
      ['margin level', g.margin, levelColor(g.margin)],
      ['liquidation guard', g.liquidation, g.liquidation === 'ok' ? GOOD : CRIT],
      ['exposure limits', g.exposure, g.exposure === 'ok' ? GOOD : CRIT],
      ['halted instruments', g.halted.length ? g.halted.join(', ') : 'none', g.halted.length ? CRIT : GOOD],
      ['reconciliation', g.reconciliation ? (g.reconciliation.findings ? g.reconciliation.findings + ' findings' : 'in sync') : 'no run yet', g.reconciliation && g.reconciliation.findings ? WARN : GOOD]
    ].map(function (x) { return '<div class="kv"><span>' + lamp(x[2]) + x[0] + '</span><span style="color:' + x[2] + '">' + esc(x[1]) + '</span></div>'; }).join('');

    var lastFunding = f.instruments.map(function (x) { return x.last_funding_ts; }).filter(Boolean).sort().pop();
    document.getElementById('feed').innerHTML = [
      ['tick gaps', f.tick_gaps_48h], ['funding gaps', f.funding_gaps_48h], ['rejections', f.rejections_48h], ['last funding row', hhmm(lastFunding)]
    ].concat(f.instruments.map(function (x) { return ['last tick · ' + x.instrument_id, x.last_tick_age_s == null ? 'none' : Math.round(x.last_tick_age_s) + ' s']; }))
     .map(function (x) { return '<div class="kv"><span>' + x[0] + '</span><span>' + x[1] + '</span></div>'; }).join('');

    var autoOn = Object.keys(L.auto_mode).filter(function (k) { return L.auto_mode[k]; });
    var closed = (autoOn.length ? 0 : 1) + (L.live_env ? 0 : 1) + (L.signal_validated ? 0 : 1);
    document.getElementById('lockshead').textContent = 'Live locks · ' + closed + ' of 3 closed';
    document.getElementById('locks').innerHTML = [
      ['ExecutionMode.AUTO', Object.keys(L.auto_mode).map(function (k) { return k + ' ' + (L.auto_mode[k] ? 'ON' : 'off'); }).join(' · '), autoOn.length],
      ['POLYMARKET_LIVE_TRADING', L.live_env ? 'true' : 'unset', L.live_env],
      ['SIGNAL_VALIDATED', L.signal_validated ? 'True' : 'False', L.signal_validated]
    ].map(function (x) { return '<div style="display:flex;align-items:center;gap:8px">' + lamp(x[2] ? WARN : CRIT) + '<span style="color:var(--hi)">' + x[0] + '</span><span class="mute">' + esc(x[1]) + '</span></div>'; }).join('');

    document.getElementById('road').innerHTML = [
      ['native funding history', rd.native_days.toFixed(0) + ' / ' + rd.native_days_required + ' days', rd.native_days / rd.native_days_required],
      ['funding periods stored', rd.funding_periods + ' / ' + rd.funding_periods_required, rd.funding_periods / rd.funding_periods_required],
      ['clean paper days', rd.paper_days.toFixed(1) + ' / ' + rd.paper_days_target + (rd.clean ? '' : ' · NOT clean'), rd.paper_days / rd.paper_days_target]
    ].map(function (x) { return '<div class="col" style="gap:4px"><div class="kv"><span>' + x[0] + '</span><span>' + x[1] + '</span></div>' + meter(100 * x[2]) + '</div>'; }).join('');
  }

  document.addEventListener('click', function (ev) {
    var b = ev.target.closest('#instseg button'); if (b) { inst = b.dataset.k; render(); return; }
    var t = ev.target.closest('#tabs button'); if (t) { tab = t.dataset.t; render(); return; }
    var r = ev.target.closest('.row.body'); if (r) { var id = Number(r.dataset.id); selected = selected === id ? null : id; render(); }
  });

  function stale(on) {
    var el = document.getElementById('stale');
    el.className = on ? 'on' : '';
    el.textContent = on ? 'stale since ' + (lastOk ? hhmm(lastOk) : 'start') + ' — /api/state is not answering; showing the last numbers' : '';
  }

  function poll() {
    fetch('/api/state', { cache: 'no-store' }).then(function (r) {
      if (!r.ok) throw new Error(String(r.status));
      return r.json();
    }).then(function (s) { S = s; lastOk = s.generated_at; stale(false); render(); })
      .catch(function () { stale(true); });
  }
  poll();
  setInterval(poll, 10000);
})();
</script>
</body>
</html>
```

The tiny `.replace('<i style', ...)` in `m()` colours the detail meters by severity without a second helper; leave it as-is unless the meters render uncoloured, in which case give `meter()` a fourth `fill` argument and use it directly.

- [ ] **Step 4: Run the tests, then look at the page**

Run: `.venv/Scripts/python -m pytest tests/test_dashboard_server.py tests/test_dashboard_state.py -v`
Expected: all pass.

Then look at it with real-ish data: run
`.venv/Scripts/python -c "from pathlib import Path; from polyperps.dashboard.server import DashboardServer; s=DashboardServer(bind=('127.0.0.1',8080), db_path=Path('data/polyperps.sqlite3'), run_id='paper-soak-1', instrument_ids=(6,7), hypothesis='h1', instruments_provider=lambda: None); print('http://127.0.0.1:8080'); s.serve_forever()"`
and open `http://127.0.0.1:8080` in a browser (the local DB may have no paper rows — the page must still render with "—" and "warming up", no console errors). Stop with Ctrl+C.

- [ ] **Step 5: Commit**

```bash
git add polyperps/dashboard/static/index.html tests/test_dashboard_server.py
git commit -m "feat(dashboard): ops-console page polling /api/state

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 6: `scripts/run_dashboard.py`, systemd unit, deploy scripts, README

**Files:**
- Create: `scripts/run_dashboard.py`
- Create: `deploy/polyperps-dashboard.service`
- Modify: `deploy/env.example` (after line 23), `deploy/bootstrap.sh:73,79,93,95-96`, `deploy/update.sh:30,37,39`, `deploy/deploy.ps1:118`
- Modify: `tests/test_deploy_files.py:15` and append one test
- Modify: `README.md` (Deploy section)

**Interfaces:**
- Consumes: `DashboardServer` (Task 4), `polyperps.config.load_settings(env) -> Settings(db_path, instrument_ids, rest_rate_per_sec, rest_burst, ...)`, `PolymarketPerpsClient.create_public(rate_per_sec=, burst=)` + `await client.fetch_instruments() -> tuple[Instrument, ...]` + `await client.close()`.
- Produces: the runnable service.

- [ ] **Step 1: Write the failing deploy tests**

In `tests/test_deploy_files.py`, change line 15 to:

```python
UNIT_FILES = ["polyperps-feed.service", "polyperps-paper.service", "polyperps-dashboard.service"]
```

and append:

```python
def test_dashboard_unit_binds_port_80_without_root():
    text = (DEPLOY / "polyperps-dashboard.service").read_text(encoding="utf-8")
    assert "AmbientCapabilities=CAP_NET_BIND_SERVICE" in text
    assert "CapabilityBoundingSet=CAP_NET_BIND_SERVICE" in text
    assert "User=polyperps" in text
    assert "run_dashboard.py" in text


def test_deploy_scripts_know_the_dashboard_unit():
    for name in ("bootstrap.sh", "update.sh"):
        assert "polyperps-dashboard" in (DEPLOY / name).read_text(encoding="utf-8")
    assert "polyperps-dashboard" in (DEPLOY / "deploy.ps1").read_text(encoding="utf-8")
    assert "POLYPERPS_DASHBOARD_BIND=0.0.0.0:80" in (DEPLOY / "env.example").read_text(encoding="utf-8")
```

(`DEPLOY` is whatever name the existing file uses for the `deploy/` path constant — read the top of the file and reuse it.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_deploy_files.py -v`
Expected: FAIL — the parametrized unit tests error on the missing `polyperps-dashboard.service`, and the two new tests fail.

- [ ] **Step 3: Write the script, unit, and deploy edits**

`scripts/run_dashboard.py`:

```python
"""Serve the read-only dashboard for a paper run.

Reads the same /etc/polyperps/env as the other units. Never writes the DB.
Binds POLYPERPS_DASHBOARD_BIND (default 127.0.0.1:8080 so a local run never
listens on all interfaces by accident; the box sets 0.0.0.0:80).
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from polyperps.config import load_settings  # noqa: E402
from polyperps.dashboard.server import DashboardServer  # noqa: E402
from polyperps.exchange.client import PolymarketPerpsClient  # noqa: E402

log = logging.getLogger("run_dashboard")
REFRESH_S = 3600


class InstrumentCache:
    """Best-effort instrument names/categories, refreshed hourly in a thread."""

    def __init__(self, *, rate_per_sec: float, burst: int) -> None:
        self._rate, self._burst = rate_per_sec, burst
        self._value = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="instruments", daemon=True)

    def get(self):
        return self._value

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    async def _fetch(self):
        client = PolymarketPerpsClient.create_public(rate_per_sec=self._rate, burst=self._burst)
        try:
            return {i.instrument_id: i for i in await client.fetch_instruments()}
        finally:
            await client.close()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._value = asyncio.run(self._fetch())
                log.info("instruments refreshed: %d", len(self._value))
            except Exception as e:   # best-effort by design: the page falls back to ids
                log.warning("instrument fetch failed (names fall back to ids): %s", e)
            self._stop.wait(REFRESH_S)


def parse_bind(text: str) -> tuple[str, int]:
    host, _, port = text.rpartition(":")
    if not host or not port.isdigit():
        raise SystemExit(f"POLYPERPS_DASHBOARD_BIND must be host:port, got {text!r}")
    return host, int(port)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    settings = load_settings()
    run_id = os.environ.get("PAPER_RUN_ID")
    if not run_id:
        raise SystemExit("PAPER_RUN_ID is required (the paper account to show)")
    hypothesis = os.environ.get("PAPER_HYPOTHESIS", "?")
    bind = parse_bind(os.environ.get("POLYPERPS_DASHBOARD_BIND", "127.0.0.1:8080"))

    cache = InstrumentCache(rate_per_sec=settings.rest_rate_per_sec, burst=settings.rest_burst)
    server = DashboardServer(
        bind=bind, db_path=settings.db_path, run_id=run_id,
        instrument_ids=settings.instrument_ids, hypothesis=hypothesis,
        instruments_provider=cache.get,
    )
    cache.start()
    log.info("dashboard on http://%s:%d for run %s (db %s)", bind[0], server.port, run_id, settings.db_path)
    try:
        server.serve_forever()
    finally:
        cache.stop()
        server.server_close()
        log.info("dashboard stopped")


def _raise_keyboard_interrupt(signum, frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)
    try:
        main()
    except KeyboardInterrupt:
        pass
```

Check how `scripts/run_paper.py` and `scripts/run_feed.py` make `polyperps` importable (a `sys.path.insert` like above, or they rely on the editable install). Match whichever they do; drop the `sys.path` line if they don't have one.

`deploy/polyperps-dashboard.service` (LF line endings):

```ini
[Unit]
Description=polyperps read-only dashboard (paper soak, port 80)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=polyperps
Group=polyperps
WorkingDirectory=/opt/polyperps
EnvironmentFile=/etc/polyperps/env
ExecStart=/opt/polyperps/.venv/bin/python scripts/run_dashboard.py
Restart=on-failure
RestartSec=10
KillSignal=SIGTERM
TimeoutStopSec=30
# Port 80 as an unprivileged user: ambient capability, nothing else.
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ProtectHome=yes
# Read-only at the SQLite level (mode=ro); the path stays writable so WAL
# readers can touch the -shm file, same user as the writer.
ReadWritePaths=/var/lib/polyperps
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

`deploy/env.example` — append after the `PAPER_RUN_ID` line:

```
# Dashboard bind address. The box serves on all interfaces, port 80; restrict
# reach with the EC2 security group (the page has no auth and no TLS).
POLYPERPS_DASHBOARD_BIND=0.0.0.0:80
```

`deploy/bootstrap.sh` — edit these lines (read the file first; line numbers are from the current master):
- line 73: `for unit in polyperps-feed.service polyperps-paper.service polyperps-dashboard.service; do`
- line 79: `systemctl enable polyperps-feed polyperps-paper polyperps-dashboard`
- line 93: `systemctl start polyperps-feed polyperps-paper polyperps-dashboard`
- after line 96 add: `echo "  journalctl -fu polyperps-dashboard"` in the same style as the two lines above it.

`deploy/update.sh`:
- line 30: `for unit in polyperps-feed.service polyperps-paper.service polyperps-dashboard.service; do`
- line 37: `systemctl restart polyperps-feed polyperps-paper polyperps-dashboard`
- line 39: `systemctl --no-pager status polyperps-feed polyperps-paper polyperps-dashboard || true`

`deploy/deploy.ps1` line 118: `Invoke-Plink -RemoteArgs @("sudo journalctl -u polyperps-feed -u polyperps-paper -u polyperps-dashboard -n 40 --no-pager")`

`README.md` — add under the Deploy section, after the "Watching" paragraph:

```markdown
**Dashboard**: `polyperps-dashboard.service` serves a read-only page on
`POLYPERPS_DASHBOARD_BIND` (`0.0.0.0:80` on the box) showing the paper account,
positions with their guard distances, router decisions, alerts, feed health, the
three live locks and the road-to-live counters, refreshed every 10 s from the DB
(opened `mode=ro`; it cannot write). No auth, no TLS: reach is controlled only by
the EC2 security group, so keep port 80 restricted to your own IP. Locally:
`POLYPERPS_INSTRUMENT_IDS=6,7 PAPER_RUN_ID=paper-soak-1 .venv/Scripts/python scripts/run_dashboard.py`
then open `http://127.0.0.1:8080`.
```

Keep every deploy file LF-only (the test checks for `\r`). On Windows, write them with an editor set to LF or run `git add --renormalize` is NOT enough — verify with `.venv/Scripts/python -c "import pathlib;print([p for p in pathlib.Path('deploy').iterdir() if b'\r' in p.read_bytes()])"` → must print `[]`.

- [ ] **Step 4: Run the whole suite**

Run: `.venv/Scripts/python -m pytest -q`
Expected: all green (307 existing + the new dashboard tests). Then a smoke run of the script:
`set POLYPERPS_INSTRUMENT_IDS=6,7 && set PAPER_RUN_ID=paper-soak-1 && .venv/Scripts/python scripts/run_dashboard.py` → logs `dashboard on http://127.0.0.1:8080 ...`, `curl http://127.0.0.1:8080/api/state` returns JSON (the instrument fetch may warn — the home ISP blocks Polymarket; that is the best-effort path working). Ctrl+C stops it with `dashboard stopped`.

- [ ] **Step 5: Commit**

```bash
git add scripts/run_dashboard.py deploy/polyperps-dashboard.service deploy/env.example deploy/bootstrap.sh deploy/update.sh deploy/deploy.ps1 tests/test_deploy_files.py README.md
git commit -m "feat(dashboard): run_dashboard.py, systemd unit on port 80, deploy wiring

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## After the last task

Deploy is the user's step (`deploy\deploy.ps1 -Session polyperps-ec2` after push; then on the box `sudo systemctl daemon-reload && sudo systemctl enable --now polyperps-dashboard` if `update.sh` did not already, and the security-group change). Acceptance per spec §8: `curl -s localhost/api/state | python -m json.tool` on the box shows `paper-soak-1` and the open positions; stopping the dashboard unit leaves feed and paper untouched.
