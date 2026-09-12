# polyperps Phase 2a — Risk Guards, Execution Core, Reconciliation (paper-only) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the execution state machine, pure risk guards, reconciliation, crash recovery, alerts, and the kill-switch mechanism, and prove them on a paper run that uses the exact code path a live run would — with the live last mile built, gated, unit-tested, and never executed.

**Architecture:** A per-instrument `InstrumentRouter` state machine (`FLAT → ENTRY_PENDING → OPEN → EXIT_PENDING → FLAT`, plus `LIQUIDATED`/`HALTED`) is driven once per closed hourly bar and once per 20 s fast tick. It asks the strategy for a target, runs it through pure guard functions (`vet_entry` → `vet_exposure`), persists a `decisions` row, then talks to an `Executor` protocol. `SimExecutor` fills against the live public feed with the Phase 1 cost model and fires its own stops; `LiveExecutor` wraps `PerpsSession` and cannot be constructed unless `gates.live_orders_allowed` says yes. Reconciliation diffs local rows against `executor.snapshot()` with pre-registered responses; recovery rebuilds state from the executor on boot.

**Tech Stack:** Python 3.12, stdlib (`sqlite3`, `asyncio`, `json`), `httpx` (Telegram), `polymarket-client==0.10.0` (only in `live_executor.py`), pytest + pytest-asyncio.

**Spec:** `docs/superpowers/specs/2026-09-12-polyperps-phase2a-design.md`. Parent: `polyperps-implementation-plan.md` Phase 2.

## Global Constraints

- Import boundary: `polymarket` only in `polyperps/exchange/client.py`, `polyperps/execution/live_executor.py`, `scripts/check_auth.py`, and tests.
- Pre-registered numbers, each pinned by a test and never edited quietly: `LIMITS = RiskLimits(max_leverage=3, min_liq_distance=0.25, stop_distance=0.15, notional_usd=100, max_funding_cost=0.02, maintenance_rate=0.02)`; `EXPOSURE = ExposureLimits(gross=1.0, cluster_net=0.6)`; `THRESHOLDS = KillThresholds(pause=None, shutdown=None)`; alert thresholds `margin WARN < 0.35 / CRITICAL < 0.28`, `pnl WARN −5 % / CRITICAL −10 %`, `funding_drift 3×`; `ack_timeout_s = 10`, `heartbeat_s = 20`, `reconcile_s = 60`, dead-man `cancel_at = now + 60 s`.
- Every side effect is preceded by its row: `decisions` and `orders` are written **before** `executor.submit`; `positions_local` after every state change.
- `client_order_id = f"{run_id}-{instrument_id}-{seq}"`; a retry after timeout reuses the id and first checks `snapshot()`.
- No live order in 2a: `scripts/run_paper.py` accepts only `--executor sim`; `LiveExecutor.__init__` raises `GateClosed` unless every instrument's gate is open.
- Kill switch with `None` thresholds → `"pause"` for live, `"run"` for paper.
- Alerts never raise into the router; Telegram token/chat id come from `key_management.load_secret` and are never logged.
- `Decimal` money, UTC-aware datetimes (`polyperps.exchange.types._require_aware`), frozen kw-only dataclasses for all value types.
- Commit trailer: blank line then `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.
- Existing 191 tests stay green throughout.

---

## File structure

```
polyperps/
├── execution/
│   ├── __init__.py
│   ├── types.py             # State, Side, Intent, OrderRequest/Ack, StopAck, OrderUpdate, FillUpdate, PositionView, AccountSnapshot, row types
│   ├── executor.py          # Executor protocol, ExecutorTimeout, GateClosed
│   ├── sim_executor.py      # SimExecutor
│   ├── live_executor.py     # LiveExecutor (gated; fake-session tests only)
│   ├── order_router.py      # InstrumentRouter, Portfolio, apply_guards
│   ├── reconciliation.py    # Mismatch, diff()
│   ├── state_recovery.py    # recover(), RecoveryReport, RecoveryHalt
│   └── live_bars.py         # LiveBarBuilder
├── risk/
│   ├── __init__.py
│   ├── liquidation_guard.py # RiskLimits, LIMITS, Verdict types, vet_entry, check_open, stop_price, funding_exit_due
│   ├── portfolio_exposure.py# ExposureLimits, EXPOSURE, cluster_of, vet_exposure
│   └── kill_switch.py       # KillThresholds, THRESHOLDS, evaluate
├── monitor/
│   ├── __init__.py
│   ├── alerts.py            # Alert, Sink protocol, LogSink, SqliteSink, TelegramSink, Alerter, THRESHOLDS
│   └── decision_trail.py    # TrailEvent, reconstruct
├── storage/db.py            # + decisions, orders, positions_local, sim_account, alerts, recovery tables + functions
scripts/
├── run_paper.py
└── nautilus_recheck.md
tests/
├── test_execution_types.py, test_storage_phase2.py, test_liquidation_guard.py, test_portfolio_exposure.py,
├── test_kill_switch.py, test_alerts.py, test_decision_trail.py, test_sim_executor.py, test_order_router.py,
├── test_reconciliation.py, test_state_recovery.py, test_live_executor.py, test_live_bars.py, test_run_paper_script.py
```

---

### Task 1: Execution value types and the Phase 2 storage tables

**Files:**
- Create: `polyperps/execution/__init__.py` (empty), `polyperps/execution/types.py`
- Modify: `polyperps/storage/db.py` (schema + functions)
- Test: `tests/test_execution_types.py`, `tests/test_storage_phase2.py`

**Interfaces:**
- Produces (`execution/types.py`, all `@dataclass(frozen=True, slots=True, kw_only=True)` with `_require_aware` in `__post_init__` where a datetime exists):
  - `State(StrEnum)`: `FLAT, ENTRY_PENDING, OPEN, EXIT_PENDING, LIQUIDATED, HALTED`; `Side = Literal["buy","sell"]`.
  - `Intent(instrument_id: int, side: Side, quantity: Decimal, notional: Decimal, reduce_only: bool = False, reason: str = "strategy")`.
  - `OrderRequest(client_order_id: str, instrument_id: int, side: Side, quantity: Decimal, reduce_only: bool, ts: datetime)`.
  - `OrderAck(client_order_id: str, exchange_order_id: str | None, status: Literal["accepted","rejected"], reason: str, ts: datetime)`.
  - `StopAck(instrument_id: int, trigger_price: Decimal, exchange_order_id: str | None, ts: datetime)`.
  - `OrderUpdate(client_order_id: str, status: Literal["accepted","open","partial","filled","cancelled","auto_cancelled","rejected"], filled_quantity: Decimal, ts: datetime)`.
  - `FillUpdate(client_order_id: str, instrument_id: int, side: Side, quantity: Decimal, price: Decimal, fee: Decimal, ts: datetime)`.
  - `PositionView(instrument_id: int, size: Decimal, entry_price: Decimal, notional: Decimal, leverage: int, liquidation_price: Decimal | None, unrealised_pnl: Decimal, cumulative_funding: Decimal)` — `size` signed (+long/−short), `notional = |size| × mark`.
  - `AccountSnapshot(equity: Decimal, positions: tuple[PositionView, ...], open_orders: tuple[str, ...], stops: dict[int, Decimal], in_liquidation: bool, ts: datetime)` with helper `position(instrument_id) -> PositionView | None`.
  - Row types: `DecisionRow(run_id, instrument_id, seq: int, ts, state_before: State, target: Decimal | None, verdicts: dict[str, str], intent: Intent | None, client_order_id: str | None, note: str = "")`; `OrderRow(client_order_id, run_id, instrument_id, side, quantity, reduce_only, status: str, exchange_order_id: str | None, filled_quantity: Decimal, avg_price: Decimal | None, submitted_at, updated_at, reason)`; `PositionLocalRow(run_id, instrument_id, state: State, size: Decimal, entry_price: Decimal | None, stop_trigger: Decimal | None, stop_order_id: str | None, cumulative_funding: Decimal, updated_at)`.
- Produces (`storage/db.py`): tables `decisions`, `orders`, `positions_local`, `sim_account`, `alerts`, `recovery` (columns in the SQL below) and functions `insert_decision(conn, row: DecisionRow) -> None`, `list_decisions(conn, run_id, instrument_id) -> list[DecisionRow]`, `upsert_order(conn, row: OrderRow) -> None`, `get_order(conn, client_order_id) -> OrderRow | None`, `list_orders(conn, run_id, *, status: str | None = None) -> list[OrderRow]`, `upsert_position_local(conn, row: PositionLocalRow) -> None`, `get_positions_local(conn, run_id) -> dict[int, PositionLocalRow]`, `save_sim_account(conn, run_id, json_text: str) -> None`, `load_sim_account(conn, run_id) -> str | None`, `insert_alert(conn, *, run_id, level, kind, instrument_id, detail_json, ts) -> None`, `list_alerts(conn, run_id) -> list[tuple[datetime, str, str, int | None, dict]]`, `insert_recovery(conn, *, run_id, ts, findings_json) -> None`, `list_recovery(conn, run_id) -> list[tuple[datetime, dict]]`.

- [ ] **Step 1: Write the failing type tests**

`tests/test_execution_types.py`:

```python
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from polyperps.execution.types import (
    AccountSnapshot, FillUpdate, Intent, OrderAck, OrderRequest, PositionView, State,
)

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


def pos(iid=6, size="1", entry="100", liq="70"):
    return PositionView(instrument_id=iid, size=Decimal(size), entry_price=Decimal(entry),
                        notional=abs(Decimal(size)) * Decimal("100"), leverage=3,
                        liquidation_price=Decimal(liq) if liq else None,
                        unrealised_pnl=Decimal(0), cumulative_funding=Decimal(0))


def test_state_values():
    assert [s.value for s in State] == ["FLAT", "ENTRY_PENDING", "OPEN", "EXIT_PENDING", "LIQUIDATED", "HALTED"]


def test_snapshot_position_lookup():
    snap = AccountSnapshot(equity=Decimal(1000), positions=(pos(6), pos(7, size="-2")), open_orders=(),
                           stops={6: Decimal(85)}, in_liquidation=False, ts=T0)
    assert snap.position(6).size == 1 and snap.position(7).size == -2 and snap.position(8) is None


def test_datetimes_must_be_utc():
    with pytest.raises(ValueError):
        OrderRequest(client_order_id="r-6-1", instrument_id=6, side="buy", quantity=Decimal(1),
                     reduce_only=False, ts=datetime(2026, 9, 12, 12, 0))


def test_frozen_kw_only():
    ack = OrderAck(client_order_id="r-6-1", exchange_order_id="x1", status="accepted", reason="", ts=T0)
    with pytest.raises(AttributeError):
        ack.status = "rejected"  # type: ignore[misc]
    with pytest.raises(TypeError):
        FillUpdate("r-6-1")  # positional


def test_intent_defaults():
    i = Intent(instrument_id=6, side="sell", quantity=Decimal("0.5"), notional=Decimal(50))
    assert i.reduce_only is False and i.reason == "strategy"
```

- [ ] **Step 2: Write the failing storage tests**

`tests/test_storage_phase2.py`:

```python
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.execution.types import DecisionRow, Intent, OrderRow, PositionLocalRow, State
from polyperps.storage.db import (
    connect, get_order, get_positions_local, insert_alert, insert_decision, insert_recovery,
    list_alerts, list_decisions, list_orders, list_recovery, load_sim_account, save_sim_account,
    upsert_order, upsert_position_local,
)

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)
RUN = "run1"


def test_decisions_round_trip_ordered_by_seq():
    conn = connect(":memory:")
    intent = Intent(instrument_id=6, side="buy", quantity=Decimal("0.01"), notional=Decimal(100))
    for seq in (2, 1):
        insert_decision(conn, DecisionRow(run_id=RUN, instrument_id=6, seq=seq, ts=T0 + timedelta(seconds=seq),
                                          state_before=State.FLAT, target=Decimal(1),
                                          verdicts={"vet_entry": "allow", "vet_exposure": "allow"},
                                          intent=intent if seq == 1 else None,
                                          client_order_id=f"{RUN}-6-{seq}" if seq == 1 else None))
    rows = list_decisions(conn, RUN, 6)
    assert [r.seq for r in rows] == [1, 2]
    assert rows[0].intent == intent and rows[0].verdicts["vet_entry"] == "allow"
    assert rows[1].intent is None and rows[1].target == Decimal(1)


def test_orders_upsert_and_query():
    conn = connect(":memory:")
    row = OrderRow(client_order_id=f"{RUN}-6-1", run_id=RUN, instrument_id=6, side="buy", quantity=Decimal("0.01"),
                   reduce_only=False, status="submitting", exchange_order_id=None, filled_quantity=Decimal(0),
                   avg_price=None, submitted_at=T0, updated_at=T0, reason="strategy")
    upsert_order(conn, row)
    upsert_order(conn, replace(row, status="filled", filled_quantity=Decimal("0.01"),
                               avg_price=Decimal("100.05"), exchange_order_id="sim-1"))
    got = get_order(conn, f"{RUN}-6-1")
    assert got.status == "filled" and got.avg_price == Decimal("100.05") and got.exchange_order_id == "sim-1"
    assert [o.client_order_id for o in list_orders(conn, RUN, status="filled")] == [f"{RUN}-6-1"]
    assert list_orders(conn, RUN, status="open") == []
    assert get_order(conn, "nope") is None


def test_positions_local_upsert():
    conn = connect(":memory:")
    upsert_position_local(conn, PositionLocalRow(run_id=RUN, instrument_id=6, state=State.OPEN, size=Decimal("0.01"),
                                                 entry_price=Decimal(100), stop_trigger=Decimal(85), stop_order_id="s1",
                                                 cumulative_funding=Decimal("-0.1"), updated_at=T0))
    upsert_position_local(conn, PositionLocalRow(run_id=RUN, instrument_id=6, state=State.FLAT, size=Decimal(0),
                                                 entry_price=None, stop_trigger=None, stop_order_id=None,
                                                 cumulative_funding=Decimal(0), updated_at=T0 + timedelta(hours=1)))
    got = get_positions_local(conn, RUN)
    assert got[6].state is State.FLAT and got[6].entry_price is None


def test_sim_account_alerts_recovery():
    conn = connect(":memory:")
    assert load_sim_account(conn, RUN) is None
    save_sim_account(conn, RUN, json.dumps({"cash": "1000"}))
    save_sim_account(conn, RUN, json.dumps({"cash": "990"}))
    assert json.loads(load_sim_account(conn, RUN))["cash"] == "990"
    insert_alert(conn, run_id=RUN, level="WARN", kind="margin_ratio", instrument_id=6, detail_json='{"d": 0.3}', ts=T0)
    (a,) = list_alerts(conn, RUN)
    assert a[1:4] == ("WARN", "margin_ratio", 6) and a[4] == {"d": 0.3}
    insert_recovery(conn, run_id=RUN, ts=T0, findings_json='{"adopted": []}')
    assert list_recovery(conn, RUN)[0][1] == {"adopted": []}
```

- [ ] **Step 3: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_execution_types.py tests/test_storage_phase2.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.execution'`

- [ ] **Step 4: Write `polyperps/execution/__init__.py`** (empty) and `polyperps/execution/types.py`

```python
"""Execution value types. Our own frozen dataclasses on both sides of the
Executor boundary; SDK models never cross it (spec section 4.1)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Literal

from polyperps.exchange.types import _require_aware


class State(StrEnum):
    FLAT = "FLAT"
    ENTRY_PENDING = "ENTRY_PENDING"
    OPEN = "OPEN"
    EXIT_PENDING = "EXIT_PENDING"
    LIQUIDATED = "LIQUIDATED"
    HALTED = "HALTED"


Side = Literal["buy", "sell"]
AckStatus = Literal["accepted", "rejected"]
OrderStatus = Literal["accepted", "open", "partial", "filled", "cancelled", "auto_cancelled", "rejected"]


@dataclass(frozen=True, slots=True, kw_only=True)
class Intent:
    instrument_id: int
    side: Side
    quantity: Decimal
    notional: Decimal
    reduce_only: bool = False
    reason: str = "strategy"


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderRequest:
    client_order_id: str
    instrument_id: int
    side: Side
    quantity: Decimal
    reduce_only: bool
    ts: datetime

    def __post_init__(self) -> None:
        _require_aware(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderAck:
    client_order_id: str
    exchange_order_id: str | None
    status: AckStatus
    reason: str
    ts: datetime

    def __post_init__(self) -> None:
        _require_aware(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class StopAck:
    instrument_id: int
    trigger_price: Decimal
    exchange_order_id: str | None
    ts: datetime

    def __post_init__(self) -> None:
        _require_aware(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderUpdate:
    client_order_id: str
    status: OrderStatus
    filled_quantity: Decimal
    ts: datetime

    def __post_init__(self) -> None:
        _require_aware(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class FillUpdate:
    client_order_id: str
    instrument_id: int
    side: Side
    quantity: Decimal
    price: Decimal
    fee: Decimal
    ts: datetime

    def __post_init__(self) -> None:
        _require_aware(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class PositionView:
    instrument_id: int
    size: Decimal            # signed: + long, - short
    entry_price: Decimal
    notional: Decimal        # |size| x mark
    leverage: int
    liquidation_price: Decimal | None
    unrealised_pnl: Decimal
    cumulative_funding: Decimal   # negative = paid


@dataclass(frozen=True, slots=True, kw_only=True)
class AccountSnapshot:
    equity: Decimal
    positions: tuple[PositionView, ...]
    open_orders: tuple[str, ...]      # client order ids resting on the venue
    stops: dict[int, Decimal]         # instrument_id -> trigger price
    in_liquidation: bool
    ts: datetime

    def __post_init__(self) -> None:
        _require_aware(self)

    def position(self, instrument_id: int) -> PositionView | None:
        for p in self.positions:
            if p.instrument_id == instrument_id:
                return p
        return None


# --- persisted rows ----------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class DecisionRow:
    run_id: str
    instrument_id: int
    seq: int
    ts: datetime
    state_before: State
    target: Decimal | None
    verdicts: dict[str, str] = field(default_factory=dict)
    intent: Intent | None = None
    client_order_id: str | None = None
    note: str = ""


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderRow:
    client_order_id: str
    run_id: str
    instrument_id: int
    side: Side
    quantity: Decimal
    reduce_only: bool
    status: str
    exchange_order_id: str | None
    filled_quantity: Decimal
    avg_price: Decimal | None
    submitted_at: datetime
    updated_at: datetime
    reason: str


@dataclass(frozen=True, slots=True, kw_only=True)
class PositionLocalRow:
    run_id: str
    instrument_id: int
    state: State
    size: Decimal
    entry_price: Decimal | None
    stop_trigger: Decimal | None
    stop_order_id: str | None
    cumulative_funding: Decimal
    updated_at: datetime
```

- [ ] **Step 5: Extend `polyperps/storage/db.py`**

Append to `_SCHEMA` (before the closing `"""`):

```sql
CREATE TABLE IF NOT EXISTS decisions (
    run_id TEXT NOT NULL, instrument_id INTEGER NOT NULL, seq INTEGER NOT NULL, ts TEXT NOT NULL,
    state_before TEXT NOT NULL, target TEXT, verdicts_json TEXT NOT NULL, intent_json TEXT,
    client_order_id TEXT, note TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (run_id, instrument_id, seq)
);
CREATE TABLE IF NOT EXISTS orders (
    client_order_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, instrument_id INTEGER NOT NULL, side TEXT NOT NULL,
    quantity TEXT NOT NULL, reduce_only INTEGER NOT NULL, status TEXT NOT NULL, exchange_order_id TEXT,
    filled_quantity TEXT NOT NULL, avg_price TEXT, submitted_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS positions_local (
    run_id TEXT NOT NULL, instrument_id INTEGER NOT NULL, state TEXT NOT NULL, size TEXT NOT NULL,
    entry_price TEXT, stop_trigger TEXT, stop_order_id TEXT, cumulative_funding TEXT NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, instrument_id)
);
CREATE TABLE IF NOT EXISTS sim_account (run_id TEXT PRIMARY KEY, json TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS alerts (
    ts TEXT NOT NULL, run_id TEXT NOT NULL, level TEXT NOT NULL, kind TEXT NOT NULL,
    instrument_id INTEGER, detail_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recovery (run_id TEXT NOT NULL, ts TEXT NOT NULL, findings_json TEXT NOT NULL);
```

Add imports `from dataclasses import asdict` and `from polyperps.execution.types import DecisionRow, Intent, OrderRow, PositionLocalRow, State`, then append the functions:

```python
def _dec(s: str | None) -> Decimal | None:
    return Decimal(s) if s is not None else None


def _intent_json(i: Intent | None) -> str | None:
    if i is None:
        return None
    return json.dumps({"instrument_id": i.instrument_id, "side": i.side, "quantity": str(i.quantity),
                       "notional": str(i.notional), "reduce_only": i.reduce_only, "reason": i.reason})


def _intent_from_json(s: str | None) -> Intent | None:
    if s is None:
        return None
    d = json.loads(s)
    return Intent(instrument_id=d["instrument_id"], side=d["side"], quantity=Decimal(d["quantity"]),
                  notional=Decimal(d["notional"]), reduce_only=d["reduce_only"], reason=d["reason"])


def insert_decision(conn: sqlite3.Connection, row: DecisionRow) -> None:
    conn.execute(
        "INSERT INTO decisions VALUES (?,?,?,?,?,?,?,?,?,?)",
        (row.run_id, row.instrument_id, row.seq, _ts(row.ts), row.state_before.value,
         str(row.target) if row.target is not None else None, json.dumps(row.verdicts, sort_keys=True),
         _intent_json(row.intent), row.client_order_id, row.note),
    )
    conn.commit()


def list_decisions(conn: sqlite3.Connection, run_id: str, instrument_id: int) -> list[DecisionRow]:
    rows = conn.execute(
        "SELECT run_id, instrument_id, seq, ts, state_before, target, verdicts_json, intent_json, client_order_id, note "
        "FROM decisions WHERE run_id=? AND instrument_id=? ORDER BY seq", (run_id, instrument_id)).fetchall()
    return [DecisionRow(run_id=r[0], instrument_id=r[1], seq=r[2], ts=_parse_ts(r[3]), state_before=State(r[4]),
                        target=_dec(r[5]), verdicts=json.loads(r[6]), intent=_intent_from_json(r[7]),
                        client_order_id=r[8], note=r[9]) for r in rows]


def upsert_order(conn: sqlite3.Connection, o: OrderRow) -> None:
    conn.execute(
        "INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(client_order_id) DO UPDATE SET "
        "status=excluded.status, exchange_order_id=excluded.exchange_order_id, filled_quantity=excluded.filled_quantity, "
        "avg_price=excluded.avg_price, updated_at=excluded.updated_at, reason=excluded.reason",
        (o.client_order_id, o.run_id, o.instrument_id, o.side, str(o.quantity), int(o.reduce_only), o.status,
         o.exchange_order_id, str(o.filled_quantity), str(o.avg_price) if o.avg_price is not None else None,
         _ts(o.submitted_at), _ts(o.updated_at), o.reason),
    )
    conn.commit()


def _order_from_row(r) -> OrderRow:
    return OrderRow(client_order_id=r[0], run_id=r[1], instrument_id=r[2], side=r[3], quantity=Decimal(r[4]),
                    reduce_only=bool(r[5]), status=r[6], exchange_order_id=r[7], filled_quantity=Decimal(r[8]),
                    avg_price=_dec(r[9]), submitted_at=_parse_ts(r[10]), updated_at=_parse_ts(r[11]), reason=r[12])


_ORDER_COLS = ("client_order_id, run_id, instrument_id, side, quantity, reduce_only, status, exchange_order_id, "
               "filled_quantity, avg_price, submitted_at, updated_at, reason")


def get_order(conn: sqlite3.Connection, client_order_id: str) -> OrderRow | None:
    r = conn.execute(f"SELECT {_ORDER_COLS} FROM orders WHERE client_order_id=?", (client_order_id,)).fetchone()
    return _order_from_row(r) if r else None


def list_orders(conn: sqlite3.Connection, run_id: str, *, status: str | None = None) -> list[OrderRow]:
    if status is None:
        rows = conn.execute(f"SELECT {_ORDER_COLS} FROM orders WHERE run_id=? ORDER BY submitted_at", (run_id,)).fetchall()
    else:
        rows = conn.execute(f"SELECT {_ORDER_COLS} FROM orders WHERE run_id=? AND status=? ORDER BY submitted_at",
                            (run_id, status)).fetchall()
    return [_order_from_row(r) for r in rows]


def upsert_position_local(conn: sqlite3.Connection, p: PositionLocalRow) -> None:
    conn.execute(
        "INSERT INTO positions_local VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(run_id, instrument_id) DO UPDATE SET "
        "state=excluded.state, size=excluded.size, entry_price=excluded.entry_price, stop_trigger=excluded.stop_trigger, "
        "stop_order_id=excluded.stop_order_id, cumulative_funding=excluded.cumulative_funding, updated_at=excluded.updated_at",
        (p.run_id, p.instrument_id, p.state.value, str(p.size), str(p.entry_price) if p.entry_price is not None else None,
         str(p.stop_trigger) if p.stop_trigger is not None else None, p.stop_order_id, str(p.cumulative_funding),
         _ts(p.updated_at)),
    )
    conn.commit()


def get_positions_local(conn: sqlite3.Connection, run_id: str) -> dict[int, PositionLocalRow]:
    rows = conn.execute(
        "SELECT run_id, instrument_id, state, size, entry_price, stop_trigger, stop_order_id, cumulative_funding, updated_at "
        "FROM positions_local WHERE run_id=?", (run_id,)).fetchall()
    return {r[1]: PositionLocalRow(run_id=r[0], instrument_id=r[1], state=State(r[2]), size=Decimal(r[3]),
                                   entry_price=_dec(r[4]), stop_trigger=_dec(r[5]), stop_order_id=r[6],
                                   cumulative_funding=Decimal(r[7]), updated_at=_parse_ts(r[8])) for r in rows}


def save_sim_account(conn: sqlite3.Connection, run_id: str, json_text: str) -> None:
    conn.execute("INSERT INTO sim_account VALUES (?,?,?) ON CONFLICT(run_id) DO UPDATE SET json=excluded.json, "
                 "updated_at=excluded.updated_at", (run_id, json_text, _ts(datetime.now(timezone.utc))))
    conn.commit()


def load_sim_account(conn: sqlite3.Connection, run_id: str) -> str | None:
    r = conn.execute("SELECT json FROM sim_account WHERE run_id=?", (run_id,)).fetchone()
    return r[0] if r else None


def insert_alert(conn: sqlite3.Connection, *, run_id: str, level: str, kind: str, instrument_id: int | None,
                 detail_json: str, ts: datetime) -> None:
    conn.execute("INSERT INTO alerts VALUES (?,?,?,?,?,?)", (_ts(ts), run_id, level, kind, instrument_id, detail_json))
    conn.commit()


def list_alerts(conn: sqlite3.Connection, run_id: str) -> list[tuple[datetime, str, str, int | None, dict]]:
    rows = conn.execute("SELECT ts, level, kind, instrument_id, detail_json FROM alerts WHERE run_id=? ORDER BY ts",
                        (run_id,)).fetchall()
    return [(_parse_ts(r[0]), r[1], r[2], r[3], json.loads(r[4])) for r in rows]


def insert_recovery(conn: sqlite3.Connection, *, run_id: str, ts: datetime, findings_json: str) -> None:
    conn.execute("INSERT INTO recovery VALUES (?,?,?)", (run_id, _ts(ts), findings_json))
    conn.commit()


def list_recovery(conn: sqlite3.Connection, run_id: str) -> list[tuple[datetime, dict]]:
    rows = conn.execute("SELECT ts, findings_json FROM recovery WHERE run_id=? ORDER BY ts", (run_id,)).fetchall()
    return [(_parse_ts(r[0]), json.loads(r[1])) for r in rows]
```

(`datetime`/`timezone` are already imported in db.py; `sqlite3` too.) Note the circular-import check: `execution/types.py` imports `exchange.types` only, and `db.py` imports `execution.types` — no cycle.

- [ ] **Step 6: Run tests**

Run: `.venv/Scripts/python -m pytest tests/test_execution_types.py tests/test_storage_phase2.py tests/test_storage.py tests/test_storage_phase1.py -v`
Expected: 5 + 4 new passed; existing storage tests unchanged.

- [ ] **Step 7: Commit**

```bash
git add polyperps/execution polyperps/storage/db.py tests/test_execution_types.py tests/test_storage_phase2.py
git commit -m "feat(phase2a): execution value types and persistence tables"
```

---

### Task 2: Liquidation guard (spec §5.1)

**Files:**
- Create: `polyperps/risk/__init__.py` (empty), `polyperps/risk/liquidation_guard.py`
- Test: `tests/test_liquidation_guard.py`

**Interfaces:**
- Produces: `RiskLimits` frozen kw-only (`max_leverage: int, min_liq_distance: Decimal, stop_distance: Decimal, notional_usd: Decimal, max_funding_cost: Decimal, maintenance_rate: Decimal`); `LIMITS`; `Allow()`, `Resize(quantity: Decimal)`, `Reject(reason: str)` frozen; `Verdict = Allow | Resize | Reject`; `verdict_label(v) -> str` (`"allow"`, `"resize:<qty>"`, `"reject:<reason>"`); `vet_entry(intent: Intent, *, mark: Decimal, snapshot: AccountSnapshot, limits=LIMITS) -> Verdict`; `check_open(position: PositionView, *, mark: Decimal, limits=LIMITS) -> Literal["hold","flatten"]`; `stop_price(*, side: Literal["long","short"], entry: Decimal, limits=LIMITS) -> Decimal`; `funding_exit_due(position: PositionView, *, limits=LIMITS) -> bool`.
- Rules: `post_leverage = (Σ p.notional + intent.notional) / equity`; `> max_leverage` → `Resize` to `max_leverage × equity − Σ notional` (as quantity `allowed_notional / mark`, quantised to 8 dp) or `Reject` if ≤ 0; `liq_distance_est = 1/post_leverage − maintenance_rate`; `< min_liq_distance` → `Resize` to leverage `1/(min_liq_distance + maintenance_rate)` (the tighter of the two resizes wins); `equity <= 0` → `Reject`.

- [ ] **Step 1: Write the failing tests**

`tests/test_liquidation_guard.py`:

```python
from datetime import datetime, timezone
from decimal import Decimal

from polyperps.execution.types import AccountSnapshot, Intent, PositionView
from polyperps.risk.liquidation_guard import (
    LIMITS, Allow, Reject, Resize, RiskLimits, check_open, funding_exit_due, stop_price, verdict_label, vet_entry,
)

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


def test_limits_pinned():
    assert LIMITS == RiskLimits(max_leverage=3, min_liq_distance=Decimal("0.25"), stop_distance=Decimal("0.15"),
                                notional_usd=Decimal("100"), max_funding_cost=Decimal("0.02"),
                                maintenance_rate=Decimal("0.02"))


def snap(equity="1000", positions=()):
    return AccountSnapshot(equity=Decimal(equity), positions=tuple(positions), open_orders=(), stops={},
                           in_liquidation=False, ts=T0)


def pos(iid, notional, size_sign=1, liq=None, entry="100", funding="0"):
    n = Decimal(notional)
    return PositionView(instrument_id=iid, size=Decimal(size_sign) * n / Decimal(entry), entry_price=Decimal(entry),
                        notional=n, leverage=3, liquidation_price=Decimal(liq) if liq else None,
                        unrealised_pnl=Decimal(0), cumulative_funding=Decimal(funding))


def intent(notional, mark="100"):
    n = Decimal(notional)
    return Intent(instrument_id=6, side="buy", quantity=n / Decimal(mark), notional=n)


def test_entry_allowed_within_leverage():
    v = vet_entry(intent("100"), mark=Decimal(100), snapshot=snap())
    assert v == Allow()


def test_entry_resized_to_leverage_cap():
    # equity 1000, existing 2500 notional, intent 1000 -> post 3.5x > 3x; allowed = 3000-2500 = 500 -> qty 5
    v = vet_entry(intent("1000"), mark=Decimal(100), snapshot=snap(positions=[pos(7, "2500")]))
    assert v == Resize(quantity=Decimal("5.00000000"))


def test_entry_rejected_when_no_room():
    v = vet_entry(intent("100"), mark=Decimal(100), snapshot=snap(positions=[pos(7, "3000")]))
    assert isinstance(v, Reject) and "leverage" in v.reason


def test_entry_rejected_on_zero_equity():
    assert isinstance(vet_entry(intent("100"), mark=Decimal(100), snapshot=snap(equity="0")), Reject)


def test_liq_distance_floor_is_not_binding_under_3x():
    # at 3x: 1/3 - 0.02 = 0.313 >= 0.25 -> the leverage cap binds first, never the floor
    limits = RiskLimits(max_leverage=10, min_liq_distance=Decimal("0.25"), stop_distance=Decimal("0.15"),
                        notional_usd=Decimal(100), max_funding_cost=Decimal("0.02"), maintenance_rate=Decimal("0.02"))
    # equity 1000, intent 5000 -> 5x -> distance 0.18 < 0.25 -> resize to 1/(0.27) = 3.7037x -> 3703.70 notional
    v = vet_entry(intent("5000"), mark=Decimal(100), snapshot=snap(), limits=limits)
    assert isinstance(v, Resize) and Decimal("37.03") < v.quantity < Decimal("37.04")


def test_check_open_flatten_when_liquidation_close():
    assert check_open(pos(6, "100", liq="80"), mark=Decimal(100)) == "flatten"   # 20% away
    assert check_open(pos(6, "100", liq="70"), mark=Decimal(100)) == "hold"      # 30% away
    assert check_open(pos(6, "100", liq=None), mark=Decimal(100)) == "hold"


def test_stop_price():
    assert stop_price(side="long", entry=Decimal(100)) == Decimal("85.00")
    assert stop_price(side="short", entry=Decimal(100)) == Decimal("115.00")


def test_funding_exit_due():
    assert funding_exit_due(pos(6, "100", funding="-2")) is True     # paid 2% of notional
    assert funding_exit_due(pos(6, "100", funding="-1.99")) is False
    assert funding_exit_due(pos(6, "100", funding="3")) is False     # received, not paid


def test_verdict_labels():
    assert verdict_label(Allow()) == "allow"
    assert verdict_label(Resize(quantity=Decimal("1.5"))) == "resize:1.5"
    assert verdict_label(Reject(reason="x")) == "reject:x"
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_liquidation_guard.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.risk'`

- [ ] **Step 3: Write `polyperps/risk/__init__.py`** (empty) and `polyperps/risk/liquidation_guard.py`

```python
"""Spec 2.2: per-position leverage cap and liquidation-distance floor. Pure functions.

LIMITS is pre-registered (2026-09-12) and pinned by tests/test_liquidation_guard.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from typing import Literal

from polyperps.execution.types import AccountSnapshot, Intent, PositionView

_Q = Decimal("0.00000001")


@dataclass(frozen=True, slots=True, kw_only=True)
class RiskLimits:
    max_leverage: int
    min_liq_distance: Decimal
    stop_distance: Decimal
    notional_usd: Decimal
    max_funding_cost: Decimal
    maintenance_rate: Decimal


LIMITS = RiskLimits(
    max_leverage=3,
    min_liq_distance=Decimal("0.25"),
    stop_distance=Decimal("0.15"),
    notional_usd=Decimal("100"),
    max_funding_cost=Decimal("0.02"),
    maintenance_rate=Decimal("0.02"),
)


@dataclass(frozen=True, slots=True)
class Allow:
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class Resize:
    quantity: Decimal


@dataclass(frozen=True, slots=True, kw_only=True)
class Reject:
    reason: str


Verdict = Allow | Resize | Reject


def verdict_label(v: Verdict) -> str:
    if isinstance(v, Allow):
        return "allow"
    if isinstance(v, Resize):
        return f"resize:{v.quantity}"
    return f"reject:{v.reason}"


def vet_entry(intent: Intent, *, mark: Decimal, snapshot: AccountSnapshot, limits: RiskLimits = LIMITS) -> Verdict:
    if snapshot.equity <= 0:
        return Reject(reason="equity <= 0")
    existing = sum((p.notional for p in snapshot.positions), Decimal(0))
    allowed = intent.notional

    cap_notional = limits.max_leverage * snapshot.equity - existing
    if cap_notional <= 0:
        return Reject(reason=f"leverage cap {limits.max_leverage}x already used")
    allowed = min(allowed, cap_notional)

    max_lev_for_floor = Decimal(1) / (limits.min_liq_distance + limits.maintenance_rate)
    floor_notional = max_lev_for_floor * snapshot.equity - existing
    if floor_notional <= 0:
        return Reject(reason="liquidation-distance floor leaves no room")
    allowed = min(allowed, floor_notional)

    if allowed >= intent.notional:
        return Allow()
    return Resize(quantity=(allowed / mark).quantize(_Q, rounding=ROUND_DOWN))


def check_open(position: PositionView, *, mark: Decimal, limits: RiskLimits = LIMITS) -> Literal["hold", "flatten"]:
    if position.liquidation_price is None or position.size == 0:
        return "hold"
    distance = abs(position.liquidation_price - mark) / mark
    return "flatten" if distance < limits.min_liq_distance else "hold"


def stop_price(*, side: Literal["long", "short"], entry: Decimal, limits: RiskLimits = LIMITS) -> Decimal:
    factor = (1 - limits.stop_distance) if side == "long" else (1 + limits.stop_distance)
    return (entry * factor).quantize(Decimal("0.01"))


def funding_exit_due(position: PositionView, *, limits: RiskLimits = LIMITS) -> bool:
    paid = -position.cumulative_funding
    return paid >= limits.max_funding_cost * position.notional
```

- [ ] **Step 4: Run tests**

Run: `.venv/Scripts/python -m pytest tests/test_liquidation_guard.py -v`
Expected: 10 passed. (`test_entry_resized_to_leverage_cap`: allowed 500 / mark 100 = `5.00000000` after quantise.)

- [ ] **Step 5: Commit**

```bash
git add polyperps/risk tests/test_liquidation_guard.py
git commit -m "feat(phase2a): liquidation guard with pre-registered limits"
```

---

### Task 3: Portfolio exposure cap and kill-switch mechanism (spec §5.2, §5.3)

**Files:**
- Create: `polyperps/risk/portfolio_exposure.py`, `polyperps/risk/kill_switch.py`
- Test: `tests/test_portfolio_exposure.py`, `tests/test_kill_switch.py`

**Interfaces:**
- Produces: `ExposureLimits(gross: Decimal, cluster_net: Decimal)`; `EXPOSURE`; `cluster_of(category: str) -> str` (identity over `{"crypto","equity","index","commodity"}`, `"other"` otherwise); `vet_exposure(intent, *, positions: Sequence[PositionView], equity: Decimal, categories: Mapping[int, str], limits=EXPOSURE) -> Verdict` (reuses `Allow/Resize/Reject` from Task 2; `mark = intent.notional / intent.quantity`).
- Produces: `KillThresholds(pause: Decimal | None, shutdown: Decimal | None)`; `THRESHOLDS = KillThresholds(pause=None, shutdown=None)`; `evaluate(*, live_sharpe: float | None, backtest_sharpe: float | None, mode: Literal["paper","live"], thresholds=THRESHOLDS) -> Literal["run","pause","shutdown"]`; `divergence(live, backtest) -> Decimal | None` (`|live − bt| / |bt|`, `None` if `bt == 0` or either is `None`).

- [ ] **Step 1: Write the failing tests**

`tests/test_portfolio_exposure.py`:

```python
from decimal import Decimal

from polyperps.execution.types import Intent, PositionView
from polyperps.risk.liquidation_guard import Allow, Reject, Resize
from polyperps.risk.portfolio_exposure import EXPOSURE, ExposureLimits, cluster_of, vet_exposure

CATS = {6: "crypto", 7: "crypto", 9: "equity"}


def pos(iid, notional, sign=1):
    n = Decimal(notional)
    return PositionView(instrument_id=iid, size=Decimal(sign) * n / 100, entry_price=Decimal(100), notional=n,
                        leverage=3, liquidation_price=None, unrealised_pnl=Decimal(0), cumulative_funding=Decimal(0))


def intent(iid, notional, side="buy"):
    n = Decimal(notional)
    return Intent(instrument_id=iid, side=side, quantity=n / 100, notional=n)


def test_limits_pinned_and_clusters():
    assert EXPOSURE == ExposureLimits(gross=Decimal("1.0"), cluster_net=Decimal("0.6"))
    assert cluster_of("crypto") == "crypto" and cluster_of("weird") == "other"


def test_spec_scenario_btc_plus_eth_same_direction_resized_by_cluster_net():
    # equity 1000; BTC long 500; ETH long 500 intent -> gross 1000 ok, cluster net 1000 > 600 -> allowed 100 -> qty 1
    v = vet_exposure(intent(7, "500"), positions=[pos(6, "500")], equity=Decimal(1000), categories=CATS)
    assert v == Resize(quantity=Decimal("1.00000000"))


def test_opposite_direction_in_cluster_is_allowed():
    v = vet_exposure(intent(7, "500", side="sell"), positions=[pos(6, "500")], equity=Decimal(1000), categories=CATS)
    assert v == Allow()


def test_gross_cap_binds_across_clusters():
    # BTC 500 + equity 400 = 900; intent 300 equity -> gross 1200 > 1000 -> allowed 100
    v = vet_exposure(intent(9, "300"), positions=[pos(6, "500"), pos(9, "400")], equity=Decimal(1000), categories=CATS)
    assert v == Resize(quantity=Decimal("1.00000000"))


def test_reject_when_no_room():
    v = vet_exposure(intent(7, "100"), positions=[pos(6, "600")], equity=Decimal(1000), categories=CATS)
    assert isinstance(v, Reject) and "cluster" in v.reason
    v = vet_exposure(intent(9, "100"), positions=[pos(6, "1000")], equity=Decimal(1000), categories=CATS)
    assert isinstance(v, Reject) and "gross" in v.reason
```

`tests/test_kill_switch.py`:

```python
from decimal import Decimal

from polyperps.risk.kill_switch import THRESHOLDS, KillThresholds, divergence, evaluate


def test_thresholds_pinned_as_none_in_2a():
    assert THRESHOLDS == KillThresholds(pause=None, shutdown=None)


def test_none_thresholds_pause_live_run_paper():
    assert evaluate(live_sharpe=1.0, backtest_sharpe=1.0, mode="live") == "pause"
    assert evaluate(live_sharpe=1.0, backtest_sharpe=1.0, mode="paper") == "run"


def test_with_numbers():
    t = KillThresholds(pause=Decimal("0.5"), shutdown=Decimal("1.0"))
    assert evaluate(live_sharpe=1.0, backtest_sharpe=1.2, mode="live", thresholds=t) == "run"
    assert evaluate(live_sharpe=0.5, backtest_sharpe=1.2, mode="live", thresholds=t) == "pause"
    assert evaluate(live_sharpe=-0.1, backtest_sharpe=1.2, mode="live", thresholds=t) == "shutdown"
    assert evaluate(live_sharpe=None, backtest_sharpe=1.2, mode="live", thresholds=t) == "pause"
    assert evaluate(live_sharpe=1.0, backtest_sharpe=0.0, mode="live", thresholds=t) == "pause"


def test_divergence():
    assert divergence(0.6, 1.2) == Decimal("0.5")
    assert divergence(1.0, 0.0) is None and divergence(None, 1.0) is None
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_portfolio_exposure.py tests/test_kill_switch.py -v`
Expected: FAIL with `ModuleNotFoundError` for both modules

- [ ] **Step 3: Write `polyperps/risk/portfolio_exposure.py`**

```python
"""Spec 2.2b: aggregate exposure caps. Pure. Clusters = instrument category; no correlation estimate."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal

from polyperps.execution.types import Intent, PositionView
from polyperps.risk.liquidation_guard import Allow, Reject, Resize, Verdict

_Q = Decimal("0.00000001")
_KNOWN = ("crypto", "equity", "index", "commodity")


@dataclass(frozen=True, slots=True, kw_only=True)
class ExposureLimits:
    gross: Decimal        # sum |notional| <= gross x equity
    cluster_net: Decimal  # |sum signed notional in a cluster| <= cluster_net x equity


EXPOSURE = ExposureLimits(gross=Decimal("1.0"), cluster_net=Decimal("0.6"))


def cluster_of(category: str) -> str:
    return category if category in _KNOWN else "other"


def vet_exposure(
    intent: Intent,
    *,
    positions: Sequence[PositionView],
    equity: Decimal,
    categories: Mapping[int, str],
    limits: ExposureLimits = EXPOSURE,
) -> Verdict:
    sign = Decimal(1) if intent.side == "buy" else Decimal(-1)
    my_cluster = cluster_of(categories.get(intent.instrument_id, "other"))
    gross_existing = sum((p.notional for p in positions), Decimal(0))
    net_existing = sum(
        ((Decimal(1) if p.size > 0 else Decimal(-1)) * p.notional
         for p in positions if cluster_of(categories.get(p.instrument_id, "other")) == my_cluster),
        Decimal(0),
    )

    allowed = intent.notional
    gross_room = limits.gross * equity - gross_existing
    if gross_room <= 0:
        return Reject(reason="gross exposure cap already used")
    allowed = min(allowed, gross_room)

    cluster_room = limits.cluster_net * equity - sign * net_existing
    if cluster_room <= 0:
        return Reject(reason=f"cluster net cap already used in {my_cluster}")
    allowed = min(allowed, cluster_room)

    if allowed >= intent.notional:
        return Allow()
    mark = intent.notional / intent.quantity
    return Resize(quantity=(allowed / mark).quantize(_Q, rounding=ROUND_DOWN))
```

- [ ] **Step 4: Write `polyperps/risk/kill_switch.py`**

```python
"""Spec 2.6 mechanism. THRESHOLDS are None in Phase 2a: with None, a live run
evaluates to "pause" (cannot start) and a paper run to "run". The numbers are
set in Phase 2b from a passing native validation record - never here."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

Mode = Literal["paper", "live"]
Action = Literal["run", "pause", "shutdown"]


@dataclass(frozen=True, slots=True, kw_only=True)
class KillThresholds:
    pause: Decimal | None
    shutdown: Decimal | None


THRESHOLDS = KillThresholds(pause=None, shutdown=None)


def divergence(live: float | None, backtest: float | None) -> Decimal | None:
    if live is None or backtest is None or backtest == 0.0:
        return None
    return Decimal(str(abs(live - backtest) / abs(backtest)))


def evaluate(
    *,
    live_sharpe: float | None,
    backtest_sharpe: float | None,
    mode: Mode,
    thresholds: KillThresholds = THRESHOLDS,
) -> Action:
    if thresholds.pause is None or thresholds.shutdown is None:
        return "pause" if mode == "live" else "run"
    d = divergence(live_sharpe, backtest_sharpe)
    if d is None:
        return "pause"
    if d >= thresholds.shutdown:
        return "shutdown"
    if d >= thresholds.pause:
        return "pause"
    return "run"
```

- [ ] **Step 5: Run tests**

Run: `.venv/Scripts/python -m pytest tests/test_portfolio_exposure.py tests/test_kill_switch.py -v`
Expected: 5 + 4 passed. (`test_with_numbers`: (1.2−0.5)/1.2 = 0.583 ≥ 0.5 → pause; (1.2+0.1)/1.2 = 1.08 ≥ 1.0 → shutdown.)

- [ ] **Step 6: Commit**

```bash
git add polyperps/risk/portfolio_exposure.py polyperps/risk/kill_switch.py tests/test_portfolio_exposure.py tests/test_kill_switch.py
git commit -m "feat(phase2a): cluster exposure caps and kill-switch mechanism (thresholds None)"
```

---

### Task 4: Alerts and the decision trail (spec §8)

**Files:**
- Create: `polyperps/monitor/__init__.py` (empty), `polyperps/monitor/alerts.py`, `polyperps/monitor/decision_trail.py`
- Test: `tests/test_alerts.py`, `tests/test_decision_trail.py`

**Interfaces:**
- Produces (`alerts.py`): `Level = Literal["INFO","WARN","CRITICAL"]`; `Alert(level, kind: str, instrument_id: int | None, detail: dict, ts: datetime)` frozen; `Sink` Protocol `def emit(self, run_id: str, alert: Alert) -> None`; `LogSink(logger=logging.getLogger("polyperps.alerts"))`; `SqliteSink(conn)`; `TelegramSink(*, token: str, chat_id: str, transport: httpx.BaseTransport | None = None, timeout_s: float = 5.0)` — synchronous `httpx.Client`, CRITICAL only, exceptions swallowed to a WARN log that never includes the token; `Alerter(run_id, sinks: Sequence[Sink])` with `emit(alert)` calling every sink and never raising; `AlertThresholds(margin_warn=Decimal("0.35"), margin_critical=Decimal("0.28"), pnl_warn=Decimal("-0.05"), pnl_critical=Decimal("-0.10"), funding_drift_x=Decimal("3"))` + `ALERT_THRESHOLDS`; pure helpers `margin_alert(liq_distance, instrument_id, ts, t=ALERT_THRESHOLDS) -> Alert | None`, `pnl_alert(equity, start_equity, ts, t=…) -> Alert | None`, `funding_drift_alert(realised_rate, expected_rate, instrument_id, ts, t=…) -> Alert | None`.
- Produces (`decision_trail.py`): `TrailEvent(ts, kind: str, instrument_id: int | None, summary: str, data: dict)`; `reconstruct(conn, run_id, instrument_id) -> list[TrailEvent]` merging `decisions`, `orders`, `alerts` (for that instrument or `None`), `recovery` by `ts` (stable order: decision, order, alert, recovery on ties).

- [ ] **Step 1: Write the failing tests**

`tests/test_alerts.py`:

```python
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal

import httpx

from polyperps.monitor.alerts import (
    ALERT_THRESHOLDS, Alert, Alerter, AlertThresholds, LogSink, SqliteSink, TelegramSink,
    funding_drift_alert, margin_alert, pnl_alert,
)
from polyperps.storage.db import connect, list_alerts

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


def test_thresholds_pinned():
    assert ALERT_THRESHOLDS == AlertThresholds(margin_warn=Decimal("0.35"), margin_critical=Decimal("0.28"),
                                               pnl_warn=Decimal("-0.05"), pnl_critical=Decimal("-0.10"),
                                               funding_drift_x=Decimal("3"))


def test_margin_pnl_funding_helpers():
    assert margin_alert(Decimal("0.40"), 6, T0) is None
    assert margin_alert(Decimal("0.30"), 6, T0).level == "WARN"
    assert margin_alert(Decimal("0.20"), 6, T0).level == "CRITICAL"
    assert pnl_alert(Decimal(960), Decimal(1000), T0) is None
    assert pnl_alert(Decimal(940), Decimal(1000), T0).level == "WARN"
    assert pnl_alert(Decimal(890), Decimal(1000), T0).level == "CRITICAL"
    assert funding_drift_alert(Decimal("0.0002"), Decimal("0.0001"), 6, T0) is None
    a = funding_drift_alert(Decimal("0.0004"), Decimal("0.0001"), 6, T0)
    assert a is not None and a.kind == "funding_drift" and a.level == "WARN"


def test_alerter_fans_out_and_sqlite_sink_writes(caplog):
    conn = connect(":memory:")
    alerter = Alerter("run1", [LogSink(), SqliteSink(conn)])
    with caplog.at_level(logging.INFO, logger="polyperps.alerts"):
        alerter.emit(Alert(level="WARN", kind="margin_ratio", instrument_id=6, detail={"distance": "0.3"}, ts=T0))
    (row,) = list_alerts(conn, "run1")
    assert row[1:4] == ("WARN", "margin_ratio", 6) and row[4] == {"distance": "0.3"}
    assert any('"kind": "margin_ratio"' in r.message for r in caplog.records)


def test_telegram_sink_posts_only_critical_and_never_leaks_token(caplog):
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((str(request.url), json.loads(request.content)))
        return httpx.Response(200, json={"ok": True})

    sink = TelegramSink(token="SECRET123", chat_id="42", transport=httpx.MockTransport(handler))
    sink.emit("run1", Alert(level="WARN", kind="x", instrument_id=None, detail={}, ts=T0))
    assert seen == []
    sink.emit("run1", Alert(level="CRITICAL", kind="reconcile_mismatch", instrument_id=6, detail={"k": 1}, ts=T0))
    assert seen[0][0] == "https://api.telegram.org/botSECRET123/sendMessage"
    assert seen[0][1]["chat_id"] == "42" and "reconcile_mismatch" in seen[0][1]["text"]

    def failing(request):
        raise httpx.ConnectError("boom")

    bad = TelegramSink(token="SECRET123", chat_id="42", transport=httpx.MockTransport(failing))
    with caplog.at_level(logging.WARNING, logger="polyperps.alerts"):
        bad.emit("run1", Alert(level="CRITICAL", kind="x", instrument_id=None, detail={}, ts=T0))  # must not raise
    assert all("SECRET123" not in r.message for r in caplog.records)


def test_alerter_swallows_sink_errors():
    class Boom:
        def emit(self, run_id, alert):
            raise RuntimeError("sink down")

    Alerter("run1", [Boom()]).emit(Alert(level="INFO", kind="x", instrument_id=None, detail={}, ts=T0))
```

`tests/test_decision_trail.py`:

```python
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.execution.types import DecisionRow, Intent, OrderRow, State
from polyperps.monitor.decision_trail import reconstruct
from polyperps.storage.db import connect, insert_alert, insert_decision, insert_recovery, upsert_order

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


def test_reconstruct_orders_events_chronologically_from_sqlite_only():
    conn = connect(":memory:")
    insert_recovery(conn, run_id="r", ts=T0 - timedelta(minutes=1), findings_json='{"adopted": []}')
    intent = Intent(instrument_id=6, side="buy", quantity=Decimal(1), notional=Decimal(100))
    insert_decision(conn, DecisionRow(run_id="r", instrument_id=6, seq=1, ts=T0, state_before=State.FLAT,
                                      target=Decimal(1), verdicts={"vet_entry": "allow"}, intent=intent,
                                      client_order_id="r-6-1"))
    upsert_order(conn, OrderRow(client_order_id="r-6-1", run_id="r", instrument_id=6, side="buy", quantity=Decimal(1),
                                reduce_only=False, status="filled", exchange_order_id="sim-1", filled_quantity=Decimal(1),
                                avg_price=Decimal("100.05"), submitted_at=T0 + timedelta(seconds=1),
                                updated_at=T0 + timedelta(seconds=2), reason="strategy"))
    insert_alert(conn, run_id="r", level="INFO", kind="stop_placed", instrument_id=6, detail_json='{"trigger": "85"}',
                 ts=T0 + timedelta(seconds=3))
    insert_alert(conn, run_id="r", level="WARN", kind="unrelated", instrument_id=7, detail_json='{}', ts=T0)
    trail = reconstruct(conn, "r", 6)
    assert [e.kind for e in trail] == ["recovery", "decision", "order", "alert"]
    assert "r-6-1" in trail[1].summary and "filled" in trail[2].summary and "stop_placed" in trail[3].summary
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_alerts.py tests/test_decision_trail.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.monitor'`

- [ ] **Step 3: Write `polyperps/monitor/__init__.py`** (empty) and `polyperps/monitor/alerts.py`

```python
"""Spec 2.5 alerts. Three sinks behind Alerter.emit(); nothing here may raise
into the router. Telegram is CRITICAL-only; the token never reaches a log line."""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal, Protocol

import httpx

from polyperps.storage import db

Level = Literal["INFO", "WARN", "CRITICAL"]
_log = logging.getLogger("polyperps.alerts")


@dataclass(frozen=True, slots=True, kw_only=True)
class Alert:
    level: Level
    kind: str
    instrument_id: int | None
    detail: dict
    ts: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class AlertThresholds:
    margin_warn: Decimal
    margin_critical: Decimal
    pnl_warn: Decimal
    pnl_critical: Decimal
    funding_drift_x: Decimal


ALERT_THRESHOLDS = AlertThresholds(
    margin_warn=Decimal("0.35"),
    margin_critical=Decimal("0.28"),
    pnl_warn=Decimal("-0.05"),
    pnl_critical=Decimal("-0.10"),
    funding_drift_x=Decimal("3"),
)


class Sink(Protocol):
    def emit(self, run_id: str, alert: Alert) -> None: ...


def _payload(run_id: str, a: Alert) -> dict:
    return {"run_id": run_id, "ts": a.ts.isoformat(), "level": a.level, "kind": a.kind,
            "instrument_id": a.instrument_id, "detail": a.detail}


class LogSink:
    def __init__(self, logger: logging.Logger = _log) -> None:
        self._log = logger

    def emit(self, run_id: str, alert: Alert) -> None:
        line = json.dumps(_payload(run_id, alert), default=str, sort_keys=True)
        level = {"INFO": logging.INFO, "WARN": logging.WARNING, "CRITICAL": logging.CRITICAL}[alert.level]
        self._log.log(level, line)


class SqliteSink:
    def __init__(self, conn) -> None:
        self._conn = conn

    def emit(self, run_id: str, alert: Alert) -> None:
        db.insert_alert(self._conn, run_id=run_id, level=alert.level, kind=alert.kind,
                        instrument_id=alert.instrument_id, detail_json=json.dumps(alert.detail, default=str), ts=alert.ts)


class TelegramSink:
    """CRITICAL only. Failures are logged (without the token) and swallowed."""

    def __init__(self, *, token: str, chat_id: str, transport: httpx.BaseTransport | None = None,
                 timeout_s: float = 5.0) -> None:
        self._url = f"https://api.telegram.org/bot{token}/sendMessage"
        self._chat_id = chat_id
        self._http = httpx.Client(timeout=timeout_s, transport=transport)

    def emit(self, run_id: str, alert: Alert) -> None:
        if alert.level != "CRITICAL":
            return
        text = (f"[polyperps {run_id}] CRITICAL {alert.kind} instrument={alert.instrument_id} "
                f"{json.dumps(alert.detail, default=str)}")
        try:
            resp = self._http.post(self._url, json={"chat_id": self._chat_id, "text": text})
            resp.raise_for_status()
        except Exception as exc:
            _log.warning("telegram sink failed: %s", type(exc).__name__)


class Alerter:
    def __init__(self, run_id: str, sinks: Sequence[Sink]) -> None:
        self._run_id = run_id
        self._sinks = list(sinks)

    def emit(self, alert: Alert) -> None:
        for sink in self._sinks:
            try:
                sink.emit(self._run_id, alert)
            except Exception as exc:
                _log.warning("alert sink %s failed: %s", type(sink).__name__, type(exc).__name__)


# --- pure threshold helpers ---------------------------------------------------


def margin_alert(liq_distance: Decimal, instrument_id: int, ts: datetime,
                 t: AlertThresholds = ALERT_THRESHOLDS) -> Alert | None:
    if liq_distance < t.margin_critical:
        level: Level = "CRITICAL"
    elif liq_distance < t.margin_warn:
        level = "WARN"
    else:
        return None
    return Alert(level=level, kind="margin_ratio", instrument_id=instrument_id,
                 detail={"liq_distance": str(liq_distance)}, ts=ts)


def pnl_alert(equity: Decimal, start_equity: Decimal, ts: datetime, t: AlertThresholds = ALERT_THRESHOLDS) -> Alert | None:
    if start_equity <= 0:
        return None
    dd = (equity - start_equity) / start_equity
    if dd <= t.pnl_critical:
        level: Level = "CRITICAL"
    elif dd <= t.pnl_warn:
        level = "WARN"
    else:
        return None
    return Alert(level=level, kind="pnl_drawdown", instrument_id=None, detail={"drawdown": str(dd)}, ts=ts)


def funding_drift_alert(realised_rate: Decimal, expected_rate: Decimal, instrument_id: int, ts: datetime,
                        t: AlertThresholds = ALERT_THRESHOLDS) -> Alert | None:
    if expected_rate == 0:
        return None
    ratio = abs(realised_rate) / abs(expected_rate)
    if ratio < t.funding_drift_x:
        return None
    return Alert(level="WARN", kind="funding_drift", instrument_id=instrument_id,
                 detail={"realised": str(realised_rate), "expected": str(expected_rate), "ratio": str(ratio)}, ts=ts)
```

- [ ] **Step 4: Write `polyperps/monitor/decision_trail.py`**

```python
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
```

- [ ] **Step 5: Run tests**

Run: `.venv/Scripts/python -m pytest tests/test_alerts.py tests/test_decision_trail.py -v`
Expected: 5 + 1 passed

- [ ] **Step 6: Commit**

```bash
git add polyperps/monitor tests/test_alerts.py tests/test_decision_trail.py
git commit -m "feat(phase2a): alerts with log/sqlite/telegram sinks and decision-trail reconstruction"
```

---

### Task 5: Executor protocol and `SimExecutor` (spec §4.2, §4.3)

**Files:**
- Create: `polyperps/execution/executor.py`, `polyperps/execution/sim_executor.py`
- Test: `tests/test_sim_executor.py`

**Interfaces:**
- Produces (`executor.py`): `class ExecutorTimeout(RuntimeError)`, `class GateClosed(RuntimeError)`, `Executor` Protocol (`name: str`; `async submit(order) -> OrderAck`; `async cancel(client_order_id) -> None`; `async place_stop(instrument_id, trigger_price) -> StopAck`; `async heartbeat() -> None`; `async snapshot() -> AccountSnapshot`; `def events() -> AsyncIterator[OrderUpdate | FillUpdate]`; `async close() -> None`).
- Produces (`sim_executor.py`): `MAINTENANCE_RATE = LIMITS.maintenance_rate`; `SimExecutor(run_id, *, equity: Decimal, taker_fee_rate: Decimal, spread_bps: Decimal = BAR.proxy_spread_bps, impact_bps: Decimal = BAR.impact_bps, leverage: int = LIMITS.max_leverage, clock=_utcnow, persist: Callable[[str], None] | None = None)`; `update_mark(instrument_id, mark)`; `apply_funding(instrument_id, rate)`; `check_triggers() -> list[FillUpdate]`; `fail_next: Literal["timeout","reject"] | None` attribute; `drain_events() -> list[OrderUpdate | FillUpdate]` (synchronous helper for tests); `to_json()` / `classmethod from_json(run_id, text, **kw)`; `heartbeat_count`.
- Fill price: `mark × (1 + s·(spread_bps/2 + impact_bps)/10_000)` with `s=+1` buy / `−1` sell; `fee = quantity × price × taker_fee_rate`, deducted from cash. Positions use average-cost entry (same rules as the harness `trade_to`). `liquidation_price` long = `entry × (1 − 1/leverage + MAINTENANCE_RATE)`, short = `entry × (1 + 1/leverage − MAINTENANCE_RATE)`. `equity = cash + Σ unrealised`. On `fail_next == "timeout"`: the fill **is applied** and events queued, then `ExecutorTimeout` is raised (the exchange got it; our ack was lost). On `"reject"`: `OrderAck(status="rejected")`, nothing applied.

- [ ] **Step 1: Write the failing tests**

`tests/test_sim_executor.py`:

```python
import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from polyperps.execution.executor import ExecutorTimeout
from polyperps.execution.sim_executor import MAINTENANCE_RATE, SimExecutor
from polyperps.execution.types import FillUpdate, OrderRequest, OrderUpdate

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)
FEE = Decimal("0.0004")


def make(persist=None, **kw):
    ex = SimExecutor("run1", equity=Decimal(1000), taker_fee_rate=FEE, spread_bps=Decimal(10),
                     impact_bps=Decimal(5), clock=lambda: T0, persist=persist, **kw)
    ex.update_mark(6, Decimal(100))
    return ex


def req(cid="run1-6-1", side="buy", qty="1", reduce_only=False):
    return OrderRequest(client_order_id=cid, instrument_id=6, side=side, quantity=Decimal(qty),
                        reduce_only=reduce_only, ts=T0)


async def test_buy_fills_at_mark_plus_costs_and_emits_events():
    ex = make()
    ack = await ex.submit(req())
    assert ack.status == "accepted" and ack.exchange_order_id == "sim-1"
    ev = ex.drain_events()
    assert [type(e) for e in ev] == [OrderUpdate, FillUpdate]
    fill = ev[1]
    # mark 100 * (1 + (5 + 5)/10000) = 100.10 ; fee = 1 * 100.10 * 0.0004 = 0.04004
    assert fill.price == Decimal("100.10") and fill.fee == Decimal("0.04004")
    snap = await ex.snapshot()
    p = snap.position(6)
    assert p.size == 1 and p.entry_price == Decimal("100.10")
    assert p.liquidation_price == (Decimal("100.10") * (1 - Decimal(1) / 3 + MAINTENANCE_RATE)).quantize(Decimal("0.01"))
    assert snap.equity == Decimal(1000) - fill.fee + (Decimal(100) - Decimal("100.10"))  # marked at mark 100


async def test_sell_short_and_funding_sign():
    ex = make()
    await ex.submit(req(side="sell"))
    ex.drain_events()
    p = (await ex.snapshot()).position(6)
    assert p.size == -1 and p.entry_price == Decimal("99.90")
    ex.apply_funding(6, Decimal("0.001"))   # positive funding: shorts RECEIVE
    p2 = (await ex.snapshot()).position(6)
    assert p2.cumulative_funding == Decimal("0.1")   # 1 * 100 * 0.001, received
    ex2 = make()
    await ex2.submit(req())
    ex2.drain_events()
    ex2.apply_funding(6, Decimal("0.001"))
    assert (await ex2.snapshot()).position(6).cumulative_funding == Decimal("-0.1")  # long pays


async def test_stop_fires_from_check_triggers_without_router():
    ex = make()
    await ex.submit(req())
    ex.drain_events()
    await ex.place_stop(6, Decimal(85))
    assert (await ex.snapshot()).stops == {6: Decimal(85)}
    ex.update_mark(6, Decimal(90))
    assert ex.check_triggers() == []
    ex.update_mark(6, Decimal(84))
    fills = ex.check_triggers()
    assert len(fills) == 1 and fills[0].side == "sell" and fills[0].quantity == 1
    snap = await ex.snapshot()
    assert snap.position(6) is None and snap.stops == {}


async def test_timeout_applies_fill_but_raises():
    ex = make()
    ex.fail_next = "timeout"
    with pytest.raises(ExecutorTimeout):
        await ex.submit(req())
    assert ex.fail_next is None
    assert (await ex.snapshot()).position(6).size == 1
    assert any(isinstance(e, FillUpdate) for e in ex.drain_events())


async def test_reject_applies_nothing():
    ex = make()
    ex.fail_next = "reject"
    ack = await ex.submit(req())
    assert ack.status == "rejected" and (await ex.snapshot()).position(6) is None


async def test_persist_and_restore_round_trip():
    saved = []
    ex = make(persist=saved.append)
    await ex.submit(req())
    ex.drain_events()
    await ex.place_stop(6, Decimal(85))
    assert saved  # persisted after each mutation
    restored = SimExecutor.from_json("run1", saved[-1], taker_fee_rate=FEE, clock=lambda: T0)
    restored.update_mark(6, Decimal(100))
    s1, s2 = await ex.snapshot(), await restored.snapshot()
    assert s1.positions == s2.positions and s1.stops == s2.stops and s1.equity == s2.equity


async def test_heartbeat_counts_and_reduce_only_cannot_open():
    ex = make()
    await ex.heartbeat()
    assert ex.heartbeat_count == 1
    ack = await ex.submit(req(reduce_only=True))
    assert ack.status == "rejected" and "reduce_only" in ack.reason
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_sim_executor.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.execution.executor'`

- [ ] **Step 3: Write `polyperps/execution/executor.py`**

```python
"""The only surface the router talks to (spec section 4.2)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Protocol

from polyperps.execution.types import AccountSnapshot, FillUpdate, OrderAck, OrderRequest, OrderUpdate, StopAck


class ExecutorTimeout(RuntimeError):
    """The executor did not acknowledge in time; the order MAY have landed."""


class GateClosed(RuntimeError):
    """A live executor was requested while a live-order gate is closed."""


class Executor(Protocol):
    name: str

    async def submit(self, order: OrderRequest) -> OrderAck: ...
    async def cancel(self, client_order_id: str) -> None: ...
    async def place_stop(self, instrument_id: int, trigger_price: Decimal) -> StopAck: ...
    async def heartbeat(self) -> None: ...
    async def snapshot(self) -> AccountSnapshot: ...
    def events(self) -> AsyncIterator[OrderUpdate | FillUpdate]: ...
    async def close(self) -> None: ...
```

- [ ] **Step 4: Write `polyperps/execution/sim_executor.py`**

```python
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
        self._n += 1
        xid = f"sim-{self._n}"
        fill = self._fill(order, now)
        self._queue.put_nowait(OrderUpdate(client_order_id=order.client_order_id, status="filled",
                                           filled_quantity=order.quantity, ts=now))
        self._queue.put_nowait(fill)
        self._save()
        if mode == "timeout":
            raise ExecutorTimeout(f"sim: injected timeout for {order.client_order_id}")
        return OrderAck(client_order_id=order.client_order_id, exchange_order_id=xid, status="accepted", reason="", ts=now)

    def _fill(self, order: OrderRequest, now: datetime) -> FillUpdate:
        mark = self._marks[order.instrument_id]
        s = Decimal(1) if order.side == "buy" else Decimal(-1)
        price = (mark * (1 + s * self._slip)).quantize(_P, rounding=ROUND_HALF_EVEN)
        fee = order.quantity * price * self._fee
        self._cash -= fee
        self._apply_position(order.instrument_id, s * order.quantity, price)
        return FillUpdate(client_order_id=order.client_order_id, instrument_id=order.instrument_id, side=order.side,
                          quantity=order.quantity, price=price, fee=fee, ts=now)

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
        })

    @classmethod
    def from_json(cls, run_id: str, text: str, **kw) -> "SimExecutor":
        d = json.loads(text)
        ex = cls(run_id, equity=Decimal(d["cash"]), **kw)
        ex._n = d["n"]
        for i, p in d["positions"].items():
            ex._pos[int(i)] = _Pos(Decimal(p["size"]), Decimal(p["entry"]), Decimal(p["funding"]))
        ex._stops = {int(i): Decimal(t) for i, t in d["stops"].items()}
        return ex

    def _save(self) -> None:
        if self._persist is not None:
            self._persist(self.to_json())
```

- [ ] **Step 5: Run tests**

Run: `.venv/Scripts/python -m pytest tests/test_sim_executor.py -v`
Expected: 7 passed. Arithmetic checks: short fill `100 × (1 − 0.001) = 99.90`; short funding `−(−1)×100×0.001 = +0.1` (received); equity after the buy = `1000 − 0.04004 + 1×(100 − 100.10)`.

- [ ] **Step 6: Commit**

```bash
git add polyperps/execution/executor.py polyperps/execution/sim_executor.py tests/test_sim_executor.py
git commit -m "feat(phase2a): executor protocol and paper SimExecutor with self-firing stops"
```

---

### Task 6: The order router (spec §6)

**Files:**
- Modify: `polyperps/execution/sim_executor.py` (add `fail_queue` + `"drop"` mode, `cancel_stop`), `polyperps/execution/executor.py` (add `cancel_stop` to the protocol), `polyperps/execution/types.py` (add `ReconcileNow`)
- Create: `polyperps/execution/order_router.py`
- Test: `tests/test_order_router.py`

**Interfaces:**
- Modify (`types.py`): `@dataclass(frozen=True, slots=True) class ReconcileNow: reason: str = ""` — an executor event asking the portfolio to reconcile immediately.
- Modify (`executor.py`): Protocol gains `async cancel_stop(self, instrument_id: int) -> None`; `events()` yields `OrderUpdate | FillUpdate | ReconcileNow`.
- Modify (`sim_executor.py`): `fail_queue: list[Literal["timeout","reject","drop"]]` consumed first (then `fail_next`); `"drop"` raises `ExecutorTimeout` **without** applying anything; `async cancel_stop(iid)` deletes the stop.
- Produces (`order_router.py`):
  - `apply_guards(intent: Intent, verdicts: Sequence[tuple[str, Verdict]]) -> tuple[Intent | None, dict[str, str]]` — any `Reject` → `(None, labels)`; else quantity = min over `Resize`s (or unchanged); notional rescaled proportionally.
  - `InstrumentRouter(*, run_id, instrument_id, category, strategy, executor, conn, alerter, categories: Mapping[int, str], clock=_utcnow, limits=LIMITS, exposure=EXPOSURE, ack_timeout_s: float = 10.0)`; attributes `state: State`, `seq: int`, `size: Decimal`, `entry: Decimal | None`, `stop_trigger: Decimal | None`, `cumulative_funding: Decimal`; methods `async on_bar(history: Sequence[Bar], snapshot: AccountSnapshot, kill: Action) -> None`, `async on_fast(mark: Decimal, snapshot: AccountSnapshot) -> None`, `async handle_event(ev: OrderUpdate | FillUpdate) -> None`, `async halt(reason: str) -> None`, `def clear_halt() -> None`, `async replace_stop() -> None`, `def load_local(row: PositionLocalRow) -> None`.
  - `Portfolio(*, run_id, executor, conn, alerter, routers: Mapping[int, InstrumentRouter], reconcile: Callable[[], Awaitable[None]] | None = None)`; `async on_bar(histories: Mapping[int, Sequence[Bar]], kill: Action)` (one `snapshot()` for all routers), `async on_fast(marks: Mapping[int, Decimal])`, `async dispatch(ev)`, `async run_event_pump()`.
- Semantics exactly as spec §6.2–6.3; decisions and orders rows are written **before** `submit`; `client_order_id = f"{run_id}-{instrument_id}-{seq}"`; timeout → `snapshot()`: if the position moved by exactly the requested quantity in the requested direction the order is **adopted** (`OrderAck(status="accepted", reason="adopted after timeout")`, WARN alert `ack_lost`); otherwise retry **once** with the same id (WARN `retry`); still nothing → `halt("order lost")` + CRITICAL. Fills update local size/entry with the same average-cost rules as the sim. Entry fill → `OPEN` + `place_stop(stop_price(...))` + INFO alert `stop_placed`. Exit fill (size → 0) → `FLAT`, stop cleared via `cancel_stop`, `strategy.on_flatten()` called. A fill whose id is not ours and takes size to 0 (a stop) → `FLAT` + WARN `stop_fired` + `on_flatten()`. `on_fast`: margin alert via `margin_alert`; `check_open == "flatten"` → exit reason `liq_distance`; `funding_exit_due` → exit reason `funding_cost`. Kill `shutdown` → exit reason `kill_shutdown` then `HALTED` + CRITICAL `kill_switch`; `pause` → no new entries (decision note `kill:pause`).

- [ ] **Step 1: Write the failing tests**

`tests/test_order_router.py`:

```python
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.backtest.bars import Bar
from polyperps.execution.order_router import InstrumentRouter, Portfolio, apply_guards
from polyperps.execution.sim_executor import SimExecutor
from polyperps.execution.types import Intent, OrderRequest, State
from polyperps.exchange.types import SourceType
from polyperps.monitor.alerts import Alerter, SqliteSink
from polyperps.monitor.decision_trail import reconstruct
from polyperps.risk.liquidation_guard import Allow, Reject, Resize
from polyperps.storage.db import connect, get_order, get_positions_local, list_alerts, list_decisions

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)
H = timedelta(hours=1)
FEE = Decimal("0.0004")
CATS = {6: "crypto", 7: "crypto"}


class Strat:
    name = "t"; params = {}
    def __init__(self, target=1):
        self.t = Decimal(target); self.flattened = 0
    def target(self, history):
        return self.t
    def on_flatten(self):
        self.flattened += 1


def bar(i, close="100", funding="0.0001"):
    ts = T0 + i * H
    c = Decimal(close)
    return Bar(instrument_id=6, source_type=SourceType.POLYMARKET_WS, open_ts=ts, open=c, high=c, low=c, close=c,
               index_close=None, funding_rate=Decimal(funding), spread_bps=Decimal(5), spread_source="constant",
               complete=True)


def make(target=1, equity="1000"):
    conn = connect(":memory:")
    ex = SimExecutor("r", equity=Decimal(equity), taker_fee_rate=FEE, clock=lambda: T0)
    ex.update_mark(6, Decimal(100)); ex.update_mark(7, Decimal(100))
    alerter = Alerter("r", [SqliteSink(conn)])
    strat = Strat(target)
    router = InstrumentRouter(run_id="r", instrument_id=6, category="crypto", strategy=strat, executor=ex, conn=conn,
                              alerter=alerter, categories=CATS, clock=lambda: T0)
    pf = Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter, routers={6: router})
    return conn, ex, strat, router, pf


async def pump(pf, ex):
    for ev in ex.drain_events():
        await pf.dispatch(ev)


def kinds(conn):
    return [a[2] for a in list_alerts(conn, "r")]


def test_apply_guards():
    i = Intent(instrument_id=6, side="buy", quantity=Decimal(1), notional=Decimal(100))
    assert apply_guards(i, [("a", Allow()), ("b", Allow())]) == (i, {"a": "allow", "b": "allow"})
    out, labels = apply_guards(i, [("a", Resize(quantity=Decimal("0.5"))), ("b", Resize(quantity=Decimal("0.25")))])
    assert out.quantity == Decimal("0.25") and out.notional == Decimal("25.00") and labels["b"] == "resize:0.25"
    assert apply_guards(i, [("a", Allow()), ("b", Reject(reason="x"))])[0] is None


async def test_entry_then_open_with_stop_and_rows():
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "run")
    assert router.state is State.ENTRY_PENDING
    order = get_order(conn, "r-6-1")
    assert order.status == "accepted" and order.exchange_order_id == "sim-1"
    d = list_decisions(conn, "r", 6)[0]
    assert d.client_order_id == "r-6-1" and d.verdicts == {"vet_entry": "allow", "vet_exposure": "allow"}
    await pump(pf, ex)
    assert router.state is State.OPEN and router.size == 1 and router.entry == Decimal("100.10")
    assert router.stop_trigger == Decimal("85.08")           # 100.10 * 0.85 = 85.085 -> ROUND_HALF_EVEN -> 85.08
    assert (await ex.snapshot()).stops == {6: router.stop_trigger}
    assert get_positions_local(conn, "r")[6].state is State.OPEN
    assert get_order(conn, "r-6-1").status == "filled"
    assert "stop_placed" in kinds(conn)


async def test_reject_and_resize_from_exposure():
    conn, ex, strat, router, pf = make()
    await ex.submit(OrderRequest(client_order_id="pre", instrument_id=7, side="buy", quantity=Decimal("6"),
                                 reduce_only=False, ts=T0)); ex.drain_events()   # 600 notional in the crypto cluster
    await pf.on_bar({6: [bar(0)]}, "run")
    assert router.state is State.FLAT and get_order(conn, "r-6-1") is None
    assert list_decisions(conn, "r", 6)[0].verdicts["vet_exposure"].startswith("reject:")
    conn2, ex2, _, router2, pf2 = make()
    await ex2.submit(OrderRequest(client_order_id="pre", instrument_id=7, side="buy", quantity=Decimal("5.5"),
                                  reduce_only=False, ts=T0)); ex2.drain_events()  # ~550 notional -> room ~50 -> qty ~0.5
    await pf2.on_bar({6: [bar(0)]}, "run")
    assert Decimal("0.49") < get_order(conn2, "r-6-1").quantity <= Decimal("0.5")


async def test_exit_on_target_zero_and_flip():
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    strat.t = Decimal(0)
    await pf.on_bar({6: [bar(0), bar(1)]}, "run")
    assert router.state is State.EXIT_PENDING and get_order(conn, "r-6-2").reduce_only is True
    await pump(pf, ex)
    assert router.state is State.FLAT and router.size == 0 and strat.flattened == 1
    assert (await ex.snapshot()).stops == {}
    strat.t = Decimal(-1)
    await pf.on_bar({6: [bar(0), bar(1), bar(2)]}, "run"); await pump(pf, ex)
    assert router.size == -1
    strat.t = Decimal(1)                                        # flip: exit this bar only
    await pf.on_bar({6: [bar(i) for i in range(4)]}, "run"); await pump(pf, ex)
    assert router.state is State.FLAT
    await pf.on_bar({6: [bar(i) for i in range(5)]}, "run"); await pump(pf, ex)
    assert router.size == 1


async def test_timeout_adopts_landed_order_exactly_one_fill():
    conn, ex, strat, router, pf = make()
    ex.fail_next = "timeout"
    await pf.on_bar({6: [bar(0)]}, "run")
    o = get_order(conn, "r-6-1")
    assert o.status == "accepted" and "adopted" in o.reason
    await pump(pf, ex)
    assert router.state is State.OPEN and router.size == 1 and (await ex.snapshot()).position(6).size == 1
    assert "ack_lost" in kinds(conn)


async def test_dropped_then_retry_succeeds_with_same_id():
    conn, ex, strat, router, pf = make()
    ex.fail_queue = ["drop"]
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    assert router.state is State.OPEN and get_order(conn, "r-6-1").status == "filled"
    assert len([a for a in list_alerts(conn, "r") if a[2] == "retry"]) == 1


async def test_dropped_twice_halts():
    conn, ex, strat, router, pf = make()
    ex.fail_queue = ["drop", "drop"]
    await pf.on_bar({6: [bar(0)]}, "run")
    assert router.state is State.HALTED and (await ex.snapshot()).position(6) is None
    assert [a for a in list_alerts(conn, "r") if a[1] == "CRITICAL"]
    await pf.on_bar({6: [bar(0), bar(1)]}, "run")            # halted: no new orders
    assert get_order(conn, "r-6-2") is None
    router.clear_halt()
    assert router.state is State.FLAT


async def test_rejected_ack_reverts_state():
    conn, ex, strat, router, pf = make()
    ex.fail_next = "reject"
    await pf.on_bar({6: [bar(0)]}, "run")
    assert router.state is State.FLAT and get_order(conn, "r-6-1").status == "rejected"


async def test_fast_loop_liq_distance_exit():
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    ex.update_mark(6, Decimal(90))     # liq ~68.74 -> distance 23.6% < 25%
    await pf.on_fast({6: Decimal(90)})
    assert router.state is State.EXIT_PENDING and get_order(conn, "r-6-2").reason == "liq_distance"
    assert "margin_ratio" in kinds(conn)


async def test_fast_loop_funding_cost_exit():
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    for _ in range(3):
        ex.apply_funding(6, Decimal("0.007"))   # long pays 0.7 each -> 2.1 >= 2% of 100
    await pf.on_fast({6: Decimal(100)})
    assert get_order(conn, "r-6-2").reason == "funding_cost"


async def test_kill_switch_pause_and_shutdown():
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "pause")
    assert router.state is State.FLAT and list_decisions(conn, "r", 6)[0].note == "kill:pause"
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    await pf.on_bar({6: [bar(0), bar(1)]}, "shutdown"); await pump(pf, ex)
    assert router.state is State.HALTED and router.size == 0
    assert get_order(conn, "r-6-2").reason == "kill_shutdown" and "kill_switch" in kinds(conn)


async def test_external_stop_fill_flattens_and_alerts():
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    ex.update_mark(6, Decimal(80))
    ex.check_triggers()
    await pump(pf, ex)
    assert router.state is State.FLAT and strat.flattened == 1 and "stop_fired" in kinds(conn)


async def test_decision_trail_reconstructable_from_sqlite_only():
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    strat.t = Decimal(0)
    await pf.on_bar({6: [bar(0), bar(1)]}, "run"); await pump(pf, ex)
    trail = reconstruct(conn, "r", 6)
    assert [e.kind for e in trail][:3] == ["decision", "order", "alert"]      # decision -> fill -> stop_placed
    assert any("r-6-2" in e.summary and "filled" in e.summary for e in trail)
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_order_router.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.execution.order_router'`

- [ ] **Step 3: Small modifications to `types.py`, `executor.py`, `sim_executor.py`**

`types.py` — append:

```python
@dataclass(frozen=True, slots=True)
class ReconcileNow:
    reason: str = ""
```

`executor.py` — add `ReconcileNow` to the imports and to the `events()` return type; add to the Protocol:

```python
    async def cancel_stop(self, instrument_id: int) -> None: ...
```

`sim_executor.py` — in `__init__` add `self.fail_queue: list[Literal["timeout", "reject", "drop"]] = []`; at the top of `submit` replace `mode, self.fail_next = self.fail_next, None` with:

```python
        if self.fail_queue:
            mode = self.fail_queue.pop(0)
        else:
            mode, self.fail_next = self.fail_next, None
        if mode == "drop":
            raise ExecutorTimeout(f"sim: injected drop for {order.client_order_id}")
```

and add:

```python
    async def cancel_stop(self, instrument_id: int) -> None:
        self._stops.pop(instrument_id, None)
        self._save()
```

- [ ] **Step 4: Write `polyperps/execution/order_router.py`**

```python
"""Spec section 6: per-instrument state machine and the portfolio that drives it.

Rows before side effects: decisions + orders are written BEFORE executor.submit;
positions_local after every state change. Client order ids are
f"{run_id}-{instrument_id}-{seq}" and are reused on a retry after timeout.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from typing import Literal

from polyperps.backtest.bars import Bar
from polyperps.backtest.strategy import Strategy, clamp_target
from polyperps.execution.executor import Executor, ExecutorTimeout
from polyperps.execution.types import (
    AccountSnapshot, DecisionRow, FillUpdate, Intent, OrderAck, OrderRequest, OrderRow, OrderUpdate,
    PositionLocalRow, ReconcileNow, State,
)
from polyperps.monitor.alerts import Alert, Alerter, margin_alert
from polyperps.risk.kill_switch import Action
from polyperps.risk.liquidation_guard import (
    LIMITS, Reject, Resize, RiskLimits, Verdict, check_open, funding_exit_due, stop_price, verdict_label, vet_entry,
)
from polyperps.risk.portfolio_exposure import EXPOSURE, ExposureLimits, vet_exposure
from polyperps.storage import db

_Q = Decimal("0.00000001")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def apply_guards(intent: Intent, verdicts: Sequence[tuple[str, Verdict]]) -> tuple[Intent | None, dict[str, str]]:
    labels = {name: verdict_label(v) for name, v in verdicts}
    if any(isinstance(v, Reject) for _, v in verdicts):
        return None, labels
    qty = min([v.quantity for _, v in verdicts if isinstance(v, Resize)] + [intent.quantity])
    if qty == intent.quantity:
        return intent, labels
    notional = (intent.notional * qty / intent.quantity).quantize(Decimal("0.01"))
    return Intent(instrument_id=intent.instrument_id, side=intent.side, quantity=qty, notional=notional,
                  reduce_only=intent.reduce_only, reason=intent.reason), labels


class InstrumentRouter:
    def __init__(
        self,
        *,
        run_id: str,
        instrument_id: int,
        category: str,
        strategy: Strategy,
        executor: Executor,
        conn,
        alerter: Alerter,
        categories: Mapping[int, str],
        clock: Callable[[], datetime] = _utcnow,
        limits: RiskLimits = LIMITS,
        exposure: ExposureLimits = EXPOSURE,
        ack_timeout_s: float = 10.0,
    ) -> None:
        self.run_id, self.instrument_id, self.category = run_id, instrument_id, category
        self.strategy, self.executor, self.conn, self.alerter = strategy, executor, conn, alerter
        self.categories, self.clock, self.limits, self.exposure = categories, clock, limits, exposure
        self.ack_timeout_s = ack_timeout_s
        self.state = State.FLAT
        self.seq = 0
        self.size = Decimal(0)
        self.entry: Decimal | None = None
        self.stop_trigger: Decimal | None = None
        self.cumulative_funding = Decimal(0)
        self._pending_cid: str | None = None

    # --- persistence helpers ------------------------------------------------
    def load_local(self, row: PositionLocalRow) -> None:
        self.state, self.size, self.entry = row.state, row.size, row.entry_price
        self.stop_trigger, self.cumulative_funding = row.stop_trigger, row.cumulative_funding

    def _persist(self) -> None:
        db.upsert_position_local(self.conn, PositionLocalRow(
            run_id=self.run_id, instrument_id=self.instrument_id, state=self.state, size=self.size,
            entry_price=self.entry, stop_trigger=self.stop_trigger, stop_order_id=None,
            cumulative_funding=self.cumulative_funding, updated_at=self.clock()))

    def _set_state(self, state: State) -> None:
        self.state = state
        self._persist()

    def _alert(self, level: Literal["INFO", "WARN", "CRITICAL"], kind: str, **detail) -> None:
        self.alerter.emit(Alert(level=level, kind=kind, instrument_id=self.instrument_id,
                                detail={k: str(v) for k, v in detail.items()}, ts=self.clock()))

    def _record(self, *, target: Decimal | None, verdicts: dict[str, str], intent: Intent | None,
                cid: str | None, note: str = "") -> None:
        self.seq += 1
        db.insert_decision(self.conn, DecisionRow(run_id=self.run_id, instrument_id=self.instrument_id, seq=self.seq,
                                                  ts=self.clock(), state_before=self.state, target=target,
                                                  verdicts=verdicts, intent=intent, client_order_id=cid, note=note))

    # --- bar cycle --------------------------------------------------------------
    async def on_bar(self, history: Sequence[Bar], snapshot: AccountSnapshot, kill: Action) -> None:
        if self.state in (State.HALTED, State.LIQUIDATED, State.ENTRY_PENDING, State.EXIT_PENDING):
            self._record(target=None, verdicts={}, intent=None, cid=None, note=f"skip:{self.state.value}")
            return
        mark = history[-1].close
        if mark is None:
            self._record(target=None, verdicts={}, intent=None, cid=None, note="skip:no_close")
            return
        if kill == "shutdown":
            if self.state is State.OPEN:
                await self._exit(mark, "kill_shutdown", target=None)
            self._alert("CRITICAL", "kill_switch", action="shutdown")
            self._set_state(State.HALTED)
            return
        target = clamp_target(self.strategy.target(history))
        if self.state is State.FLAT:
            if target == 0 or kill == "pause":
                self._record(target=target, verdicts={}, intent=None, cid=None,
                             note="kill:pause" if kill == "pause" else "flat:no_target")
                return
            side = "buy" if target > 0 else "sell"
            qty = (self.limits.notional_usd / mark).quantize(_Q, rounding=ROUND_DOWN)
            intent = Intent(instrument_id=self.instrument_id, side=side, quantity=qty, notional=self.limits.notional_usd)
            final, labels = apply_guards(intent, [
                ("vet_entry", vet_entry(intent, mark=mark, snapshot=snapshot, limits=self.limits)),
                ("vet_exposure", vet_exposure(intent, positions=snapshot.positions, equity=snapshot.equity,
                                              categories=self.categories, limits=self.exposure)),
            ])
            if final is None:
                self._record(target=target, verdicts=labels, intent=None, cid=None, note="rejected")
                return
            await self._send(final, target=target, verdicts=labels)
            return
        # OPEN
        flip = (self.size > 0 and target < 0) or (self.size < 0 and target > 0)
        if target == 0 or flip:
            await self._exit(mark, "strategy" if target == 0 else "flip", target=target)
        else:
            self._record(target=target, verdicts={}, intent=None, cid=None, note="hold")

    async def _exit(self, mark: Decimal, reason: str, *, target: Decimal | None) -> None:
        side = "sell" if self.size > 0 else "buy"
        intent = Intent(instrument_id=self.instrument_id, side=side, quantity=abs(self.size),
                        notional=(abs(self.size) * mark).quantize(Decimal("0.01")), reduce_only=True, reason=reason)
        await self._send(intent, target=target, verdicts={})

    # --- fast loop --------------------------------------------------------------
    async def on_fast(self, mark: Decimal, snapshot: AccountSnapshot) -> None:
        if self.state is not State.OPEN:
            return
        pos = snapshot.position(self.instrument_id)
        if pos is None:
            return  # vanished: reconciliation decides
        if pos.liquidation_price is not None:
            a = margin_alert(abs(pos.liquidation_price - mark) / mark, self.instrument_id, self.clock())
            if a is not None:
                self.alerter.emit(a)
        if check_open(pos, mark=mark, limits=self.limits) == "flatten":
            await self._exit(mark, "liq_distance", target=None)
        elif funding_exit_due(pos, limits=self.limits):
            await self._exit(mark, "funding_cost", target=None)

    # --- sending with idempotent retry ----------------------------------------
    async def _send(self, intent: Intent, *, target: Decimal | None, verdicts: dict[str, str]) -> None:
        cid = f"{self.run_id}-{self.instrument_id}-{self.seq + 1}"
        self._record(target=target, verdicts=verdicts, intent=intent, cid=cid, note=intent.reason)
        now = self.clock()
        db.upsert_order(self.conn, OrderRow(client_order_id=cid, run_id=self.run_id, instrument_id=self.instrument_id,
                                            side=intent.side, quantity=intent.quantity, reduce_only=intent.reduce_only,
                                            status="submitting", exchange_order_id=None, filled_quantity=Decimal(0),
                                            avg_price=None, submitted_at=now, updated_at=now, reason=intent.reason))
        prior = self.state
        self._pending_cid = cid
        self._set_state(State.EXIT_PENDING if intent.reduce_only else State.ENTRY_PENDING)
        req = OrderRequest(client_order_id=cid, instrument_id=self.instrument_id, side=intent.side,
                           quantity=intent.quantity, reduce_only=intent.reduce_only, ts=now)
        ack = await self._submit_with_recovery(req)
        if ack is None:
            await self.halt("order lost after timeout and retry")
            return
        if ack.status == "rejected":
            self._update_order(cid, status="rejected", reason=ack.reason)
            self._alert("WARN", "order_rejected", client_order_id=cid, reason=ack.reason)
            self._pending_cid = None
            self._set_state(prior)
            return
        self._update_order(cid, status="accepted", exchange_order_id=ack.exchange_order_id,
                           reason=ack.reason or intent.reason)

    async def _submit_with_recovery(self, req: OrderRequest) -> OrderAck | None:
        for attempt in (1, 2):
            try:
                return await asyncio.wait_for(self.executor.submit(req), self.ack_timeout_s)
            except (asyncio.TimeoutError, ExecutorTimeout):
                snap = await self.executor.snapshot()
                if self._landed(req, snap):
                    self._alert("WARN", "ack_lost", client_order_id=req.client_order_id, attempt=attempt)
                    return OrderAck(client_order_id=req.client_order_id, exchange_order_id=None, status="accepted",
                                    reason="adopted after timeout", ts=self.clock())
                if attempt == 1:
                    self._alert("WARN", "retry", client_order_id=req.client_order_id)
        return None

    def _landed(self, req: OrderRequest, snap: AccountSnapshot) -> bool:
        if req.client_order_id in snap.open_orders:
            return True
        delta = req.quantity if req.side == "buy" else -req.quantity
        pos = snap.position(self.instrument_id)
        actual = pos.size if pos is not None else Decimal(0)
        return actual == self.size + delta

    def _update_order(self, cid: str, **changes) -> None:
        row = db.get_order(self.conn, cid)
        if row is None:
            return
        db.upsert_order(self.conn, replace(row, **changes, updated_at=self.clock()))

    # --- events -------------------------------------------------------------------
    async def handle_event(self, ev: OrderUpdate | FillUpdate) -> None:
        if isinstance(ev, OrderUpdate):
            if ev.client_order_id == self._pending_cid and ev.status in ("cancelled", "auto_cancelled", "rejected"):
                self._update_order(ev.client_order_id, status=ev.status)
                self._alert("WARN", "order_" + ev.status, client_order_id=ev.client_order_id)
                self._pending_cid = None
                self._set_state(State.OPEN if self.size != 0 else State.FLAT)
            elif db.get_order(self.conn, ev.client_order_id) is not None:
                self._update_order(ev.client_order_id, status=ev.status, filled_quantity=ev.filled_quantity)
            return
        if ev.instrument_id != self.instrument_id:
            return
        ours = db.get_order(self.conn, ev.client_order_id) is not None
        self._apply_fill(ev)
        if ours:
            self._update_order(ev.client_order_id, status="filled", filled_quantity=ev.quantity, avg_price=ev.price)
        if self.state is State.ENTRY_PENDING and ev.client_order_id == self._pending_cid and self.size != 0:
            self._pending_cid = None
            self._set_state(State.OPEN)
            await self.replace_stop()
        elif self.size == 0:
            was_pending = self.state is State.EXIT_PENDING and ev.client_order_id == self._pending_cid
            self._pending_cid = None
            self.stop_trigger = None
            await self.executor.cancel_stop(self.instrument_id)
            self._set_state(State.FLAT)
            hook = getattr(self.strategy, "on_flatten", None)
            if callable(hook):
                hook()
            if not was_pending and not ours:
                self._alert("WARN", "stop_fired", price=ev.price, quantity=ev.quantity)

    def _apply_fill(self, ev: FillUpdate) -> None:
        delta = ev.quantity if ev.side == "buy" else -ev.quantity
        old, new = self.size, self.size + delta
        entry = self.entry or Decimal(0)
        if old == 0 or new == 0 or (old > 0) != (new > 0):
            self.entry = ev.price if new != 0 else None
        elif abs(new) > abs(old):
            self.entry = (abs(old) * entry + abs(delta) * ev.price) / abs(new)
        self.size = new
        if new == 0:
            self.cumulative_funding = Decimal(0)
        self._persist()

    async def replace_stop(self) -> None:
        if self.size == 0 or self.entry is None:
            return
        trigger = stop_price(side="long" if self.size > 0 else "short", entry=self.entry, limits=self.limits)
        await self.executor.place_stop(self.instrument_id, trigger)
        self.stop_trigger = trigger
        self._persist()
        self._alert("INFO", "stop_placed", trigger=trigger)

    async def halt(self, reason: str) -> None:
        self._alert("CRITICAL", "halted", reason=reason)
        self._pending_cid = None
        self._set_state(State.HALTED)

    def clear_halt(self) -> None:
        self._set_state(State.OPEN if self.size != 0 else State.FLAT)


class Portfolio:
    def __init__(self, *, run_id: str, executor: Executor, conn, alerter: Alerter,
                 routers: Mapping[int, InstrumentRouter],
                 reconcile: Callable[[], Awaitable[None]] | None = None) -> None:
        self.run_id, self.executor, self.conn, self.alerter = run_id, executor, conn, alerter
        self.routers = dict(routers)
        self.reconcile = reconcile

    async def on_bar(self, histories: Mapping[int, Sequence[Bar]], kill: Action) -> None:
        snapshot = await self.executor.snapshot()
        for iid, history in histories.items():
            router = self.routers.get(iid)
            if router is not None and history:
                await router.on_bar(history, snapshot, kill)

    async def on_fast(self, marks: Mapping[int, Decimal]) -> None:
        snapshot = await self.executor.snapshot()
        for iid, mark in marks.items():
            router = self.routers.get(iid)
            if router is not None:
                await router.on_fast(mark, snapshot)

    async def dispatch(self, ev: OrderUpdate | FillUpdate | ReconcileNow) -> None:
        if isinstance(ev, ReconcileNow):
            if self.reconcile is not None:
                await self.reconcile()
            return
        if isinstance(ev, FillUpdate):
            router = self.routers.get(ev.instrument_id)
            if router is not None:
                await router.handle_event(ev)
            return
        for router in self.routers.values():
            await router.handle_event(ev)

    async def run_event_pump(self) -> None:
        async for ev in self.executor.events():
            await self.dispatch(ev)
```

- [ ] **Step 5: Run tests**

Run: `.venv/Scripts/python -m pytest tests/test_order_router.py tests/test_sim_executor.py tests/test_storage_phase2.py -v`
Expected: 13 + 7 + 4 passed. Arithmetic: entry 100.10 → liq = 100.10 × (1 − 1/3 + 0.02) = 68.74; at mark 90 distance = 21.26/90 = 23.6 % < 25 % → flatten.

- [ ] **Step 6: Commit**

```bash
git add polyperps/execution tests/test_order_router.py
git commit -m "feat(phase2a): order router state machine with guard chain, idempotent retry, and portfolio driver"
```

---

### Task 7: Reconciliation (spec §7.1)

**Files:**
- Create: `polyperps/execution/reconciliation.py`
- Modify: `polyperps/execution/order_router.py` (`Portfolio.reconcile_now()` applying the response table)
- Test: `tests/test_reconciliation.py`

**Interfaces:**
- Produces: `Mismatch(kind: Literal["size","unknown_order","missing_stop","stop_without_position","stop_drift"], instrument_id: int | None, local: str, remote: str)` frozen; `diff(*, local: Mapping[int, PositionLocalRow], remote: AccountSnapshot, run_id: str, known_orders: set[str], stop_drift_tolerance: Decimal = Decimal("0.05")) -> list[Mismatch]`, pure.
- **Plan amendment vs spec §7.1:** `liq_price_drift` needs a locally stored liquidation price we don't keep; replaced by `stop_drift` (remote stop trigger differs from `stop_trigger` by > 5 %) → WARN. Recorded here as a spec amendment.
- `Portfolio.reconcile_now() -> list[Mismatch]`: snapshot → `diff` → responses: `size` → `router.halt("reconcile: size mismatch")` (CRITICAL via halt), or CRITICAL `unknown_position` if no router; `unknown_order` → `executor.cancel(id)` + WARN `unknown_order`; `missing_stop` → `router.replace_stop()` + WARN `stop_missing`; `stop_without_position` → `executor.cancel_stop(iid)` + INFO `stop_orphan_cancelled`; `stop_drift` → WARN `stop_drift`. Returns the mismatches. `Portfolio.__init__` defaults `reconcile` to `self.reconcile_now` when `None`.

- [ ] **Step 1: Write the failing tests**

`tests/test_reconciliation.py`:

```python
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.backtest.bars import Bar
from polyperps.execution.order_router import InstrumentRouter, Portfolio
from polyperps.execution.reconciliation import Mismatch, diff
from polyperps.execution.sim_executor import SimExecutor
from polyperps.execution.types import AccountSnapshot, PositionLocalRow, PositionView, State
from polyperps.exchange.types import SourceType
from polyperps.monitor.alerts import Alerter, SqliteSink
from polyperps.storage.db import connect, list_alerts

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


def local(iid, state, size, stop=None):
    return PositionLocalRow(run_id="r", instrument_id=iid, state=state, size=Decimal(size), entry_price=Decimal(100),
                            stop_trigger=Decimal(stop) if stop else None, stop_order_id=None,
                            cumulative_funding=Decimal(0), updated_at=T0)


def remote(positions=(), open_orders=(), stops=None):
    return AccountSnapshot(equity=Decimal(1000), positions=tuple(positions), open_orders=tuple(open_orders),
                           stops=stops or {}, in_liquidation=False, ts=T0)


def pv(iid, size):
    return PositionView(instrument_id=iid, size=Decimal(size), entry_price=Decimal(100), notional=abs(Decimal(size)) * 100,
                        leverage=3, liquidation_price=Decimal(70), unrealised_pnl=Decimal(0), cumulative_funding=Decimal(0))


def test_diff_clean():
    assert diff(local={6: local(6, State.OPEN, "1", "85")}, remote=remote([pv(6, "1")], stops={6: Decimal(85)}),
                run_id="r", known_orders=set()) == []


def test_diff_each_kind():
    ms = diff(local={6: local(6, State.OPEN, "1", "85"), 7: local(7, State.FLAT, "0")},
              remote=remote([pv(6, "2")], open_orders=("r-6-9", "alien-1"), stops={7: Decimal(50), 6: Decimal(70)}),
              run_id="r", known_orders={"r-6-9"})
    kinds = sorted(m.kind for m in ms)
    assert kinds == ["size", "stop_without_position", "unknown_order"]   # size mismatch on 6 short-circuits stop checks for 6
    assert next(m for m in ms if m.kind == "unknown_order").remote == "alien-1"


def test_diff_stop_drift_and_missing_stop_and_unknown_position():
    ms = diff(local={6: local(6, State.OPEN, "1", "85")}, remote=remote([pv(6, "1")], stops={6: Decimal(70)}),
              run_id="r", known_orders=set())
    assert [m.kind for m in ms] == ["stop_drift"]
    ms = diff(local={6: local(6, State.OPEN, "1")}, remote=remote([pv(6, "1"), pv(8, "3")]), run_id="r", known_orders=set())
    assert [m.kind for m in ms] == ["missing_stop", "size"]
    assert ms[1].instrument_id == 8 and ms[1].local == "0"


class Strat:
    name = "t"; params = {}
    def target(self, h): return Decimal(1)
    def on_flatten(self): pass


def bar(i):
    c = Decimal(100)
    return Bar(instrument_id=6, source_type=SourceType.POLYMARKET_WS, open_ts=T0 + i * timedelta(hours=1), open=c, high=c,
               low=c, close=c, index_close=None, funding_rate=Decimal("0.0001"), spread_bps=Decimal(5),
               spread_source="constant", complete=True)


async def make_open():
    conn = connect(":memory:")
    ex = SimExecutor("r", equity=Decimal(1000), taker_fee_rate=Decimal("0.0004"), clock=lambda: T0)
    ex.update_mark(6, Decimal(100))
    alerter = Alerter("r", [SqliteSink(conn)])
    router = InstrumentRouter(run_id="r", instrument_id=6, category="crypto", strategy=Strat(), executor=ex, conn=conn,
                              alerter=alerter, categories={6: "crypto"}, clock=lambda: T0)
    pf = Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter, routers={6: router})
    await pf.on_bar({6: [bar(0)]}, "run")
    for ev in ex.drain_events():
        await pf.dispatch(ev)
    assert router.state is State.OPEN
    return conn, ex, router, pf


async def test_reconcile_replaces_missing_stop():
    conn, ex, router, pf = await make_open()
    await ex.cancel_stop(6)                       # simulate the venue losing our stop
    ms = await pf.reconcile_now()
    assert [m.kind for m in ms] == ["missing_stop"]
    assert (await ex.snapshot()).stops == {6: router.stop_trigger}
    assert "stop_missing" in [a[2] for a in list_alerts(conn, "r")]


async def test_reconcile_size_mismatch_halts():
    conn, ex, router, pf = await make_open()
    ex.update_mark(6, Decimal(50))
    ex.check_triggers()                           # stop fires on the venue; we never process the event
    ms = await pf.reconcile_now()
    assert [m.kind for m in ms] == ["size"]
    assert router.state is State.HALTED and any(a[1] == "CRITICAL" for a in list_alerts(conn, "r"))
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_reconciliation.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.execution.reconciliation'`

- [ ] **Step 3: Write `polyperps/execution/reconciliation.py`**

```python
"""Spec 2.4: local rows vs the executor's view. Pure diff; responses live in Portfolio.reconcile_now().

Amendment vs spec section 7.1: `liq_price_drift` is replaced by `stop_drift` (we do not
store a local liquidation price; we do store our stop trigger)."""

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
        lsize = local[iid].size if iid in local else Decimal(0)
        rsize = remote_pos[iid].size if iid in remote_pos else Decimal(0)
        if lsize != rsize:
            out.append(Mismatch(kind="size", instrument_id=iid, local=str(lsize), remote=str(rsize)))
            continue
        if rsize != 0 and iid in local and local[iid].state is State.OPEN:
            rstop = remote.stops.get(iid)
            lstop = local[iid].stop_trigger
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
```

- [ ] **Step 4: Add `reconcile_now` to `Portfolio` in `order_router.py`**

Add the import `from polyperps.execution.reconciliation import Mismatch, diff`, make `__init__` set `self.reconcile = reconcile if reconcile is not None else self.reconcile_now`, and add:

```python
    async def reconcile_now(self) -> list[Mismatch]:
        snapshot = await self.executor.snapshot()
        local = db.get_positions_local(self.conn, self.run_id)
        known = {o.client_order_id for o in db.list_orders(self.conn, self.run_id)}
        mismatches = diff(local=local, remote=snapshot, run_id=self.run_id, known_orders=known)
        for m in mismatches:
            router = self.routers.get(m.instrument_id) if m.instrument_id is not None else None
            detail = {"local": m.local, "remote": m.remote}
            now = _utcnow()
            if m.kind == "size":
                if router is not None:
                    await router.halt(f"reconcile: size mismatch local={m.local} remote={m.remote}")
                else:
                    self.alerter.emit(Alert(level="CRITICAL", kind="unknown_position", instrument_id=m.instrument_id,
                                            detail=detail, ts=now))
            elif m.kind == "unknown_order":
                await self.executor.cancel(m.remote)
                self.alerter.emit(Alert(level="WARN", kind="unknown_order", instrument_id=None, detail=detail, ts=now))
            elif m.kind == "missing_stop":
                if router is not None:
                    await router.replace_stop()
                self.alerter.emit(Alert(level="WARN", kind="stop_missing", instrument_id=m.instrument_id, detail=detail, ts=now))
            elif m.kind == "stop_without_position":
                await self.executor.cancel_stop(m.instrument_id)
                self.alerter.emit(Alert(level="INFO", kind="stop_orphan_cancelled", instrument_id=m.instrument_id,
                                        detail=detail, ts=now))
            else:
                self.alerter.emit(Alert(level="WARN", kind="stop_drift", instrument_id=m.instrument_id, detail=detail, ts=now))
        return mismatches
```

- [ ] **Step 5: Run tests**

Run: `.venv/Scripts/python -m pytest tests/test_reconciliation.py tests/test_order_router.py -v`
Expected: 5 + 13 passed

- [ ] **Step 6: Commit**

```bash
git add polyperps/execution/reconciliation.py polyperps/execution/order_router.py tests/test_reconciliation.py
git commit -m "feat(phase2a): reconciliation diff with pre-registered responses"
```

---

### Task 8: Cold-start recovery (spec §7.2)

**Files:**
- Create: `polyperps/execution/state_recovery.py`
- Test: `tests/test_state_recovery.py`

**Interfaces:**
- Produces: `class RecoveryHalt(RuntimeError)`; `RecoveryReport(adopted: list[str], abandoned: list[str], cancelled: list[str], stops_replaced: list[int], unknown_positions: list[int], states: dict[int, str])` (a plain `@dataclass`, mutable, with `to_dict()`); `async recover(*, conn, run_id, executor, routers: Mapping[int, InstrumentRouter], alerter, clock=_utcnow) -> RecoveryReport`.
- Rules: `snapshot()` failure → `RecoveryHalt`. For each router: local row loaded first; if local state is `HALTED` it is preserved; else remote position → `OPEN`, `size`/`entry`/`cumulative_funding` from remote, stop re-placed if `iid not in snapshot.stops` (else `stop_trigger` adopted from the snapshot); no remote position → `FLAT`, size 0. Local orders with status `submitting`/`accepted`: id in `snapshot.open_orders` → `adopted`; else `abandoned`. Venue open orders not prefixed `f"{run_id}-"` → `executor.cancel`. Remote positions with no router → `unknown_positions` + CRITICAL `unknown_position`. `recovery` row written; INFO alert `recovery`.

- [ ] **Step 1: Write the failing tests**

`tests/test_state_recovery.py`:

```python
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from polyperps.execution.order_router import InstrumentRouter
from polyperps.execution.sim_executor import SimExecutor
from polyperps.execution.state_recovery import RecoveryHalt, recover
from polyperps.execution.types import OrderRequest, OrderRow, PositionLocalRow, State
from polyperps.monitor.alerts import Alerter, SqliteSink
from polyperps.storage.db import connect, get_order, list_alerts, list_recovery, upsert_order, upsert_position_local

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


class Strat:
    name = "t"; params = {}
    def target(self, h): return Decimal(0)
    def on_flatten(self): pass


def setup(equity="1000"):
    conn = connect(":memory:")
    ex = SimExecutor("r", equity=Decimal(equity), taker_fee_rate=Decimal("0.0004"), clock=lambda: T0)
    ex.update_mark(6, Decimal(100)); ex.update_mark(8, Decimal(100))
    alerter = Alerter("r", [SqliteSink(conn)])
    router = InstrumentRouter(run_id="r", instrument_id=6, category="crypto", strategy=Strat(), executor=ex, conn=conn,
                              alerter=alerter, categories={6: "crypto"}, clock=lambda: T0)
    return conn, ex, alerter, router


def order_row(cid, status):
    return OrderRow(client_order_id=cid, run_id="r", instrument_id=6, side="buy", quantity=Decimal(1), reduce_only=False,
                    status=status, exchange_order_id=None, filled_quantity=Decimal(0), avg_price=None,
                    submitted_at=T0, updated_at=T0, reason="strategy")


async def test_crash_between_decision_and_submit_is_abandoned_and_flat():
    conn, ex, alerter, router = setup()
    upsert_order(conn, order_row("r-6-1", "submitting"))
    rep = await recover(conn=conn, run_id="r", executor=ex, routers={6: router}, alerter=alerter, clock=lambda: T0)
    assert rep.abandoned == ["r-6-1"] and router.state is State.FLAT and rep.states == {6: "FLAT"}
    assert get_order(conn, "r-6-1").status == "abandoned"
    assert list_recovery(conn, "r")[0][1]["abandoned"] == ["r-6-1"]


async def test_crash_after_fill_rebuilds_open_and_replaces_stop():
    conn, ex, alerter, router = setup()
    await ex.submit(OrderRequest(client_order_id="r-6-1", instrument_id=6, side="buy", quantity=Decimal(1),
                                 reduce_only=False, ts=T0)); ex.drain_events()
    upsert_order(conn, order_row("r-6-1", "accepted"))
    upsert_position_local(conn, PositionLocalRow(run_id="r", instrument_id=6, state=State.ENTRY_PENDING, size=Decimal(0),
                                                 entry_price=None, stop_trigger=None, stop_order_id=None,
                                                 cumulative_funding=Decimal(0), updated_at=T0))
    rep = await recover(conn=conn, run_id="r", executor=ex, routers={6: router}, alerter=alerter, clock=lambda: T0)
    assert router.state is State.OPEN and router.size == 1 and router.entry == Decimal("100.10")
    assert rep.stops_replaced == [6] and (await ex.snapshot()).stops == {6: Decimal("85.08")}
    assert rep.abandoned == ["r-6-1"]   # not resting on the venue; its fill is already in the position


async def test_unknown_remote_position_is_critical():
    conn, ex, alerter, router = setup()
    await ex.submit(OrderRequest(client_order_id="x", instrument_id=8, side="sell", quantity=Decimal(2),
                                 reduce_only=False, ts=T0)); ex.drain_events()
    rep = await recover(conn=conn, run_id="r", executor=ex, routers={6: router}, alerter=alerter, clock=lambda: T0)
    assert rep.unknown_positions == [8]
    assert any(a[1] == "CRITICAL" and a[2] == "unknown_position" for a in list_alerts(conn, "r"))


async def test_halted_is_preserved():
    conn, ex, alerter, router = setup()
    upsert_position_local(conn, PositionLocalRow(run_id="r", instrument_id=6, state=State.HALTED, size=Decimal(0),
                                                 entry_price=None, stop_trigger=None, stop_order_id=None,
                                                 cumulative_funding=Decimal(0), updated_at=T0))
    await recover(conn=conn, run_id="r", executor=ex, routers={6: router}, alerter=alerter, clock=lambda: T0)
    assert router.state is State.HALTED


async def test_snapshot_failure_halts_recovery():
    conn, ex, alerter, router = setup()

    async def boom():
        raise RuntimeError("venue down")

    ex.snapshot = boom  # type: ignore[assignment]
    with pytest.raises(RecoveryHalt):
        await recover(conn=conn, run_id="r", executor=ex, routers={6: router}, alerter=alerter, clock=lambda: T0)
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_state_recovery.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.execution.state_recovery'`

- [ ] **Step 3: Write `polyperps/execution/state_recovery.py`**

```python
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
```

- [ ] **Step 4: Run tests**

Run: `.venv/Scripts/python -m pytest tests/test_state_recovery.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add polyperps/execution/state_recovery.py tests/test_state_recovery.py
git commit -m "feat(phase2a): cold-start recovery from the executor snapshot"
```

---

### Task 9: `LiveExecutor` (gated, fake-session tests only) and the 2.0 re-check

**Files:**
- Create: `polyperps/execution/live_executor.py`, `scripts/nautilus_recheck.md`
- Test: `tests/test_live_executor.py`

**Interfaces:**
- Produces: `LiveExecutor(session, *, instrument_ids: Sequence[int], modes: Mapping[int, ExecutionMode], gate: Callable[[int], GateDecision] | None = None, clock=_utcnow, dead_man_s: int = 60)`; `name = "live"`; constructor calls `gate(iid)` (default `lambda iid: live_orders_allowed(iid, modes=modes)`) for every id and raises `GateClosed(reason)` on the first `allowed=False`. Method mapping (spec §4.4): `submit` → `session.place_order(instrument_id=, side=, quantity=, time_in_force="ioc", reduce_only=, client_order_id=)` → `OrderAck` (`accepted` unless `RequestRejectedError` → `rejected`); `cancel` → `session.cancel_order(client_order_id=)`; `place_stop` → `session.place_position_tp_sl(instrument_id=, stop_loss=PerpsPositionTpSlTrigger(trigger_price=))` remembering the returned order id in `_stop_ids`; `cancel_stop` → `session.cancel_order(order_id=_stop_ids[iid])`; `heartbeat` → `session.arm_auto_cancel(cancel_at=clock()+dead_man_s)`; `snapshot` → `fetch_portfolio()` + `fetch_open_orders()` mapped to `AccountSnapshot` (stops from open orders whose `tp_sl.kind == "sl"`); `events` → iterate the session, mapping order events → `OrderUpdate`, fill events → `FillUpdate`, `PerpsResyncEvent` → `ReconcileNow("resync")`, everything else skipped; `close` → `session.close()`.
- **This module is the only new `polymarket` importer**. Unit tests use a `FakeSession` recording calls; the real session is never touched.

- [ ] **Step 1: Write the failing tests**

`tests/test_live_executor.py`:

```python
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from polyperps.execution.executor import GateClosed
from polyperps.execution.live_executor import LiveExecutor
from polyperps.execution.types import FillUpdate, OrderRequest, OrderUpdate, ReconcileNow
from polyperps.gates import ExecutionMode, GateDecision

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


class FakeSession:
    def __init__(self):
        self.calls = []
        self._events = []

    async def place_order(self, **kw):
        self.calls.append(("place_order", kw))
        return SimpleNamespace(order=SimpleNamespace(id=777))

    async def cancel_order(self, **kw):
        self.calls.append(("cancel_order", kw))

    async def place_position_tp_sl(self, **kw):
        self.calls.append(("tp_sl", kw))
        return SimpleNamespace(stop_loss=SimpleNamespace(order_id=888))

    async def arm_auto_cancel(self, **kw):
        self.calls.append(("arm", kw))

    async def fetch_portfolio(self):
        return SimpleNamespace(
            positions=(SimpleNamespace(instrument_id=6, size=Decimal("0.5"), entry_price=Decimal(100), leverage=3,
                                       position_value=Decimal(50), liquidation_price=Decimal(70),
                                       unrealized_pnl=Decimal(1), cumulative_funding=Decimal("-0.2")),),
            margin=SimpleNamespace(total_account_value=Decimal(1000)), withdrawable=Decimal(900),
            in_liquidation=False, timestamp=T0)

    async def fetch_open_orders(self):
        return (SimpleNamespace(client_order_id="r-6-3", id=1, tp_sl=None, instrument_id=6),
                SimpleNamespace(client_order_id=None, id=888, instrument_id=6,
                                tp_sl=SimpleNamespace(kind="sl", trigger_price=Decimal(85))))

    def __aiter__(self):
        async def gen():
            for e in self._events:
                yield e
        return gen()

    async def close(self):
        self.calls.append(("close", {}))


OPEN = lambda iid: GateDecision(True, "all gates passed")


def test_constructor_refuses_by_default_gate():
    with pytest.raises(GateClosed, match="SIGNAL_VALIDATED|manual_review"):
        LiveExecutor(FakeSession(), instrument_ids=[6], modes={6: ExecutionMode.AUTO})
    with pytest.raises(GateClosed):
        LiveExecutor(FakeSession(), instrument_ids=[6], modes={}, gate=lambda i: GateDecision(False, "closed"))


async def test_submit_maps_payload_and_ack():
    s = FakeSession()
    ex = LiveExecutor(s, instrument_ids=[6], modes={}, gate=OPEN, clock=lambda: T0)
    ack = await ex.submit(OrderRequest(client_order_id="r-6-1", instrument_id=6, side="buy", quantity=Decimal("0.5"),
                                       reduce_only=False, ts=T0))
    assert s.calls[0] == ("place_order", {"instrument_id": 6, "side": "buy", "quantity": Decimal("0.5"),
                                          "time_in_force": "ioc", "reduce_only": False, "client_order_id": "r-6-1"})
    assert ack.status == "accepted" and ack.exchange_order_id == "777"


async def test_stop_heartbeat_cancel():
    s = FakeSession()
    ex = LiveExecutor(s, instrument_ids=[6], modes={}, gate=OPEN, clock=lambda: T0)
    st = await ex.place_stop(6, Decimal(85))
    assert s.calls[-1][0] == "tp_sl" and s.calls[-1][1]["instrument_id"] == 6
    assert s.calls[-1][1]["stop_loss"].trigger_price == Decimal(85) and st.exchange_order_id == "888"
    await ex.cancel_stop(6)
    assert s.calls[-1] == ("cancel_order", {"order_id": 888})
    await ex.heartbeat()
    assert s.calls[-1] == ("arm", {"cancel_at": T0 + timedelta(seconds=60)})
    await ex.cancel("r-6-1")
    assert s.calls[-1] == ("cancel_order", {"client_order_id": "r-6-1"})


async def test_snapshot_maps_portfolio_and_stops():
    ex = LiveExecutor(FakeSession(), instrument_ids=[6], modes={}, gate=OPEN, clock=lambda: T0)
    snap = await ex.snapshot()
    assert snap.equity == Decimal(1000) and snap.position(6).liquidation_price == Decimal(70)
    assert snap.position(6).notional == Decimal(50) and snap.open_orders == ("r-6-3",)
    assert snap.stops == {6: Decimal(85)}


async def test_events_mapping():
    s = FakeSession()
    from polymarket.models.perps.events import PerpsResyncEvent
    s._events = [
        SimpleNamespace(type="order", payload=SimpleNamespace(client_order_id="r-6-1", status="filled", filled_quantity=Decimal(1)), timestamp=T0),
        SimpleNamespace(type="fill", payload=[SimpleNamespace(client_order_id="r-6-1", instrument_id=6, side="buy",
                                                             quantity=Decimal(1), price=Decimal(100), fee=Decimal("0.04"))], timestamp=T0),
        SimpleNamespace(type="balance", payload=None, timestamp=T0),
        PerpsResyncEvent.model_construct(),
    ]
    ex = LiveExecutor(s, instrument_ids=[6], modes={}, gate=OPEN, clock=lambda: T0)
    out = [e async for e in ex.events()]
    assert [type(e) for e in out] == [OrderUpdate, FillUpdate, ReconcileNow]
    assert out[1].instrument_id == 6 and out[1].fee == Decimal("0.04")
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_live_executor.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.execution.live_executor'`

- [ ] **Step 3: Write `polyperps/execution/live_executor.py`**

```python
"""Live executor over polymarket-client's PerpsSession (spec section 4.4).

The ONLY execution module that imports the SDK. Never run in Phase 2a: the
constructor calls the three-lock gate for every instrument and raises
GateClosed unless all are open, and scripts/run_paper.py refuses --executor live.
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
                                         side=f.side, quantity=f.quantity, price=f.price, fee=f.fee, ts=ev.timestamp)

    async def close(self) -> None:
        await self._s.close()
```

Before finalising, verify two SDK details against the installed package and adjust the **mapping** (not the tests' intent) if they differ: (a) the attribute holding the placed order id on `PerpsOrderPlacement` (`polymarket/models/perps/results.py`) and on `PerpsPlacedTpSlOrders`; (b) the field names on `PerpsTpSlOrderFields` (`polymarket/models/perps/orders.py`) for the stop kind and trigger price, and the fill payload's `side` literal (`buy`/`sell` vs `long`/`short` — if the SDK uses `long`/`short`, map to `buy`/`sell` in `events()`). Record findings in the report and the module docstring.

- [ ] **Step 4: Write `scripts/nautilus_recheck.md`** (spec row 2.0) after one lookup

Run: `curl -s https://api.github.com/repos/nautechsystems/nautilus_trader/contents/nautilus_trader/adapters | grep '"name"'` and record:

```markdown
# NautilusTrader Perps adapter re-check (spec 2.0)

| Date | Adapters listed | Polymarket perps adapter? | Decision |
|------|-----------------|---------------------------|----------|
| 2026-09-12 | <paste names> | <yes/no> | Keep our ExchangeClient/Executor boundary; re-check before Phase 3 (migration only pre-live). |
```

- [ ] **Step 5: Run tests**

Run: `.venv/Scripts/python -m pytest tests/test_live_executor.py -v`
Expected: 5 passed

- [ ] **Step 6: Commit**

```bash
git add polyperps/execution/live_executor.py scripts/nautilus_recheck.md tests/test_live_executor.py
git commit -m "feat(phase2a): gated LiveExecutor over PerpsSession (fake-session tests only) and 2.0 re-check"
```

---

### Task 10: Live bar builder, `scripts/run_paper.py`, README

**Files:**
- Create: `polyperps/execution/live_bars.py`, `scripts/run_paper.py`
- Modify: `README.md`
- Test: `tests/test_live_bars.py`, `tests/test_run_paper_script.py`

**Interfaces:**
- Produces: `LiveBarBuilder(*, spread_bps: Decimal = BAR.proxy_spread_bps, max_history: int = 500)`; `on_tick(tick: Tick) -> Bar | None` — returns the just-closed bar when `floor_hour(tick.exchange_ts)` advances for that instrument (OHLC from `mark_price`, `index_close` = last `index_price`, `funding_rate` = last tick's `funding_rate`, `spread_source="constant"`, `complete=True`); `history(instrument_id) -> list[Bar]`; `close_all(now) -> list[Bar]` (used on shutdown). Hours with zero ticks produce no bar.
- Produces: `scripts/run_paper.py` with `build_parser()` and `main()`; args `--executor {sim,live}` (`live` → `main()` exits 2 with the gate reasons before constructing anything), `--hypothesis`, `--params-from RUN_ID` or `--grid-index N` (default 0), `--run-id`, `--equity` (default `"1000"`), `--fee-category` (default `"equity"`), `--clear-halt IID`; boot sequence per spec §9; `HEARTBEAT_S=20`, `RECONCILE_S=60`; SIGTERM → clean stop; restart-with-backoff around `run_once`.

- [ ] **Step 1: Write the failing tests**

`tests/test_live_bars.py`:

```python
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.execution.live_bars import LiveBarBuilder
from polyperps.exchange.types import SourceType, Tick

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


def tick(minutes, mark, index="100", funding="0.0001", iid=6):
    ts = T0 + timedelta(minutes=minutes)
    return Tick(instrument_id=iid, mark_price=Decimal(mark), index_price=Decimal(index), last_price=Decimal(mark),
                funding_rate=Decimal(funding), next_funding=ts, exchange_ts=ts, received_ts=ts,
                source_type=SourceType.POLYMARKET_WS, sequence=minutes)


def test_bar_closes_when_hour_advances():
    b = LiveBarBuilder()
    assert b.on_tick(tick(1, "100")) is None
    assert b.on_tick(tick(30, "105", index="104")) is None
    assert b.on_tick(tick(59, "98", funding="0.0002")) is None
    closed = b.on_tick(tick(61, "99"))
    assert closed is not None and closed.open_ts == T0
    assert (closed.open, closed.high, closed.low, closed.close) == (Decimal(100), Decimal(105), Decimal(98), Decimal(98))
    assert closed.index_close == Decimal(100) and closed.funding_rate == Decimal("0.0002")
    assert closed.complete and closed.spread_source == "constant"
    assert b.history(6) == [closed]


def test_instruments_are_independent_and_history_capped():
    b = LiveBarBuilder(max_history=2)
    for h in range(4):
        b.on_tick(tick(60 * h + 1, "100"))
        b.on_tick(tick(60 * h + 2, "100", iid=7))
    assert len(b.history(6)) == 2 and len(b.history(7)) == 2
    assert b.history(6)[-1].open_ts == T0 + timedelta(hours=2)


def test_close_all_on_shutdown():
    b = LiveBarBuilder()
    b.on_tick(tick(5, "100"))
    (bar,) = b.close_all(T0 + timedelta(minutes=10))
    assert bar.close == Decimal(100) and b.history(6) == [bar]
```

`tests/test_run_paper_script.py`:

```python
import importlib.util
import sys

import pytest


def load():
    spec = importlib.util.spec_from_file_location("run_paper", "scripts/run_paper.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_live_executor_refused_in_phase_2a(monkeypatch, tmp_path):
    monkeypatch.setenv("POLYPERPS_INSTRUMENT_IDS", "6")
    monkeypatch.setenv("POLYPERPS_DB_PATH", str(tmp_path / "t.sqlite3"))
    monkeypatch.setattr(sys, "argv", ["run_paper.py", "--executor", "live", "--hypothesis", "h1"])
    mod = load()
    with pytest.raises(SystemExit) as e:
        mod.main()
    assert e.value.code == 2


def test_parser_defaults():
    mod = load()
    args = mod.build_parser().parse_args(["--executor", "sim", "--hypothesis", "h1"])
    assert args.equity == "1000" and args.fee_category == "equity" and args.grid_index == 0
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_live_bars.py tests/test_run_paper_script.py -v`
Expected: FAIL (`ModuleNotFoundError` for `live_bars`; the script file does not exist)

- [ ] **Step 3: Write `polyperps/execution/live_bars.py`**

```python
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
        return [self._close(iid) for iid in list(self._acc)]
```

- [ ] **Step 4: Write `scripts/run_paper.py`**

```python
"""Phase 2a paper run: the exact router/guards/reconciliation path, SimExecutor last mile.

    POLYPERPS_INSTRUMENT_IDS=6,7 .venv/Scripts/python scripts/run_paper.py --executor sim --hypothesis h1

`--executor live` is refused in Phase 2a (exit 2) before anything is constructed.
Needs a network path where Polymarket resolves (public WS only; no credentials).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import sys
from datetime import datetime, timezone
from decimal import Decimal

from polyperps.config import load_settings
from polyperps.data_ingest.market_feed import MarketFeed
from polyperps.exchange.client import PolymarketPerpsClient
from polyperps.execution.live_bars import LiveBarBuilder
from polyperps.execution.order_router import InstrumentRouter, Portfolio
from polyperps.execution.sim_executor import SimExecutor
from polyperps.execution.state_recovery import recover
from polyperps.gates import ExecutionMode, live_orders_allowed
from polyperps.monitor.alerts import Alerter, LogSink, SqliteSink, TelegramSink
from polyperps.risk.kill_switch import evaluate as kill_evaluate
from polyperps.security.key_management import SecretUnavailable, load_secret
from polyperps.signal.validation_log import read_records
from polyperps.storage import db
from polyperps.strategies import GRIDS, build_strategy

log = logging.getLogger("polyperps.paper")
HEARTBEAT_S = 20
RECONCILE_S = 60


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--executor", choices=["sim", "live"], required=True)
    ap.add_argument("--hypothesis", choices=sorted(GRIDS), required=True)
    ap.add_argument("--params-from", default=None, help="run_id in validation_log.jsonl")
    ap.add_argument("--grid-index", type=int, default=0)
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--equity", default="1000")
    ap.add_argument("--fee-category", default="equity")
    ap.add_argument("--clear-halt", type=int, default=None)
    return ap


def _params(args) -> dict:
    if args.params_from:
        for r in read_records():
            if r["run_id"] == args.params_from:
                out = {}
                for k, v in r["params_chosen"].items():
                    out[k] = Decimal(v) if isinstance(v, str) and "." in v else int(v)
                return out
        raise SystemExit(f"run_id {args.params_from} not found in validation log")
    return GRIDS[args.hypothesis][args.grid_index]


def _alerter(run_id: str, conn) -> Alerter:
    sinks = [LogSink(), SqliteSink(conn)]
    try:
        sinks.append(TelegramSink(token=load_secret("TELEGRAM_BOT_TOKEN"), chat_id=load_secret("TELEGRAM_CHAT_ID")))
    except SecretUnavailable:
        log.info("telegram sink not configured")
    return Alerter(run_id, sinks)


async def run_once(args, settings) -> None:
    if args.hypothesis == "h2":
        raise SystemExit("h2 needs a live proxy feed; not wired in Phase 2a")
    conn = db.connect(settings.db_path)
    run_id = args.run_id or f"paper-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    alerter = _alerter(run_id, conn)
    fee = db.latest_fee(conn, args.fee_category)
    if fee is None:
        raise SystemExit(f"no fee row for {args.fee_category!r}; run scripts/store_fees.py")
    saved = db.load_sim_account(conn, run_id)

    def persist(text: str) -> None:
        db.save_sim_account(conn, run_id, text)

    executor = (SimExecutor.from_json(run_id, saved, taker_fee_rate=fee.taker_fee_rate, persist=persist) if saved
                else SimExecutor(run_id, equity=Decimal(args.equity), taker_fee_rate=fee.taker_fee_rate, persist=persist))

    client = PolymarketPerpsClient.create_public(rate_per_sec=settings.rest_rate_per_sec, burst=settings.rest_burst)
    instruments = {i.instrument_id: i for i in await client.fetch_instruments()}
    unknown = [i for i in settings.instrument_ids if i not in instruments]
    if unknown:
        raise SystemExit(f"unknown instrument ids {unknown}")
    categories = {i: instruments[i].category for i in settings.instrument_ids}
    params = _params(args)
    routers = {
        iid: InstrumentRouter(run_id=run_id, instrument_id=iid, category=categories[iid],
                              strategy=build_strategy(args.hypothesis, params),
                              executor=executor, conn=conn, alerter=alerter, categories=categories)
        for iid in settings.instrument_ids
    }
    pf = Portfolio(run_id=run_id, executor=executor, conn=conn, alerter=alerter, routers=routers)
    rep = await recover(conn=conn, run_id=run_id, executor=executor, routers=routers, alerter=alerter)
    log.info("recovery: %s", rep.to_dict())
    if args.clear_halt is not None and args.clear_halt in routers:
        routers[args.clear_halt].clear_halt()
        log.warning("cleared HALT on %s by operator request", args.clear_halt)

    builder = LiveBarBuilder()
    marks: dict[int, Decimal] = {}
    closed_bars: asyncio.Queue = asyncio.Queue()
    stop = asyncio.Event()

    def on_accept(tick):
        db.insert_tick(conn, tick)
        marks[tick.instrument_id] = tick.mark_price
        executor.update_mark(tick.instrument_id, tick.mark_price)
        bar = builder.on_tick(tick)
        if bar is not None:
            executor.apply_funding(bar.instrument_id, bar.funding_rate)
            closed_bars.put_nowait(bar)

    ticks = client.stream_ticks(settings.instrument_ids)
    feed = MarketFeed(ticks=ticks, bounds=settings.bounds, on_accept=on_accept)

    async def bar_loop():
        while not stop.is_set():
            bar = await closed_bars.get()
            kill = kill_evaluate(live_sharpe=None, backtest_sharpe=None, mode="paper")
            await pf.on_bar({bar.instrument_id: builder.history(bar.instrument_id)}, kill)

    async def fast_loop():
        while not stop.is_set():
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), HEARTBEAT_S)
            if stop.is_set():
                break
            await executor.heartbeat()
            for ev in executor.check_triggers():
                await pf.dispatch(ev)
            await pf.on_fast(dict(marks))

    async def reconcile_loop():
        while not stop.is_set():
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), RECONCILE_S)
            if stop.is_set():
                break
            await pf.reconcile_now()

    tasks = [asyncio.create_task(t()) for t in (bar_loop, fast_loop, reconcile_loop, pf.run_event_pump)]
    try:
        await feed.run()
    finally:
        stop.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        with contextlib.suppress(Exception):
            await ticks.aclose()
        await client.close()
        conn.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = build_parser().parse_args()
    settings = load_settings()
    if args.executor == "live":
        reasons = [live_orders_allowed(i, modes={i: ExecutionMode.AUTO for i in settings.instrument_ids}).reason
                   for i in settings.instrument_ids]
        print(f"--executor live is not available in Phase 2a. Gate says: {reasons}", file=sys.stderr)
        raise SystemExit(2)
    asyncio.run(_supervise(args, settings))


async def _supervise(args, settings) -> None:
    backoff = 1.0
    while True:
        started = asyncio.get_running_loop().time()
        try:
            await run_once(args, settings)
            log.warning("feed ended; restarting")
        except (KeyboardInterrupt, asyncio.CancelledError, SystemExit):
            raise
        except Exception:
            log.exception("paper run crashed; restarting in %.0fs", backoff)
        ran = asyncio.get_running_loop().time() - started
        backoff = 1.0 if ran > 300 else min(backoff * 2, 60.0)
        await asyncio.sleep(backoff)


def _raise_keyboard_interrupt(signum, frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)
    try:
        main()
    except KeyboardInterrupt:
        pass
```

- [ ] **Step 5: Run tests, then `--help` and the live-refusal path**

Run: `.venv/Scripts/python -m pytest tests/test_live_bars.py tests/test_run_paper_script.py -v`
Expected: 3 + 2 passed

Run: `POLYPERPS_INSTRUMENT_IDS=6 .venv/Scripts/python scripts/run_paper.py --executor live --hypothesis h1; echo exit=$?`
Expected: a stderr line containing `SIGNAL_VALIDATED is False` and `exit=2`.

If Polymarket resolves from this machine (`nslookup api.perpetuals.polymarket.com` must NOT return 175.139.142.25), run a 3-minute paper smoke: `POLYPERPS_INSTRUMENT_IDS=6 timeout 180 .venv/Scripts/python scripts/run_paper.py --executor sim --hypothesis h1 --run-id smoke`; expected: recovery INFO alert, ticks flowing, at most one bar closed, no traceback. Otherwise record "smoke skipped: geo-blocked" in the report — the 48 h paper run is the user's step either way.

- [ ] **Step 6: Append to `README.md`**

```markdown
## Phase 2a — paper execution (no live orders)

Spec: `docs/superpowers/specs/2026-09-12-polyperps-phase2a-design.md`. The router,
guards, reconciliation, recovery, and alerts run on the exact path a live run
would, with `SimExecutor` as the last mile. Pre-registered limits: 3x leverage,
25 % liquidation-distance floor, 15 % exchange-side stop, 2 % funding-cost
exit, gross exposure 1.0x / cluster net 0.6x equity, kill-switch thresholds `None`.

| Step | Command |
|------|---------|
| 48 h paper run | `POLYPERPS_INSTRUMENT_IDS=6,7 scripts/run_paper.py --executor sim --hypothesis h1` |
| clear a halted instrument (human decision) | add `--clear-halt 6` to the run command |
| Telegram CRITICAL alerts (optional) | store `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` via `key_management` |

`--executor live` exits 2 in Phase 2a. `LiveExecutor` cannot be constructed
unless all three locks are open; the kill switch refuses to `run` live while its
thresholds are `None` (set in Phase 2b from a passing native record).
```

- [ ] **Step 7: Full suite and commit**

Run: `.venv/Scripts/python -m pytest -q -W error`
Expected: 191 + 5+4+10+5+4+5+1+7+13+5+5+5+3+2 = 265 passed, no warnings (report what pytest prints).

```bash
git add polyperps/execution/live_bars.py scripts/run_paper.py README.md tests/test_live_bars.py tests/test_run_paper_script.py
git commit -m "feat(phase2a): live bar builder and paper run script (live executor refused)"
```

---

## Phase 2a exit mapping

| Spec row | Evidence |
|---|---|
| 2.0 | `scripts/nautilus_recheck.md` row. |
| 2.2 | `test_liquidation_guard.py` (limits pinned; leverage/liquidation cases); `test_order_router.py::test_fast_loop_liq_distance_exit`. |
| 2.2b | `test_portfolio_exposure.py::test_spec_scenario_btc_plus_eth_same_direction_resized_by_cluster_net`. |
| 2.3 | State machine + gate + stop + idempotent id: `test_order_router.py` (entry/exit/flip/timeout-adopt/drop-retry/drop-halt/reject/kill), `test_sim_executor.py::test_stop_fires_from_check_triggers_without_router`, funding-cost exit standalone. Paper clean 1–2 weeks with the real signal → 2b. |
| 2.4 | `test_reconciliation.py` (five kinds; missing-stop replaced; size mismatch halts). |
| 2.4b | `test_state_recovery.py` (three crash scenarios + halt preserved + snapshot failure). |
| 2.5 | `test_alerts.py` (kinds → sinks; Telegram CRITICAL-only, failure swallowed, token never logged); `test_order_router.py::test_decision_trail_reconstructable_from_sqlite_only`. |
| 2.6 mechanism | `test_kill_switch.py` (`None` → pause live / run paper). Numbers → 2b. |

## Self-review notes

- **Spec coverage:** §4.1→T1; §4.2→T5; §4.3→T5(+T6 additions); §4.4→T9; §4.5→T6 `_submit_with_recovery`; §5.1→T2; §5.2→T3; §5.3→T3; §6→T6; §7.1→T7 (with the `stop_drift` amendment recorded); §7.2→T8; §8.1→T4; §8.2→T4+T6 test; §9→T10; §10 error handling→T5 (`ExecutorTimeout`), T6 (retry/halt), T7/T4 (never raise), T8 (`RecoveryHalt`), T9 (`GateClosed`); §11 tests→each task.
- **Type consistency:** `Verdict`/`Allow`/`Resize`/`Reject` defined once (T2) and imported by T3/T6; `AccountSnapshot.position()` used by T2/T6/T7/T8; `PositionLocalRow` fields identical in T1/T6/T7/T8; `Executor` protocol gains `cancel_stop` in T6 and both executors implement it (T6 sim, T9 live); `ReconcileNow` added in T6, produced in T9, consumed by `Portfolio.dispatch`; `Bar(..., spread_source=...)` matches Phase 1's amended dataclass; `FillUpdate.side` is `"buy"/"sell"` in both executors (T9 flags the SDK literal for verification); slots dataclasses are copied via `__slots__`/`dataclasses.replace`, never `__dict__`; stop price `85.08` (half-even) used consistently in T6 and T8 tests.
- **Judgment calls recorded:** flips exit this bar and re-enter next bar (no atomic reverse); `liq_price_drift` → `stop_drift`; sim stops fill at the trigger plus slippage; a fill event for an unknown id that takes size to 0 is treated as a stop; `LiveExecutor` SDK attribute names are verified by the implementer against the installed package and corrected in the mapping if they differ; h2 is refused by `run_paper.py` (needs a live proxy feed).
