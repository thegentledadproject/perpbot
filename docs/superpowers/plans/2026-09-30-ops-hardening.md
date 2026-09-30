# Ops Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the paper soak's hourly crash-restarts and make every crash, stale feed, full disk or failed prune reach an operator within 5 minutes.

**Architecture:** Three changes. (1) The paper trader no longer writes ticks (the feed already does) and every DB connection waits 30 s for the write lock. (2) Background loops in `run_paper.py` run as one group: if any loop dies, the whole process exits, and systemd (not an in-process loop) restarts it, so crashes are counted by systemd. (3) A 5-minute `polyperps-health.timer` runs `scripts/healthcheck.py`, which checks disk, tick freshness, unit state/restart count and the last prune result, and alerts once when a problem appears and once when it clears.

**Tech Stack:** Python 3.11+ stdlib (`asyncio`, `sqlite3`, `shutil`, `subprocess`, `json`), systemd (Ubuntu 24.04, systemd 255), pytest.

**Spec:** Architecture review delivered in chat 2026-09-30, items 1–3 (no separate spec file). Review excerpts:
- Item 1: `run_paper.py:164` duplicates the feed's `insert_tick`; a lock error there crashes paper. `db.connect` has no busy timeout (Python default 5 s).
- Item 2: `run_paper.py:204` creates four background tasks that are only inspected at shutdown; a dead loop is silent.
- Item 3: Telegram not wired; prune has no failure hook; no disk check; in-process restart loops hide crashes from systemd.

## Global Constraints

- Never add `--executor live` or `POLYMARKET_LIVE_TRADING` to any deploy file (enforced by `tests/test_deploy_files.py`).
- Do not change `PAPER_RUN_ID` (`paper-soak-1`) or the paper unit's `--run-id ${PAPER_RUN_ID}`.
- New settings must default in code: `/etc/polyperps/env` is NOT updated by deploys.
- All deploy files are LF only (no `\r`).
- No new dependencies.
- Run tests with `.venv/Scripts/python -m pytest` (Windows venv). Baseline is 361 passed.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. The state file `health-state.json` is missing (first run) or holds truncated or corrupt JSON: the health check must treat it as empty, not crash every 5 minutes. (Task 3, `test_load_state_*`.)
2. A deploy resets `NRestarts` to a lower number: no false "restarted" alert. (Task 3, `test_restart_counter_reset_is_silent`.)
3. An instrument with no stored ticks at all counts as stale, not as a crash. (Task 3, `test_missing_ticks_count_as_stale`.)
4. A problem that persists must not alert every 5 minutes: alert once when it appears, once when it clears. (Task 3, `test_disk_alert_once_then_recovered`.)
5. A background loop that returns cleanly (not only one that raises) must also end the run, so a stuck bot never looks healthy. (Task 2, `test_run_until_first_exits_returns_when_one_finishes_cleanly`.)

---

### Task 1: One tick writer, 30 s lock wait

**Files:**
- Modify: `polyperps/storage/db.py:119-124` (`connect`)
- Modify: `scripts/run_paper.py:163-164` (`on_accept`)
- Test: `tests/test_storage.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `db.connect(path)` returns a connection whose `PRAGMA busy_timeout` is `30000`.

- [ ] **Step 1: Write the failing test** (append to `tests/test_storage.py`; `connect` is already imported there)

```python
def test_connect_waits_30s_for_the_write_lock(tmp_path):
    # feed, paper, prune and healthcheck share one file; the 5 s default made paper crash under prune.
    conn = connect(tmp_path / "t.sqlite3")
    assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 30_000
```

- [ ] **Step 2: Run it to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_storage.py::test_connect_waits_30s_for_the_write_lock -v`
Expected: FAIL, `assert 5000 == 30000`

- [ ] **Step 3: Implement**

In `polyperps/storage/db.py`, change `connect` to:

```python
def connect(path: str | Path) -> sqlite3.Connection:
    # timeout=30: feed, paper, prune and healthcheck share this file; the 5 s default made
    # writers give up while prune held the lock.
    conn = sqlite3.connect(str(path), timeout=30)
    # WAL: cheaper commits, readers never block the writer (feed + backfill share the file).
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_SCHEMA)
    return conn
```

In `scripts/run_paper.py`, delete the line `db.insert_tick(conn, tick)` from `on_accept` (the feed service already stores every tick; paper only needs them in memory). `on_accept` becomes:

```python
    def on_accept(tick):
        marks[tick.instrument_id] = tick.mark_price
        executor.update_mark(tick.instrument_id, tick.mark_price)
        bar = builder.on_tick(tick)
        if bar is not None:
            executor.apply_funding(bar.instrument_id, bar.funding_rate)
            closed_bars.put_nowait(bar)
```

- [ ] **Step 4: Run the full suite**

Run: `.venv/Scripts/python -m pytest -q`
Expected: 362 passed

- [ ] **Step 5: Commit**

```bash
git add polyperps/storage/db.py scripts/run_paper.py tests/test_storage.py
git commit -m "fix(storage): 30 s lock wait on every connection; paper stops re-writing feed ticks"
```

---

### Task 2: A dead loop ends the process; systemd restarts it

**Files:**
- Modify: `scripts/run_paper.py` (add `run_until_first_exits`; rewire the end of `run_once`; delete `_supervise`, `INITIAL_BACKOFF_S`, `MAX_BACKOFF_S`; `main` calls `run_once` once)
- Modify: `scripts/run_feed.py:124-145` (`main`: no restart loop)
- Modify: `deploy/polyperps-feed.service`, `deploy/polyperps-paper.service` (`Restart=always` plus backoff)
- Test: `tests/test_run_paper_script.py`, `tests/test_deploy_files.py`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `run_paper.run_until_first_exits(*coros) -> None` (async). The first coroutine to finish ends the rest; its exception propagates.

- [ ] **Step 1: Write the failing tests**

In `tests/test_run_paper_script.py`, add `import asyncio` to the imports, then **replace** `test_run_id_minted_once_across_supervised_restarts` (it tests the in-process supervisor being deleted) with:

```python
def test_main_runs_once_and_lets_a_crash_exit(monkeypatch, tmp_path):
    """systemd (Restart=always) is the supervisor: a crash must leave the process so systemd
    counts it, not loop inside it. run_id is still minted once in main()."""
    monkeypatch.setenv("POLYPERPS_INSTRUMENT_IDS", "6")
    monkeypatch.setenv("POLYPERPS_DB_PATH", str(tmp_path / "t.sqlite3"))
    monkeypatch.setattr(sys, "argv", ["run_paper.py", "--executor", "sim", "--hypothesis", "h1"])
    mod = load()
    seen = []

    async def fake_run_once(args, settings):
        seen.append(args.run_id)
        raise RuntimeError("database is locked")

    monkeypatch.setattr(mod, "run_once", fake_run_once)
    with pytest.raises(RuntimeError, match="locked"):
        mod.main()
    assert len(seen) == 1 and seen[0].startswith("paper-")


def test_run_until_first_exits_propagates_a_dead_loop_and_cancels_the_rest():
    mod = load()
    cancelled = []

    async def forever():
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    async def dies():
        raise RuntimeError("database is locked")

    with pytest.raises(RuntimeError, match="locked"):
        asyncio.run(mod.run_until_first_exits(forever(), dies()))
    assert cancelled == [True]


def test_run_until_first_exits_returns_when_one_finishes_cleanly():
    mod = load()

    async def forever():
        await asyncio.sleep(3600)

    async def ends():
        return None

    asyncio.run(asyncio.wait_for(mod.run_until_first_exits(forever(), ends()), 5))
```

In `tests/test_deploy_files.py`, append:

```python
@pytest.mark.parametrize("name", ["polyperps-feed.service", "polyperps-paper.service"])
def test_long_running_units_are_supervised_by_systemd(name):
    # The scripts no longer restart themselves; systemd must, even after a clean exit.
    text = _read(name)
    assert "Restart=always" in text
    assert "RestartMaxDelaySec=" in text
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_run_paper_script.py tests/test_deploy_files.py -v --deselect tests/test_run_paper_script.py::test_main_runs_once_and_lets_a_crash_exit`
Expected: FAIL: `run_until_first_exits` missing (AttributeError); `Restart=always` missing. (The deselected test would loop forever against the old in-process supervisor, which restarts on every crash with growing sleeps; it is run after Step 3.)

- [ ] **Step 3: Implement `run_paper.py`**

Delete the constants `INITIAL_BACKOFF_S = 1.0` and `MAX_BACKOFF_S = 60.0`, and delete the whole `_supervise` function.

Add above `run_once`:

```python
async def run_until_first_exits(*coros) -> None:
    """Run every coroutine; the first to finish, by returning or raising, ends the rest. Its
    exception propagates, so a dead background loop takes the process down (systemd restarts
    it and counts it) instead of the bot running on without that loop."""
    tasks = [asyncio.ensure_future(c) for c in coros]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in done:
            t.result()
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
```

In `run_once`, change the comment on `run_id = args.run_id` to:
`# minted once in main(); systemd passes --run-id so every restart reopens the same paper account`

Replace the block from `tasks = [asyncio.create_task(...` through the end of `run_once` with:

```python
    try:
        await run_until_first_exits(feed.run(), bar_loop(), fast_loop(), reconcile_loop(), pf.run_event_pump())
        log.warning("paper run ended; exiting so systemd restarts it")
    finally:
        stop.set()
        # Deliberately not flushing builder.close_all() here: the currently-open hour is
        # partial, and close_all() stamps whatever it has as complete=True. Dropping it
        # is correct - it picks back up on the next tick after restart.
        with contextlib.suppress(Exception):
            await ticks.aclose()
        await client.close()
        conn.close()
```

In `main`, replace `asyncio.run(_supervise(args, settings))` with `asyncio.run(run_once(args, settings))`.

- [ ] **Step 4: Implement `run_feed.py`**

Replace `main` with:

```python
async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if "--list-instruments" in sys.argv:
        await list_instruments()
        return
    # No in-process restart loop: systemd (Restart=always) restarts the process, so a crash
    # shows in NRestarts and the health check instead of hiding inside an "active" unit.
    await run_once(load_settings())
    log.warning("feed stream ended; exiting so systemd restarts it")
```

- [ ] **Step 5: Update both units**

In `deploy/polyperps-feed.service` and `deploy/polyperps-paper.service`, replace

```
Restart=on-failure
RestartSec=10
```

with

```
# The scripts do not restart themselves; systemd does, even after a clean exit
# (stream ended). Backoff 10 s -> 120 s over 5 steps if it keeps crashing.
Restart=always
RestartSec=10
RestartSteps=5
RestartMaxDelaySec=120
```

(`systemctl stop` is never followed by a restart, whatever `Restart=` says.)

- [ ] **Step 6: Run the full suite**

Run: `.venv/Scripts/python -m pytest -q`
Expected: 366 passed (362 − 1 replaced test + 3 new run_paper tests + 2 parametrized deploy cases), 0 failures.

- [ ] **Step 7: Commit**

```bash
git add scripts/run_paper.py scripts/run_feed.py deploy/polyperps-feed.service deploy/polyperps-paper.service tests/test_run_paper_script.py tests/test_deploy_files.py
git commit -m "fix(paper): a dead background loop ends the run; systemd, not the script, restarts it"
```

---

### Task 3: Health check timer with Telegram alerts

**Files:**
- Create: `scripts/healthcheck.py`
- Create: `deploy/polyperps-health.service`, `deploy/polyperps-health.timer`
- Modify: `polyperps/monitor/alerts.py` (add `default_sinks`)
- Modify: `scripts/run_paper.py` (`_alerter` uses `default_sinks`)
- Modify: `deploy/polyperps-prune.service` (`OnFailure=`)
- Modify: `deploy/polyperps-paper.service` (Telegram via `ImportCredential=`)
- Modify: `deploy/update.sh`, `deploy/bootstrap.sh` (install + enable the health timer)
- Modify: `deploy/env.example`, `README.md` (document)
- Test: `tests/test_healthcheck.py` (create), `tests/test_deploy_files.py`

**Interfaces:**
- Consumes: `db.connect` (Task 1).
- Produces:
  - `alerts.default_sinks(conn) -> list[Sink]`: `[LogSink(), SqliteSink(conn)]`, plus `TelegramSink` when both secrets load.
  - `healthcheck.evaluate(*, now: datetime, disk_pct: float, last_ticks: dict[int, datetime | None], units: dict[str, dict[str, str]], prune_result: str, state: dict, disk_limit: float = 95.0, stale_s: float = 300.0) -> tuple[list[Alert], dict]`
  - `healthcheck.load_state(path: Path) -> dict`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_healthcheck.py`:

```python
import importlib.util
from datetime import datetime, timedelta, timezone

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
UP = {"ActiveState": "active", "NRestarts": "0"}


def load():
    spec = importlib.util.spec_from_file_location("healthcheck", "scripts/healthcheck.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run(mod, state=None, **kw):
    args = dict(now=NOW, disk_pct=50.0, last_ticks={6: NOW, 7: NOW},
                units={"polyperps-feed.service": UP, "polyperps-paper.service": UP},
                prune_result="success", state=state or {})
    args.update(kw)
    return mod.evaluate(**args)


def kinds(alerts):
    return [(a.level, a.kind) for a in alerts]


def test_healthy_box_is_silent():
    alerts, state = run(load())
    assert alerts == [] and state["open"] == []


def test_disk_alert_once_then_recovered():
    mod = load()
    alerts, state = run(mod, disk_pct=96.0)
    assert kinds(alerts) == [("CRITICAL", "disk_full")]
    alerts, state = run(mod, state=state, disk_pct=96.5)
    assert alerts == []                      # still full: no repeat every 5 minutes
    alerts, state = run(mod, state=state, disk_pct=80.0)
    assert kinds(alerts) == [("INFO", "recovered")] and state["open"] == []


def test_stale_ticks_alert_per_instrument():
    alerts, _ = run(load(), last_ticks={6: NOW - timedelta(minutes=10), 7: NOW})
    assert kinds(alerts) == [("CRITICAL", "ticks_stale")]
    assert alerts[0].instrument_id == 6


def test_missing_ticks_count_as_stale():
    alerts, _ = run(load(), last_ticks={6: None, 7: NOW})
    assert kinds(alerts) == [("CRITICAL", "ticks_stale")]


def test_unit_down_and_prune_failed():
    alerts, _ = run(load(), units={"polyperps-feed.service": {"ActiveState": "failed", "NRestarts": "0"},
                                   "polyperps-paper.service": UP},
                    prune_result="exit-code")
    assert sorted(kinds(alerts)) == [("CRITICAL", "prune_failed"), ("CRITICAL", "unit_down")]


def test_restart_alerts_on_each_increase_not_on_first_sight():
    mod = load()
    units = {"polyperps-feed.service": UP, "polyperps-paper.service": {"ActiveState": "active", "NRestarts": "4"}}
    alerts, state = run(mod, units=units)
    assert alerts == []                      # first run: no baseline yet
    units["polyperps-paper.service"] = {"ActiveState": "active", "NRestarts": "6"}
    alerts, state = run(mod, state=state, units=units)
    assert kinds(alerts) == [("CRITICAL", "unit_restarted")]
    assert alerts[0].detail == {"unit": "polyperps-paper.service", "restarts": 6, "new": 2}
    alerts, _ = run(mod, state=state, units=units)
    assert alerts == []


def test_restart_counter_reset_is_silent():
    mod = load()
    _, state = run(mod, units={"polyperps-feed.service": {"ActiveState": "active", "NRestarts": "9"},
                               "polyperps-paper.service": UP})
    alerts, state = run(mod, state=state)    # deploy restarted the unit: counter back to 0
    assert alerts == [] and state["restarts"]["polyperps-feed.service"] == 0


def test_load_state_missing_or_corrupt_is_empty(tmp_path):
    mod = load()
    assert mod.load_state(tmp_path / "nope.json") == {}
    bad = tmp_path / "bad.json"
    bad.write_text('{"open": [', encoding="utf-8")
    assert mod.load_state(bad) == {}
```

In `tests/test_deploy_files.py`, add `"polyperps-health.service", "polyperps-health.timer"` to `LF_ONLY_FILES` (append to the list expression), and append:

```python
def test_health_check_is_wired():
    health = _read("polyperps-health.service")
    assert "User=polyperps" in health
    assert "ImportCredential=TELEGRAM_BOT_TOKEN" in health
    for script in EXEC_START_RE.findall(health):
        assert (REPO_ROOT / script).is_file()
    assert "OnUnitActiveSec=5min" in _read("polyperps-health.timer")
    assert "OnFailure=polyperps-health.service" in _read("polyperps-prune.service")
    assert "ImportCredential=TELEGRAM_BOT_TOKEN" in _read("polyperps-paper.service")
    for name in ("bootstrap.sh", "update.sh"):
        assert "polyperps-health.timer" in _read(name)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_healthcheck.py tests/test_deploy_files.py -v`
Expected: FAIL: `scripts/healthcheck.py` not found; health unit files missing.

- [ ] **Step 3: Add `default_sinks` to `polyperps/monitor/alerts.py`**

Add after the `Alerter` class:

```python
def default_sinks(conn) -> list[Sink]:
    """Journal + alerts table always; Telegram (CRITICAL only) when both secrets load."""
    from polyperps.security.key_management import SecretUnavailable, load_secret

    sinks: list[Sink] = [LogSink(), SqliteSink(conn)]
    try:
        sinks.append(TelegramSink(token=load_secret("TELEGRAM_BOT_TOKEN"), chat_id=load_secret("TELEGRAM_CHAT_ID")))
    except SecretUnavailable:
        _log.info("telegram sink not configured")
    return sinks
```

In `scripts/run_paper.py`, replace the body of `_alerter` with `return Alerter(run_id, default_sinks(conn))`; change the alerts import to `from polyperps.monitor.alerts import Alerter, default_sinks`; delete the now-unused `from polyperps.security.key_management import SecretUnavailable, load_secret` import (check with grep that nothing else in the file uses them).

- [ ] **Step 4: Create `scripts/healthcheck.py`**

```python
"""Ops health check, run every 5 minutes by polyperps-health.timer (and by prune's OnFailure=).

Raises a CRITICAL alert (Telegram when configured; always the journal and the alerts table,
run_id "ops-health") when:
  - the DB's disk is at least HEALTH_DISK_PCT full (default 95)
  - an instrument's newest stored tick is older than HEALTH_TICK_STALE_S (default 300)
  - polyperps-feed / polyperps-paper is not active, or systemd restarted it since the last check
  - the last polyperps-prune run did not succeed
A lasting problem alerts once when it appears and once (INFO "recovered") when it clears; that
memory lives in health-state.json next to the DB. Thresholds default here, not in
/etc/polyperps/env, because deploys never update that file.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from polyperps.config import load_settings
from polyperps.monitor.alerts import Alert, Alerter, default_sinks
from polyperps.storage import db

RUN_ID = "ops-health"   # not the paper run id: ops alerts must not change the soak's own counters
UNITS = ("polyperps-feed.service", "polyperps-paper.service")
PRUNE = "polyperps-prune.service"
log = logging.getLogger("polyperps.health")


def _alert(now: datetime, level: str, key: str, detail: dict) -> Alert:
    return Alert(level=level, kind=key.split(":")[0], instrument_id=detail.get("instrument_id"),
                 detail=detail, ts=now)


def evaluate(*, now: datetime, disk_pct: float, last_ticks: dict[int, datetime | None],
             units: dict[str, dict[str, str]], prune_result: str, state: dict,
             disk_limit: float = 95.0, stale_s: float = 300.0) -> tuple[list[Alert], dict]:
    """Pure: current readings + last state -> (alerts to send, next state)."""
    problems: dict[str, dict] = {}
    if disk_pct >= disk_limit:
        problems["disk_full"] = {"used_pct": round(disk_pct, 1), "limit_pct": disk_limit}
    for iid, ts in last_ticks.items():
        age = None if ts is None else (now - ts).total_seconds()
        if age is None or age > stale_s:
            problems[f"ticks_stale:{iid}"] = {"instrument_id": iid, "age_s": None if age is None else round(age)}
    for unit, props in units.items():
        if props.get("ActiveState") != "active":
            problems[f"unit_down:{unit}"] = {"unit": unit, "state": props.get("ActiveState")}
    if prune_result not in ("success", ""):
        problems["prune_failed"] = {"result": prune_result}

    alerts: list[Alert] = []
    seen = state.get("restarts", {})
    restarts = {unit: int(props.get("NRestarts") or 0) for unit, props in units.items()}
    for unit, n in restarts.items():
        before = seen.get(unit, n)   # first sight: baseline, no alert; a lower count (reset by a deploy): silent
        if n > before:
            alerts.append(_alert(now, "CRITICAL", "unit_restarted", {"unit": unit, "restarts": n, "new": n - before}))
    was = set(state.get("open", []))
    for key in sorted(problems.keys() - was):
        alerts.append(_alert(now, "CRITICAL", key, problems[key]))
    for key in sorted(was - problems.keys()):
        alerts.append(_alert(now, "INFO", "recovered", {"problem": key}))
    return alerts, {"open": sorted(problems), "restarts": restarts}


def load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return {}   # first run, or a write cut short: start fresh rather than fail every run


def unit_props(unit: str) -> dict[str, str]:
    out = subprocess.run(["systemctl", "show", unit, "-p", "ActiveState,NRestarts,Result"],
                         capture_output=True, text=True, check=True).stdout
    return dict(line.split("=", 1) for line in out.splitlines() if "=" in line)


def last_tick(conn, instrument_id: int) -> datetime | None:
    (ts,) = conn.execute("SELECT MAX(exchange_ts) FROM ticks WHERE instrument_id=?", (instrument_id,)).fetchone()
    return None if ts is None else datetime.fromisoformat(ts)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = load_settings()
    now = datetime.now(timezone.utc)
    conn = db.connect(settings.db_path)
    alerter = Alerter(RUN_ID, default_sinks(conn))
    state_path = settings.db_path.parent / "health-state.json"
    try:
        usage = shutil.disk_usage(settings.db_path.parent)
        alerts, state = evaluate(
            now=now,
            disk_pct=100 * usage.used / usage.total,
            last_ticks={i: last_tick(conn, i) for i in settings.instrument_ids},
            units={u: unit_props(u) for u in UNITS},
            prune_result=unit_props(PRUNE).get("Result", ""),
            state=load_state(state_path),
            disk_limit=float(os.environ.get("HEALTH_DISK_PCT") or 95),
            stale_s=float(os.environ.get("HEALTH_TICK_STALE_S") or 300),
        )
        for a in alerts:
            alerter.emit(a)
        state_path.write_text(json.dumps(state), encoding="utf-8")
        log.info("health: %d alert(s), open=%s", len(alerts), state["open"])
    except Exception as exc:
        alerter.emit(Alert(level="CRITICAL", kind="healthcheck_error", instrument_id=None,
                           detail={"error": repr(exc)}, ts=now))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run the healthcheck tests**

Run: `.venv/Scripts/python -m pytest tests/test_healthcheck.py -v`
Expected: 8 passed

- [ ] **Step 6: Create the units** (LF line endings)

`deploy/polyperps-health.service`:

```
[Unit]
Description=polyperps ops health check (disk, tick freshness, unit restarts, prune result)

[Service]
Type=oneshot
User=polyperps
Group=polyperps
WorkingDirectory=/opt/polyperps
EnvironmentFile=/etc/polyperps/env
# Telegram secrets, if present in /etc/credstore/ (root:root 0600). Missing files are skipped.
ImportCredential=TELEGRAM_BOT_TOKEN
ImportCredential=TELEGRAM_CHAT_ID
ExecStart=/opt/polyperps/.venv/bin/python scripts/healthcheck.py
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=/var/lib/polyperps
StandardOutput=journal
StandardError=journal
```

`deploy/polyperps-health.timer`:

```
[Unit]
Description=polyperps ops health check every 5 minutes

[Timer]
OnBootSec=5min
OnUnitActiveSec=5min

[Install]
WantedBy=timers.target
```

In `deploy/polyperps-prune.service`, add under `[Unit]` after the `Description=` line:

```
# A failed prune is checked (and alerted) right away, not at the next 5-minute tick.
OnFailure=polyperps-health.service
```

In `deploy/polyperps-paper.service`, replace the four lines from `# Telegram alert secrets (optional).` through `#LoadCredential=TELEGRAM_CHAT_ID:/etc/credstore/TELEGRAM_CHAT_ID` with:

```
# Telegram alert secrets, if present in /etc/credstore/ (root:root 0600).
# Missing files are skipped, so nothing needs uncommenting.
ImportCredential=TELEGRAM_BOT_TOKEN
ImportCredential=TELEGRAM_CHAT_ID
```

- [ ] **Step 7: Wire the deploy scripts**

In both `deploy/update.sh` and `deploy/bootstrap.sh`, append `polyperps-health.service polyperps-health.timer` to the `for unit in ...` list, and directly after the existing `systemctl enable --now polyperps-prune.timer` line add:

```bash
systemctl enable --now polyperps-health.timer
```

In `deploy/bootstrap.sh`, replace next-steps item 2 with:

```
2. Optionally drop Telegram secrets into /etc/credstore/TELEGRAM_BOT_TOKEN and
   /etc/credstore/TELEGRAM_CHAT_ID (root:root 0600); the paper and health units
   import them automatically on their next start.
```

In `deploy/env.example`, append:

```
# Health check (polyperps-health.timer, every 5 min). Optional: the defaults
# live in scripts/healthcheck.py because deploys never update this file.
# HEALTH_DISK_PCT=95
# HEALTH_TICK_STALE_S=300
```

In `README.md`: in the Phase 2a table, change the Telegram row to `| Telegram CRITICAL alerts (optional) | put TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID files in /etc/credstore/ (root:root 0600) |`; in the Deploy "First time" paragraph, replace the sentence about uncommenting `LoadCredential=` lines with `optionally drop TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID files into /etc/credstore/ (root:root 0600; the units import them automatically);`; and after the **Watching** paragraph add:

```
**Health check**: `polyperps-health.timer` runs `scripts/healthcheck.py` every 5 minutes
(and right after any failed prune). It alerts CRITICAL, once per problem, on: disk ≥95 %,
newest tick older than 5 minutes, feed/paper not active or restarted by systemd, or a
failed prune; and INFO when the problem clears. Alerts go to the journal
(`journalctl -u polyperps-health`), the `alerts` table (run_id `ops-health`) and Telegram
when configured. Without Telegram, nobody is paged.
```

- [ ] **Step 8: Run the full suite**

Run: `.venv/Scripts/python -m pytest -q`
Expected: all pass, 0 failures.

- [ ] **Step 9: Commit**

```bash
git add scripts/healthcheck.py polyperps/monitor/alerts.py scripts/run_paper.py deploy/ README.md tests/test_healthcheck.py tests/test_deploy_files.py
git commit -m "feat(ops): 5-minute health check alerts on disk, stale ticks, restarts and failed prunes"
```

---

## After all tasks (controller, not an implementer)

- Whole-branch review (opus).
- Deploy (user's call, restarts paper): `deploy\deploy.ps1 -Session polyperps-ec2` (move `HANDOFF.md` aside first). On the box, verify: `systemctl --version` ≥ 254 (`ImportCredential`, `RestartSteps`), `systemctl list-timers polyperps-health`, `sudo systemctl start polyperps-health && journalctl -u polyperps-health -n 5 -o cat` shows `health: 0 alert(s)` (or a real `disk_full` if ≥95 %).
- User action: create the Telegram bot and chat files in `/etc/credstore/`, or nobody is paged.
