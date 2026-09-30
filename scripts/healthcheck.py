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
