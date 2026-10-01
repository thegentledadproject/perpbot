# Soak Warm-up Ops Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a paper-soak restart cheap: the strategy re-seeds real history in seconds, and Ubuntu's auto-updates stop restarting the trading services.

**Architecture:** (1) An hourly `polyperps-backfill.timer` stores closed 1h candles and funding (nothing has since 2026-09-23, so every restart re-warms from zero); the backfill never stores a still-open candle; the health check alerts when it fails. (2) `query_last_index_by_hour` computes "last index price per hour" in SQL instead of iterating millions of tick rows in Python (~3 min per instrument per restart today). (3) A needrestart override stops library upgrades from restarting `polyperps-*` units.

**Tech Stack:** Python 3.11+, SQLite, systemd (Ubuntu 24.04, systemd 259), needrestart 3.11, pytest.

**Spec:** Findings from the 2026-10-01 deploy investigation (in chat, no spec file):
- Box candles/funding max timestamp = 2026-09-23T14:00 for instruments 6 and 7; every restart logs `seeded 0 bars`.
- Seeding takes ~3 min per instrument (01:05:54 → 01:09:14 → 01:12:37), all in the tick scan.
- 2026-10-01 06:05 UTC unattended-upgrade (libssl3t64/openssl) → needrestart ran `systemctl restart ... polyperps-dashboard polyperps-feed polyperps-paper ...`.
- `scripts/backfill.py` sets `end = now`, and `db.insert_candle` is `INSERT OR IGNORE`, so a candle fetched while its hour is open would be stored half-built and never corrected.

## Global Constraints

- Never add `--executor live` or `POLYMARKET_LIVE_TRADING` to any deploy file.
- `PAPER_RUN_ID` stays `paper-soak-1`; do not change `deploy/polyperps-paper.service`.
- New settings default in code (`/etc/polyperps/env` is not updated by deploys).
- Deploy files are LF only.
- No new dependencies.
- Tests run with `.venv/Scripts/python -m pytest`; baseline 480 passed.
- Commit messages end with a blank line then `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. A backfill run inside an hour that is still open must not store that hour's candle (it would never be corrected). Pinned in Task 1: `test_open_candle_is_not_stored`.
2. Two ticks in the same hour with the same `exchange_ts`: the result must still be one value for that hour (no crash, no duplicate keys). Pinned in Task 2: `test_query_last_index_by_hour_same_timestamp_tie_is_one_value`.
3. A health-state file written by the previous version (open problem key `prune_failed`) must not crash the new check or leave a stale open problem forever. Pinned in Task 1: `test_old_prune_failed_key_clears`.
4. Hours with no native ticks (feed down) must be absent from the result, not `None` or zero. Existing test `test_query_last_index_by_hour_last_per_hour_wins_native_only` keeps covering it; Task 2 must keep it green unchanged.
5. A needrestart config file with a Perl syntax error would break every future unattended upgrade on the box. Pinned in Task 3 by a static test of the exact line, plus a box-side `perl -c` check in the deploy step.

---

### Task 1: Hourly backfill of closed candles and funding, alerted on failure

**Files:**
- Modify: `scripts/backfill.py` (`_fetch_window`, `main`)
- Create: `deploy/polyperps-backfill.service`, `deploy/polyperps-backfill.timer`
- Modify: `scripts/healthcheck.py` (`evaluate`, `main`, docstring)
- Modify: `deploy/update.sh`, `deploy/bootstrap.sh` (unit loop + enable timer)
- Modify: `README.md` (Health check paragraph; new "Backfill" line)
- Test: `tests/test_backfill_script.py` (create), `tests/test_healthcheck.py`, `tests/test_deploy_files.py`

**Interfaces:**
- Produces: `backfill.closed(candles, now) -> list[Candle]` (pure filter); `healthcheck.evaluate(..., oneshot_results: dict[str, str], ...)` replacing `prune_result: str`; alert kind `oneshot_failed` with detail `{"unit": ..., "result": ...}`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_backfill_script.py`:

```python
import importlib.util
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.exchange.types import Candle, SourceType

T0 = datetime(2026, 10, 1, 14, 0, tzinfo=timezone.utc)
H = timedelta(hours=1)


def load():
    spec = importlib.util.spec_from_file_location("backfill", "scripts/backfill.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _c(open_ts, interval="1h"):
    return Candle(instrument_id=6, interval=interval, open_ts=open_ts, open=Decimal(1), high=Decimal(1),
                  low=Decimal(1), close=Decimal(1), volume=Decimal(1), trades=1, received_ts=open_ts,
                  source_type=SourceType.POLYMARKET_REST)


def test_open_candle_is_not_stored():
    """The 14:00 candle is still open at 14:05; INSERT OR IGNORE would freeze it half-built."""
    mod = load()
    out = mod.closed([_c(T0 - H), _c(T0)], now=T0 + timedelta(minutes=5))
    assert [c.open_ts for c in out] == [T0 - H]


def test_candle_closing_exactly_now_is_kept():
    mod = load()
    out = mod.closed([_c(T0 - H)], now=T0)
    assert len(out) == 1


def test_minute_candles_use_their_own_interval():
    mod = load()
    out = mod.closed([_c(T0, "1m"), _c(T0 + timedelta(minutes=1), "1m")], now=T0 + timedelta(minutes=1, seconds=30))
    assert [c.open_ts for c in out] == [T0]
```

Note: check `Candle`'s real constructor in `polyperps/exchange/types.py` before running and adjust the field names in `_c` if they differ (keep the test's meaning).

In `tests/test_healthcheck.py`: in `run()` replace `prune_result="success"` with `oneshot_results={"polyperps-prune.service": "success", "polyperps-backfill.service": "success"}`; replace `test_unit_down_and_prune_failed` with:

```python
def test_unit_down_and_oneshot_failed():
    alerts, _ = run(load(), units={"polyperps-feed.service": {"ActiveState": "failed", "NRestarts": "0"},
                                   "polyperps-paper.service": UP},
                    oneshot_results={"polyperps-prune.service": "success",
                                     "polyperps-backfill.service": "exit-code"})
    assert sorted(kinds(alerts)) == [("CRITICAL", "oneshot_failed"), ("CRITICAL", "unit_down")]
    failed = [a for a in alerts if a.kind == "oneshot_failed"][0]
    assert failed.detail == {"unit": "polyperps-backfill.service", "result": "exit-code"}


def test_old_prune_failed_key_clears():
    """State written by the previous version used the key 'prune_failed'; it clears as recovered."""
    alerts, state = run(load(), state={"open": ["prune_failed"], "restarts": {}})
    assert kinds(alerts) == [("INFO", "recovered")] and state["open"] == []
```

In `tests/test_deploy_files.py`: add `"polyperps-backfill.service", "polyperps-backfill.timer"` to `LF_ONLY_FILES`, add `"polyperps-backfill.service"` to the parametrized list at the `test_...` that checks units never enable live trading (the one parametrized over `UNIT_FILES + SHELL_FILES + ["polyperps-health.service", "polyperps-prune.service"]`), and append:

```python
def test_backfill_timer_is_wired():
    svc = _read("polyperps-backfill.service")
    assert "User=polyperps" in svc
    assert "OnFailure=polyperps-health.service" in svc
    assert "scripts/backfill.py --days 1 --interval 1h" in svc
    for script in EXEC_START_RE.findall(svc):
        assert (REPO_ROOT / script).is_file()
    assert "OnCalendar=*:05" in _read("polyperps-backfill.timer")
    for name in ("bootstrap.sh", "update.sh"):
        assert "polyperps-backfill.timer" in _read(name)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_backfill_script.py tests/test_healthcheck.py tests/test_deploy_files.py -q`
Expected: FAIL: `closed` missing; `evaluate()` got an unexpected keyword `oneshot_results`; backfill unit files missing.

- [ ] **Step 3: Implement `scripts/backfill.py`**

Add next to `_fetch_window` (import `Candle` from `polyperps.exchange.types`; `parse_interval` is already imported):

```python
def closed(candles: list[Candle], now: datetime) -> list[Candle]:
    """Only candles whose interval has ended. db.insert_candle is INSERT OR IGNORE, so a candle
    stored while its interval is still open would stay half-built forever."""
    return [c for c in candles if c.open_ts + parse_interval(c.interval) <= now]
```

In `_fetch_window`, add a `now: datetime` parameter and wrap the candle fetch:

```python
            n_c = sum(
                db.insert_candle(conn, c)
                for c in closed(await client.fetch_candles(iid, interval=interval, start=w_start, end=w_end), now)
            )
```

and pass `end` (the `now` computed once in `main`) at the call site: `await _fetch_window(client, conn, iid, args.interval, w_start, w_end, end)`.

- [ ] **Step 4: Implement `scripts/healthcheck.py`**

Replace `PRUNE = "polyperps-prune.service"` with:

```python
ONESHOTS = ("polyperps-prune.service", "polyperps-backfill.service")
```

In `evaluate`, replace the `prune_result: str` parameter with `oneshot_results: dict[str, str]` and the prune block with:

```python
    for unit, result in oneshot_results.items():
        if result not in ("success", ""):
            problems[f"oneshot_failed:{unit}"] = {"unit": unit, "result": result}
```

In `main`, replace `prune_result=unit_props(PRUNE).get("Result", ""),` with
`oneshot_results={u: unit_props(u).get("Result", "") for u in ONESHOTS},`.
Update the module docstring: "(and by prune's and backfill's OnFailure=)" and "the last polyperps-prune or polyperps-backfill run did not succeed".

- [ ] **Step 5: Create the units** (LF)

`deploy/polyperps-backfill.service`:

```
[Unit]
Description=polyperps hourly backfill of closed 1h candles and funding (strategy warm-up history)
# A failed backfill is checked (and alerted) right away.
OnFailure=polyperps-health.service

[Service]
Type=oneshot
User=polyperps
Group=polyperps
WorkingDirectory=/opt/polyperps
EnvironmentFile=/etc/polyperps/env
ExecStart=/opt/polyperps/.venv/bin/python scripts/backfill.py --days 1 --interval 1h
TimeoutStartSec=10min
NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=/var/lib/polyperps
StandardOutput=journal
StandardError=journal
```

`deploy/polyperps-backfill.timer`:

```
[Unit]
Description=hourly polyperps backfill, 5 minutes past the hour (the hour's candle and funding have closed)

[Timer]
OnCalendar=*:05
Persistent=true

[Install]
WantedBy=timers.target
```

- [ ] **Step 6: Wire the deploy scripts and README**

In `deploy/update.sh` and `deploy/bootstrap.sh`, append `polyperps-backfill.service polyperps-backfill.timer` to the `for unit in ...` list. In `update.sh` add `systemctl enable --now polyperps-backfill.timer` next to the existing `systemctl enable --now polyperps-prune.timer`; in `bootstrap.sh` add the same line next to its prune-timer enable.

In `README.md`'s Health check paragraph, change "or a failed prune" to "or a failed prune or backfill" and "(and right after any failed prune)" to "(and right after any failed prune or backfill)". After it, add:

```
**Backfill**: `polyperps-backfill.timer` runs `scripts/backfill.py --days 1 --interval 1h` at 5 past
every hour, storing closed 1h candles and funding so a restarted paper run seeds its strategy
history instead of re-warming from zero. Only closed candles are stored (inserts never overwrite).
```

- [ ] **Step 7: Run tests, full suite, commit**

Run: `.venv/Scripts/python -m pytest tests/test_backfill_script.py tests/test_healthcheck.py tests/test_deploy_files.py -q`, then `.venv/Scripts/python -m pytest -q`.
Expected: all pass.

```bash
git add scripts/backfill.py scripts/healthcheck.py deploy/ README.md tests/test_backfill_script.py tests/test_healthcheck.py tests/test_deploy_files.py
git commit -m "feat(ops): hourly backfill of closed candles and funding, alerted on failure

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Last index price per hour computed in SQL

**Files:**
- Modify: `polyperps/storage/db.py` (`query_last_index_by_hour`)
- Test: `tests/test_storage_phase1.py`

**Interfaces:**
- Consumes/produces: `query_last_index_by_hour(conn, instrument_id, *, start, end) -> dict[datetime, Decimal]` — signature and result unchanged.

- [ ] **Step 1: Write the failing test** (append to `tests/test_storage_phase1.py`; `connect`, `insert_tick`, `query_last_index_by_hour`, `_tick`, `T0`, `H`, `SourceType` already exist in that file)

```python
def test_query_last_index_by_hour_same_timestamp_tie_is_one_value():
    """Two native ticks share the hour's last exchange_ts: one value for that hour, no crash."""
    conn = connect(":memory:")
    ts = T0 + timedelta(minutes=30)
    insert_tick(conn, _tick(ts, "100", seq=1))
    insert_tick(conn, _tick(ts, "101", seq=2))
    out = query_last_index_by_hour(conn, 6, start=T0, end=T0 + H - timedelta(microseconds=1))
    assert list(out) == [T0] and out[T0] in (Decimal("100"), Decimal("101"))


def test_query_last_index_by_hour_reduces_in_sql():
    """The reduction happens in SQLite (GROUP BY hour), not by iterating every tick in Python."""
    import inspect

    import polyperps.storage.db as dbmod
    assert "GROUP BY" in inspect.getsource(dbmod.query_last_index_by_hour)
```

The second test is a cheap guard against reverting to the row loop; keep it.

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_storage_phase1.py -q -k last_index`
Expected: the GROUP BY test fails (the current code iterates rows); the tie test may already pass — that is fine, it pins Review Focus #2.

- [ ] **Step 3: Implement**

Replace the body of `query_last_index_by_hour` in `polyperps/storage/db.py`:

```python
def query_last_index_by_hour(
    conn: sqlite3.Connection, instrument_id: int, *, start: datetime, end: datetime
) -> dict[datetime, Decimal]:
    """Last native index_price per hour in [start, end], reduced in SQLite: one row per hour
    comes back instead of every tick (millions per day; the Python loop took ~3 min per
    instrument at every paper restart). exchange_ts is UTC ISO text from _ts(), so its first
    13 chars ('YYYY-MM-DDTHH') are the hour; SQLite returns the bare index_price from the row
    holding MAX(exchange_ts)."""
    # ponytail: ties on the hour's last exchange_ts pick either row (the old loop broke ties by
    # sequence); add sequence to the reduction if same-timestamp ticks ever disagree in practice.
    cur = conn.execute(
        "SELECT substr(exchange_ts, 1, 13) AS hour, index_price, MAX(exchange_ts) FROM ticks "
        "WHERE instrument_id=? AND source_type IN (?, ?) AND exchange_ts BETWEEN ? AND ? "
        "GROUP BY hour",
        (instrument_id, *_NATIVE_TICK_SOURCES, _ts(start), _ts(end)),
    )
    return {datetime.fromisoformat(hour + ":00:00+00:00"): Decimal(index_price)
            for hour, index_price, _ in cur}
```

- [ ] **Step 4: Run tests and full suite**

Run: `.venv/Scripts/python -m pytest tests/test_storage_phase1.py tests/test_bars.py -q`, then `.venv/Scripts/python -m pytest -q`.
Expected: all pass, including the unchanged existing `last_index` tests.

- [ ] **Step 5: Commit**

```bash
git add polyperps/storage/db.py tests/test_storage_phase1.py
git commit -m "perf(storage): last index price per hour reduced in SQL, not a Python row loop

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Library upgrades no longer restart the trading services

**Files:**
- Create: `deploy/needrestart-polyperps.conf`
- Modify: `deploy/update.sh`, `deploy/bootstrap.sh`, `README.md`
- Test: `tests/test_deploy_files.py`

**Interfaces:** none.

- [ ] **Step 1: Write the failing test** (append to `tests/test_deploy_files.py`; also add `"needrestart-polyperps.conf"` to `LF_ONLY_FILES`)

```python
def test_needrestart_never_restarts_polyperps_units():
    conf = _read("needrestart-polyperps.conf")
    assert "$nrconf{override_rc}{qr(^polyperps-)} = 0;" in conf
    for name in ("bootstrap.sh", "update.sh"):
        assert "/etc/needrestart/conf.d/polyperps.conf" in _read(name)
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_deploy_files.py -q`
Expected: FAIL: `missing deploy file: .../needrestart-polyperps.conf`.

- [ ] **Step 3: Implement**

`deploy/needrestart-polyperps.conf` (LF):

```
# polyperps: never let needrestart restart the trading services after a library upgrade.
# 2026-10-01 06:05 an openssl security update restarted feed, paper and dashboard mid-soak,
# which resets the strategy's warm-up. They pick up new libraries on the next deploy.
$nrconf{override_rc}{qr(^polyperps-)} = 0;
```

In `deploy/update.sh` and `deploy/bootstrap.sh`, after the systemd unit loop, add (paths relative to each script's own convention — `update.sh` runs from `/opt/polyperps`, `bootstrap.sh` uses `/opt/polyperps/deploy/...`):

```bash
mkdir -p /etc/needrestart/conf.d
cp deploy/needrestart-polyperps.conf /etc/needrestart/conf.d/polyperps.conf   # update.sh
```

```bash
mkdir -p /etc/needrestart/conf.d
cp /opt/polyperps/deploy/needrestart-polyperps.conf /etc/needrestart/conf.d/polyperps.conf   # bootstrap.sh
```

In `README.md`'s Deploy section, after the **Health check** paragraph, add:

```
**Auto-updates**: unattended-upgrades keeps patching the OS, but
`/etc/needrestart/conf.d/polyperps.conf` (from `deploy/needrestart-polyperps.conf`) stops it from
restarting `polyperps-*` units. They pick up patched libraries on the next deploy, so deploy (or
restart them deliberately) after a security update you care about.
```

- [ ] **Step 4: Run tests and full suite, commit**

Run: `.venv/Scripts/python -m pytest tests/test_deploy_files.py -q`, then `.venv/Scripts/python -m pytest -q`.

```bash
git add deploy/needrestart-polyperps.conf deploy/update.sh deploy/bootstrap.sh README.md tests/test_deploy_files.py
git commit -m "fix(ops): library upgrades no longer restart the polyperps services mid-soak

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## After all tasks (controller)

- Whole-branch review, PR, merge, deploy (user's standing preference: PR + merge + deploy).
- On the box after deploy: `sudo perl -c /etc/needrestart/conf.d/polyperps.conf` (syntax OK); `systemctl list-timers polyperps-backfill`; one-time catch-up of the gap since 2026-09-23: `sudo systemd-run --wait --pipe -p User=polyperps -p EnvironmentFile=/etc/polyperps/env -p WorkingDirectory=/opt/polyperps /opt/polyperps/.venv/bin/python scripts/backfill.py --days 9 --interval 1h`; then `sudo systemctl restart polyperps-paper` and confirm `seeded N bars` with N > 0 and seeding in seconds, not minutes.
