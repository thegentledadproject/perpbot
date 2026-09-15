# polyperps Dashboard — read-only ops page for the paper soak: Design

Date: 2026-09-15
Status: approved in brainstorming; implementation plan follows.
Builds on: Phases 0–2a merged to `master`; the paper soak `paper-soak-1` running on EC2 (`polyperps-feed.service`, `polyperps-paper.service`).
Visual reference: the approved "Ops console" artboard (Claude Design artifact "Polyperps Dashboard", 2026-09-15). This spec is the source of truth where the two differ.

## 1. Purpose

One web page, served from the EC2 box, that shows what the paper run is doing right now: account, positions and their guard distances, router decisions, alerts, feed health, the three live-trading locks, and the road-to-live counters. It reads the trading DB; it never writes it and it cannot influence `run_paper.py`.

Not in scope: an equity time series (nothing stores one), the "open human items" checklist (lives in memory/docs, not in the bot), any control (halts are cleared with `--clear-halt` on the CLI, a human decision), TLS, authentication.

## 2. Decisions taken in brainstorming

| Decision | Choice | Consequence |
|---|---|---|
| Access | **Port 80 on the EC2 box, plain HTTP, no auth** | The user restricts the security group to their own IP. The page exposes run state (positions, decisions) to anyone who can reach the port; nothing on it is a secret or a control. |
| Stack | **Python stdlib `http.server`, no new dependencies** | The repo pins three deps deliberately; one JSON route does not justify FastAPI/uvicorn. Rejected: FastAPI (dep tree on a box holding a wallet key), rendering inside `run_paper.py` (couples the dashboard to the trading loop). |
| Process | **Third systemd unit, same user and hardening as the siblings** | `AmbientCapabilities=CAP_NET_BIND_SERVICE` lets the `polyperps` user bind port 80. A dashboard crash cannot touch the feed or the paper run. |
| DB access | **Own connection per request, opened read-only (`mode=ro` URI)** | WAL mode already makes a second reader safe (`gap_report.py` relies on it). Same OS user as the writer, so the `-shm` file is writable as WAL readers require; the `mode=ro` flag is what stops the dashboard from ever writing rows. |
| Refresh | **Page polls `/api/state` every 10 s** | No websockets, no push. Interactions (instrument switch, tabs, position detail) are client-side and survive a refresh. |
| Instrument names / clusters | **Fetched once at start from the exchange client, refreshed hourly, best-effort** | Not stored in the DB. On failure: names fall back to `inst <id>`, cluster-net exposure is `null` and shows "n/a". |
| Open items panel | **Dropped** | Page shows only what the bot itself knows. |

## 3. Module layout (additions)

```
polyperps/
└── dashboard/
    ├── __init__.py
    ├── state.py            # build_state(conn, run_id, instrument_ids, instruments, now) -> dict  (pure; all the logic)
    ├── server.py           # DashboardServer: ThreadingHTTPServer; routes "/", "/api/state"; everything else 404
    └── static/
        └── index.html      # one file, vanilla JS, no build step; the approved ops-console look
scripts/
└── run_dashboard.py        # env -> settings; instrument fetch; serve; clean SIGTERM stop
deploy/
├── polyperps-dashboard.service
├── env.example             # + POLYPERPS_DASHBOARD_BIND=0.0.0.0:80
├── bootstrap.sh / update.sh / deploy.ps1   # know about the third unit (install, enable, restart, tail)
tests/
├── test_dashboard_state.py
├── test_dashboard_server.py
└── test_deploy_files.py    # extended
```

`pyproject.toml` gains `package-data` for `polyperps/dashboard/static/*.html` so the page ships with the package on the box.

## 4. `build_state` — the JSON and where each number comes from

All reads go through existing `polyperps/storage/db.py` helpers (`load_sim_account`, `get_positions_local`, `list_decisions`, `list_orders`, `list_alerts`, `list_recovery`, `count_rejections`) and `polyperps/storage/gaps.find_gaps`. All maths reuse `risk/liquidation_guard.LIMITS`, the sim liquidation-price formula (`sim_executor.py`), `risk/portfolio_exposure.vet_exposure`, and `signal/sufficiency.check_dataset`. Decimals are serialized as strings where the DB stores them as strings; percentages as floats in [0, 1]; timestamps ISO-8601 UTC.

```jsonc
{
  "generated_at": "...",
  "run": {
    "run_id": "paper-soak-1", "executor": "sim", "hypothesis": "h1", "host": "<hostname>",
    "started_at": "...",            // earliest ts across decisions/orders/alerts for run_id; null if none
    "uptime_s": 0
  },
  "account": {                      // null until sim_account has a row for run_id
    "equity": "…", "start_equity": "…", "pnl_since_start": "…", "unrealized": "…",
    "gross_exposure": 0.74, "gross_limit": 1.0,
    "cluster_net": 0.41, "cluster_limit": 0.6,   // cluster_net null when instruments unknown
    "leverage": 3, "kill_switch": "unarmed"       // literal in 2a: thresholds are None
  },
  "positions": [{
    "instrument_id": 6, "name": "BTC", "state": "OPEN", "side": "LONG", "size": "…",
    "entry_price": "…", "mark": "…", "pnl": "…",
    "liq_price": "…", "liq_distance": 0.41,          // |mark - liq_price| / mark
    "adverse_move": 0.006,                            // max(0, unfavourable move since entry / entry)
    "funding_paid": 0.003,                            // cumulative_funding / (size * entry)
    "stop_trigger": "…", "opened_at": "…"             // opened_at = submitted_at of the fill that opened it
  }],
  "guards": {
    "margin": "WARN",                 // level of the latest alerts row with kind=margin_ratio, else "ok"
    "liquidation": "ok" | "breach",   // breach if any liq_distance < LIMITS.min_liq_distance
    "exposure": "ok" | "breach",      // from vet_exposure on current positions
    "halted": [7],                    // instruments with positions_local.state in {HALTED, LIQUIDATED}
    "reconciliation": {"findings": 0, "at": "..."}   // latest recovery row for run_id; null if none
  },
  "feed": {
    "instruments": [{"instrument_id": 6, "last_tick_age_s": 2.1, "last_funding_ts": "..."}],
    "tick_gaps_48h": 0, "funding_gaps_48h": 0, "rejections_48h": 0
  },
  "decisions": [{"ts": "...", "instrument_id": 6, "note": "hold", "state_before": "OPEN",
                 "target": "…", "verdicts": {...}, "client_order_id": null}],   // newest first, last 50
  "alerts":    [{"ts": "...", "level": "WARN", "kind": "margin_ratio", "instrument_id": 6, "detail": {...}}],  // newest first, last 50
  "locks": {
    "auto_mode": {"6": false, "7": false},   // ExecutionMode per instrument as gates.py reads it
    "live_env": false,                       // POLYMARKET_LIVE_TRADING == "true"
    "signal_validated": false                // polyperps.signal.base.SIGNAL_VALIDATED
  },
  "road": {
    "native_days": 34, "native_days_required": 60,
    "funding_periods": 816, "funding_periods_required": 1000,   // check_dataset on the first instrument, native source; the bar's numbers, not restated
    "paper_days": 2.1, "paper_days_target": 14,
    "clean": true      // no CRITICAL alert and no HALTED/LIQUIDATED position since started_at
  }
}
```

Rules that matter:

- Unrealized and equity use the sim formula: `equity = cash + Σ size × (mark − entry)`, marks from `sim_account.json["marks"]`.
- `liq_distance`, `adverse_move`, `funding_paid` are computed per position from `sim_account.json` + `positions_local`; they are not stored anywhere, so the tests pin them against hand-computed values.
- "Latest alert level = current level" holds because margin/pnl alerts fire on transitions only (`alerts.py`). The page labels the guard with the level text, never colour alone.
- The road-to-live thresholds (60 days, 1,000 periods) are read from `signal/sufficiency.BAR`, not retyped.
- `build_state` takes `now` and the instrument list as arguments so it is deterministic under test.

## 5. Server and page

- `DashboardServer(bind, db_path, run_id, instrument_ids, instruments_provider)`; `serve_forever()` on a `ThreadingHTTPServer`, `shutdown()` on SIGTERM.
- `GET /` → `index.html`, `text/html`. `GET /api/state` → JSON, `Cache-Control: no-store`. Anything else → 404 JSON. Only `GET`/`HEAD`.
- Each `/api/state` request: `sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)`, `build_state(...)`, close. Errors: DB missing/unreadable → 503 `{"error": "db_unavailable"}`; any other exception → 500 `{"error": "internal"}`, logged with traceback; the server keeps serving.
- Page: the approved artboard's layout and interactions (All / 6 / 7 switch, Decisions / Alerts tabs, click a position for its three guard meters with the floor marked). Polls every 10 s. On a non-200 or network error it keeps the last state and shows a "stale since HH:MMZ" banner; clears it on the next 200. Before the first successful load it shows "—" everywhere. Fonts: JetBrains Mono from Google Fonts with a monospace fallback — the page loads in the user's browser, which has internet; nothing on the box needs egress.
- `scripts/run_dashboard.py`: reads `POLYPERPS_DB_PATH`, `POLYPERPS_INSTRUMENT_IDS`, `PAPER_RUN_ID`, `PAPER_HYPOTHESIS`, `POLYPERPS_DASHBOARD_BIND` (default `127.0.0.1:8080` so a local run never binds 0.0.0.0 by accident); fetches instruments via the same client `run_paper.py` uses (best-effort, hourly refresh in a daemon thread); installs the SIGTERM → `KeyboardInterrupt` handler the sibling scripts use.

## 6. Deploy

- `deploy/polyperps-dashboard.service`: `User=polyperps`, `EnvironmentFile=/etc/polyperps/env`, `ExecStart=/opt/polyperps/.venv/bin/python scripts/run_dashboard.py`, `Restart=on-failure`, the sibling units' hardening verbatim (`NoNewPrivileges=yes`, `PrivateTmp`, `ProtectSystem=strict`, `ProtectHome`, `ReadWritePaths=/var/lib/polyperps`; ambient capabilities work under `NoNewPrivileges=yes`), plus `AmbientCapabilities=CAP_NET_BIND_SERVICE` and `CapabilityBoundingSet=CAP_NET_BIND_SERVICE`. No `--executor` flag anywhere.
- `env.example`: `POLYPERPS_DASHBOARD_BIND=0.0.0.0:80`.
- `bootstrap.sh` installs and enables the third unit; `update.sh` restarts it; `deploy.ps1` tails it with the other two. README gets a "Dashboard" subsection under Deploy: what it shows, the bind variable, and that the security group is the operator's responsibility.

## 7. Testing

- `tests/test_dashboard_state.py`: schema created via `db.connect(":memory:")`; rows inserted through the same helpers the router uses. Cases: (a) empty run → `account` null, `positions` empty, `road.clean` true, `started_at` null; (b) one LONG and one SHORT with known cash/marks/entries → equity, unrealized, per-position pnl, liq_distance, adverse_move, funding_paid, gross exposure asserted against hand-computed values; (c) HALTED instrument → in `guards.halted`, `road.clean` false; (d) margin alerts WARN then ok → `guards.margin` follows the latest row; (e) a CRITICAL alert → `clean` false; (f) ticks with a >30 s hole → `tick_gaps_48h` 1; (g) unknown instruments → `cluster_net` null, names `inst 6`.
- `tests/test_dashboard_server.py`: server on port 0 in a thread against a temp DB: `/` is HTML containing `/api/state`; `/api/state` is JSON with the top-level keys; `/nope` 404; DB path deleted → 503; `POST /` 405/404; the connection is read-only (a write attempt through the same URI raises).
- `tests/test_deploy_files.py`: the new unit is covered by the existing "never enables live" checks and additionally must carry `CAP_NET_BIND_SERVICE` and reference an existing script path.
- No JS test harness: `index.html` is checked by the server test for the `/api/state` reference and by eye on the box after the first deploy.

## 8. Acceptance

On the box, after `deploy.ps1`: `systemctl status polyperps-dashboard` active; `curl -s localhost/api/state | python -m json.tool` shows the live run id and two positions; the page in a browser (SG permitting) updates within 10 s of a router tick; `systemctl stop polyperps-dashboard` leaves the feed and paper units untouched and the DB WAL clean.
