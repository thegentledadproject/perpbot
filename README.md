# polyperps

Polymarket perps trading bot. **Phase 0 only**: read-only market data,
storage, and safety plumbing. No signal, no capital, no order path exists
in this codebase.

Plan: `polyperps-implementation-plan.md` (roadmap) and
`docs/superpowers/plans/2026-09-11-polyperps-phase0.md` (this phase).

## Setup

    python -m venv .venv
    .venv/Scripts/python -m pip install -e ".[dev]"
    .venv/Scripts/python -m pytest

## Phase 0 runbook

| Spec | Command | Needs a key? |
|------|---------|--------------|
| 0.1 auth check | `scripts/check_auth.py` | yes - see `polyperps/security/key_management.py` docstring |
| 0.2 client | covered by `tests/test_client.py` + `run_feed.py --list-instruments` | no |
| 0.3 48h soak | `POLYPERPS_INSTRUMENT_IDS=a,b scripts/run_feed.py` | no |
| 0.4 backfill + gaps | `scripts/backfill.py --days 7` | no |
| 0.5 key mgmt | `tests/test_key_management.py`; residual risk documented in module docstring | - |
| 0.6 eligibility | `docs/ops/eligibility-checklist.md` (human, monthly) | - |

## Gates

Three conjuncts, all required for any live order (`polyperps/gates.py`):
per-instrument `ExecutionMode.AUTO`, env `POLYMARKET_LIVE_TRADING=true`,
and `polyperps.signal.base.SIGNAL_VALIDATED`. The last is `False` and is
flipped only by a manual, reviewed change after Phase 1 clears on native data.

## Soak runbook

Evidence for spec 0.3 (48h soak) and its exit criterion:

    POLYPERPS_INSTRUMENT_IDS=a,b .venv/Scripts/python scripts/run_feed.py
    # ... let it run for 48h, then in a second shell:
    POLYPERPS_INSTRUMENT_IDS=a,b .venv/Scripts/python scripts/gap_report.py --hours 48

`gap_report.py` is offline (no exchange client, no network) and safe to
run against the DB file while `run_feed.py` is still writing to it (WAL
mode). It reports per-instrument tick gaps, funding gaps, and rejection
counts over the trailing window.

Clean-exit check the reviewer asked for: with the service running under
systemd, `systemctl stop <unit>` sends SIGTERM; `run_feed.py` installs a
SIGTERM handler that raises `KeyboardInterrupt`, which unwinds through
`run_once()`'s `finally` (stop.set(), drain the book/health tasks, close
the WS generator, close the client, close the DB connection) instead of
being killed mid-write. Confirm the unit's journal shows the run ending
without a traceback and the DB file has no stale WAL after the stop.

Spec 0.5 note: "a compromised trading key cannot move funds" is **not**
verifiable with SDK 0.10.0 - see the RESIDUAL RISK section of
`polyperps/security/key_management.py`'s module docstring for why (the
public API only opens a session via a wallet private key, so that key is
still on-host even though the resulting delegated session cannot
withdraw). This must be explicitly acknowledged, not silently assumed,
before Phase 3.

## Dependency pin

`polymarket-client==0.10.0`. All perps APIs are marked experimental by the
SDK; bumping is a deliberate task that re-verifies `exchange/client.py`.

## Phase 1 — signal research

Spec: `docs/superpowers/specs/2026-09-11-polyperps-phase1-design.md`. The
sufficiency bar is pre-registered in `polyperps/signal/sufficiency.py` (Strict:
native ≥60 days, ≥1,000 funding periods, 30 % holdout, OOS Sharpe ≥1.0 after
costs, one-sided 80 % bootstrap lower bound on mean net return > 0, spec §8.4). Native data cannot meet it before
~2026-10-11 (60 days after the first stored native funding row, 2026-08-12;
re-check with `scripts/sufficiency.py`).

| Step | Command |
|------|---------|
| store fees (once, and after any fee change) | `scripts/store_fees.py` |
| proxy backfill | `scripts/backfill_hyperliquid.py --days 400 --map 6=BTC,7=ETH` |
| sufficiency re-check (monthly) | `scripts/sufficiency.py` |
| screen a hypothesis | `scripts/run_backtest.py --hypothesis h1 --instrument 6 --source hyperliquid` |
| confirm on native (after the bar is met) | `scripts/run_backtest.py --hypothesis h1 --instrument 6 --source native` |
| native re-check (monthly, after a pass) | `scripts/run_backtest.py --hypothesis <h> --instrument <id> --source native`; a later failing record revokes validated.json (spec 8.4) |

Every run appends to `polyperps/signal/validation_log.jsonl` (committed).
`SIGNAL_VALIDATED` flips to `True` only when `polyperps/signal/validated.json`
names a `passed=True` native record **and** carries `approved_by`/`approved_at`
— a deliberate, reviewed commit by a human. No code path writes that file.
Since the 2026-09-12 amendment (spec §8.3) `passed` also requires zero hourly-open
fallback fills on the holdout and a backtested span of at least 60 days, and the
gate re-checks those from the record rather than trusting the flag.
Spec §8.4 adds two more conditions: the robustness run (every last-trade fill priced at the
hourly open) must also screen, and a later failing native record for the same hypothesis and
instrument revokes the approval.

### Re-run the screens under harness_version 3 (operator step)

The gate accepts only records from the current harness, so the h1/h3 screens must be re-run.
Locally:

    POLYPERPS_INSTRUMENT_IDS=6 .venv/Scripts/python scripts/run_backtest.py --hypothesis h1 --instrument 6 --source hyperliquid --fee-category equity
    POLYPERPS_INSTRUMENT_IDS=6 .venv/Scripts/python scripts/run_backtest.py --hypothesis h3 --instrument 6 --source hyperliquid --fee-category equity

On the box (its sudo rejects `-E`, so pass the environment through systemd-run):

    sudo systemd-run --wait --pipe -p User=polyperps -p EnvironmentFile=/etc/polyperps/env -p WorkingDirectory=/opt/polyperps /opt/polyperps/.venv/bin/python scripts/run_backtest.py --hypothesis h1 --instrument 6 --source hyperliquid --fee-category equity --log-path /var/lib/polyperps/screens-v3.jsonl

and the same with `--hypothesis h3`; then copy the new lines of
`/var/lib/polyperps/screens-v3.jsonl` into `polyperps/signal/validation_log.jsonl` and commit.
The box holds no Hyperliquid proxy bars, so proxy screens run on the PC.
h3 reads stored ticks (`index_close`) and tick retention is 3 days, so an h3 screen longer than
that trades nothing until a tick rollup exists.

### H4 / H5 (pre-registered 2026-10-03)

Spec: `docs/superpowers/specs/2026-10-03-h4-h5-price-lag-design.md`. H4 (lead-lag) is native-only
and needs the Hyperliquid proxy bars in the same DB (refresh them first with
`scripts/backfill_hyperliquid.py`). H5 (overshoot) runs on either source; its Hyperliquid
screen is informational only. Run both instruments with `--hypothesis h4` / `h5` alongside
h1-h3 at the native run (earliest ~2026-10-12 14:00 UTC), e.g. on the box:

    sudo systemd-run --wait --pipe -p User=polyperps -p EnvironmentFile=/etc/polyperps/env -p WorkingDirectory=/opt/polyperps /opt/polyperps/.venv/bin/python scripts/run_backtest.py --hypothesis h4 --instrument 6 --source native --fee-category equity

Approval rule (operator, spec §3): add an H4 or H5 record to `validated.json` for one instrument
only if the same hypothesis also has `passed = true` on the other instrument at the current
harness_version. A failed H4/H5 is not re-tuned; a changed rule is a new hypothesis with its own spec.

## Phase 2a — paper execution (no live orders)

Specs: `docs/superpowers/specs/2026-09-12-polyperps-phase2a-design.md`,
`docs/superpowers/specs/2026-09-30-polyperps-phase2b-parta-design.md`. One runner,
`scripts/run_trader.py`, drives the router, guards, reconciliation, recovery and alerts in
every mode; only the last mile changes. Pre-registered limits: 3x leverage, 25 %
liquidation-distance floor, 15 % exchange-side stop, 2 % funding-cost exit, gross exposure
1.0x / cluster net 0.6x equity, loss limit -5 % pause / -10 % flatten and halt,
kill-switch divergence thresholds `None`.

| Step | Command |
|------|---------|
| paper run (sim) | `POLYPERPS_INSTRUMENT_IDS=6,7 .venv/Scripts/python scripts/run_trader.py --executor sim --hypothesis h1 --run-id soak-2026-09-13` |
| shadow run (real account, read-only; needs the wallet key) | same with `--executor shadow --run-id shadow-1` |
| clear a halted (or liquidated) instrument (human decision) | add `--clear-halt 6` to the run command |
| Telegram CRITICAL alerts (optional) | put TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID files in /etc/credstore/ (root:root 0600) |

`--run-id` is required for a soak: it names the account rows recovery reads, so a restart
(manual or systemd) reopens the same book. Without it a fresh id is minted per process
start (sim only: shadow and live exit 2 without `--run-id`, and refuse a run_id whose rows
another executor wrote, e.g. a sim soak's). Strategy warm-up is seeded from stored 1h candles at start (`seeded N bars for
instrument I` in the log); until the history is long enough the router writes
`skip:warmup` decisions and sends nothing.

`--executor live` exits 2 while any of the three locks is closed. `LiveExecutor` cannot be
constructed unless all three are open, and re-checks them on every order.

## Deploy (EC2, systemd, PuTTY)

Files: `deploy/bootstrap.sh` (first-time box setup), `deploy/update.sh` (redeploy
a running box), `deploy/deploy.ps1` (driver, run from this machine over PuTTY),
`deploy/*.service` (systemd units, `--executor sim` only - never `live`),
`deploy/env.example` (non-secret settings). `tests/test_deploy_files.py` checks
all of the above never enable live trading or leak a script path that doesn't
exist.

**Prerequisites**: a fresh EC2 instance, Ubuntu 24.04, in a **non-US region**
(jurisdiction, per the eligibility checklist); a security group open for SSH
only; a PuTTY saved session (`-load <name>`) already configured with that
host's key.

**First time**:

    deploy\deploy.ps1 -Session <name> -Bootstrap -RepoUrl <git-url>

Then on the box: edit `/etc/polyperps/env` (instrument ids, db path,
hypothesis, run id); optionally drop `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`
files into `/etc/credstore/` (root:root 0600; the units import them automatically);
run the one-time data setup as the service user
(the paper run refuses to start without a fee row, and seeds its strategy
history from stored 1h candles):

    cd /opt/polyperps && sudo -u polyperps env $(grep -v '^#' /etc/polyperps/env | xargs) .venv/bin/python scripts/store_fees.py
    cd /opt/polyperps && sudo -u polyperps env $(grep -v '^#' /etc/polyperps/env | xargs) .venv/bin/python scripts/backfill.py --days 31 --interval 1h

then `systemctl start polyperps-feed polyperps-paper`.

**Every later deploy**:

    deploy\deploy.ps1 -Session <name>

Refuses to run against a dirty working tree or an unpushed commit.

**Watching**: `journalctl -fu polyperps-paper` (or `-feed`); `deploy.ps1` also
tails the last 40 lines of both units after every deploy.

**Health check**: `polyperps-health.timer` runs `scripts/healthcheck.py` every 5 minutes
(and right after any failed prune or backfill). It alerts CRITICAL, once per problem, on: disk ≥95 %,
newest tick older than 5 minutes, feed/paper not active or restarted by systemd, or a
failed prune or backfill; and INFO when the problem clears ("recovered" notices go to the journal and
alerts table only; Telegram is CRITICAL-only). Alerts go to the journal
(`journalctl -u polyperps-health`), the `alerts` table (run_id `ops-health`) and Telegram
when configured. Without Telegram, nobody is paged.

**Auto-updates**: unattended-upgrades keeps patching the OS, but
`/etc/needrestart/conf.d/polyperps.conf` (from `deploy/needrestart-polyperps.conf`) stops it from
restarting `polyperps-*` units. They pick up patched libraries on the next deploy, so deploy (or
restart them deliberately) after a security update you care about.

**Backfill**: `polyperps-backfill.timer` runs `scripts/backfill.py --days 1 --interval 1h` at 5 past
every hour, storing closed 1h candles and funding so a restarted paper run seeds its strategy
history instead of re-warming from zero. Only closed candles are stored (inserts never overwrite).

**Stopping**: `systemctl stop polyperps-feed polyperps-paper` sends SIGTERM;
both scripts unwind through their `finally` block cleanly (see the soak
runbook above).

**Dashboard**: `polyperps-dashboard.service` serves a read-only page on
`POLYPERPS_DASHBOARD_BIND` (`0.0.0.0:80` on the box) showing the paper account,
positions with their guard distances, router decisions, alerts, feed health, the
three live locks and the road-to-live counters, refreshed every 10 s from the DB
(opened `mode=ro`; it cannot write). No auth, no TLS: reach is controlled only by
the EC2 security group, so keep port 80 restricted to your own IP. Locally:
`POLYPERPS_INSTRUMENT_IDS=6,7 PAPER_RUN_ID=paper-soak-1 .venv/Scripts/python scripts/run_dashboard.py`
then open `http://127.0.0.1:8080`.

After the first successful feed run from the EC2 box, add the egress-IP row
to `docs/ops/eligibility-checklist.md` - the geo-block check is host-specific
and the Malaysia-ISP entry there does not cover EC2.
