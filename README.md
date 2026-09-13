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
costs, bootstrap 95 % CI excluding zero). Native data cannot meet it before
~2026-10-11 (60 days after the first stored native funding row, 2026-08-12;
re-check with `scripts/sufficiency.py`).

| Step | Command |
|------|---------|
| store fees (once, and after any fee change) | `scripts/store_fees.py` |
| proxy backfill | `scripts/backfill_hyperliquid.py --days 400 --map 6=BTC,7=ETH` |
| sufficiency re-check (monthly) | `scripts/sufficiency.py` |
| screen a hypothesis | `scripts/run_backtest.py --hypothesis h1 --instrument 6 --source hyperliquid` |
| confirm on native (after the bar is met) | `scripts/run_backtest.py --hypothesis h1 --instrument 6 --source native` |

Every run appends to `polyperps/signal/validation_log.jsonl` (committed).
`SIGNAL_VALIDATED` flips to `True` only when `polyperps/signal/validated.json`
names a `passed=True` native record **and** carries `approved_by`/`approved_at`
— a deliberate, reviewed commit by a human. No code path writes that file.
Since the 2026-09-12 amendment (spec §8.3) `passed` also requires zero hourly-open
fallback fills on the holdout and a backtested span of at least 60 days, and the
gate re-checks those from the record rather than trusting the flag.

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
