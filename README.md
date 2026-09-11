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

## Dependency pin

`polymarket-client==0.10.0`. All perps APIs are marked experimental by the
SDK; bumping is a deliberate task that re-verifies `exchange/client.py`.
