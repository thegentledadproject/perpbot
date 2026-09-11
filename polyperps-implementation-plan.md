# Polymarket Perps Trading Bot — Implementation Plan

Framework name: `polyperps`
Convention alignment: follows the same safety-gate conventions as `polyweather` (dual execution gate, no-fabrication logging, EC2/systemd deployment).

---

## Module tree

```
polyperps/
├── exchange/
│   ├── client.py            # wraps Polymarket/py-sdk perps module; rate limiter; clean interface boundary for future swap (e.g. NautilusTrader)
│   └── types.py              # position, order, funding-rate data classes
├── security/
│   └── key_management.py     # POLYMARKET_PK / API creds — not plaintext on host; trade-only scoped key, no withdrawal permission
├── data_ingest/
│   └── market_feed.py        # WS stream: mark price, funding rate, order book depth; source_type tagging; staleness/bad-tick filtering
├── signal/
│   └── base.py                # interface: generate_signal(market_state) -> Position; NotImplementedError until Phase 1 clears; sets SIGNAL_VALIDATED flag
├── backtest/
│   └── harness.py             # slippage + signal-to-fill latency modeling; point-in-time data discipline (no look-ahead); block-bootstrap for funding-rate autocorrelation
├── risk/
│   ├── sizing.py               # Kelly / fixed-fractional, off Phase 1 measured edge
│   ├── liquidation_guard.py    # per-position pre-trade + ongoing margin-distance floor, leverage cap below exchange max
│   └── portfolio_exposure.py   # aggregate exposure cap across concurrent/correlated positions
├── execution/
│   ├── order_router.py         # entry/exit state machine; dual gate + SIGNAL_VALIDATED check; exchange-side stop order at entry; idempotent client-order-IDs
│   ├── reconciliation.py       # fixed-interval internal-state vs exchange-state check while running
│   └── state_recovery.py       # cold-start reconciliation after crash/restart, exchange as source of truth
├── monitor/
│   └── alerts.py                # funding drift, margin ratio, P&L, reconciliation mismatches; structured per-trade decision logging
└── feedback/                    # Phase 3+ only
    ├── recalibration.py         # scheduled (not per-trade) Kelly/signal-calibration refit; clamps within fixed ceilings, never widens them
    ├── drift_monitor.py         # halts new entries beyond pause threshold; pre-registered shutdown threshold triggers full stop
    └── review_log.py            # every auto-adjustment logged: before/after, sample size, trigger reason; trade record-keeping for accounting
```

**Hard boundary (all phases):** signal logic, leverage ceilings, and asset kill-decisions never self-adjust — always manual, regardless of live data volume.

---

## Phase 0 — Plumbing + security foundation (no signal, no capital)

| Step | Task | Exit criteria |
|---|---|---|
| 0.1 | Install `Polymarket/py-sdk`, set up perps auth | Successful authenticated call to `/v1/account/balances` |
| 0.2 | Build `exchange/client.py` — thin wrapper, clean interface boundary, rate limiter wrapping all calls | All methods callable read-only against live API; no rate-limit errors under sustained polling |
| 0.3 | Build `data_ingest/market_feed.py` — WS stream, 2–3 target assets, sanity bounds/staleness filtering before data reaches downstream consumers | 48hr continuous stream, no drops, `source_type` tagged, bad-tick rejection tested |
| 0.4 | Storage for funding-rate + price time series, reusing polyweather's DB pattern | Queryable historical table, gap-checked |
| 0.5 | `security/` — key management: credentials not plaintext on host, trading key scoped without withdrawal permission if API supports scoping | Verified: a compromised trading key cannot move funds out, only trade |
| 0.6 | Jurisdiction/eligibility check — confirm access isn't geo-restricted; note product is ~1 week old, re-check periodically | Documented pass; added to a recurring checklist, not assumed permanent |

**Gate:** 0.1–0.6 done, feed clean for 48hrs unattended.

---

## Phase 1 — Signal research (the actual bottleneck, no fixed timeline)

| Step | Task | Exit criteria |
|---|---|---|
| 1.0 | Set the sufficiency bar (minimum sample size, minimum out-of-sample holdout) **before** any backtest runs | Written threshold exists before 1.2 produces any result — never set retroactively |
| 1.1 | Pull historical funding-rate + price data; backfill from Hyperliquid/CEX as proxy given thin native history (perps launched Sept 3, 2026) | Dataset meets or explicitly falls short of the 1.0 bar — shortfall logged, not waived |
| 1.2 | Test hypotheses: funding-rate mean reversion, cross-venue basis vs Hyperliquid/CEX, spot-perp correlation lag. Backtest must model slippage and signal-to-fill latency — not assume perfect fills at mark price. Explicit point-in-time data discipline, no look-ahead bias | Pass/fail per hypothesis with realistic execution assumptions and out-of-sample holdout; negative results documented, not discarded |
| 1.3 | If nothing clears, or the 1.0 bar isn't met, stop | Null/insufficient-data result logged in `signal/`, nothing shipped |

**Gate — code-enforced, not discipline-only:** `signal/base.py` sets a `SIGNAL_VALIDATED` flag only after logging a passing 1.2 result against the 1.0 bar. The dual execution gate additionally checks this flag — live orders are structurally blocked until Phase 1 clears on record. Any edge found only in proxy data, not native Polymarket data, stays unconfirmed.

---

## Phase 2 — Risk, execution, reconciliation

| Step | Task | Exit criteria |
|---|---|---|
| 2.0 | Recurring (not one-time) re-check: has a NautilusTrader Perps adapter landed (following the Hyperliquid-adapter pattern)? | Migrate only pre-live — no execution-core migration once Phase 3 capital is live |
| 2.1 | `risk/sizing.py` — Kelly / fixed-fractional off Phase 1's measured edge | Backtested against Phase 1 dataset |
| 2.2 | `risk/liquidation_guard.py` — per-position pre-trade + ongoing margin-distance floor, leverage cap below exchange max (20x/10x ceilings), defaults conservative given thin (<2 week) sample window | Simulated against historical volatility |
| 2.2b | `risk/portfolio_exposure.py` — aggregate exposure cap across concurrent positions; correlated moves (e.g. BTC + crypto-linked equity) can breach risk no single position's guard would catch | Tested with a correlated multi-asset scenario |
| 2.3 | `execution/order_router.py` — entry/exit state machine (FLAT→ENTRY_PENDING→OPEN→EXIT_PENDING→FLAT / LIQUIDATED), dual gate (`EXECUTION_MODE` + `POLYMARKET_LIVE_TRADING=true`) + `SIGNAL_VALIDATED` check. Places an exchange-side stop order at entry as a backstop independent of bot-process uptime. Every order carries a unique client-order-ID for idempotent retries | Paper-trading clean for 1–2 weeks; funding-cost exit tested standalone (independent of price-based stop/target — perps-specific, easy to drop if porting polyweather logic naively); exchange-side stop confirmed to fire with bot process killed; retry-after-timeout tested for no double-fill |
| 2.4 | `execution/reconciliation.py` — fixed-interval internal-state vs exchange-state check while running | Tested against injected WS-drop/restart desync scenarios |
| 2.4b | `execution/state_recovery.py` — cold-start reconciliation rebuilding full state (positions, pending orders, stop levels) from the exchange as source of truth after any crash/restart | Tested against simulated crash-and-restart |
| 2.5 | `monitor/alerts.py` — funding drift, margin ratio, P&L, reconciliation mismatches. Structured per-trade decision logging (full context, not just threshold alerts), queryable after the fact | Alerts fire on injected conditions; a sample trade's full decision trail is reconstructable from logs alone |

---

## Phase 2.5 — Kill criterion (must be set before Phase 3 starts)

| Step | Task | Exit criteria |
|---|---|---|
| 2.6 | Pre-register two thresholds in writing, before any live capital: (a) **pause threshold** — live-vs-backtest divergence that halts new entries only; (b) **shutdown threshold** — divergence severe enough to close all positions and stop entirely | Both numbers exist and are logged before 3.1 begins — never decided in the moment a bad week happens |

---

## Phase 3 — Live, small size

| Step | Task | Exit criteria |
|---|---|---|
| 3.1 | Deploy EC2/systemd, no Docker, no CloudWatch | Running unattended; `state_recovery.py` validated on first real restart |
| 3.2 | Live capital at minimum size, single asset | 2–4 weeks live tracking vs backtest — watch for calibration-parity issues like the open one in polyweather |
| 3.3 | Scale only after live P&L matches backtest within tolerance. Periodic re-check that live fee/funding schedule still matches what `risk/sizing.py` assumed (product is new, terms could shift) | Divergence beyond 2.6's shutdown threshold triggers full stop, not just review |

---

## Phase 3+ — Self-adjustment (only after sufficient live trade history)

| Step | Task | Exit criteria |
|---|---|---|
| 3.4 | `feedback/recalibration.py` — scheduled (weekly, not per-trade) refit of Kelly sizing and isotonic/Platt signal calibration; volatility-based stop/target adjustment | Adjustments clamp within fixed leverage/risk ceilings, never widen them |
| 3.5 | `feedback/drift_monitor.py` — halts new entries if realized funding cost, win rate, or volatility diverges from backtest assumptions beyond the 2.6 pause threshold | Tested against injected drift scenarios |
| 3.6 | `feedback/review_log.py` — every auto-adjustment logged with before/after value, sample size, trigger reason. Trade record-keeping extended for accounting/tax purposes, captured from trade one | Full audit trail, no silent adjustments |

---

## Non-negotiable conventions carried from polyweather

- Dual execution gate: `EXECUTION_MODE` + `POLYMARKET_LIVE_TRADING=true`, no exceptions
- No-fabrication: stubs raise `NotImplementedError`, unverified endpoints flagged explicitly, `source_type` provenance enforced
- EC2 via systemd, no Docker, no CloudWatch
- Signal logic, leverage ceilings, and asset kill-decisions are always manual — never touched by any self-adjustment mechanism, at any phase

## Known reference material (not to be imported wholesale)

- `Polymarket/py-sdk` — official, perps support live since ~July 2026. Use directly for `exchange/client.py`.
- `nautilus-polymarket` (NautilusTrader) — CLOB/binary only today, no Perps adapter. Re-check per 2.0.
- `aulekator/Polymarket-BTC-15-Minute-Trading-Bot` — reference for phase-separation/adapter pattern only. Do not import strategy code, win-rate claims, or the "self-learning" framing (their equivalent is an unimplemented placeholder, and their live/sim toggle is self-flagged "not stable yet").
- `alsk1992/CloddsBot` and generic "Polymarket trading bot" repos (Benjamin-cup, Ronesfe, MrFadiAi, PMTraderAdam, brishowkanem4) — no perps coverage, largely unaudited or closed-source. Skip.
