# polyperps Phase 2b, Part A: live-path safety and parity

Date: 2026-09-30. Parent: `polyperps-implementation-plan.md` Phase 2 (rows 2.1–2.6) and
`docs/superpowers/specs/2026-09-12-polyperps-phase2a-design.md` §12 (out of scope for 2a).
Source of the findings: the 2026-09-30 architecture review (design-seams report, findings 1–9).

## 1. Goal and scope

Make the live order path safe and make backtest, paper and live follow the same rules, so the
code that goes live is the code the paper soak has run. Everything here can be built and
tested now with fakes; none of it needs a passing native record.

**Part A (this spec):** one runner for sim/shadow/live; stop invariant driven by exchange state;
pending timeouts; retry and error fixes; hard loss limit; backtest parity; shadow mode; a
manual live smoke script.

**Part B (later, separate spec):** Kelly sizing (2.1) and the kill-switch divergence numbers
(2.6), both derived from a passing native record (earliest ~2026-10-11).

Decisions taken in brainstorming (2026-09-30):

| Question | Decision |
|---|---|
| Scope | Part A only |
| Parity direction | The backtest learns the router's rules; router trading rules do not change |
| Real-exchange contact | Fakes + read-only shadow mode + a user-run smoke script |
| Loss limit | Yes: pause entries at −5 %, flatten and halt at −10 % of start equity |
| Runner shape | One runner, `--executor sim|shadow|live` |

## 2. Global constraints

- Router trading rules (entry, exit, flip-as-exit-then-reenter, guards, limits) are unchanged.
- `live` stays behind all three locks (`gates.live_orders_allowed`); nothing in this spec opens one.
- No deploy file ever contains `--executor live` or `POLYMARKET_LIVE_TRADING` (existing test).
- `PAPER_RUN_ID` stays `paper-soak-1`.
- Limits are fixed in code and never self-adjust (roadmap hard boundary).
- No new dependencies. SDK stays `polymarket-client==0.10.0`.
- All new tests use fakes; no network, no real orders.

## 3. Runner and executor interface

### 3.1 `scripts/run_trader.py` (replaces `scripts/run_paper.py`)

`--executor sim|shadow|live`, otherwise the same arguments as `run_paper.py`. The wiring
(recover, seed, feed, bar/fast/reconcile/event-pump loops under `run_until_first_exits`) is
shared by all three modes. It builds the executor by mode:

- `sim`: `SimExecutor`, as today.
- `shadow`: `ShadowExecutor` (§3.3). Needs the authenticated perps session; no lock is opened.
- `live`: `LiveExecutor`; construction still raises `GateClosed` unless all three locks are open,
  and the script exits 2 in that case.

`deploy/polyperps-paper.service` switches its `ExecStart` to `run_trader.py --executor sim`.
`run_paper.py` is deleted (tests and README updated).

### 3.2 Executor hooks

The runner currently calls three `SimExecutor`-only methods. They move onto the `Executor`
protocol (`polyperps/execution/executor.py`):

| Hook | Called | Sim | Live / shadow |
|---|---|---|---|
| `on_tick(tick)` | every accepted tick | `update_mark` | no-op |
| `on_bar(bar)` | every closed bar | `apply_funding` | no-op |
| `poll_fills() -> list[FillUpdate]` | fast loop | `check_triggers` | `[]` |

### 3.3 `ShadowExecutor`

`LiveExecutor`'s constructor raises `GateClosed` while any lock is closed, so shadow cannot wrap
it. Instead `LiveExecutor`'s read side (`snapshot`, `events`, payload mapping, `close`) moves into
a base class `LiveReader` that has no gate; `LiveExecutor(LiveReader)` adds the write methods and
keeps its constructor gate unchanged. `ShadowExecutor(LiveReader)` implements `submit`, `cancel`,
`place_stop`, `cancel_stop` by raising `ShadowRefused`, and `heartbeat` as a no-op (nothing to
keep alive: it never has orders). The router treats
`ShadowRefused` like any submit error (§4.5), so in shadow mode strategy decisions are logged,
every would-be order is recorded with status `shadow_refused`, and instruments halt rather than
retry. Reconciliation and recovery run against the real account. Running it requires the wallet
key on the box (residual risk in `polyperps/security/key_management.py`), which is the user's call.

### 3.4 Dashboard account source

The dashboard reads the sim's private JSON (`dashboard/state.py:73`), which is blank in
shadow/live. The runner writes each fast-loop `AccountSnapshot` (equity, margin, positions) to a
new `account_snapshots` table (latest row per run_id kept); the dashboard reads that instead.

## 4. Safety fixes on the order path

**Invariant:** if the exchange reports a non-zero position for an instrument, an exchange-side
stop exists for it, whatever the router's local state.

1. **Reconciliation** (`reconciliation.diff`): raise `missing_stop` whenever remote size ≠ 0 and no
   remote stop, for every local state (OPEN, HALTED, LIQUIDATED, ENTRY_PENDING, EXIT_PENDING, FLAT,
   or no local row). Response: place the stop (`replace_stop`) at the router's stop distance from
   the exchange-reported entry price.
2. **Recovery** (`state_recovery.recover`): read exchange size and stops for every instrument,
   HALTED rows included (today they are skipped); apply the same invariant.
3. **Pending timeout:** `PENDING_TIMEOUT_S = 30`. In the fast loop, a router in ENTRY_PENDING or
   EXIT_PENDING longer than that snapshots the account and adopts the exchange size: non-zero →
   OPEN with a stop placed; zero → FLAT. The order row becomes `adopted`; WARN `pending_timeout`.
4. **Late fill while HALTED:** apply the fill to size and place a stop; state stays HALTED.
5. **Submit errors:** any exception from `submit` other than `ExecutorTimeout` → order row
   `error`, CRITICAL `submit_error`, adopt exchange size (as in 3), then halt the instrument.
6. **Retry correctness** (`order_router._submit_with_recovery` / `_landed`):
   landed = position moved in the order's direction by any amount (partial fills count);
   `duplicate_order` from the exchange maps to landed, not rejected (`live_executor.py:224`);
   an order row's filled quantity accumulates across `FillUpdate`s instead of being overwritten.
7. **Fills without a client order id** (stop fills, liquidations): `LiveExecutor.events` maps them
   to their instrument's router instead of dropping them.
8. **Gate on every order:** `LiveExecutor.submit` re-checks `live_orders_allowed` for the
   instrument; closed → raise `GateClosed` → handled as a submit error (5).
9. **Exposure counts in-flight orders:** `vet_entry` / `vet_exposure` add the notional of the
   instrument's pending order (at most one per instrument) to the positions they sum.
10. **Event stream end:** if `LiveExecutor.events` ends or raises, the event pump returns and
    `run_until_first_exits` ends the process; systemd restarts it and recovery runs.
11. **Funding drift:** `funding_drift_alert` is wired in live/shadow: on each closed bar, compare
    funding the exchange charged per position (delta of the snapshot's cumulative funding) with
    `bar.funding_rate × notional`. The SDK sign convention is confirmed by the smoke script (§7).

## 5. Hard loss limit

In `polyperps/risk/kill_switch.py`, `evaluate` gains `equity` and `start_equity` inputs:

- `equity / start_equity − 1 ≤ ALERT_THRESHOLDS.pnl_warn` (−5 %) → `pause` (no new entries).
- `≤ ALERT_THRESHOLDS.pnl_critical` (−10 %) → `shutdown`: the Portfolio flattens every open
  position and halts every router; CRITICAL `loss_limit`. Clearing it is a human decision
  (`--clear-halt`, existing mechanism).
- The result is combined with the divergence check (thresholds still `None` in Part A); the
  stricter action wins (`shutdown` > `pause` > `run`). Paper evaluates the loss limit too.

The runner passes the current snapshot equity and the executor's start equity on every bar.

## 6. Backtest parity

The backtest (`polyperps/backtest/harness.py`) follows the router's rules:

1. **Flip = exit, then re-enter next bar** if the strategy still targets the other side.
2. **Protective exits via the same code:** `liquidation_guard` open-position check and
   `funding_exit_due` evaluated at each bar close; the 15 % stop fires intrabar when the bar's
   high/low crosses it, at the stop price. Marked `ponytail:` — bar-granular checks versus the
   router's 20 s loop; upgrade path is 1m bars once the 1m backfill exists.
3. **One cost model:** `SimExecutor` fills price through `costs.fill_cost`; its own cost code goes.
4. **Gaps in live bars:** `LiveBarBuilder` marks the first bar after a missing hour
   `complete=False`; the router, on an incomplete bar, exits to flat (reason `data_gap`) and does
   not enter — the backtest's existing rule.
5. **Parity test:** one fixed bar sequence (flip, stop hit, funding exit, gap) through
   `run_backtest` and through `Portfolio` + `SimExecutor`: identical trades (time, side,
   quantity) and P&L equal to the cent.
6. **`harness_version`:** a constant in `harness.py`, written into every validation record;
   `gates`/`validated.json` checks accept only records at the current version. The Hyperliquid
   screens for h1/h3 are re-run under the new harness (one plan step).

## 7. Live smoke script (`scripts/live_smoke.py`)

User-run only, in Phase 3, never by a unit or test. It refuses to start unless the user types
the instrument id and `I ACCEPT REAL ORDERS`. Steps: open the perps session; place a minimum-size
market order; place the exchange-side stop 1 % away; print "kill the bot now, press Enter after
the stop fills"; poll until the position is zero; verify the stop fill came from the exchange;
report the funding charged and the SDK's cumulative-funding sign. It calls the SDK perps session
directly (not `LiveExecutor`, whose gate stays closed until a signal validates, and not the
router): what it proves is exchange behaviour, not our code. It reads fills and positions through
`LiveReader`. It is tested with a fake session only.

## 8. Testing (fakes only)

- Stop invariant: every local state × "remote position, no stop", in reconcile and recover.
- Pending timeout: lost fill → adopt + stop; zero size → FLAT.
- Retry: partial fill + timeout; `duplicate_order`; filled quantity accumulates.
- Submit error and `GateClosed` mid-run → halted + CRITICAL.
- Fill without client id reaches the right router.
- In-flight order counts toward exposure.
- Loss limit: −5 % pause, −10 % flatten + halt, strictest-wins with divergence.
- Shadow: every write refused and recorded; reconcile still runs.
- Parity golden test; gap marking; old `harness_version` rejected.
- `run_trader.py` in all three modes with fakes; `live` exits 2 while any lock is closed.
- Dashboard reads `account_snapshots`.
- Smoke script refuses without the typed confirmation.

## 9. Exit criteria

1. All tests pass.
2. `run_trader.py --executor sim` deployed; the paper soak restarts on this code and runs 1–2
   clean weeks (this is the 2a→2b gate's soak).
3. When the user chooses to run it: 24 h of shadow mode against the real account with zero
   reconciliation mismatches.

## 10. Out of scope

Kelly sizing and divergence numbers (Part B); running shadow or the smoke script (user);
any real order; raw tick storage → hourly rollup (separate ops task); NautilusTrader (2.0 stays
a recorded lookup).
