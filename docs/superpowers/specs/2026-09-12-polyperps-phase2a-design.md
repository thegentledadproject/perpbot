# polyperps Phase 2a — Risk Guards, Execution Core, Reconciliation (paper-only): Design

Date: 2026-09-12
Status: approved in brainstorming; implementation plan follows.
Parent spec: `polyperps-implementation-plan.md` Phase 2 rows 2.0, 2.2, 2.2b, 2.3, 2.4, 2.4b, 2.5, and the 2.6 mechanism (numbers excluded).
Builds on: Phase 0 + Phase 1, merged to `master` (Phase 1 ledger: `docs/superpowers/plans/2026-09-11-polyperps-phase1-ledger.md`).

## 1. Purpose

Build everything in Phase 2 that does not consume a validated signal — the execution state machine, the risk guards, reconciliation, crash recovery, alerts, and the kill-switch mechanism — and prove it on a **paper** run that exercises the exact code path a live run would, minus the last mile.

Nothing in 2a places a live order. `LiveExecutor` is built and unit-tested against a fake session; it cannot be instantiated unless all three locks are open, and 2a's run script refuses `--executor live` outright.

## 2. Decisions taken in brainstorming

| Decision | Choice | Consequence |
|---|---|---|
| Paper mode | **Local simulation, same router code** | `Executor` protocol with `SimExecutor` (public feed + Phase 1 cost model, no credentials) and `LiveExecutor` (real `PerpsSession`). Router/state machine/reconciliation code is identical in both. |
| Risk limits (2.2) | **3× leverage cap, 25 % liquidation-distance floor, 15 % exchange-side stop** | Pre-registered in `risk/liquidation_guard.py::LIMITS`, pinned by a test. Exchange allows 20×. |
| Exposure cap (2.2b) | **Cluster caps** | `gross ≤ 1.0× equity` across all positions; `net directional ≤ 0.6× equity` within a cluster; clusters = instrument category (crypto/equity/index/commodity). No correlation estimate. |
| Alerts (2.5) | **Log + SQLite + Telegram (CRITICAL only)** | Telegram token/chat id via `key_management`; delivery failure never propagates. |
| Architecture | **A: cycle engine + executor interface** | Rows persisted before each side effect give an audit trail without event-sourcing machinery. B (event-sourced) and C (SDK-event-driven) rejected. |
| Cadence | Decisions on hourly bar close (as the harness); 20 s fast loop for heartbeat / margin / sim stops; 60 s reconciliation | Matches Phase 1 semantics; dead-man switch needs ≤ 60 s re-arm. |
| Sizing in 2a | Fixed notional per instrument (`LIMITS.notional_usd = 100`) | Kelly (2.1) is 2b; it fills the same `intent.quantity` slot. |
| Kill switch (2.6) | Mechanism now, **numbers `None`** | With `None`, live evaluates to `pause`, paper to `run`. Numbers are set in 2b from a passing record. |

## 3. Module layout (additions)

```
polyperps/
├── execution/
│   ├── __init__.py
│   ├── types.py               # OrderRequest, OrderAck, StopAck, OrderUpdate, FillUpdate, AccountSnapshot, PositionView, State enum
│   ├── executor.py            # Executor protocol
│   ├── sim_executor.py        # SimExecutor (in-memory account, cost-model fills, self-firing stops, persisted to SQLite)
│   ├── live_executor.py       # LiveExecutor (PerpsSession); constructor gated; unit-tested only
│   ├── order_router.py        # InstrumentRouter state machine + Portfolio; idempotent client-order-ids
│   ├── reconciliation.py      # diff(local, remote) -> list[Mismatch]; pre-registered responses
│   ├── state_recovery.py      # cold-start rebuild from executor snapshot
│   └── live_bars.py           # LiveBarBuilder: accepted ticks -> hourly Bar history for the router
├── risk/
│   ├── __init__.py
│   ├── liquidation_guard.py   # RiskLimits, LIMITS, vet_entry, check_open, stop_price
│   ├── portfolio_exposure.py  # CLUSTERS, ExposureLimits, EXPOSURE, vet_exposure
│   └── kill_switch.py         # KillThresholds(None, None), evaluate()
├── monitor/
│   ├── __init__.py
│   ├── alerts.py              # Alert, emit(), sinks: log, sqlite, telegram
│   └── decision_trail.py      # reconstruct(run_id, instrument_id) -> ordered story from SQLite only
├── storage/
│   └── db.py                  # + decisions, orders, positions_local, sim_account, alerts, recovery tables
scripts/
├── run_paper.py               # --executor sim [--hypothesis h1 --params-from RUN_ID] ; --executor live -> exits "Phase 2b"
└── nautilus_recheck.md        # 2.0 lookup result (date, finding)
```

Import boundary: `polymarket` is imported only by `exchange/client.py`, `execution/live_executor.py`, `scripts/check_auth.py`, and tests. `httpx` is used by `monitor/alerts.py` for Telegram.

## 4. Executor boundary

### 4.1 Types (`execution/types.py`, all frozen kw-only dataclasses, Decimal/UTC discipline)
- `OrderRequest(client_order_id: str, instrument_id: int, side: Literal["buy","sell"], quantity: Decimal, reduce_only: bool, ts: datetime)` — market-style: `time_in_force="ioc"`, no limit price in 2a.
- `OrderAck(client_order_id, exchange_order_id: str | None, status: Literal["accepted","rejected"], reason: str, ts)`.
- `StopAck(instrument_id, trigger_price: Decimal, exchange_order_id: str | None, ts)`.
- `OrderUpdate(client_order_id, status: Literal["accepted","open","partial","filled","cancelled","auto_cancelled","rejected"], filled_quantity: Decimal, ts)`.
- `FillUpdate(client_order_id, instrument_id, side, quantity: Decimal, price: Decimal, fee: Decimal, ts)`.
- `PositionView(instrument_id, size: Decimal (signed), entry_price, leverage: int, liquidation_price: Decimal | None, unrealised_pnl, cumulative_funding)`.
- `AccountSnapshot(equity: Decimal, positions: tuple[PositionView, ...], open_orders: tuple[str, ...] (client ids), stops: dict[int, Decimal] (instrument → trigger), in_liquidation: bool, ts)`.
- `State = StrEnum("FLAT","ENTRY_PENDING","OPEN","EXIT_PENDING","LIQUIDATED","HALTED")`.

### 4.2 `Executor` protocol
```python
class Executor(Protocol):
    name: str                      # "sim" | "live"
    async def submit(self, order: OrderRequest) -> OrderAck: ...
    async def cancel(self, client_order_id: str) -> None: ...
    async def place_stop(self, instrument_id: int, trigger_price: Decimal) -> StopAck: ...
    async def heartbeat(self) -> None: ...
    async def snapshot(self) -> AccountSnapshot: ...
    def events(self) -> AsyncIterator[OrderUpdate | FillUpdate]: ...
    async def close(self) -> None: ...
```
No SDK type crosses this boundary in either direction.

### 4.3 `SimExecutor`
- State: cash, per-instrument position (size, entry, leverage = `LIMITS.max_leverage`), open orders, stops. Persisted to `sim_account` after every mutation (so a paper restart exercises recovery).
- `submit`: fills immediately at the latest accepted mark from the feed **plus** `fill_cost(...)` from the Phase 1 cost model (taker fee from `fee_schedule`, spread from the latest book snapshot or `BAR.proxy_spread_bps`, `BAR.impact_bps`). Emits `OrderUpdate(filled)` and `FillUpdate` on `events()`. A configurable `ack_delay_s` and an injectable `fail_next: Literal["timeout","reject"] | None` support the timeout/retry tests.
- Funding: charged each hour from the bar's `funding_rate` (longs pay positive).
- Liquidation price: `entry × (1 − 1/leverage + maintenance_rate)` for longs (mirror for shorts), `maintenance_rate` pre-registered `Decimal("0.02")` (documented assumption; the live path uses the exchange's own number).
- Stops: `place_stop` records a trigger; a `check_triggers(mark)` method fires it (reduce-only fill at trigger price + slippage) — called from the fast loop and **also** callable with the router stopped, which is how "stop fires with bot process killed" is tested in paper.

### 4.4 `LiveExecutor`
- `__init__(session, instrument_ids, *, gate=gates.live_orders_allowed)` calls the gate for every instrument and raises `GateClosed(reason)` if any is not allowed. No other constructor.
- `submit` → `session.place_order(instrument_id=, side=, quantity=, time_in_force="ioc", reduce_only=, client_order_id=)`; `place_stop` → `session.place_position_tp_sl(instrument_id=, stop_loss=PerpsPositionTpSlTrigger(trigger_price=))`; `heartbeat` → `session.arm_auto_cancel(cancel_at=now+60s)`; `snapshot` → `fetch_portfolio()` + `fetch_open_orders()`; `events` → the session's async iterator mapped to our types (order/fill events; `PerpsResyncEvent` becomes a `reconcile_now` signal).
- 2a unit-tests it against a fake session object; it is never executed against the exchange.

### 4.5 Idempotency
`client_order_id = f"{run_id}-{instrument_id}-{seq}"` where `seq` is the router's persisted transition counter. The id is written to `orders` **before** `submit`. On ack timeout the router calls `snapshot()`; if the id is in `open_orders` or a fill for it arrived, it adopts it; only if absent does it re-`submit` with the **same** id. Test: timeout → retry → exactly one fill.

## 5. Risk guards (`polyperps/risk/`)

All pure functions. `Verdict = Allow | Resize(quantity) | Reject(reason)`; the router applies guards in order and takes the tightest.

### 5.1 `liquidation_guard.py`
```python
LIMITS = RiskLimits(
    max_leverage=3,                       # exchange max is 20
    min_liq_distance=Decimal("0.25"),     # |liq − mark| / mark must stay ≥ 25 %
    stop_distance=Decimal("0.15"),        # exchange-side stop at 15 % adverse
    notional_usd=Decimal("100"),          # fixed size in 2a
    max_funding_cost=Decimal("0.02"),     # exit if cumulative funding paid ≥ 2 % of notional
)
```
- `vet_entry(intent, mark, snapshot, limits=LIMITS) -> Verdict`: post-trade leverage = (existing + new notional) / equity; reject if > `max_leverage`; computed liquidation distance < `min_liq_distance` → `Resize` down to the largest quantity that satisfies it (or `Reject` if none).
- `check_open(position, mark, limits=LIMITS) -> Literal["hold","flatten"]`: `flatten` if `|position.liquidation_price − mark| / mark < min_liq_distance` (uses the executor-reported liquidation price).
- `stop_price(side, entry, limits=LIMITS) -> Decimal`: `entry × (1 − 0.15)` for longs, `× (1 + 0.15)` for shorts.
- `funding_exit_due(position, limits=LIMITS) -> bool`: `−cumulative_funding ≥ max_funding_cost × notional`.

### 5.2 `portfolio_exposure.py`
```python
CLUSTERS = {"crypto": "crypto", "equity": "equity", "index": "index", "commodity": "commodity"}  # by instrument category
EXPOSURE = ExposureLimits(gross=Decimal("1.0"), cluster_net=Decimal("0.6"))  # multiples of equity
```
- `vet_exposure(intent, positions, equity, categories, limits=EXPOSURE) -> Verdict`: gross = Σ|notional| incl. the intent; cluster net = Σ signed notional within the intent's cluster incl. the intent. Exceed → `Resize` to the boundary or `Reject`.
- Test scenario from the spec: BTC long at 0.5× equity + ETH long intent at 0.5× → resized to 0.1× (cluster net cap 0.6), even though gross 1.0 would allow it.

### 5.3 `kill_switch.py`
```python
THRESHOLDS = KillThresholds(pause=None, shutdown=None)   # set in Phase 2b from a passing native record
def evaluate(*, live_stats, backtest_ref, mode: Literal["paper","live"], thresholds=THRESHOLDS) -> Literal["run","pause","shutdown"]
```
`None` thresholds → `"pause"` for `live`, `"run"` for `paper`. With numbers: divergence = |live_sharpe − backtest_sharpe| / backtest_sharpe (definition pre-registered here; the numbers come later). Pinned by tests.

## 6. Router (`execution/order_router.py`)

### 6.1 State machine (per instrument)
`FLAT → ENTRY_PENDING → OPEN → EXIT_PENDING → FLAT`; `OPEN → LIQUIDATED` (executor reports `in_liquidation` or size forced to 0 without our order); any → `HALTED` (reconciliation quantity mismatch, unknown exchange position at recovery, or kill switch `shutdown`). `HALTED` requires a human restart (`--clear-halt INSTRUMENT` on the run script, logged).

### 6.2 Per-bar cycle (on each closed hourly `Bar` from `LiveBarBuilder`)
1. `kill = kill_switch.evaluate(...)` → `shutdown`: flatten all, set `HALTED`, CRITICAL alert; `pause`: skip entries (exits still allowed).
2. `target = strategy.target(history)`; `on_flatten` hook honoured as in the harness.
3. If `target` implies a change: build `intent`; `vet_entry` → `vet_exposure` → tightest verdict; `Reject` → log decision, no order.
4. Write the `decisions` row (state before, strategy target, each guard's verdict, final intent or `None`, client_order_id if any). **Then** `submit`.
5. `OrderAck(accepted)` → `ENTRY_PENDING`/`EXIT_PENDING`; `rejected` → stay, WARN alert.
6. On `FillUpdate` completing the order → `OPEN` (then immediately `place_stop(stop_price(...))`, persisted) or `FLAT`.
7. Ack/fill timeout (`ack_timeout_s = 10`) → `snapshot()` reconciliation of that id → adopt or retry once with the same id → still nothing → `HALTED` + CRITICAL.

### 6.3 Fast loop (every 20 s)
`executor.heartbeat()`; for each `OPEN`: `check_open` → `flatten` → reduce-only exit order (same pipeline, reason `liq_distance`); `funding_exit_due` → exit (reason `funding_cost`); `SimExecutor.check_triggers(mark)`.

### 6.4 Persistence (`storage/db.py` additions)
- `decisions(run_id, instrument_id, seq, ts, state_before, target, verdicts_json, intent_json, client_order_id)` PK `(run_id, instrument_id, seq)`.
- `orders(client_order_id PK, run_id, instrument_id, side, quantity, reduce_only, status, exchange_order_id, filled_quantity, avg_price, submitted_at, updated_at, reason)`.
- `positions_local(run_id, instrument_id, state, size, entry_price, stop_trigger, stop_order_id, cumulative_funding, updated_at)` PK `(run_id, instrument_id)`.
- `sim_account(run_id PK, json)`; `alerts(ts, run_id, level, kind, instrument_id, detail_json)`; `recovery(run_id, ts, findings_json)`.
Rows are written before the side effect they describe.

## 7. Reconciliation and recovery

### 7.1 `reconciliation.py`
`diff(local: LocalState, remote: AccountSnapshot) -> list[Mismatch]`, pure. `Mismatch(kind, instrument_id, local, remote)` kinds: `size`, `unknown_order`, `missing_stop`, `stop_without_position`, `liq_price_drift` (live only; informational). Pre-registered responses (in the router):

| kind | response |
|---|---|
| `size` | `HALTED` for that instrument + CRITICAL |
| `unknown_order` (exchange has an order we don't) | `cancel` it + WARN |
| `missing_stop` | `place_stop` + WARN |
| `stop_without_position` | `cancel` + INFO |
| `liq_price_drift` > 5 % | WARN |

Runs every 60 s and on `reconcile_now` (SDK resync). Tests inject each kind.

### 7.2 `state_recovery.py`
`recover(conn, run_id, executor) -> RecoveryReport`, run before any strategy: read local rows; `snapshot()`; for each instrument set state from the **exchange** (`OPEN` iff size ≠ 0; `FLAT` otherwise), adopt open orders whose id starts with `f"{run_id}-"`, cancel other open orders, re-place any missing stop, write a `recovery` row. Exchange position with no local row → `HALTED` + CRITICAL. Paper: `SimExecutor` reloads `sim_account`, so a paper restart follows the same path. Tests: crash between decision row and submit (no order → state `FLAT`, decision marked `abandoned`); crash after submit before ack (order adopted); unknown position.

## 8. Alerts and decision trail

### 8.1 `monitor/alerts.py`
`Alert(level: Literal["INFO","WARN","CRITICAL"], kind, instrument_id: int | None, detail: dict, ts)`; `emit(alert, *, sinks)`. Sinks: `LogSink` (one JSON line via `logging`), `SqliteSink` (`alerts` row), `TelegramSink` (CRITICAL only; `POST https://api.telegram.org/bot{token}/sendMessage` with `chat_id`, `text`; token/chat via `key_management.load_secret("TELEGRAM_BOT_TOKEN")`/`("TELEGRAM_CHAT_ID")`; 5 s timeout; any exception → WARN log, never raised). Threshold config (pre-registered): `funding_drift` if realised hourly funding deviates > 3× from the bar's rate; `margin_ratio` WARN at liq distance < 35 %, CRITICAL < 28 %; `pnl_drawdown` WARN at −5 % of equity from run start, CRITICAL at −10 %.

### 8.2 `monitor/decision_trail.py`
`reconstruct(conn, run_id, instrument_id) -> list[TrailEvent]` merges `decisions`, `orders`, fills (from `orders`), `alerts`, `recovery` by `ts` into one ordered story. Test: run a scripted paper trade through the router, then assert the reconstructed trail contains decision → order → fill → stop placed → exit reason, using SQLite only.

## 9. Run script (`scripts/run_paper.py`)

`--executor sim` (only accepted value in 2a; `live` prints the gate's reason and exits 2), `--hypothesis h1|h2|h3`, `--params-from RUN_ID` (reads `params_chosen` from `validation_log.jsonl`), `--run-id` (default timestamp), `--clear-halt INSTRUMENT_ID`. Boot: `recover()` → `MarketFeed` on the public WS → `LiveBarBuilder` closes hourly bars → router cycle; fast loop; reconciliation loop; alerts. SIGTERM handling and restart-with-backoff as `run_feed.py`. Exit criterion for 2a: 48 h paper run, reconciliation clean, trail reconstructable.

## 10. Error handling

- Executor calls wrapped with `ack_timeout_s`; timeouts feed the idempotent retry path, never a blind resend.
- Guards are pure and never raise on valid input; a `None` liquidation price (sim before first fill) is treated as "no position".
- Reconciliation and alerts never raise into the router; failures are logged and counted.
- Recovery refuses to proceed (raises `RecoveryHalt`) if the executor snapshot is unavailable — better no run than a blind run.
- `LiveExecutor` construction raises `GateClosed`; `run_paper.py --executor live` never reaches construction in 2a.

## 11. Testing (all with fakes; no network; ~60 new tests)

Guards (hand-computed leverage/liquidation/exposure incl. the BTC+ETH cluster case; LIMITS/EXPOSURE/THRESHOLDS pinned); kill switch (`None` → pause live / run paper); router (every transition; reject path; ack timeout → snapshot adopt; timeout → retry once → exactly one fill; funding-cost exit standalone; liq-distance flatten); sim executor (fill price = mark + cost model, funding sign, liquidation price formula, stop fires via `check_triggers` with no router); live executor (constructor raises without gate; payload mapping against a fake session; heartbeat re-arms with `cancel_at` 60 s ahead); reconciliation (five mismatch kinds → responses); recovery (three crash scenarios); alerts (each kind → three sinks; Telegram failure swallowed; token never logged); decision trail reconstruction; `run_paper.py --executor live` exits 2.

## 12. Out of scope (Phase 2b)

Kelly sizing (2.1); kill-switch numbers (2.6); any live executor run; "paper clean 1–2 weeks with the real signal"; NautilusTrader migration (2.0 is a recorded lookup only); deployment (Phase 3).

## 13. Prerequisites the user still owns

Phase 0: `check_auth.py`, 48 h soak, key-risk acknowledgement, geo/eligibility (home ISP currently sinkholes Polymarket). Phase 1: the CI-power decision on the bar; funding-timestamp verification; instrument 7 backfill. None block writing 2a's code; the 48 h paper run needs a network path where Polymarket resolves.
