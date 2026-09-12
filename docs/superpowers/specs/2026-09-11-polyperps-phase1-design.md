# polyperps Phase 1 — Signal Research Harness: Design

Date: 2026-09-11
Status: approved in brainstorming; implementation plan follows.
Parent spec: `polyperps-implementation-plan.md` (Phase 1 rows 1.0–1.3 and the Phase 1 gate).
Builds on: Phase 0 (`docs/superpowers/plans/2026-09-11-polyperps-phase0.md`), merged to `master` at `de8facd`.

## 1. Purpose

Find out, honestly, whether any of three hypothesised edges exists on Polymarket perps — and make it structurally impossible to promote a signal to live trading without a passing, logged, human-approved result on native data.

Phase 1 produces code (a backtest harness, data ingest, three strategies, a sufficiency check, a validation log) and *results*, not a trading bot. The only interface it changes for later phases is how `SIGNAL_VALIDATED` is computed.

## 2. Decisions taken in brainstorming

| Decision | Choice | Consequence |
|---|---|---|
| Sufficiency bar (spec 1.0) | **Strict** — see §7 | Native data cannot meet it before ~2026-10-11 (60 days after the first stored native funding row, 2026-08-12; re-check with `scripts/sufficiency.py`). Correction: this table originally assumed perps launched 2026-09-03, giving ~2 Nov 2026 — the exchange in fact serves native history from 2026-08-12, so the 60-day mark falls about three weeks earlier than first estimated. Phase 1 code is built and validated now; hypotheses are *screened* on proxy data; the bar is re-checked on a schedule. |
| Proxy venue (spec 1.1) | **Hyperliquid only** | Public `/info` API, no key, no read geo-block, years of BTC/ETH funding + candles. Also the comparator for H2. `SourceType.PROXY_HYPERLIQUID`. |
| Hypotheses (spec 1.2) | **All three** from the parent spec | H1 funding mean reversion, H2 cross-venue basis, H3 mark-vs-index lag. |
| Harness architecture | **A: hourly funding-period bars, event-driven** | Point-in-time discipline is structural (strategy receives `bars[:t+1]`). Same code runs on native and proxy bars. Tick-level replay (B) rejected: no proxy ticks exist. Vectorised (C) rejected: look-ahead is one missing shift away. |
| Sizing / leverage in backtest | Fixed notional, 1× | Kelly / fractional sizing is Phase 2 (spec 2.1). |
| Costs | Real taker fee from `fetch_perps_fees()`, slippage from stored book snapshots, pre-registered impact and latency | See §5. |

## 3. Module layout (additions to Phase 0)

```
polyperps/
├── data_ingest/
│   └── hyperliquid.py          # public /info client → FundingObservation / Candle with PROXY_HYPERLIQUID
├── backtest/
│   ├── __init__.py
│   ├── bars.py                 # Bar dataclass; build_bars(); align_pair() for H2
│   ├── costs.py                # fill_cost() pure function
│   ├── strategy.py             # Strategy protocol
│   ├── harness.py              # run_backtest(bars, strategy, params) -> BacktestResult
│   └── stats.py                # sharpe, drawdown, hit rate, turnover, block bootstrap, chronological split
├── strategies/
│   ├── __init__.py
│   ├── funding_reversion.py    # H1
│   ├── basis.py                # H2
│   └── index_lag.py            # H3
├── signal/
│   ├── base.py                 # SIGNAL_VALIDATED now derived from validated.json (see §8)
│   ├── sufficiency.py          # SufficiencyBar (frozen, tested), check_dataset()
│   ├── validation_log.py       # append/read validation_log.jsonl
│   ├── validation_log.jsonl    # committed; every run, pass or fail
│   └── validated.json          # committed; absent or {} until a human approves a passing native record
├── storage/
│   └── db.py                   # + fee_schedule table, + query_candles(), + query_book_spread()
scripts/
├── backfill_hyperliquid.py     # --days N --coins BTC,ETH
├── store_fees.py               # fetch_perps_fees() → fee_schedule
├── run_backtest.py             # --hypothesis h1|h2|h3 --instrument ID --source SOURCE
└── sufficiency.py              # prints SufficiencyReport per instrument/source
```

Import boundary unchanged: only `exchange/client.py` and `scripts/check_auth.py` import `polymarket`. `data_ingest/hyperliquid.py` uses `httpx` (already a transitive dependency; add it explicitly to `pyproject.toml`).

## 4. Data layer

### 4.1 Hyperliquid ingest
- Endpoint: `POST https://api.hyperliquid.xyz/info`, JSON bodies
  - `{"type": "fundingHistory", "coin": "BTC", "startTime": <ms>, "endTime": <ms>}` → list of `{coin, fundingRate, premium, time}`
  - `{"type": "candleSnapshot", "req": {"coin": "BTC", "interval": "1h"|"1m", "startTime": <ms>, "endTime": <ms>}}` → list of `{t, T, s, i, o, c, h, l, v, n}`
- **These shapes are written from memory and are flagged UNVERIFIED in the plan**; the implementer confirms them against one live call before relying on them, and stops if they differ.
- Instrument mapping: proxy rows are stored under the Polymarket instrument id for the same asset (`BTC → 6`, `ETH → 7`), with `source_type = PROXY_HYPERLIQUID`. The Phase 0 primary keys already include `source_type`, so native and proxy rows never collide and every query must name its source.
- Hyperliquid funding is hourly, matching Polymarket's `funding_interval = "1h"`. If a Polymarket instrument ever reports a different interval, bars for that instrument are marked incomplete rather than resampled.
- Pacing: reuse `TokenBucket` (2 req/s, burst 4); windows of 24 h per request like `backfill.py`; same transient-retry policy (Hyperliquid 429s are `httpx.HTTPStatusError`, mapped to a local `TransientProxyError`).

### 4.2 Bars
```python
@dataclass(frozen=True, slots=True, kw_only=True)
class Bar:
    instrument_id: int
    source_type: SourceType
    open_ts: datetime          # UTC, on the hour
    open: Decimal; high: Decimal; low: Decimal; close: Decimal   # mark, from the 1h candle
    index_close: Decimal | None   # native only: last tick's index_price in the hour; None on proxy
    funding_rate: Decimal | None  # the rate settled at open_ts + 1h; None if missing
    spread_bps: Decimal           # median (ask-bid)/mid from book_snapshots in the hour; proxy uses PROXY_SPREAD_BPS (pre-registered constant, 5 bps, flagged as assumption)
    complete: bool                # False if the candle or funding row is missing
```
- `build_bars(conn, instrument_id, source_type, start, end) -> list[Bar]` — one bar per hour in `[start, end)`, incomplete bars present (not dropped) so the harness can see gaps.
- `align_pair(native: list[Bar], proxy: list[Bar]) -> list[tuple[Bar, Bar]]` — hours present and complete in both; used by H2.
- Minute candles are loaded separately by the harness for fills (§5.2), not embedded in `Bar`.

### 4.3 Fees
- `fee_schedule(category TEXT, taker_fee_rate TEXT, maker_fee_rate TEXT, fetched_at TEXT, PRIMARY KEY(category, fetched_at))`.
- `scripts/store_fees.py` fetches once; the harness reads the latest row for the instrument's category. A backtest record (§8) stores the fee it used, so results are reproducible after fees change (spec 3.3 baseline).

## 5. Harness

### 5.1 Strategy interface
```python
class Strategy(Protocol):
    name: str
    params: Mapping[str, Decimal | int]
    def target(self, history: Sequence[Bar]) -> Decimal: ...   # in [-1, +1], fraction of NOTIONAL
```
The harness calls `target(bars[:t+1])` after bar `t` closes. It never passes bar `t+1`. This is the entire look-ahead defence and is tested directly (a recording strategy asserts `len(history)` increments by exactly one per call).

### 5.2 Execution loop (`run_backtest`)
For each bar index `t` from `warmup` to `len(bars) - 2`:
1. **Funding**: if a position is open and `bars[t].funding_rate` is not None, cash += `-position × NOTIONAL × funding_rate` (longs pay when positive).
2. **Gap rule**: if `bars[t+1].complete` is False, flatten at bar `t` close with full costs and skip the strategy call; do not re-enter until the next complete bar.
3. **Decision**: `target = strategy.target(bars[:t+1])`, clamped to `[-1, 1]`.
4. **Fill**: if `target != position`, fill the delta at `fill_price = minute_close_at(bars[t+1].open_ts + LATENCY_S)` (the close of the 1-minute candle containing that instant, same source). **Amendment 2026-09-11 (Task 5 finding):** Hyperliquid retains only ~5,000 candles per interval, so 1-minute candles exist for ~3.5 days only; refusing every other fill would make proxy screening impossible. When no minute candle exists at that instant the fill uses the next bar's hourly `open` instead, is tagged `fill_source="hourly_open"`, and is counted in `fills_at_hourly_open`; the validation-log record carries that count so a reader sees how much of a result rests on the fallback. 2 s of drift is far below the pre-registered half-spread + 5 bps impact already charged. `fill_unavailable` remains only for the defensive case of a complete bar with no `open`. Native confirmation runs should backfill native 1-minute candles so their fallback count is ~0.
5. **Costs**: `fill_cost(abs(delta) × NOTIONAL, bars[t].spread_bps, taker_fee_rate, IMPACT_BPS)`.
6. **Mark**: equity at bar `t+1` close = cash + position × NOTIONAL × (close / entry − 1) for the open leg.

Ledger rows: `funding`, `fill`, `fill_unavailable`, `gap_flatten`, `mark`. `BacktestResult` = ledger, hourly equity series, hourly net returns, parameters (including fee used, latency, impact, notional), bar range and counts.

### 5.3 Cost model (`costs.py`)
`fill_cost(notional_delta, spread_bps, taker_fee_rate, impact_bps) = notional_delta × (taker_fee_rate + spread_bps/2/1e4 + impact_bps/1e4 × turnover_fraction)` where `turnover_fraction = notional_delta / NOTIONAL`. Pure, `Decimal`, tested with hand-computed values.

### 5.4 Statistics (`stats.py`)
- `sharpe(hourly_returns)` annualised by `sqrt(24 × 365)`; `max_drawdown(equity)`; `hit_rate(fills)`; `turnover(fills)`.
- `block_bootstrap_ci(hourly_returns, block=24, resamples=2000, ci=0.95, seed: int) -> (lo, hi)` on the mean net return. `run_backtest.py` uses a fixed default seed of 42 and records it.
- `chronological_split(bars, holdout_fraction=0.30) -> (train, holdout)`.
- The holdout slice is passed only to `run_backtest` for the single final evaluation; parameter selection sees only `train`.

## 6. Strategies (pre-registered grids)

Grids are fixed lists, not ranges. Every grid point is a trial; selection is by train Sharpe; the chosen point runs once on holdout. Adding a grid point after seeing results is a spec change and must be logged as such in the validation log.

| Hyp. | File | Signal | Grid |
|---|---|---|---|
| H1 funding mean reversion | `strategies/funding_reversion.py` | `z = zscore(funding_rate, lookback)`; short when `z ≥ entry_z`, long when `z ≤ −entry_z`, flat when `abs(z) < exit_z` | `lookback ∈ {48, 168}`, `entry_z ∈ {1.5, 2.0}`, `exit_z = 0.5` |
| H2 cross-venue basis | `strategies/basis.py` | `basis = pm_close / hl_close − 1` on `align_pair`; `z = zscore(basis, lookback)`; short PM when `z ≥ entry_z`, long when `z ≤ −entry_z`, flat inside `±0.5` | `lookback ∈ {24, 72}`, `entry_z ∈ {2.0, 3.0}` |
| H3 mark-vs-index lag | `strategies/index_lag.py` | `premium_bps = (close / index_close − 1) × 1e4`; if `abs(premium_bps) ≥ entry_bps` take the position that closes the premium; hold `hold_bars` bars then flat; returns `0` when `index_close` is None (proxy) | `entry_bps ∈ {10, 25}`, `hold_bars ∈ {1, 3}` |

Warm-up = the strategy's `lookback` (H1/H2) or 1 (H3); bars inside warm-up are not traded.

## 7. Sufficiency bar (spec 1.0) — pre-registered, code-enforced

`polyperps/signal/sufficiency.py`:
```python
BAR = SufficiencyBar(
    native_only=True,                 # proxy data can screen, never pass
    min_days=60,
    min_funding_periods=1000,
    holdout_fraction=Decimal("0.30"),
    min_oos_sharpe=Decimal("1.0"),    # after modelled costs
    bootstrap_ci=Decimal("0.95"),     # CI on mean net return must exclude zero
    # pre-registered execution assumptions
    latency_s=2,
    impact_bps=Decimal("5"),
    proxy_spread_bps=Decimal("5"),
    block_len=24,
    resamples=2000,
    notional_usd=Decimal("100"),
)
```
- A test asserts every field's value. Changing any number is a visible test change and a logged spec amendment.
- `check_dataset(conn, instrument_id, source_type, *, now) -> SufficiencyReport(met: bool, days: Decimal, funding_periods: int, shortfall: dict[str, str])`.
- `scripts/sufficiency.py` prints the report per configured instrument for both sources. The eligibility checklist gains a row: "re-run `scripts/sufficiency.py`; earliest possible native pass ≈ 2026-10-11 (60 days after the first stored native funding row, 2026-08-12)".

## 8. Validation log and the gate (Phase 1 gate in the parent spec)

### 8.1 Validation log
`scripts/run_backtest.py` appends one JSON object per line to `polyperps/signal/validation_log.jsonl` (committed) for every run:
```
run_id, ts, hypothesis, instrument_id, source_type, params_chosen, grid_tried,
dataset {start, end, bars, complete_bars, funding_periods, days},
fee_used, latency_s, impact_bps, seed,
train {sharpe, max_dd, hit_rate, turnover, n},
holdout {sharpe, max_dd, hit_rate, turnover, n, ci_lo, ci_hi},
sufficiency {met, shortfall},
screened: bool,   # holdout cleared the stats thresholds, any source
passed: bool      # screened AND source is native AND sufficiency.met
```
`passed` can only be `True` for `POLYMARKET_*` sources with `sufficiency.met`. Negative and insufficient results are appended too — nothing is discarded. `validation_log.read_passing() -> list[record]`.

### 8.2 The gate
`polyperps/signal/base.py`:
- `SIGNAL_VALIDATED` is computed at import from `polyperps/signal/validated.json`:
  ```json
  {"run_id": "<run_id from validation_log.jsonl>", "approved_by": "<git user.name>", "approved_at": "<ISO-8601 UTC>", "note": "<why>"}
  ```
  It is `True` only if the file exists, names a `run_id` whose log record has `passed == True`, and `approved_by`/`approved_at` are non-empty. Any other state (missing file, empty object, unknown run_id, record not passed, proxy record, no approver) → `False`.
- Two keys therefore: a passing native record written by code, and a deliberate human commit of `validated.json`. Neither alone flips the flag. This satisfies the parent spec's "code-enforced, not discipline-only" and its "manual, never self-adjusting" boundary simultaneously.
- `generate_signal` remains `NotImplementedError`. Selecting which validated strategy to run live is a Phase 2 decision.
- `gates.live_orders_allowed` is unchanged; it already reads `SIGNAL_VALIDATED` at call time.

## 9. Error handling

- Ingest: transient HTTP errors retried (3×, honouring `Retry-After`); anything else aborts loudly. Rows are idempotent (`INSERT OR IGNORE`).
- Bars: never fabricate — missing data yields `complete=False`, never interpolation.
- Harness: fills at the 1-minute close after latency, falling back to the next hourly open (counted) when no minute candle exists; refuses to trade across gaps; clamps targets; raises on non-finite numbers.
- Stats: bootstrap on `< 2 × block_len` returns raise `ValueError("insufficient for block bootstrap")` — a run on a tiny dataset fails visibly rather than reporting a CI.
- Gate: any malformed `validated.json` → `SIGNAL_VALIDATED = False` and a logged warning; never an exception at import (importing the package must not crash the feed).

## 10. Testing

Synthetic, deterministic, no network:
- **Point-in-time**: recording strategy proves `len(history) == t + 1` on every call; harness never passes a bar beyond `t`.
- **Gap rule**: a series with an incomplete bar forces a flatten and blocks re-entry until the next complete bar.
- **Fill fallback**: no minute candle at `open_ts + latency` → fill at the next bar's hourly open, `fills_at_hourly_open` incremented; a complete bar with no `open` → `fill_unavailable`, position unchanged.
- **Costs**: hand-computed expected costs for a known delta/spread/fee.
- **Funding sign**: long position with positive funding loses exactly `notional × rate`.
- **H1 sanity**: a constructed series where funding spikes then decays must be profitable net of costs on H1 with the pre-registered grid; a constant-funding series must trade never.
- **H2/H3 sanity**: analogous constructed series; H3 returns `0` on proxy bars.
- **Bootstrap**: iid zero-mean noise → CI contains zero; a constant positive series → CI excludes zero; too-short input raises.
- **Split**: chronological, no overlap, holdout is the last 30 %.
- **Sufficiency**: every BAR field pinned; `check_dataset` reports shortfall on a 10-day dataset and `met` on a synthetic 61-day / 1,464-period one.
- **Gate**: `SIGNAL_VALIDATED` is `False` for missing file, empty object, unknown `run_id`, proxy record, `passed=False`, no approver; `True` only for the full combination. Tests write temp log/validated files and reload the module.
- **Hyperliquid parser**: response-shape fixtures (captured from the one live verification call) parse to `FundingObservation`/`Candle` with `PROXY_HYPERLIQUID`; a changed shape fails the test, not the backfill.

## 11. Out of scope (later plans)

Kelly sizing, liquidation guard, order routing, reconciliation (Phase 2); kill thresholds (2.5); deployment (3); recalibration (3+); any wiring of a strategy into `generate_signal`.

## 12. Timeline reality

Hyperliquid screening can run immediately after the harness lands. Native confirmation of any hypothesis is impossible before ~2026-10-11 under the Strict bar (60 days after the first stored native funding row, 2026-08-12 — not 2026-09-03 as §2 originally assumed); the checklist row makes the re-check routine rather than remembered.
