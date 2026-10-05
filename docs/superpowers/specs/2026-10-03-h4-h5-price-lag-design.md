# H4 lead-lag and H5 overshoot: pre-registered hypotheses

Date: 2026-10-03. Status: **APPROVED 2026-10-05** (plan `docs/superpowers/plans/2026-10-05-h4-h5-price-lag.md`). Amends
`docs/superpowers/specs/2026-09-11-polyperps-phase1-design.md` §6 (two new grid rows).

**Pre-registration.** Before writing this spec, nobody had looked at a backtest, P&L or holdout result for
H4 or H5, on either venue. The only data looked at was:
- the hourly move distributions;
- how often each entry trigger would fire (counted on the box DB on 2026-10-03, with no outcome attached).

That count set the grid ranges, so no grid would be dead the way H3 was. This file and the grid code must be
committed before the first H4/H5 backtest is run. The commit timestamps are the evidence.

## 1. Why these two

The funding ideas are dead on signal data alone. Polymarket's rate is capped at the 1.25e-5/h default, and
extreme episodes last 1–2 h. The venue gap averages about −1.7 %/yr. See memory / HANDOFF 2026-10-03.

That leaves price ideas. Existing price ideas:
- **H2** fades the Polymarket/Hyperliquid level gap against its rolling mean.
- **H3** fades the gap between Polymarket's mark and its index.

The two new ideas are different:
- **H4** follows a fresh one-hour move that Hyperliquid led and Polymarket has not matched yet.
- **H5** fades a one-hour Polymarket move that was unusually large for the past week.

Both need the move to be large and the trade to be held for hours, so that one trade can repay the round trip:
- 2 × 4 bps taker fee (equity fee schedule);
- 2 × 5 bps impact;
- the spread.

That is about 20 bps in all.

## 2. Rules

Both strategies follow the existing interface (spec §5.1): `target(history) -> Decimal` in {−1, 0, +1},
`warmup`, `params`, `on_flatten`, `on_recover`.

The decision is taken at the close of `history[-1]` (bar `t`), as for H1–H3.

`hold_bars` uses H3's semantics:
- While a position is open, each `target` call increments a counter.
- When the counter reaches `hold_bars`, the strategy goes flat.
- It cannot re-enter on that same bar.
- Missing data while a position is open does not stop the counter. The position still exits on schedule.

### 2.1 H4 lead-lag (`polyperps/strategies/lead_lag.py`, class `LeadLag`, name `h4_lead_lag`)

**Inputs.** Let `p = history[-2]` and `c = history[-1]`, and `hl(x) = proxy_close_by_hour.get(x.open_ts)`.
There is no entry unless all of the following hold:
- `c.open_ts − p.open_ts == 1 h`;
- `p.close` and `c.close` are present and non-zero;
- `hl(p)` and `hl(c)` are present and non-zero.

The proxy is looked up only for `open_ts` values already in `history`, exactly as in H2, so there is no
look-ahead.

**Returns.**
- `r_hl = hl(c) / hl(p) − 1`
- `r_pm = c.close / p.close − 1`
- `lag = r_hl − r_pm`

**Entry (only when flat).** All three must hold:
- `abs(r_hl) ≥ gap_bps / 1e4`;
- `lag` has the same sign as `r_hl`;
- `abs(lag) ≥ gap_bps / 1e4`.

Then the position is `sign(r_hl)`.

**Exit.** After `hold_bars` bars.

**Warm-up.** 2.

**Grid.** 4 trials:
- `gap_bps ∈ {25, 50}`
- `hold_bars ∈ {1, 3}`

**Venue.** Native only: `--source native`, like H2. `run_backtest.py` loads the Hyperliquid closes for it in
the same way as for H2. `run_trader.py` refuses `--hypothesis h4`, because there is no live proxy feed yet.

### 2.2 H5 overshoot (`polyperps/strategies/overshoot.py`, class `Overshoot`, name `h5_overshoot`)

**Returns.** `r = close_t / close_{t−1} − 1`, taken over consecutive hourly bars (`open_ts` exactly 1 h
apart) where both closes are present and non-zero.

**Window.** Walking back from `t`, collect the last `168` valid returns. Pairs that have a gap or a missing
close are skipped, not fatal, as in H2. The window must include the current return `r_t`. There is no entry
when either:
- `r_t` is invalid, or
- fewer than 168 valid returns are found.

**Signal.** `z = zscore(window)`, using the existing helper in `polyperps/strategies/_zscore.py`. It gives the
z-score of the last value against the whole window.

**Entry (only when flat).** Both must hold:
- `abs(z) ≥ entry_z`;
- `abs(r_t) ≥ 30 bps`. This is a fixed constant `_MIN_MOVE_BPS = 30`, not a grid axis.

Then the position is `−sign(r_t)`.

**Exit.** After `hold_bars` bars.

**Warm-up.** 169.

**Grid.** 4 trials:
- `entry_z ∈ {2.5, 3.5}`
- `hold_bars ∈ {3, 6}`
- lookback fixed at 168 (constant `_LOOKBACK = 168`)

**Venue.** Native, plus a proxy screen on Hyperliquid's own hourly history (about 208 days). The proxy
screen is informational only (§3).

### 2.3 Trigger counts seen before pre-registration

2026-08-13 to 2026-10-03, about 1,220 hours per instrument. These are entry conditions only, not trades or
results.

| | BTC (6) | ETH (7) |
|---|---|---|
| H4 `gap_bps` 25 / 50 | 32 / 19 | 33 / 21 |
| H5 `entry_z` 2.5 / 3.5 (with the 30 bps floor, window excluding the current return) | 27 / 11 | 24 / 13 |

The H5 counts were computed with the window excluding the current return, while the binding rule (§2.2) includes it, which lowers z slightly (e.g. 3.5 becomes about 3.36 at n = 168), so the 3.5 grid point will fire somewhat less often than counted; the rule stands as written.

The holdout is the last 30 % (about 15 days), so expect roughly 3–10 holdout trades per run. Some H4 gaps may
come from a stale Polymarket close: the last trade in a quiet hour can be minutes old. The fill rules (§8.4
B: last-trade fill at most 60 min old, plus the hourly-open robustness run) price that in.

## 3. Judging

**The bar is unchanged** (spec §7, §8.4).
- At least 60 native days.
- Holdout Sharpe ≥ 1.0.
- One-sided 80 % bootstrap lower bound > 0.
- Zero hourly-open fills.
- `robust_screened`.
- The pass is provisional and is revoked by a later failing native record.

`HARNESS_VERSION` stays 3. The harness does not change, so H1–H3 records stay comparable.

**New approval rule, for H4 and H5 only (operator rule, not code).** Do not add an H4 or H5 record to
`validated.json` for one instrument unless the same hypothesis has a native record at the current
`harness_version` that also has `passed = true` on the other instrument.

Why: 5 hypotheses × 2 instruments = 10 tries, so a lucky pass is likely somewhere. A real lag or overshoot
effect should show on both coins. Approval is already a manual step, so this costs no code.

**The H5 proxy screen is informational.** A failing Hyperliquid screen is recorded and is a warning. It does
not veto the native run, because Hyperliquid is a different market. It can never pass (the bar is native
only).

**No re-tuning.** If H4 or H5 fails, the record stands. A changed rule or grid is a new hypothesis (H6+),
with its own spec committed before any run.

## 4. Code changes

- **New files:** `polyperps/strategies/lead_lag.py` and `polyperps/strategies/overshoot.py`.
- **`polyperps/strategies/__init__.py`:**
  - `GRIDS["h4"]` and `GRIDS["h5"]`, exactly as in §2;
  - `build_strategy` handles `h4` (requires `proxy_close_by_hour`, as for h2) and `h5`.
- **`scripts/run_backtest.py`:** the h2 proxy-close branch becomes `args.hypothesis in ("h2", "h4")`, with the
  same native-only check.
- **`scripts/run_trader.py`:** the h2 refusal also covers h4.
- **Phase 1 spec §6:** add H4 and H5 rows pointing here.
- **No deploy needed.** The paper trader keeps running h1, so the soak is unaffected. HANDOFF/README: the
  operator runs h4/h5 alongside h1–h3 at the 2026-10-12 native run, and the h5 proxy screen any time.

## 5. Tests

Added to `tests/test_strategies.py` and the script tests.

- **H4:**
  - enters with the leader in both directions;
  - no entry when Polymarket moved as much as Hyperliquid, or further;
  - no entry when Hyperliquid moved less than `gap_bps`;
  - no entry on a missing proxy close, a missing native close, or a non-adjacent previous bar;
  - exits after exactly `hold_bars`, and does not re-enter on the exit bar;
  - keeps counting through missing data while a position is open;
  - a proxy close for an hour not yet in `history` is never read: the test mapping raises on any unexpected key.
- **H5:**
  - fades a large up-move and a large down-move;
  - no entry under `entry_z`;
  - no entry under 30 bps even at a high z;
  - no entry with fewer than 168 valid returns;
  - gaps are skipped in the window;
  - exit timing as for H4.
- **GRIDS:** h4 and h5 are pinned to the exact lists, so any later edit fails a test.
- **Scripts:**
  - `run_backtest.py --hypothesis h4 --source hyperliquid` exits with the native-only message;
  - `run_trader.py --hypothesis h4` exits with the "needs a live proxy feed" message.

## 6. Out of scope

- A live Hyperliquid feed for trading H2/H4.
- Minute-level strategies. Hyperliquid 1m data recording is a separate ops step (transient timer `polyperps-hl-1m`, running since 2026-10-05).
- The fix for half-finished candles in `backfill_hyperliquid.py`, which is a separate bounded change.
- Any change to the bar or the harness.
