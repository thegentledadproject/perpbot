# Sufficiency-bar amendments A and B (approved 2026-10-03)

Date: 2026-10-03. Status: **APPROVED 2026-10-03, implemented as harness_version 3** (spec §8.4). The monthly re-check is an operator step (README), not part of scripts/sufficiency.py.
Amends `docs/superpowers/specs/2026-09-11-polyperps-phase1-design.md` §5.2, §7, §8.1, §8.3 and
`polyperps/signal/sufficiency.py::BAR`. Pre-registration: no native validation record exists yet
(`validation_log.jsonl` holds proxy screens only), so both changes are still made before any native
result is seen. They must be adopted, or rejected, before the first native run (earliest ~2026-10-11).

## Amendment A: the confidence-interval rule

**Problem.** §7 requires the 95 % block-bootstrap CI on the holdout's mean hourly net return to
exclude zero. That is a t-test, so it needs annualised Sharpe ≥ 1.96 × √(365 / holdout_days).
The holdout is 30 % of the tested span:

| tested days | holdout days | Sharpe needed (95 % two-sided, today) | Sharpe needed (80 % one-sided, proposed) |
|---|---|---|---|
| 60 (min) | 18 | 8.8 | 3.8 |
| 100 | 30 | 6.8 | 2.9 |
| 200 | 60 | 4.8 | 2.1 |
| 400 | 120 | 3.4 | 1.5 |

The block bootstrap widens these a little further. Native history starts 2026-08-12, so a 200-day
test is ~2027-03. As written, the rule makes any real strategy fail for months. That is not a
safety margin; it is a closed gate.

**Options.**
1. **Wait for longer history** (keep the rule). Honest, but no native pass before 2027.
2. **Lower the confidence level and make the pass provisional** (recommended, below).
3. **Drop the CI as a gate and keep it only as a reported number.** Simplest, but then only the
   Sharpe ≥ 1.0 holdout check stands between 18 days of luck and real money.

**Recommended (option 2).**
- The CI check becomes one-sided: the **20th percentile** of the bootstrap distribution of the
  mean must be > 0 (80 % confidence that the edge is positive). `BAR.bootstrap_ci` becomes
  `Decimal("0.80")`, documented as a one-sided lower bound. `min_oos_sharpe = 1.0` stays.
- **A pass is provisional.** `load_validated` rejects an approved record if a *later* native record
  for the same hypothesis and instrument at the current `harness_version` has `passed = False`.
  The monthly `scripts/sufficiency.py` re-check reruns the native backtest on the longer history,
  so luck on 18 days is caught as data grows, and the gate closes by itself (code-enforced, not
  discipline).
- Part B sizing starts from the smallest size the exchange allows until a pass survives a re-run
  at ≥ 100 tested days. This is recorded here and decided in the Part B spec.

## Amendment B: fill price when the fill minute had no trade

**Problem.** §5.2 fills at the close of the 1-minute candle containing `next bar open + 2 s`
(the HH:00 minute). Polymarket emits no 1-minute candle for a minute without trades. Measured on
the box 2026-10-03 (2026-08-12 .. 2026-10-03, 1,214 hours each): the HH:00 candle is missing in
362 hours for instrument 6 (30 %) and 474 for instrument 7 (39 %). §8.3 requires zero hourly-open
fallback fills on the holdout, so a native pass is out of reach on this data however long we wait.

How stale the last trade before HH:00 is, in the missing hours:

| instrument | median | p90 | p99 | max | within 60 min | gap to hourly open (bps, p50 / p90 / p99) |
|---|---|---|---|---|---|---|
| 6 | 2 min | 8 min | 58 min | 62 min | 360 / 362 | 3.1 / 9.7 / 29.7 |
| 7 | 3 min | 13 min | 43 min | 63 min | 472 / 474 | 5.9 / 19.5 / 72.3 |

The two candidate prices sit on either side of the true fill instant. The last trade before is
**stale**; the hourly open (the hour's first trade) is **late**. The gap between them (median
3–6 bps) is about the size of the costs already charged (half spread plus 5 bps impact).

**Recommended.**
1. **Primary price:** if the fill minute has no candle, use the close of the most recent native
   1-minute candle at or before the fill instant, if it is at most **60 minutes** old. The fill is
   tagged `fill_source="last_trade"` and counted in `fills_at_last_trade`; the record also stores
   `max_last_trade_age_min`. If nothing is that recent, use the hourly open as today, counted in
   `fills_at_hourly_open`.
2. **Robustness check:** the holdout is evaluated twice, once with the primary price and once
   with every `last_trade` fill priced at the hourly open instead. `passed` requires `screened`
   (Sharpe ≥ 1.0 and the Amendment A CI) under **both**. A result that only works with one of
   the two proxy prices was never an edge.
3. §8.3's `fills_at_hourly_open == 0` stays, applied to the primary run. With a 60-minute cap,
   that would have been met in 832 of 836 missing hours across both instruments.

Skipped: pricing from the tick feed's mark price at HH:00:02. That would be the exact answer, but
ticks are kept for 3 days only. Add it to the planned hourly tick rollup (one stored mark per
hour at the fill instant); once that history is long enough it can replace both proxies.

## If approved: changes

- `sufficiency.py`: `bootstrap_ci=Decimal("0.80")`, with a docstring line naming the amendment;
  `tests/test_sufficiency.py` pins the new value.
- `stats.py` / harness: the one-sided lower bound; last-trade fallback; the second holdout pass;
  new record fields `fills_at_last_trade`, `max_last_trade_age_min`, `robust_screened`.
- `validation_log.evaluate_run` / `base.load_validated`: `passed` uses both runs; later failing
  native record revokes.
- `HARNESS_VERSION = 3` (re-run the h1/h3 proxy screens again; they still cannot pass).
- Spec §5.2, §7, §8.1 and a new §8.4 carry the text above; the next validation-log record notes
  the amendment.
- Code only; no deploy needed during the soak (the paper trader does not use the harness).
