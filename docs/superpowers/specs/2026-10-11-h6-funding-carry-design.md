# H6 funding carry on high-funding perps: pre-registered hypothesis

Date: 2026-10-11. Status: **APPROVED 2026-10-11** (plan docs/superpowers/plans/2026-10-11-h6-funding-carry.md). Amends
`docs/superpowers/specs/2026-09-11-polyperps-phase1-design.md` §6 (one new grid row).

**Pre-registration.** Before this spec was written, nobody had looked at any backtest, P&L or holdout
result for H6, or for any strategy on instruments 52, 46 or 48. The only data seen was the 2026-10-07
instrument survey, run on the box over 14 days of native funding plus one book snapshot per instrument.
For each instrument it measured:
- funding standard deviation (bps/h);
- the share of hours at the venue default rate of 1.25e-5;
- spread.

Nothing in it has an outcome attached. This file and the grid code must be committed before any price,
candle or funding history for 52/46/48 is downloaded into a DB that a backtest reads. The commit
timestamps are the evidence.

## 1. Why

The 2026-10-11 native run on BTC failed all five hypotheses (h5 screened on BTC but failed on ETH).
h1 trades funding *deviations*, but on BTC funding pays only about 4 % of h1's execution cost
(measured 2026-10-03): BTC's funding barely moves (sd 0.16 bps/h, 25 % of hours at the default).

Some perps on the venue move funding 4–7× more, and are never at the default:

| Instrument | id | Funding sd (bps/h) | Hours at default | Spread (bps) |
|---|---|---|---|---|
| SAMSUNG | 52 | 1.17 | 0 % | 4.97 |
| BRENTOIL | 46 | 0.50 | 0 % | 6.00 |
| ZM | 48 | 0.46 | 0 % | 2.11 |

H6 tries to collect that funding directly: when the recent rate is large, hold the side that is paid.

**Instrument choice.** These are the three highest-sd instruments that have a recorded spread of at most
6 bps, after keeping only one per correlated family:
- SAMSUNG stands for Korean/Chinese memory (SKHYNIX and CXMT are excluded as the same bet);
- BRENTOIL stands for oil (WTIOIL is excluded);
- ZM is a US single stock.

CBRS (sd 0.84, spread 8.60) is excluded on cost. Selection used signal presence and cost only, never
results.

## 2. Rule (`polyperps/strategies/carry.py`, class `FundingCarry`, name `h6_funding_carry`)

H6 follows the existing interface (spec §5.1):
- `target(history) -> Decimal` in {−1, 0, +1};
- `warmup = smooth_hours`;
- `params`, `on_flatten`, `on_recover`, the same as H1.

The decision is taken at the close of `history[-1]`.

Let `f` be the mean of the last `smooth_hours` non-missing `Bar.funding_rate` values in `history`, in
bps/h (rate × 10 000). If there are fewer than `smooth_hours` such values, hold the current position.

The enter rules are checked first, then the exit rules.

- **Enter / stay:** if `f ≥ entry_bps_h`, the target is −1 (short; positive funding is paid by longs to
  shorts). If `f ≤ −entry_bps_h`, the target is +1.
- **Exit:**
  - If long and `f > −entry_bps_h / 2`, the target is 0.
  - If short and `f < entry_bps_h / 2`, the target is 0.
  - This covers a sign flip too.
- **Hold:** otherwise keep the current position. The rule can flip directly from −1 to +1 when `f`
  crosses both thresholds in one bar. The harness handles the flip as it does for H1
  (the harness exits on that bar; re-entry is the next bar's decision, exactly as for H1).

All existing harness rules apply unchanged:
- funding is paid at bar close (harness rule 2);
- the stop and liquidation guard;
- the funding-cost exit;
- gap flattening;
- the −5 %/−10 % loss limits;
- fills at the next bar's 1m price.

## 3. Grid (fixed; spec §6 row H6)

| entry_bps_h | smooth_hours |
|---|---|
| 0.5 | 1 |
| 0.5 | 6 |
| 1.0 | 1 |
| 1.0 | 6 |

Why these values:
- The default rate is 0.125 bps/h. These instruments' funding sd is 0.46–1.17 bps/h, so both thresholds
  fire on all three instruments.
- The round trip costs about 20–25 bps: 2 × 4 bps taker fee, 2 × 5 bps impact, and the spread. At
  1.0 bps/h, a held position repays that in about a day.
- `smooth_hours` 1 versus 6 tests whether a single hour's rate is noise.

Adding or changing a grid point after any H6 result has been seen is a spec amendment, recorded in the
validation log.

## 4. Data and run

1. Commit this spec, the strategy and its grid. This commit is the pre-registration.
2. On the box, backfill native 1h candles, funding and 1m candles for 52, 46 and 48 from the start of
   venue history (2026-08-12), and store the fee row. Fee category: `equity`, which is the instruments'
   true category, so no substitution is involved.
3. There is no Hyperliquid proxy for these instruments, so there is no proxy screen. The native run is
   the only test, as for H4.
4. Run `scripts/run_backtest.py --hypothesis h6 --source native --fee-category equity` once per
   instrument. The usual harness rules apply:
   - chronological train/holdout split;
   - the best grid point is chosen by train Sharpe and run once on the holdout;
   - block-bootstrap CI;
   - hourly-open robustness re-run.

   Every record goes to `polyperps/signal/validation_log.jsonl`, pass or fail.
5. If an instrument has fewer than 60 tested days, do not judge it. Re-run it once when it reaches 60.

## 5. Approval rule

H6 is approved only if `passed` is True (current harness rules, including amendments 8.3/8.4) on **at
least 2 of the 3** instruments. Only records with at least 60 tested days count toward approval; a run under 60 tested days is never judged, and each such instrument gets exactly one re-run once it reaches 60 days (§4.5).

- **Approved:** paper-trade H6 on the passing instruments for 2 weeks. No real money until a later,
  separate decision.
- **Not approved:** H6 is closed. Re-testing with changed parameters, instruments or rules is not
  allowed.

## 6. Known risks, recorded in advance

- **Weekends.** Equity perps trade 24/7 while the underlying market is shut, so funding on weekends may
  reflect basis drift rather than demand. No weekend filter is added.
- **Short survey.** The survey covered only 14 days, with a single book snapshot per instrument.
- **Price risk.** Price moves can swamp the funding collected. The holdout will show whether they do.
- **Funding convention.** The convention that a row stamped T is the payment for the hour ending at T
  was confirmed on 2026-10-03 (phase-1 spec §4.2) and is used unchanged.

## 7. Out of scope

- A paper-trading unit for H6. It is built only if H6 is approved.
- Any change to H1–H5, the harness, or the pass rules.
- Live trading.
