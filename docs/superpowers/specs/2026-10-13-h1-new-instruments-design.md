# H1 funding reversion on untouched high-funding perps: pre-registered hypothesis

Date: 2026-10-13. Status: **DRAFT, pre-registered 2026-10-22 (this commit); not approved until the §3 checks are done.** Amends nothing in the harness, the pass rules or the
H1 grid. It only adds instruments to H1's test list.

**Pre-registration.** Before this spec was written, nobody had looked at any backtest, P&L or holdout
result for any strategy on the instruments chosen here. The only data seen for them is the 2026-10-07
instrument survey: 14 days of native funding plus one book snapshot each, with no outcome attached.
This file must be committed before any price, candle or funding history for these instruments is
downloaded into a DB that a backtest reads. The commit timestamps are the evidence.

## 1. Why

The 2026-10-12 native run failed H1–H5 on BTC/ETH at 63 tested days. H1 trades funding *deviations*,
but BTC's funding barely moves (sd 0.16 bps/h, 25 % of hours at the venue default 1.25e-5), so the
signal is mostly noise next to an ~8 bps+ round trip. Instruments whose funding moves more give the
signal room to mean something. Whether it does is what this test asks.

## 2. Which instruments are already used up

Choosing instruments after seeing results on them, or on a close sibling, is choosing lottery numbers
after the draw. These are excluded:

| Excluded | Why |
|---|---|
| BTC(6), ETH(7) | H1–H5 results seen |
| SAMSUNG(52), BRENTOIL(46), ZM(48) | H6 results seen (first run 2026-10-11; the 60-day re-run was in progress, results unseen, when this spec was committed) |
| SKHYNIX(14), CXMT(50), SKHY, DRAM, MU, SOXL(89) | same family as SAMSUNG (memory / semiconductors) |
| WTIOIL(3) | same family as BRENTOIL (oil) |
| SOL(8), ETHFI(82) | crypto, same family as BTC/ETH |
| CBRS(85) | spread 8.60 bps, above the cost cap (same reason as H6) |
| GOLD(2) | funding sd 0.17 bps/h, no better than BTC |

## 3. Selection rule (fixed before any data)

From the 2026-10-07 survey, keep instruments that are:
1. not excluded in §2;
2. funding sd ≥ 0.25 bps/h, at least 1.5× BTC's;
3. recorded spread ≤ 6 bps, the same cap H6 used;
4. at most one per correlated family, keeping the highest sd.

Applied to the survey figures on record, this gives:

| Instrument | id | Funding sd (bps/h) | Spread (bps) |
|---|---|---|---|
| UNITREE | 90 | 0.61 | **to confirm** |
| CRCL | 39 | 0.28 | **to confirm** |

Spreads for these two are not in the saved survey notes. Before approval, read them from the
2026-10-07 survey output on the box. That is survey data, not an outcome, so the step is allowed.
Drop any instrument above 6 bps. Do not add a replacement after that: the list only shrinks.

The rule is applied to the **full** 89-instrument survey output, not to the summary notes above. If that
output was not saved, re-run the same survey (funding sd and spread only; no outcomes) and record it here
before any backfill; any extra instrument it qualifies under rules 1–4 joins the list.

**History length.** Before any backfill, read each selected instrument's first native funding row
(listing date). Drop any instrument that cannot reach 60 tested days by 2026-12-31.

**Minimum count.** If fewer than 2 instruments survive these checks, the test is not run and H1 is
closed on this venue.

## 4. Rule and grid

H1 is unchanged (`polyperps/strategies/funding_reversion.py`, harness version 4, flat-window fix
included). Its grid is unchanged (`polyperps/strategies/__init__.py` `GRIDS["h1"]`): lookback 48 / 168
× entry_z 1.5 / 2.0, exit_z 0.5. No new code.

## 5. Data and run

1. Commit this spec. That commit is the pre-registration.
2. On the box, backfill native 1h candles, funding and 1m candles for the selected ids from the start
   of venue history, and store the fee row. Fee category: `equity`. Both instruments are equities
   and `equity` is the only category the venue publishes, so no substitution is involved.
3. There is no Hyperliquid proxy, so there is no proxy screen. The native run is the only test.
4. Run `scripts/run_backtest.py --hypothesis h1 --source native --fee-category equity` once per instrument. The usual rules
   apply: chronological split, train-chosen grid point, one holdout run, block-bootstrap CI, and the
   hourly-open robustness re-run. Every record goes to `polyperps/signal/validation_log.jsonl`, pass
   or fail.
5. Under 60 tested days, do not judge. Re-run once when it reaches 60.

## 6. Approval rule

H1 is approved on these instruments only if `passed` is True on **every** selected instrument with at
least 60 tested days. With only two names, one pass out of two is too easy to get by luck.

- **Approved:** paper-trade H1 on them for 2 weeks. No real money until a later, separate decision.
- **Not approved:** H1 is closed on this venue. Re-testing with changed parameters, instruments or
  rules is not allowed.

## 7. Known risks, recorded in advance

- **Thin pool.** H6 already used the strongest markets, so these are weaker. Even SAMSUNG was
  borderline on cost (about 12 bps of funding movement per hold against a 20–25 bps round trip once impact is counted),
  and UNITREE moves about half as much. A fail is the likely result. This test is worth running only
  because it costs no new code.
- **Weekends.** Equity perps trade 24/7 while the underlying market is shut. No filter is added.
- **Short survey.** The survey covered 14 days with one book snapshot.
- **Sequencing.** H6's 60-day re-run started 2026-10-22 13:32 UTC (two days late). This spec was
  committed while it was running, before its results were seen, so they cannot have influenced the
  selection.

## 8. Out of scope

- Any change to H1–H6, the harness or the pass rules.
- A paper unit for these instruments, built only if approved.
- Live trading.
