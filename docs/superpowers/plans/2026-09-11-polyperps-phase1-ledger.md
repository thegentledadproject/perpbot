# SDD ledger — plan: docs/superpowers/plans/2026-09-11-polyperps-phase1.md

Spec: docs/superpowers/specs/2026-09-11-polyperps-phase1-design.md (reachable); parent polyperps-implementation-plan.md Phase 1.
Branch: phase-1 from master @ 155e1bb. Model policy (CLAUDE.md): sonnet implementers/reviewers, opus final review.

## Pre-flight conflict scan

### Cross-task interfaces
| Producer → Consumer | Produces vs consumes | Finding |
|---|---|---|
| T1 sufficiency → T3/T6/T8/T10/T11 | BAR fields (latency_s, impact_bps, proxy_spread_bps, block_len, resamples, notional_usd, holdout_fraction, bootstrap_ci); NATIVE_SOURCES; SufficiencyReport; stats_clear_bar | Names match in every consumer |
| T2 db → T3/T6/T10 | query_funding/query_ticks(source_type=None kw), query_candles(interval=,source_type=), query_book_spread_bps, insert_fee/latest_fee, FeeSchedule | Match; existing callers (feed, backfill, gap_report) pass no source_type → unchanged |
| T4 client → T4 script | fetch_fees() -> tuple[FeeSchedule]; fee_from_rest | Match |
| T5 hyperliquid → T5 script | HyperliquidClient(limiter=,transport=,clock=), funding_history/candles kw signatures, TransientProxyError.retry_after | Match |
| T6 bars → T8/T9/T10 | Bar fields (Optional OHLC, index_close, funding_rate, spread_bps, complete); floor_minute; align_pair; load_minute_closes | Match; T8/T9 tests construct Bar with all fields |
| T7 costs/stats → T8/T10 | fill_cost kw-only; sharpe/max_drawdown/hit_rate/turnover/block_bootstrap_ci/chronological_split signatures | Match |
| T8 harness → T9/T10 | run_backtest(bars, strategy, *, minute_closes, taker_fee_rate, warmup, latency_s, impact_bps, notional); BacktestResult fields; Strategy.name/.params/.warmup | Match; T9 strategies expose warmup |
| T9 strategies → T10 | GRIDS, build_strategy(hyp, params, proxy_close_by_hour=) | Match |
| T10 validation_log → T11 base | LOG_PATH, read_records(path=), append_record(path=) | Match; base imports validation_log (no cycle: validation_log imports sufficiency only) |
| T11 base ↔ existing gates tests | SIGNAL_VALIDATED still a module attribute; monkeypatch test unchanged | OK |

### Per-task self-consistency
| Task | Checked | Finding |
|---|---|---|
| T1 | 7 tests vs code incl. negative-CI "clears" case | Consistent |
| T2 | insert_fee tuple vs schema (4 cols); spread math 100/20 bps | Consistent |
| T3 | 239h/24 → 9.96 quantized; 61d load → 1465 periods | Consistent |
| T4 | fake SDK fee entries; limiter count 1 | Consistent |
| T5 | MockTransport handler bodies vs client bodies; 429 → retry_after 7.0; 400 → HTTPStatusError | Consistent; API shapes UNVERIFIED by design |
| T6 | hour-alignment math, funding key = open_ts+1h, median spread | Consistent |
| T7 | cost 0.2875 hand-computed; bootstrap thresholds; split 7/3 | Consistent |
| T8 | return accounting starts at equity 0 (self-review fix); gap test timeline; flip PnL | Consistent |
| T9 | H1 synthetic z-score math; H3 fresh instance (self-review fix) | Consistent |
| T10 | json default isoformat (self-review fix); h2 requires native source | Consistent; Step 6 live results are whatever they are |
| T11 | 9 gate tests vs load_validated; reload default False | Consistent |

Rulings:
- Ruling: git branch phase-1 created by controller — implementers skip any branch step — no cost.
- Ruling: model policy sonnet/opus per CLAUDE.md — no cost.

## Progress
Task 1: minor (deferred): no boundary-equality test (days==60 / periods==1000, inclusive by `<`); stats_clear_bar takes floats (brief-mandated).
Task 1: complete (commits 155e1bb..595efc2, review clean)
Task 2: minor (deferred): loop var `l` (E741) in query_book_spread_bps (matches existing _levels_json style); report test-count arithmetic off.
Task 2: complete (commits 595efc2..ac16e04, review clean)
Task 3: observation: native funding for id 6 spans 2026-08-12..09-11 (29.96 days, 693 periods) — exchange history predates the roadmap's "launched 2026-09-03" claim; id 7 has 0 funding rows (never backfilled). Ruling: the bar is unchanged; earliest native pass is ~60 days after the first stored row (≈ 2026-10-11 for id 6), surface to user; run `backfill.py` for id 7 before Phase 1 screening — cost if wrong: none, sufficiency.py measures the real data.
Task 3: minor (deferred): unused `timedelta` import in sufficiency.py (brief-mandated).
Task 3: complete (commits ac16e04..67b6b30, review clean)
Task 4: observation: live fee schedule has a single category "equity" (taker 0.0004, maker 0.000125, 3 tiers) while ids 6/7 are category "crypto". Ruling: Task 10's run_backtest.py gets `--fee-category` (default = instrument category "crypto"); if no row exists it refuses, unless the user passes `--fee-category equity` explicitly, and the record stores `fee_category_used` — an explicit, logged assumption, not a silent substitution — cost if wrong: a crypto-specific fee published later changes results by a few bps; the record shows which rate was used.
Task 4: live evidence: fee row stored — category equity taker 0.0004 maker 0.000125 (only category published).
Task 4: minor (deferred): fee_from_rest drops SDK `tiers` undocumented; store_fees.py silent on empty result.
Task 4: complete (commits 67b6b30..83dfc5e, review clean)
Task 5: live evidence: shapes from memory held; 400-day pull BTC+ETH: 9,600 funding rows/coin, 1h candles capped at ~5,000/coin (~208 days), 1m candles capped at ~5,000/coin (~3.5 days) — Hyperliquid candleSnapshot retention limit; 0 failed windows.
Task 5: Ruling (spec amendment §5.2/§9): 1m candles are unavailable on proxy beyond ~3.5 days, so the harness falls back to the NEXT bar's hourly open when no minute candle exists at open+latency; each such fill is counted (`fills_at_hourly_open`) and the validation-log record carries the count. `fill_unavailable` remains only for the defensive case of a missing open. Rationale: 2 s of drift is far below the pre-registered 5 bps impact + half-spread already charged; refusing would make proxy screening impossible (defeats spec 1.1) — cost if wrong: proxy results are slightly optimistic on fill price; native confirmation runs should backfill native 1m candles so the fallback count there is ~0, and the record makes that visible.
Task 5: fix round 1/5 (1 addressed, 0 open — Retry-After HTTP-date parsing + tests; commits cb0219a..1ab9613)
Task 5: minor (deferred): parse_map no error message on malformed --map; naive-datetime branch of _retry_after_seconds untested.
Task 5: complete (commits 83dfc5e..1ab9613 incl. docs amendment cb0219a, review clean)
Task 6: Ruling: plan's funding query window under-fetched the last bar (upper bound was a single instant) — widened to last_open+2h−1µs — cost if wrong: none (dict lookup ignores extra keys).
Task 6: fix round 1/5 (1 addressed, 0 open — funding window + regression test; commits 747bb02..ced5833)
Task 6: complete (commits 1ab9613..ced5833, review clean)
Task 7: Ruling: plan's chronological_split used float(fraction) — now Decimal ROUND_CEILING — cost if wrong: none.
Task 7: fix round 1/5 (1 addressed, 0 open; commits c8db5b1..206d3a3)
Task 7: minor (deferred): bootstrap percentile uses truncation ("lower" method) not linear interpolation; fill_cost does not abs() notional_delta (harness always passes abs).
Task 7: complete (commits ced5833..206d3a3, review clean)
Task 8: Ruling: plan's trade_to realised the whole leg on any change — now average-cost entry on increases, partial realisation on reductions, full on close/flip — cost if wrong: none for binary strategies (identical), correct basis for sized ones.
Task 8: fix round 1/5 (1 addressed, 0 open; commits 17a001e..9cc8231)
Task 8: minor (deferred): no test for fills_unavailable path or two consecutive incomplete bars (invariant documented in a comment); strategy.params could clobber harness param keys (e.g. "warmup"); fills use the decision bar's spread, undocumented.
Task 8: complete (commits 206d3a3..9cc8231, review clean)
Task 9: Ruling (spec amendment): Strategy protocol gains on_flatten(); the harness calls it after a gap flatten (duck-typed, optional) so strategy state cannot drift from the book — cost if wrong: none for strategies that don't implement it.
Task 9: fix round 1/5 (2 addressed, 0 open; commits 0a250f0..5ad15d1)
Task 9: minor (deferred): H1 silently drops None funding from its window while H2 requires a full window (asymmetry undocumented); self-inclusive z-score ceiling for small windows; zscore float round-trip.
Task 9: complete (commits 9cc8231..5ad15d1, review clean)
Task 10: Ruling: plan's `[b for b in bars if b.open_ts >= _first_complete(bars)]` re-evaluated the scan per element (O(n²) over ~234k bars, 1,000+ s CPU) — cutoff computed once — cost if wrong: none.
Task 10: Ruling applied: `--fee-category` override recorded as fee_category_used (fee schedule publishes only "equity").
Task 10: live results (validation_log.jsonl, 3 records): H1/hyperliquid lookback 168 entry_z 2.0 → holdout Sharpe −3.91, CI (−1.09e-4, −2e-6), 70 fills (65 at hourly open) → screened False; H3/hyperliquid → 0 fills (native-only) → False; H2/native lookback 72 entry_z 3.0 → Sharpe 2.52 but 2 fills, CI straddles 0, dataset 29.96 d/693 periods → False. Nothing screened or passed.
Task 10: Ruling: a holdout too short for the bootstrap must still be logged (ci=None → screened False); run_id gains microseconds; script gets --log-path and an end-to-end test on a synthetic DB — cost if wrong: none.
Task 10: fix round 1/5 (3 addressed, 0 open; commits 29e0496..26be5d5)
Task 10: minor (deferred): unused Decimal import in run_backtest.py.
Task 10: complete (commits 5ad15d1..26be5d5, review clean)
Task 11: Ruling: strip() approval fields (whitespace-only approver no longer counts); corrected the plan's stale "~2026-11-02" to ~2026-10-11 in README/checklist/sufficiency.py/spec (roadmap's launch-date assumption was wrong) — cost if wrong: none.
Task 11: fix round 1/5 (2 addressed, 0 open; commits 5f67657..e82f67d)
Task 11: minor (deferred): importing signal.base now transitively imports storage.db (inert at import); test_default_module_flag_is_false reloads the real module against real repo files (deliberate CI property).
Task 11: complete (commits 26be5d5..e82f67d, review clean)

## Final review (opus, 155e1bb..d9e36ec)
Final: 0 Critical, 9 Important: (1) H2 window = last `lookback` hours not last `lookback` aligned pairs → flat after every gap, deviates from spec §6; (2) native bars silently use the 5 bps constant when no book snapshot (only 11 exist) — unlabelled; (3) `passed` not required to have 0 fallback fills; (4) CI-on-mean test needs Sharpe ≈ 8.8 at the 60-day minimum — bar practically unreachable [USER DECISION]; (5) record's dataset days/periods come from funding table, not the tested candle-limited span; (6) `end=now` makes runs irreproducible, trailing incomplete bars inside holdout; (7) load_validated trusts `passed` verbatim; (8) native build_bars materialises every tick (50M rows at 60 days); (9) Polymarket funding-timestamp semantic asserted, never verified.
Final: Ruling: accept 1,2,3,5,6,7,8,9 + FIX-BEFORE-MERGE minors (T3 timedelta, T10 Decimal, T8 params namespace) into ONE fix wave; 3+5+7 are pre-registered gate tightenings recorded as a spec amendment — cost if wrong: stricter `passed` than the original spec; recorded, reversible only by a further amendment.
Final: Ruling: item 4 (CI power) is the user's call on the sufficiency bar — NOT changed; surfaced in the final message. Minor "one-sided CI" left as-is for the same reason — cost if wrong: the bar stays unreachable until the user amends it.
Final: Ruling challenge accepted: T9 H1/H2 asymmetry is a spec deviation, fixed in this wave.
FIX_BASE = d9e36ec
Final: fix wave 1/1 (8 addressed + 3 minors, 0 open; commits d9e36ec..047d766; 191 tests green)
Final: parked — item 4 (CI-on-mean requires Sharpe ≈ 8.8 at the 60-day minimum; Strict bar practically unreachable) — Ruling: user's decision on the pre-registered bar; surfaced in the final message — cost if wrong: months waiting for a pass that cannot statistically occur.
Final: parked — F9 funding-timestamp semantic INCONCLUSIVE (rate pinned at 1.25e-5 for 253 h; no discriminating settlement observed) — Ruling: documented TODO with recipe in spec §4.2 and bars.py; must be re-checked across an unpinned settlement before any native `passed` is approved — cost if wrong: if the convention is off by one hour, H1 native results are mis-specified and must be re-run.
Final: minor (deferred, from final review): ledger timestamp conventions mixed (funding/gap_flatten at open_ts, marks at nxt.open_ts); align_pair dead in production; _EPOCH start builds ~234k empty bars; warmup > len(train) unguarded; spec §9 "raises on non-finite" not implemented; no test that the holdout warm-up prefix yields no returns; 61% of native funding rows sit at the exchange default 1.25e-5 (H1 z-score is mostly discretisation noise there); one-sided CI clarity; plan doc shows pre-amendment evaluate_run signature (historical artifact); Basis backward walk O(n) per bar.
Final: review clean after adjudication. Branch phase-1 155e1bb..047d766.
