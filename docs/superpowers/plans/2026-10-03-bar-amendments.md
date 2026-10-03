# Sufficiency-bar amendments A and B: implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a native pass reachable without weakening it into luck: a one-sided 80 % CI with a provisional, self-revoking pass (A), and a last-trade fill price for minutes with no trade, checked against the hourly open (B).

**Architecture:** The pre-registered numbers live in `SufficiencyBar` (`polyperps/signal/sufficiency.py`). The harness gains a fill-fallback mode, and `scripts/run_backtest.py` runs the holdout twice: primary `last_trade`, robustness `hourly_open`. `evaluate_run` and the gate in `polyperps/signal/base.py` require both runs to clear. The gate also revokes an approval when a later native record fails. `HARNESS_VERSION` 2 → 3.

**Tech Stack:** Python 3.11+, stdlib only (`bisect`), pytest.

**Spec:** `docs/superpowers/specs/2026-10-03-bar-amendments-draft.md` (approved by the user 2026-10-03), amending `docs/superpowers/specs/2026-09-11-polyperps-phase1-design.md` §5.2, §7, §8.

## Global Constraints

- No deploy. The paper trader does not run the harness; the soak clock (2026-10-01 16:00 UTC) must not restart.
- Router, executors, risk limits: unchanged. `tests/test_parity.py` must pass untouched (it uses `minute_closes={}`).
- No new dependencies.
- Pre-registered values, verbatim: `bootstrap_ci=Decimal("0.80")` (one-sided lower bound), `last_trade_max_age_min=60`, `min_oos_sharpe=Decimal("1.0")` unchanged, `min_days=60` unchanged.
- `HARNESS_VERSION = 3`.
- LF line endings: check `git ls-files --eol <file>` shows `i/lf` for every touched file before each commit.
- Run tests with `.venv/Scripts/python -m pytest -q tests` (Windows venv).

## Review Focus

1. **Look-ahead in the last-trade lookup.** A 1m candle *after* the fill minute must never price the fill (e.g. HH:00 missing, HH:05 present, nothing before) → hourly open. Pinned in Task 2.
2. **The 60-minute boundary.** Exactly 60 min old is used; 61 is not. Pinned in Task 2.
3. **Revocation by a re-run of an *older* window.** A later-written record whose data window `end` is not after the approved one must not revoke (a repeat with the same `--end` is a replay, not new data). Pinned in Task 3.
4. **Approved record with no `end`, `hypothesis` or `instrument_id`.** It cannot be placed in time, so the gate must read False. Pinned in Task 3.
5. **A negative-edge strategy whose CI sits below zero.** It used to "clear" (two-sided); one-sided it must not. Pinned in Task 1.

---

### Task 1: Pre-registered numbers and the one-sided CI rule

**Files:**
- Modify: `polyperps/signal/sufficiency.py` (module docstring, `SufficiencyBar`, `BAR`, `stats_clear_bar`, new `two_sided_level`)
- Modify: `polyperps/signal/validation_log.py:70` (the `stats_clear_bar` call)
- Test: `tests/test_sufficiency.py`

**Interfaces:**
- Produces: `SufficiencyBar.last_trade_max_age_min: int` (= 60). `stats_clear_bar(*, oos_sharpe: float, ci_lo: float, bar=BAR) -> bool`; the `ci_hi` parameter is removed. `two_sided_level(bar=BAR) -> float` returns `2 * bootstrap_ci - 1` (0.60): the two-sided level whose lower end is the one-sided bound.

- [ ] **Step 1: Write the failing tests.** In `tests/test_sufficiency.py`, change `test_bar_values_are_pinned` to `bootstrap_ci=Decimal("0.80")` and add `last_trade_max_age_min=60` after `notional_usd`. Replace `test_stats_clear_bar_requires_sharpe_and_ci_excluding_zero` with:

```python
def test_stats_clear_bar_requires_sharpe_and_a_positive_one_sided_lower_bound():
    # Amendment A (2026-10-03): one-sided 80 % lower bound on the mean must be > 0.
    assert stats_clear_bar(oos_sharpe=1.2, ci_lo=0.0001) is True
    assert stats_clear_bar(oos_sharpe=0.9, ci_lo=0.0001) is False
    assert stats_clear_bar(oos_sharpe=1.5, ci_lo=-0.0001) is False
    assert stats_clear_bar(oos_sharpe=1.5, ci_lo=0.0) is False


def test_negative_edge_no_longer_clears():
    # Two-sided "excludes zero" let a CI wholly below zero clear; one-sided it cannot.
    assert stats_clear_bar(oos_sharpe=1.5, ci_lo=-0.002) is False


def test_two_sided_level_puts_its_lower_end_at_the_one_sided_bound():
    assert two_sided_level() == pytest.approx(0.60)
```

Add `two_sided_level` to the import from `polyperps.signal.sufficiency`.

- [ ] **Step 2: Run, expect FAIL.** `.venv/Scripts/python -m pytest -q tests/test_sufficiency.py`: the import error / pinned-value mismatch.

- [ ] **Step 3: Implement.** In `sufficiency.py`:
  - add the field `last_trade_max_age_min: int` at the end of `SufficiencyBar`
  - set `bootstrap_ci=Decimal("0.80")` and `last_trade_max_age_min=60` in `BAR`
  - append to the module docstring: `Amendment 2026-10-03 (spec 8.4): bootstrap_ci is a ONE-SIDED lower bound (was two-sided 0.95); last_trade_max_age_min prices a fill whose minute had no trade.`
  - replace `stats_clear_bar`:

```python
def stats_clear_bar(*, oos_sharpe: float, ci_lo: float, bar: SufficiencyBar = BAR) -> bool:
    """Holdout statistics clear the bar: Sharpe at/above the floor and the one-sided
    bar.bootstrap_ci lower bound on the mean net return above zero (amendment 2026-10-03)."""
    return oos_sharpe >= float(bar.min_oos_sharpe) and ci_lo > 0.0


def two_sided_level(bar: SufficiencyBar = BAR) -> float:
    """The two-sided CI level whose lower end is the one-sided bar.bootstrap_ci bound (0.80 -> 0.60)."""
    return 2 * float(bar.bootstrap_ci) - 1
```

  In `validation_log.py`, change the call to `stats_clear_bar(oos_sharpe=holdout_sharpe, ci_lo=ci_lo, bar=bar)`.

- [ ] **Step 4: Run, expect PASS.** `.venv/Scripts/python -m pytest -q tests/test_sufficiency.py tests/test_validation_log.py`. `test_validation_log.py` must still pass unchanged: its default `ci_lo=0.001` clears.

- [ ] **Step 5: Commit.** `git add polyperps/signal/sufficiency.py polyperps/signal/validation_log.py tests/test_sufficiency.py`, then `git commit -m "feat(signal): amendment A - one-sided 80 % CI lower bound; pre-register last_trade_max_age_min=60"`.

---

### Task 2: Harness last-trade fill fallback (harness_version 3)

**Files:**
- Modify: `polyperps/backtest/harness.py` (docstring, `HARNESS_VERSION`, `BacktestResult`, `run_backtest`)
- Modify: `tests/test_signal_gate.py:118` (version pin → 3)
- Test: `tests/test_harness.py`

**Interfaces:**
- Consumes: `BAR.last_trade_max_age_min` (Task 1).
- Produces: `run_backtest(..., fill_fallback: Literal["last_trade", "hourly_open"] = "last_trade", last_trade_max_age_min: int = BAR.last_trade_max_age_min)`. New `BacktestResult` fields `fills_at_last_trade: int = 0` and `max_last_trade_age_min: int = 0`. Fill note `f"fill_source=last_trade age_min={age}"`. `params` gains `"fill_fallback"`.

- [ ] **Step 1: Write the failing tests.** Append to `tests/test_harness.py` (it already has `bar`, `Const`, `T0`, `H`, `FEE`):

```python
M = timedelta(minutes=1)


def _fill(res):
    (fill,) = [r for r in res.ledger if r.kind == "fill"]
    return fill


def test_missing_fill_minute_uses_last_trade_before_it_and_counts_it():
    # fill instant = T0+H+2s; its minute T0+H has no candle; last trade at T0+H-3m
    bars = [bar(0), bar(1, close="103"), bar(2)]
    res = run_backtest(bars, Const(1), minute_closes={T0 + H - 3 * M: Decimal("101")}, taker_fee_rate=FEE, warmup=0)
    fill = _fill(res)
    assert fill.price == Decimal("101") and fill.note == "fill_source=last_trade age_min=3"
    assert res.fills_at_last_trade == 1 and res.fills_at_hourly_open == 0 and res.max_last_trade_age_min == 3


def test_last_trade_exactly_max_age_is_used_one_minute_older_is_not():
    bars = [bar(0), bar(1, close="103"), bar(2)]
    at_cap = run_backtest(bars, Const(1), minute_closes={T0 + H - 60 * M: Decimal("101")}, taker_fee_rate=FEE, warmup=0)
    assert _fill(at_cap).price == Decimal("101") and at_cap.max_last_trade_age_min == 60
    too_old = run_backtest(bars, Const(1), minute_closes={T0 + H - 61 * M: Decimal("101")}, taker_fee_rate=FEE, warmup=0)
    assert _fill(too_old).price == Decimal("103") and too_old.fills_at_hourly_open == 1 and too_old.fills_at_last_trade == 0


def test_a_trade_after_the_fill_minute_is_never_used():
    # no look-ahead: only a later minute exists -> hourly open
    bars = [bar(0), bar(1, close="103"), bar(2)]
    res = run_backtest(bars, Const(1), minute_closes={T0 + H + 5 * M: Decimal("999")}, taker_fee_rate=FEE, warmup=0)
    assert _fill(res).price == Decimal("103") and res.fills_at_hourly_open == 1 and res.fills_at_last_trade == 0


def test_hourly_open_mode_ignores_the_last_trade():
    bars = [bar(0), bar(1, close="103"), bar(2)]
    res = run_backtest(bars, Const(1), minute_closes={T0 + H - 3 * M: Decimal("101")}, taker_fee_rate=FEE, warmup=0,
                       fill_fallback="hourly_open")
    assert _fill(res).price == Decimal("103") and res.fills_at_hourly_open == 1 and res.fills_at_last_trade == 0
    assert res.params["fill_fallback"] == "hourly_open"


def test_fill_minute_candle_still_wins_over_last_trade():
    bars = [bar(0), bar(1, close="103"), bar(2)]
    mc = {T0 + H - 3 * M: Decimal("101"), T0 + H: Decimal("102")}
    res = run_backtest(bars, Const(1), minute_closes=mc, taker_fee_rate=FEE, warmup=0)
    assert _fill(res).price == Decimal("102") and _fill(res).note == "" and res.fills_at_last_trade == 0
```

In `tests/test_signal_gate.py`, change `assert HARNESS_VERSION == 2` to `assert HARNESS_VERSION == 3`.

- [ ] **Step 2: Run, expect FAIL.** `.venv/Scripts/python -m pytest -q tests/test_harness.py tests/test_signal_gate.py`: unexpected keyword `fill_fallback` / missing attribute / version 2.

- [ ] **Step 3: Implement** in `harness.py`.
  - Add `from bisect import bisect_right` to the imports.
  - Set `HARNESS_VERSION = 3` and extend its comment: `3 = amendments A+B (one-sided CI; last-trade fill fallback, spec 8.4).`
  - In the module docstring, replace `no minute candle -> the hourly open (counted)` with `no minute candle -> the last 1m close at or before that minute if <= 60 min old (counted), else the hourly open (counted); fill_fallback="hourly_open" skips the last-trade step (the robustness run)`.
  - Add the two fields to `BacktestResult`, after `fills_at_hourly_open`:

```python
    fills_at_last_trade: int = 0
    max_last_trade_age_min: int = 0
```

  - In `run_backtest`, add the two keyword parameters after `notional` and put `"fill_fallback": fill_fallback` into `params`. After `latency = timedelta(seconds=latency_s)`, add:

```python
    # Amendment B (spec 8.4): most recent 1m close at or before the fill minute, if fresh enough.
    minute_keys = sorted(minute_closes) if fill_fallback == "last_trade" else []
    max_age = timedelta(minutes=last_trade_max_age_min)

    def last_trade(at: datetime) -> datetime | None:
        i = bisect_right(minute_keys, at) - 1
        return minute_keys[i] if i >= 0 and at - minute_keys[i] <= max_age else None
```

  - Replace the fill block in step 5 (from `fill_ts = nxt.open_ts + latency` through the `fill_unavailable` log) with:

```python
            fill_ts = nxt.open_ts + latency
            fill_minute = floor_minute(fill_ts)
            price = minute_closes.get(fill_minute)
            last = last_trade(fill_minute) if price is None else None
            if price is not None:
                trade_to(target, price, bar.spread_bps, nxt.open_ts, "fill")
            elif last is not None:
                age = int((fill_minute - last).total_seconds() // 60)
                res.fills_at_last_trade += 1
                res.max_last_trade_age_min = max(res.max_last_trade_age_min, age)
                trade_to(target, minute_closes[last], bar.spread_bps, nxt.open_ts, "fill",
                         note=f"fill_source=last_trade age_min={age}")
            elif nxt.open is not None:
                # Spec amendment (Task 5): proxy 1m candles exist for ~3.5 days only.
                # Fall back to the hourly open and COUNT it so records show the reliance.
                res.fills_at_hourly_open += 1
                trade_to(target, nxt.open, bar.spread_bps, nxt.open_ts, "fill",
                         note="fill_source=hourly_open")
            else:
                res.fills_unavailable += 1
                log(nxt.open_ts, "fill_unavailable", None, Decimal(0), f"no price at {fill_ts.isoformat()}")
```

- [ ] **Step 4: Run, expect PASS.** Run the full suite with `.venv/Scripts/python -m pytest -q tests`. `test_parity.py` and the existing harness fallback tests must pass unchanged.

- [ ] **Step 5: Commit.** `git add polyperps/backtest/harness.py tests/test_harness.py tests/test_signal_gate.py`, then `git commit -m "feat(backtest): amendment B - last-trade fill fallback (<=60 min), harness_version 3"`.

---

### Task 3: Robustness run, pass rule, and the self-revoking gate

**Files:**
- Modify: `polyperps/signal/validation_log.py` (`evaluate_run`, module docstring)
- Modify: `scripts/run_backtest.py` (`_stats`, `main`)
- Modify: `polyperps/signal/base.py` (`_record_passes`, `load_validated`, new `_revokes`, module docstring)
- Test: `tests/test_validation_log.py`, `tests/test_run_backtest_script.py`, `tests/test_signal_gate.py`

**Interfaces:**
- Consumes: `two_sided_level`, `stats_clear_bar(oos_sharpe, ci_lo)` (Task 1); `run_backtest(fill_fallback=...)`, `fills_at_last_trade`, `max_last_trade_age_min` (Task 2).
- Produces:
  - `evaluate_run(..., robust_screened: bool)`, a required keyword.
  - New record keys `holdout_robust` (the same shape as `holdout`) and `robust_screened: bool`.
  - `holdout` gains `fills_at_last_trade` and `max_last_trade_age_min`.
  - Gate: `_record_passes` also requires `robust_screened is True` plus string `end`, string `hypothesis` and int `instrument_id`. `load_validated` returns False when a revoking record exists.

- [ ] **Step 1: Write the failing tests.**

  In `tests/test_validation_log.py`, give `_eval` a `robust=True` keyword passed as `robust_screened=robust`, and add:

```python
def test_evaluate_run_native_passes_only_if_the_robustness_run_also_screens():
    st = SourceType.POLYMARKET_REST
    assert _eval(st) == (True, True)
    assert _eval(st, robust=False) == (True, False)   # screened stays primary-only
```

  In `tests/test_signal_gate.py`:
  - extend `PASSING` with `"robust_screened": True, "hypothesis": "h1", "instrument_id": 6, "end": "2026-10-12T00:00:00+00:00"`
  - add a helper and these tests:

```python
def _log(tmp_path, records, validated=APPROVAL):
    log = tmp_path / "log.jsonl"
    for r in records:
        append_record(r, path=log)
    val = tmp_path / "validated.json"
    val.write_text(json.dumps(validated), encoding="utf-8")
    return log, val


def test_passed_flag_without_robust_screen_is_false(tmp_path):
    for i, bad in enumerate([{**PASSING, "robust_screened": False},
                             {k: v for k, v in PASSING.items() if k != "robust_screened"}]):
        d = tmp_path / str(i)
        d.mkdir()
        log, val = _files(d, record=bad, validated=APPROVAL)
        assert load_validated(validated_path=val, log_path=log) is False


def test_approved_record_that_cannot_be_placed_in_time_is_false(tmp_path):
    for i, key in enumerate(("end", "hypothesis", "instrument_id")):
        d = tmp_path / str(i)
        d.mkdir()
        log, val = _files(d, record={k: v for k, v in PASSING.items() if k != key}, validated=APPROVAL)
        assert load_validated(validated_path=val, log_path=log) is False, key


LATER_FAIL = {**PASSING, "run_id": "r2", "passed": False, "end": "2026-11-12T00:00:00+00:00"}


def test_later_failing_native_record_revokes_the_approval(tmp_path):
    log, val = _log(tmp_path, [PASSING, LATER_FAIL])
    assert load_validated(validated_path=val, log_path=log) is False


def test_later_passing_record_does_not_revoke(tmp_path):
    log, val = _log(tmp_path, [PASSING, {**LATER_FAIL, "passed": True}])
    assert load_validated(validated_path=val, log_path=log) is True


def test_failing_record_that_does_not_revoke(tmp_path):
    # same or earlier data window (a replay), other hypothesis/instrument, proxy source, older harness
    others = [{**LATER_FAIL, "end": PASSING["end"]},
              {**LATER_FAIL, "end": "2026-09-12T00:00:00+00:00"},
              {**LATER_FAIL, "hypothesis": "h3"},
              {**LATER_FAIL, "instrument_id": 7},
              {**LATER_FAIL, "source_type": "proxy_hyperliquid"},
              {**LATER_FAIL, "harness_version": HARNESS_VERSION - 1}]
    for i, other in enumerate(others):
        d = tmp_path / str(i)
        d.mkdir()
        log, val = _log(d, [PASSING, other])
        assert load_validated(validated_path=val, log_path=log) is True, other
```

  In `tests/test_run_backtest_script.py`, inside `test_run_backtest_h1_native_appends_one_record`, add after the existing holdout asserts:

```python
    assert holdout["fills_at_last_trade"] == 0 and holdout["max_last_trade_age_min"] == 0  # no 1m candles
    robust = record["holdout_robust"]
    assert robust["fills"] == holdout["fills"] and robust["fills_at_last_trade"] == 0
    assert isinstance(record["robust_screened"], bool)
```

- [ ] **Step 2: Run, expect FAIL.** Run `.venv/Scripts/python -m pytest -q tests/test_validation_log.py tests/test_signal_gate.py tests/test_run_backtest_script.py`.

- [ ] **Step 3a: Implement `evaluate_run`.**
  - Add the parameter `robust_screened: bool` after `tested_days`.
  - Add `and robust_screened  # amendment B: also clears with every last-trade fill at the hourly open` to the `passed` conjunction.
  - Append to the module docstring: `Amendment 2026-10-03 (spec 8.4): passed also requires robust_screened - the holdout re-run with every last-trade fill priced at the hourly open clears the bar too.`

- [ ] **Step 3b: Implement `scripts/run_backtest.py`.**
  - Import `two_sided_level` from `polyperps.signal.sufficiency`.
  - In `_stats`, add `"fills_at_last_trade": res.fills_at_last_trade, "max_last_trade_age_min": res.max_last_trade_age_min,`.
  - Change the bootstrap call to `ci=two_sided_level(), seed=seed`, with the comment `# (lo, hi) = 20th/80th percentiles; lo is the one-sided 80 % bound (spec 8.4)`.
  - After `hstats = ...`, add:

```python
        # Amendment B robustness run: the same holdout with every last-trade fill at the hourly open.
        rres = run_backtest(hold_input, build_strategy(args.hypothesis, best_params, proxy_close_by_hour=proxy_closes),
                            minute_closes=minute_closes, taker_fee_rate=fee.taker_fee_rate,
                            warmup=strat.warmup, fill_fallback="hourly_open")
        rstats = _stats(rres, bootstrap=True, seed=args.seed)
        robust_screened = rstats["ci_lo"] is not None and stats_clear_bar(oos_sharpe=rstats["sharpe"],
                                                                          ci_lo=rstats["ci_lo"])
```

  (A fresh strategy is needed because strategies keep state, e.g. h3's hold counter.)
  - Import `stats_clear_bar`.
  - Pass `robust_screened=robust_screened` to `evaluate_run`.
  - Add `"holdout_robust": rstats, "robust_screened": robust_screened,` to the record, after `"holdout": hstats,`.
  - Add ` robust_screened={robust_screened}` to the printed line.

- [ ] **Step 3c: Implement the gate** in `base.py`.
  - Add `from datetime import datetime`.
  - In `_record_passes`, before the `holdout` check, add:

```python
    if record.get("robust_screened") is not True:
        return False   # amendment B: must also clear with last-trade fills at the hourly open
    if not (isinstance(record.get("end"), str) and isinstance(record.get("hypothesis"), str)
            and type(record.get("instrument_id")) is int):
        return False   # cannot be placed in time, so revocation could not be checked
```

  - Add:

```python
def _revokes(later: dict, approved: dict) -> bool:
    """Amendment A (spec 8.4): a pass is provisional. A native record at the current harness for the
    same hypothesis and instrument, on a LATER data window, that did not pass, closes the gate."""
    try:
        newer = datetime.fromisoformat(later["end"]) > datetime.fromisoformat(approved["end"])
    except (KeyError, TypeError, ValueError):
        return False
    return (newer and later.get("hypothesis") == approved["hypothesis"]
            and later.get("instrument_id") == approved["instrument_id"]
            and later.get("harness_version") == HARNESS_VERSION
            and later.get("source_type") in _NATIVE_VALUES
            and later.get("passed") is not True)
```

  - In `load_validated`, replace the final `return any(...)` with:

```python
        records = read_records(path=log_path)
        approved = next((r for r in records if _record_passes(r, run_id)), None)
        return approved is not None and not any(_revokes(r, approved) for r in records)
```

  - In the module docstring, add `robust_screened is True,` to the re-checked list, then add a line: `AND no later-window native record for the same hypothesis/instrument at this harness failed (provisional pass, spec 8.4).`

- [ ] **Step 4: Run, expect PASS.** Run the full suite with `.venv/Scripts/python -m pytest -q tests`.

- [ ] **Step 5: Commit.** `git add polyperps/signal/validation_log.py polyperps/signal/base.py scripts/run_backtest.py tests/test_validation_log.py tests/test_signal_gate.py tests/test_run_backtest_script.py`, then `git commit -m "feat(signal): robustness run, pass needs both fill prices, later native fail revokes approval"`.

---

### Task 4: Spec, README, and the re-run screens

**Files:**
- Modify: `docs/superpowers/specs/2026-09-11-polyperps-phase1-design.md` (§5.2 step 4, §7, §8.1, new §8.4)
- Modify: `docs/superpowers/specs/2026-10-03-bar-amendments-draft.md` (status line)
- Modify: `README.md` (§ "Re-run the screens", Phase 1 table)
- Modify: `polyperps/signal/validation_log.jsonl` (two appended records)

- [ ] **Step 1: Spec edits.**
  - §5.2 step 4: append `**Amendment 2026-10-03 (§8.4):** before the hourly open, use the close of the most recent 1-minute candle at or before the fill minute if it is ≤ 60 min old (`fill_source="last_trade"`, counted in `fills_at_last_trade`, oldest in `max_last_trade_age_min`).`
  - §7: in the code block, set `bootstrap_ci=Decimal("0.80"),    # ONE-SIDED lower bound on mean net return must be > 0 (§8.4)` and add `last_trade_max_age_min=60,`.
  - §8.1: add `fills_at_last_trade, max_last_trade_age_min` to `holdout`; add the lines `holdout_robust {same keys as holdout}` and `robust_screened: bool`; extend `passed` with `AND robust_screened`.
  - New §8.4, `Amendment 2026-10-03 (pre-registered before any native result)`: the two amendment sections of the draft condensed to the rules, the Sharpe table, and the 2026-10-03 staleness table. Name the revocation rule precisely (the same hypothesis and instrument, current harness, native, a later data-window `end`, not passed). State that the re-check is an operator step: re-run `scripts/run_backtest.py --source native` monthly, alongside `scripts/sufficiency.py`.
  - Draft doc: change line 4 to `Status: **APPROVED 2026-10-03, implemented as harness_version 3** (spec §8.4). The monthly re-check is an operator step (README), not part of scripts/sufficiency.py.`

- [ ] **Step 2: README.**
  - Rename the section to `Re-run the screens under harness_version 3 (operator step)`.
  - Change the box command's log path to `screens-v3.jsonl`.
  - Add a sentence: the box holds no Hyperliquid proxy bars, so proxy screens run on the PC.
  - Add a Phase 1 table row: `| native re-check (monthly, after a pass) | scripts/run_backtest.py --hypothesis <h> --instrument <id> --source native; a later failing record revokes validated.json (spec 8.4) |`.

- [ ] **Step 3: Full suite.** Run `.venv/Scripts/python -m pytest -q tests` and expect everything to pass.

- [ ] **Step 4: Re-run the screens (controller, local PC).** Run each of these, then check that `git diff --stat` shows `+2` lines on `validation_log.jsonl`, each with `"harness_version": 3` and a `holdout_robust` key:

```bash
POLYPERPS_INSTRUMENT_IDS=6 .venv/Scripts/python scripts/run_backtest.py --hypothesis h1 --instrument 6 --source hyperliquid --fee-category equity
```

```bash
POLYPERPS_INSTRUMENT_IDS=6 .venv/Scripts/python scripts/run_backtest.py --hypothesis h3 --instrument 6 --source hyperliquid --fee-category equity
```

- [ ] **Step 5: Commit.** Check `git ls-files --eol` on every touched file, then `git add` the spec, the draft, `README.md` and `polyperps/signal/validation_log.jsonl`, then `git commit -m "docs(spec): amendments A+B (8.4); screens re-run under harness_version 3"`.
