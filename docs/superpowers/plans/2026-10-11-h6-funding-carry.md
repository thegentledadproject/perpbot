# H6 Funding Carry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add pre-registered hypothesis H6 (funding carry) and run its one native backtest on SAMSUNG (52), BRENTOIL (46) and ZM (48).

**Architecture:** H6 is one new strategy class, `FundingCarry`. It has the same interface as H1 and is registered in `GRIDS`/`build_strategy`, so `scripts/run_backtest.py` picks it up with no script change. The pre-registration commit is merged to GitHub *before* any 52/46/48 history is downloaded. The backtest runs on the box from a scratch clone of master via `PYTHONPATH`, so no deploy or service restart touches the paper soak.

**Tech Stack:** Python 3, Decimal, pytest; EC2 box via `plink -batch -load polyperps-ec2`.

**Spec:** `docs/superpowers/specs/2026-10-11-h6-funding-carry-design.md` (approved 2026-10-11)

## Global Constraints

- The grid is exactly `entry_bps_h` ∈ {0.5, 1.0} × `smooth_hours` ∈ {1, 6}, in that nesting order. Never change it after a result is seen.
- Instruments are exactly 52 (SAMSUNG), 46 (BRENTOIL) and 48 (ZM). Use `--fee-category equity` and `--source native`.
- **Ordering rule:** no candle or funding history for 52/46/48 may be downloaded before Task 1 is merged to `origin/master`.
- Approval rule: `passed` on at least 2 of 3. Instruments with fewer than 60 tested days are not judged.
- No `--executor live`. Never edit `/etc/polyperps/env`. No deploy and no restart of box services in this plan. Do not commit `HANDOFF.md`.
- In PowerShell, never append `2>&1` to plink or git; prefer the Bash tool.
- End commit messages with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. End PR bodies with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.

## Review Focus

1. **Missing funding (`None`) inside the window.** The mean uses the last `smooth_hours` *non-missing* values. If there are fewer than that, the position holds and nothing raises.
2. **Exactly at a threshold.** `f == entry` enters. A short holds at `f == entry/2` and exits only when `f < entry/2`; the mirror case applies to longs. Use Decimal maths, so float rounding never moves a boundary.
3. **Direct flip.** With a short open, a bar with `f ≤ −entry` goes straight to +1, not via 0.
4. **Restart.** After `on_recover(-1)`, an in-band bar (`entry/2 ≤ f < entry`) keeps the short.
5. **Harness end-to-end.** A short collects positive funding through `run_backtest` and ends with positive equity on a synthetic series.

---

### Task 1: FundingCarry strategy, grid and tests (the pre-registration commit)

**Files:**
- Create: `polyperps/strategies/carry.py`
- Modify: `polyperps/strategies/__init__.py` (import, `GRIDS["h6"]`, `build_strategy` branch)
- Test: `tests/test_strategies.py` (append tests; extend `test_grids_are_pre_registered` and `test_build_strategy`)

**Interfaces:**
- Produces: `FundingCarry(*, entry_bps_h: Decimal, smooth_hours: int)`, with:
  - `.name == "h6_funding_carry"`;
  - `.warmup == smooth_hours`;
  - `.params`;
  - `.target(history) -> Decimal`;
  - `.on_flatten()`;
  - `.on_recover(position_sign: int)`.
- Produces: `GRIDS["h6"]`, and `build_strategy("h6", params)`.

- [ ] **Step 1: Write the failing tests.** Append to `tests/test_strategies.py`. It already has `bar(i, close="100", funding="0", ...)`, `minutes(bars)` and `run_backtest` imported. Add `from polyperps.strategies.carry import FundingCarry` to the imports.

```python
def carry(entry="1.0", smooth=1):
    return FundingCarry(entry_bps_h=Decimal(entry), smooth_hours=smooth)


def fb(i, bps):
    """Bar whose funding is `bps` basis points per hour (None = missing)."""
    b = bar(i)
    if bps is None:
        from dataclasses import replace
        return replace(b, funding_rate=None)
    return bar(i, funding=str(Decimal(str(bps)) / 10_000))


def test_h6_enters_the_side_that_is_paid():
    assert carry().target([fb(0, "1.0")]) == Decimal(-1)     # positive funding: shorts are paid
    assert carry().target([fb(0, "-1.0")]) == Decimal(1)
    assert carry().target([fb(0, "0.99")]) == Decimal(0)


def test_h6_exit_thresholds_are_half_entry_and_inclusive_hold():
    s = carry()
    assert s.target([fb(0, "1.0")]) == Decimal(-1)
    assert s.target([fb(1, "0.5")]) == Decimal(-1)            # f == entry/2: hold
    assert s.target([fb(2, "0.49")]) == Decimal(0)            # f < entry/2: exit
    s = carry()
    assert s.target([fb(0, "-1.0")]) == Decimal(1)
    assert s.target([fb(1, "-0.5")]) == Decimal(1)
    assert s.target([fb(2, "-0.49")]) == Decimal(0)


def test_h6_flips_directly_on_a_sign_reversal():
    s = carry()
    assert s.target([fb(0, "2")]) == Decimal(-1)
    assert s.target([fb(1, "-2")]) == Decimal(1)


def test_h6_smooths_over_the_last_n_non_missing_values():
    s = carry(smooth=6)
    assert s.warmup == 6
    hist = [fb(i, "1.2") for i in range(5)]
    assert s.target(hist) == Decimal(0)                       # only 5 values: hold (flat)
    hist = [fb(0, "1.2"), fb(1, None)] + [fb(i, "1.2") for i in range(2, 7)]
    assert s.target(hist) == Decimal(-1)                      # 6 non-missing values, mean 1.2
    hist = [fb(i, "0") for i in range(10)] + [fb(10, "6")]
    assert s.target(hist) == Decimal(-1)                      # mean of last 6 = 1.0 -> enter
    assert s.target(hist + [fb(11, None)]) == Decimal(-1)     # missing current bar: same window, hold


def test_h6_on_flatten_and_on_recover():
    s = carry()
    s.target([fb(0, "1.0")])
    s.on_flatten()
    assert s.target([fb(1, "0.7")]) == Decimal(0)             # in band after a flatten: stays flat
    s = carry()
    s.on_recover(-1)
    assert s.target([fb(0, "0.7")]) == Decimal(-1)            # in band after recover: keeps the short


def test_h6_collects_funding_end_to_end():
    bars = [fb(i, "0") for i in range(3)] + [fb(3 + j, "100") for j in range(10)]   # 1 %/h at a flat price
    s = carry()
    res = run_backtest(bars, s, minute_closes=minutes(bars), taker_fee_rate=Decimal("0.0005"), warmup=s.warmup)
    fills = [r for r in res.ledger if r.kind == "fill"]
    assert fills and fills[0].position == Decimal(-1)
    assert res.equity[-1][1] > Decimal("3")
```

In `test_grids_are_pre_registered`, add:

```python
    assert GRIDS["h6"] == [
        {"entry_bps_h": Decimal("0.5"), "smooth_hours": 1}, {"entry_bps_h": Decimal("0.5"), "smooth_hours": 6},
        {"entry_bps_h": Decimal("1.0"), "smooth_hours": 1}, {"entry_bps_h": Decimal("1.0"), "smooth_hours": 6},
    ]
```

In `test_build_strategy`, add `assert build_strategy("h6", GRIDS["h6"][0]).name == "h6_funding_carry"`.

- [ ] **Step 2: Run the tests and confirm they fail.**

Run: `.venv/Scripts/python -m pytest tests/test_strategies.py -q`
Expected: collection error `ModuleNotFoundError: No module named 'polyperps.strategies.carry'`.

- [ ] **Step 3: Implement `polyperps/strategies/carry.py`.**

```python
"""H6: funding carry (spec 2026-10-11-h6). Hold the side that is paid while the mean of the last
smooth_hours non-missing funding rates is large: f >= entry -> short, f <= -entry -> long; exit when
f falls back inside entry/2 (or flips). Fewer than smooth_hours values: hold."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from itertools import islice

from polyperps.backtest.bars import Bar

_BPS = Decimal(10_000)


class FundingCarry:
    name = "h6_funding_carry"

    def __init__(self, *, entry_bps_h: Decimal, smooth_hours: int) -> None:
        self.entry_bps_h = entry_bps_h
        self.smooth_hours = smooth_hours
        self.params = {"entry_bps_h": entry_bps_h, "smooth_hours": smooth_hours}
        self.warmup = smooth_hours
        self._position = Decimal(0)

    def target(self, history: Sequence[Bar]) -> Decimal:
        rates = list(islice((b.funding_rate for b in reversed(history) if b.funding_rate is not None),
                            self.smooth_hours))
        if len(rates) < self.smooth_hours:
            return self._position
        f = sum(rates) / len(rates) * _BPS
        half = self.entry_bps_h / 2
        if f >= self.entry_bps_h:
            self._position = Decimal(-1)
        elif f <= -self.entry_bps_h:
            self._position = Decimal(1)
        elif (self._position < 0 and f < half) or (self._position > 0 and f > -half):
            self._position = Decimal(0)
        return self._position

    def on_flatten(self) -> None:
        self._position = Decimal(0)

    def on_recover(self, position_sign: int) -> None:
        self._position = Decimal(position_sign)
```

In `polyperps/strategies/__init__.py`:
- add `from polyperps.strategies.carry import FundingCarry`;
- add this after the `"h5"` entry in `GRIDS`:

```python
    "h6": [
        {"entry_bps_h": eb, "smooth_hours": sh}
        for eb in (Decimal("0.5"), Decimal("1.0"))
        for sh in (1, 6)
    ],
```

- add this before the final `raise` in `build_strategy`:

```python
    if hypothesis == "h6":
        return FundingCarry(**params)
```

- [ ] **Step 4: Run the full suite and confirm it passes.**

Run: `.venv/Scripts/python -m pytest -q`
Expected: all pass (578 before, plus 6 new tests).

- [ ] **Step 5: Mark the spec approved, commit, open the PR and merge.**
  1. In the spec's status line, replace `**DRAFT, awaiting user review**` with `**APPROVED 2026-10-11** (plan docs/superpowers/plans/2026-10-11-h6-funding-carry.md)`.
  2. Then run:

```bash
git add polyperps/strategies/carry.py polyperps/strategies/__init__.py tests/test_strategies.py docs/superpowers/specs/2026-10-11-h6-funding-carry-design.md docs/superpowers/plans/2026-10-11-h6-funding-carry.md
git commit -m "h6: funding carry strategy + fixed grid (pre-registration)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push -u origin h6-funding-carry
gh pr create --title "H6 funding carry: pre-registration (spec, grid, strategy)" --body "<summary + test count; end with the Claude Code line>"
gh pr merge --merge --delete-branch
git checkout master && git pull
```

---

### Task 2: Download 52/46/48 history on the box (only after Task 1 is on origin/master)

**Files:** none in the repo. This writes to the box DB `/var/lib/polyperps/polyperps.sqlite3`.

- [ ] **Step 1: Confirm the ordering rule.** `git log origin/master --oneline -3` must show the Task 1 merge. Stop if it doesn't.

- [ ] **Step 2: Store the current fee row** (the backtest refuses to run without one):

```bash
plink -batch -load polyperps-ec2 "sudo systemd-run --wait --pipe -q -p User=polyperps -p EnvironmentFile=/etc/polyperps/env -p WorkingDirectory=/opt/polyperps /opt/polyperps/.venv/bin/python scripts/store_fees.py"
```

Expected: an `equity taker=0.0004 ...` line.

- [ ] **Step 3: Backfill the 1h candles and funding, then the 1m candles, for all three markets.** Venue history starts 2026-08-12, so `--days 61` covers it:

```bash
plink -batch -load polyperps-ec2 "for iv in 1h 1m; do sudo systemd-run --wait --pipe -q -p User=polyperps -p EnvironmentFile=/etc/polyperps/env -p Environment=POLYPERPS_INSTRUMENT_IDS=52,46,48 -p WorkingDirectory=/opt/polyperps /opt/polyperps/.venv/bin/python scripts/backfill.py --days 61 --interval \$iv | tail -12; done"
```

Expected: `+N funding rows, +N candles` for each instrument, plus a gap list. Gaps of a few minutes in 1m data are normal (minutes with no trades). Record the earliest funding timestamp per instrument. Any instrument whose history starts after 2026-08-12 14:00 will have fewer than 60 tested days. That is expected and is handled in Task 3.

---

### Task 3: Run H6 once per instrument and record every result

**Files:**
- Modify: `polyperps/signal/validation_log.jsonl` (append only)

- [ ] **Step 1: Make a scratch clone of master on the box.** No deploy and no service restart:

```bash
plink -batch -load polyperps-ec2 "sudo -u polyperps rm -rf /var/lib/polyperps/h6-src && sudo -u polyperps git clone -q https://github.com/thegentledadproject/perpbot.git /var/lib/polyperps/h6-src && sudo -u polyperps git -C /var/lib/polyperps/h6-src log --oneline -1 && cd /var/lib/polyperps/h6-src && sudo -u polyperps env PYTHONPATH=/var/lib/polyperps/h6-src /opt/polyperps/.venv/bin/python -c 'import polyperps, polyperps.strategies as s; print(polyperps.__file__, sorted(s.GRIDS))'"
```

Expected:
- the Task 1 merge commit;
- `/var/lib/polyperps/h6-src/polyperps/__init__.py`;
- a list of hypotheses that includes `h6`.

- [ ] **Step 2: Run the backtests**, once per instrument:

```bash
plink -batch -load polyperps-ec2 "for i in 52 46 48; do sudo systemd-run --wait --pipe -q -p User=polyperps -p EnvironmentFile=/etc/polyperps/env -p Environment=POLYPERPS_INSTRUMENT_IDS=\$i -p Environment=PYTHONPATH=/var/lib/polyperps/h6-src -p WorkingDirectory=/var/lib/polyperps/h6-src /opt/polyperps/.venv/bin/python scripts/run_backtest.py --hypothesis h6 --instrument \$i --source native --fee-category equity --log-path /var/lib/polyperps/h6-2026-10-11.jsonl; done"
```

Expected: one line per instrument, showing `holdout_sharpe`, `ci`, `screened`, `passed` and `robust_screened`, plus any `sufficiency shortfall`.

- [ ] **Step 3: Read the full records** and note, for each instrument:
  - `dataset.tested_days`;
  - `holdout.fills`, `final_equity`, `hit_rate` and `fills_at_hourly_open`;
  - `holdout_robust.sharpe`.

```bash
plink -batch -load polyperps-ec2 "cat /var/lib/polyperps/h6-2026-10-11.jsonl"
```

- [ ] **Step 4: Append the records verbatim** (LF endings) to `polyperps/signal/validation_log.jsonl`. Then commit, open a PR and merge:
  1. `git pull` on master, then branch `h6-native-2026-10-11`.
  2. Run `.venv/Scripts/python -m pytest -q`. It must pass.
  3. Commit `polyperps/signal/validation_log.jsonl` only, with a message giving pass/fail per instrument.
  4. Push, then `gh pr create`, then `gh pr merge --merge --delete-branch`, then `git checkout master && git pull`.

- [ ] **Step 5: Apply the approval rule and record it.**
  - **Approved:** `passed` on 2 or more instruments.
  - **Closed:** fewer than 2 pass, *and* every instrument had at least 60 tested days.
  - **Pending:** fewer than 2 pass, and some instruments had under 60 tested days. Those instruments get exactly one re-run when they reach 60. Note the date in memory.

  Append a dated paragraph to `C:\Users\user\.claude\projects\C--Users-user-Downloads-perpbot\memory\polyperps-open-items.md` with the numbers and the verdict. Remove the scratch clone with `sudo -u polyperps rm -rf /var/lib/polyperps/h6-src`.
