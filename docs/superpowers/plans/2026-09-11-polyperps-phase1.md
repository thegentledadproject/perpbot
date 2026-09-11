# polyperps Phase 1 — Signal Research Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the pre-registered sufficiency bar, a point-in-time hourly backtest harness with realistic costs, Hyperliquid proxy ingest, three hypothesis strategies, a committed validation log, and the two-key `SIGNAL_VALIDATED` gate — so a signal can be screened now on proxy data and confirmed later on native data without any path to promote it by hand-waving.

**Architecture:** Hourly `Bar`s are built from the Phase 0 SQLite tables (candles + funding + ticks + book snapshots) per `(instrument, source_type)`. `run_backtest` feeds a `Strategy` exactly `bars[:t+1]`, fills at the 1-minute close `latency_s` after the next bar opens, charges fee + half-spread + impact, pays funding hourly, and refuses to trade across gaps. `stats` computes annualised Sharpe and a block-bootstrap CI. Every run appends a record to `polyperps/signal/validation_log.jsonl`; `SIGNAL_VALIDATED` becomes `True` only if a committed `validated.json` names a `passed=True` native record *and* carries a human approval.

**Tech Stack:** Python 3.12, stdlib (`sqlite3`, `statistics`, `random`, `json`), `httpx` (already a transitive dep of `polymarket-client`; made explicit), pytest + pytest-asyncio. No pandas/numpy — datasets are small (≤ ~10⁴ bars) and Decimal discipline matters more than speed.

**Spec:** `docs/superpowers/specs/2026-09-11-polyperps-phase1-design.md` (this plan argues from it; executors read both). Parent spec: `polyperps-implementation-plan.md` Phase 1.

## Global Constraints

- Import boundary unchanged: only `polyperps/exchange/client.py` and `scripts/check_auth.py` import `polymarket`. `data_ingest/hyperliquid.py` uses `httpx` only.
- Add `"httpx>=0.27,<1"` to `pyproject.toml` dependencies (already installed transitively at 0.28.1).
- Prices, rates, returns in the ledger are `Decimal`; datetimes are timezone-aware UTC (`polyperps.exchange.types` rejects anything else). Statistics (`sharpe`, bootstrap) may use `float` internally; money never does.
- Pre-registered numbers (spec §7) live in `polyperps/signal/sufficiency.py::BAR` and are pinned by a test: `min_days=60`, `min_funding_periods=1000`, `holdout_fraction=0.30`, `min_oos_sharpe=1.0`, `bootstrap_ci=0.95`, `latency_s=2`, `impact_bps=5`, `proxy_spread_bps=5`, `block_len=24`, `resamples=2000`, `notional_usd=100`, `native_only=True`. Changing any is a logged spec amendment, never a quiet edit.
- Strategy grids (spec §6) are fixed lists in code: H1 `lookback ∈ {48,168}`, `entry_z ∈ {1.5,2.0}`, `exit_z=0.5`; H2 `lookback ∈ {24,72}`, `entry_z ∈ {2.0,3.0}`; H3 `entry_bps ∈ {10,25}`, `hold_bars ∈ {1,3}`.
- No fabrication: missing data → `complete=False`, never interpolated; no minute candle at the fill instant → `fill_unavailable`, never filled at the hourly open; no fee row → refuse to run.
- `passed` can only be `True` for `source_type in (polymarket_ws, polymarket_rest)` with the dataset meeting the bar. Proxy runs produce `screened` only.
- `polyperps/signal/validation_log.jsonl` and `polyperps/signal/validated.json` are committed files. `validated.json` starts as `{}`.
- `generate_signal` stays `NotImplementedError`. `gates.live_orders_allowed` is not modified.
- Proxy rows use the Polymarket instrument id of the same asset (`6` = BTC, `7` = ETH) with `source_type=proxy_hyperliquid`.
- Hyperliquid request/response shapes in Task 5 are **UNVERIFIED** (written from memory): the implementer confirms them with one live call and stops if they differ.
- Commit trailer: blank line then `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.

---

## File structure

```
polyperps/
├── exchange/
│   ├── types.py                # + FeeSchedule
│   └── client.py               # + fee_from_rest(), fetch_fees()
├── storage/
│   └── db.py                   # + fee_schedule table, insert_fee, latest_fee, source_type filters, query_candles, query_book_spread_bps
├── data_ingest/
│   └── hyperliquid.py          # HyperliquidClient (httpx), parse_funding_history, parse_candles, TransientProxyError
├── backtest/
│   ├── __init__.py
│   ├── bars.py                 # Bar, floor_hour, floor_minute, is_native, build_bars, align_pair, load_minute_closes
│   ├── costs.py                # fill_cost
│   ├── stats.py                # sharpe, max_drawdown, hit_rate, turnover, block_bootstrap_ci, chronological_split
│   ├── strategy.py             # Strategy protocol, clamp_target
│   └── harness.py              # LedgerRow, BacktestResult, run_backtest
├── strategies/
│   ├── __init__.py             # GRIDS, build_strategy
│   ├── _zscore.py              # zscore helper shared by H1/H2
│   ├── funding_reversion.py    # H1
│   ├── basis.py                # H2
│   └── index_lag.py            # H3
├── signal/
│   ├── base.py                 # SIGNAL_VALIDATED derived from validated.json + log
│   ├── sufficiency.py          # SufficiencyBar, BAR, SufficiencyReport, dataset_meets_bar, stats_clear_bar, check_dataset
│   ├── validation_log.py       # RunRecord helpers: append_record, read_records, read_passing, make_run_id
│   ├── validation_log.jsonl    # committed, starts empty
│   └── validated.json          # committed, starts as {}
scripts/
├── store_fees.py
├── backfill_hyperliquid.py
├── sufficiency.py
└── run_backtest.py
tests/
├── test_sufficiency.py
├── test_storage_phase1.py
├── test_fees.py
├── test_hyperliquid.py
├── test_bars.py
├── test_costs.py
├── test_stats.py
├── test_harness.py
├── test_strategies.py
├── test_validation_log.py
└── test_signal_gate.py
```

All existing tests (83) must stay green throughout.

---

### Task 1: The pre-registered sufficiency bar (spec §7) — written before any strategy exists

**Files:**
- Create: `polyperps/signal/sufficiency.py`
- Test: `tests/test_sufficiency.py`

**Interfaces:**
- Produces:
  - `SufficiencyBar` frozen kw-only dataclass with fields exactly: `native_only: bool`, `min_days: int`, `min_funding_periods: int`, `holdout_fraction: Decimal`, `min_oos_sharpe: Decimal`, `bootstrap_ci: Decimal`, `latency_s: int`, `impact_bps: Decimal`, `proxy_spread_bps: Decimal`, `block_len: int`, `resamples: int`, `notional_usd: Decimal`.
  - `BAR: SufficiencyBar` — the constants in Global Constraints.
  - `SufficiencyReport(met: bool, days: Decimal, funding_periods: int, source_type: SourceType, shortfall: dict[str, str])` frozen.
  - `dataset_meets_bar(*, days: Decimal, funding_periods: int, source_type: SourceType, bar: SufficiencyBar = BAR) -> SufficiencyReport`
  - `stats_clear_bar(*, oos_sharpe: float, ci_lo: float, ci_hi: float, bar: SufficiencyBar = BAR) -> bool` — `True` iff `oos_sharpe >= min_oos_sharpe` and the CI excludes zero (`ci_lo > 0 or ci_hi < 0`).
  - `NATIVE_SOURCES = (SourceType.POLYMARKET_WS, SourceType.POLYMARKET_REST)`.
- `check_dataset(conn, …)` is added in Task 3 (needs Task 2's storage queries).

- [ ] **Step 1: Write the failing tests**

`tests/test_sufficiency.py`:

```python
from decimal import Decimal

from polyperps.exchange.types import SourceType
from polyperps.signal.sufficiency import (
    BAR,
    NATIVE_SOURCES,
    SufficiencyBar,
    dataset_meets_bar,
    stats_clear_bar,
)


def test_bar_values_are_pinned():
    # Spec §7. Changing any of these is a logged spec amendment, not a quiet edit.
    assert BAR == SufficiencyBar(
        native_only=True,
        min_days=60,
        min_funding_periods=1000,
        holdout_fraction=Decimal("0.30"),
        min_oos_sharpe=Decimal("1.0"),
        bootstrap_ci=Decimal("0.95"),
        latency_s=2,
        impact_bps=Decimal("5"),
        proxy_spread_bps=Decimal("5"),
        block_len=24,
        resamples=2000,
        notional_usd=Decimal("100"),
    )


def test_bar_is_frozen():
    import pytest

    with pytest.raises(AttributeError):
        BAR.min_days = 1  # type: ignore[misc]


def test_native_sources():
    assert NATIVE_SOURCES == (SourceType.POLYMARKET_WS, SourceType.POLYMARKET_REST)


def test_dataset_short_on_days_and_periods_reports_both():
    r = dataset_meets_bar(days=Decimal("10"), funding_periods=240, source_type=SourceType.POLYMARKET_REST)
    assert r.met is False
    assert set(r.shortfall) == {"days", "funding_periods"}
    assert "10" in r.shortfall["days"] and "60" in r.shortfall["days"]


def test_dataset_meets_bar_on_native():
    r = dataset_meets_bar(days=Decimal("61"), funding_periods=1464, source_type=SourceType.POLYMARKET_REST)
    assert r.met is True and r.shortfall == {}


def test_proxy_never_meets_bar_even_with_years_of_data():
    r = dataset_meets_bar(days=Decimal("900"), funding_periods=21600, source_type=SourceType.PROXY_HYPERLIQUID)
    assert r.met is False
    assert "source_type" in r.shortfall


def test_stats_clear_bar_requires_sharpe_and_ci_excluding_zero():
    assert stats_clear_bar(oos_sharpe=1.2, ci_lo=0.0001, ci_hi=0.001) is True
    assert stats_clear_bar(oos_sharpe=0.9, ci_lo=0.0001, ci_hi=0.001) is False
    assert stats_clear_bar(oos_sharpe=1.5, ci_lo=-0.0001, ci_hi=0.001) is False
    assert stats_clear_bar(oos_sharpe=1.5, ci_lo=-0.002, ci_hi=-0.001) is True  # negative edge also "clears" statistically; sign is the strategy's job
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_sufficiency.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.signal.sufficiency'`

- [ ] **Step 3: Write `polyperps/signal/sufficiency.py`**

```python
"""Spec 1.0: the sufficiency bar, pre-registered before any hypothesis is tested.

BAR is frozen and pinned by tests/test_sufficiency.py. Every number here was
chosen on 2026-09-11 before any backtest ran. Changing one is a spec
amendment: edit the spec, edit this file, edit the test, and say so in the
validation log's next record. Never adjust it to make a result pass.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from polyperps.exchange.types import SourceType

NATIVE_SOURCES: tuple[SourceType, ...] = (SourceType.POLYMARKET_WS, SourceType.POLYMARKET_REST)


@dataclass(frozen=True, slots=True, kw_only=True)
class SufficiencyBar:
    native_only: bool
    min_days: int
    min_funding_periods: int
    holdout_fraction: Decimal
    min_oos_sharpe: Decimal
    bootstrap_ci: Decimal
    # pre-registered execution assumptions used by the harness
    latency_s: int
    impact_bps: Decimal
    proxy_spread_bps: Decimal
    block_len: int
    resamples: int
    notional_usd: Decimal


BAR = SufficiencyBar(
    native_only=True,
    min_days=60,
    min_funding_periods=1000,
    holdout_fraction=Decimal("0.30"),
    min_oos_sharpe=Decimal("1.0"),
    bootstrap_ci=Decimal("0.95"),
    latency_s=2,
    impact_bps=Decimal("5"),
    proxy_spread_bps=Decimal("5"),
    block_len=24,
    resamples=2000,
    notional_usd=Decimal("100"),
)


@dataclass(frozen=True, slots=True, kw_only=True)
class SufficiencyReport:
    met: bool
    days: Decimal
    funding_periods: int
    source_type: SourceType
    shortfall: dict[str, str] = field(default_factory=dict)


def dataset_meets_bar(
    *,
    days: Decimal,
    funding_periods: int,
    source_type: SourceType,
    bar: SufficiencyBar = BAR,
) -> SufficiencyReport:
    shortfall: dict[str, str] = {}
    if bar.native_only and source_type not in NATIVE_SOURCES:
        shortfall["source_type"] = f"{source_type.value} is a proxy source; only native data can meet the bar"
    if days < bar.min_days:
        shortfall["days"] = f"{days} < {bar.min_days}"
    if funding_periods < bar.min_funding_periods:
        shortfall["funding_periods"] = f"{funding_periods} < {bar.min_funding_periods}"
    return SufficiencyReport(
        met=not shortfall,
        days=days,
        funding_periods=funding_periods,
        source_type=source_type,
        shortfall=shortfall,
    )


def stats_clear_bar(*, oos_sharpe: float, ci_lo: float, ci_hi: float, bar: SufficiencyBar = BAR) -> bool:
    """Holdout statistics clear the bar: Sharpe at/above the floor and a CI that excludes zero."""
    if oos_sharpe < float(bar.min_oos_sharpe):
        return False
    return ci_lo > 0.0 or ci_hi < 0.0
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_sufficiency.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add polyperps/signal/sufficiency.py tests/test_sufficiency.py
git commit -m "feat(phase1): pre-register the sufficiency bar (spec 1.0)"
```

---

### Task 2: Storage extensions — source-filtered queries, candles, book spread, fee schedule

**Files:**
- Modify: `polyperps/exchange/types.py` (append `FeeSchedule`)
- Modify: `polyperps/storage/db.py` (schema + functions)
- Test: `tests/test_storage_phase1.py`

**Interfaces:**
- Consumes: existing `connect`, `insert_*`, `_ts`, `_parse_ts`, `SourceType`, `Candle`, `Tick`, `FundingObservation`, `BookSnapshot`.
- Produces:
  - `FeeSchedule(category: str, taker_fee_rate: Decimal, maker_fee_rate: Decimal, fetched_at: datetime)` frozen kw-only, tz-checked like the other types.
  - `fee_schedule` table: `(category TEXT, taker_fee_rate TEXT, maker_fee_rate TEXT, fetched_at TEXT, PRIMARY KEY(category, fetched_at))`.
  - `insert_fee(conn, fee: FeeSchedule) -> bool`; `latest_fee(conn, category: str) -> FeeSchedule | None`.
  - `query_funding(conn, instrument_id, *, start, end, source_type: SourceType | None = None)` and `query_ticks(..., source_type: SourceType | None = None)` — new optional filter; `None` keeps today's behaviour.
  - `query_candles(conn, instrument_id, *, interval: str, source_type: SourceType, start: datetime, end: datetime) -> list[Candle]` ordered by `open_ts`; `start`/`end` inclusive like the other queries.
  - `query_book_spread_bps(conn, instrument_id, *, start: datetime, end: datetime) -> list[tuple[datetime, Decimal]]` — for each snapshot with at least one bid and one ask: `(exchange_ts, (best_ask − best_bid) / mid × 10_000)`.

- [ ] **Step 1: Write the failing tests**

`tests/test_storage_phase1.py`:

```python
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.exchange.types import (
    BookLevel, BookSnapshot, Candle, FeeSchedule, FundingObservation, SourceType, Tick,
)
from polyperps.storage.db import (
    connect, insert_book, insert_candle, insert_fee, insert_funding, insert_tick,
    latest_fee, query_book_spread_bps, query_candles, query_funding, query_ticks,
)

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)
H = timedelta(hours=1)


def funding(ts, st, rate="0.0001"):
    return FundingObservation(instrument_id=6, funding_rate=Decimal(rate), exchange_ts=ts,
                              received_ts=ts, source_type=st)


def candle(ts, st, interval="1h", close="100"):
    return Candle(instrument_id=6, interval=interval, open_ts=ts, open=Decimal("99"), high=Decimal("101"),
                  low=Decimal("98"), close=Decimal(close), volume=Decimal("1"), trades=1,
                  received_ts=ts, source_type=st)


def test_fee_schedule_round_trip_and_latest():
    conn = connect(":memory:")
    older = FeeSchedule(category="crypto", taker_fee_rate=Decimal("0.0005"), maker_fee_rate=Decimal("0.0002"), fetched_at=T0)
    newer = FeeSchedule(category="crypto", taker_fee_rate=Decimal("0.0006"), maker_fee_rate=Decimal("0.0002"), fetched_at=T0 + H)
    assert insert_fee(conn, older) is True
    assert insert_fee(conn, newer) is True
    assert insert_fee(conn, newer) is False
    assert latest_fee(conn, "crypto") == newer
    assert latest_fee(conn, "equity") is None


def test_query_funding_filters_by_source_type():
    conn = connect(":memory:")
    insert_funding(conn, funding(T0, SourceType.POLYMARKET_REST, "0.0001"))
    insert_funding(conn, funding(T0, SourceType.PROXY_HYPERLIQUID, "0.0009"))
    both = query_funding(conn, 6, start=T0, end=T0)
    native = query_funding(conn, 6, start=T0, end=T0, source_type=SourceType.POLYMARKET_REST)
    proxy = query_funding(conn, 6, start=T0, end=T0, source_type=SourceType.PROXY_HYPERLIQUID)
    assert len(both) == 2
    assert [f.funding_rate for f in native] == [Decimal("0.0001")]
    assert [f.funding_rate for f in proxy] == [Decimal("0.0009")]


def test_query_ticks_filters_by_source_type():
    conn = connect(":memory:")
    for st in (SourceType.POLYMARKET_WS, SourceType.POLYMARKET_REST):
        insert_tick(conn, Tick(instrument_id=6, mark_price=Decimal("100"), index_price=Decimal("100"),
                               last_price=Decimal("100"), funding_rate=Decimal("0"), next_funding=T0,
                               exchange_ts=T0, received_ts=T0, source_type=st, sequence=1))
    assert len(query_ticks(conn, 6, start=T0, end=T0)) == 2
    assert len(query_ticks(conn, 6, start=T0, end=T0, source_type=SourceType.POLYMARKET_WS)) == 1


def test_query_candles_by_interval_and_source_ordered():
    conn = connect(":memory:")
    insert_candle(conn, candle(T0 + H, SourceType.POLYMARKET_REST, close="102"))
    insert_candle(conn, candle(T0, SourceType.POLYMARKET_REST, close="101"))
    insert_candle(conn, candle(T0, SourceType.POLYMARKET_REST, interval="1m"))
    insert_candle(conn, candle(T0, SourceType.PROXY_HYPERLIQUID, close="999"))
    rows = query_candles(conn, 6, interval="1h", source_type=SourceType.POLYMARKET_REST, start=T0, end=T0 + H)
    assert [c.close for c in rows] == [Decimal("101"), Decimal("102")]


def test_query_book_spread_bps():
    conn = connect(":memory:")
    snap = BookSnapshot(instrument_id=6,
                        bids=(BookLevel(price=Decimal("99.5"), quantity=Decimal("1")),
                              BookLevel(price=Decimal("99.0"), quantity=Decimal("5"))),
                        asks=(BookLevel(price=Decimal("100.5"), quantity=Decimal("1")),),
                        exchange_ts=T0, received_ts=T0, source_type=SourceType.POLYMARKET_REST)
    empty = BookSnapshot(instrument_id=6, bids=(), asks=(), exchange_ts=T0 + H, received_ts=T0 + H,
                         source_type=SourceType.POLYMARKET_REST)
    insert_book(conn, snap)
    insert_book(conn, empty)
    rows = query_book_spread_bps(conn, 6, start=T0, end=T0 + H)
    assert rows == [(T0, Decimal("100"))]  # (100.5-99.5)/100 * 1e4 = 100 bps
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_storage_phase1.py -v`
Expected: FAIL with `ImportError: cannot import name 'FeeSchedule'`

- [ ] **Step 3: Append `FeeSchedule` to `polyperps/exchange/types.py`**

Append at the end of the file (after `Candle`):

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class FeeSchedule:
    category: str
    taker_fee_rate: Decimal
    maker_fee_rate: Decimal
    fetched_at: datetime

    def __post_init__(self) -> None:
        _require_aware(self)
```

- [ ] **Step 4: Extend `polyperps/storage/db.py`**

Add to `_SCHEMA` (before the closing `"""`):

```sql
CREATE TABLE IF NOT EXISTS fee_schedule (
    category       TEXT NOT NULL,
    taker_fee_rate TEXT NOT NULL,
    maker_fee_rate TEXT NOT NULL,
    fetched_at     TEXT NOT NULL,
    PRIMARY KEY (category, fetched_at)
);
```

Add `FeeSchedule` to the `from polyperps.exchange.types import (...)` list, and `import json` is already present. Replace `query_ticks` and `query_funding` with source-filtered versions and add the new functions:

```python
def insert_fee(conn: sqlite3.Connection, fee: FeeSchedule) -> bool:
    cur = conn.execute(
        "INSERT OR IGNORE INTO fee_schedule VALUES (?,?,?,?)",
        (fee.category, str(fee.taker_fee_rate), str(fee.maker_fee_rate), _ts(fee.fetched_at)),
    )
    conn.commit()
    return cur.rowcount == 1


def latest_fee(conn: sqlite3.Connection, category: str) -> FeeSchedule | None:
    row = conn.execute(
        "SELECT category, taker_fee_rate, maker_fee_rate, fetched_at FROM fee_schedule "
        "WHERE category=? ORDER BY fetched_at DESC LIMIT 1",
        (category,),
    ).fetchone()
    if row is None:
        return None
    return FeeSchedule(category=row[0], taker_fee_rate=Decimal(row[1]),
                       maker_fee_rate=Decimal(row[2]), fetched_at=_parse_ts(row[3]))


def _source_clause(source_type: SourceType | None) -> tuple[str, tuple[str, ...]]:
    if source_type is None:
        return "", ()
    return " AND source_type=?", (source_type.value,)


def query_ticks(
    conn: sqlite3.Connection,
    instrument_id: int,
    *,
    start: datetime,
    end: datetime,
    source_type: SourceType | None = None,
) -> list[Tick]:
    clause, extra = _source_clause(source_type)
    rows = conn.execute(
        "SELECT instrument_id, source_type, exchange_ts, received_ts, sequence, mark_price, "
        "index_price, last_price, funding_rate, next_funding FROM ticks "
        f"WHERE instrument_id=? AND exchange_ts BETWEEN ? AND ?{clause} ORDER BY exchange_ts, sequence",
        (instrument_id, _ts(start), _ts(end), *extra),
    ).fetchall()
    return [
        Tick(
            instrument_id=r[0], source_type=SourceType(r[1]), exchange_ts=_parse_ts(r[2]),
            received_ts=_parse_ts(r[3]), sequence=r[4], mark_price=Decimal(r[5]),
            index_price=Decimal(r[6]), last_price=Decimal(r[7]), funding_rate=Decimal(r[8]),
            next_funding=_parse_ts(r[9]),
        )
        for r in rows
    ]


def query_funding(
    conn: sqlite3.Connection,
    instrument_id: int,
    *,
    start: datetime,
    end: datetime,
    source_type: SourceType | None = None,
) -> list[FundingObservation]:
    clause, extra = _source_clause(source_type)
    rows = conn.execute(
        "SELECT instrument_id, source_type, exchange_ts, received_ts, funding_rate "
        f"FROM funding_rates WHERE instrument_id=? AND exchange_ts BETWEEN ? AND ?{clause} "
        "ORDER BY exchange_ts",
        (instrument_id, _ts(start), _ts(end), *extra),
    ).fetchall()
    return [
        FundingObservation(
            instrument_id=r[0], source_type=SourceType(r[1]), exchange_ts=_parse_ts(r[2]),
            received_ts=_parse_ts(r[3]), funding_rate=Decimal(r[4]),
        )
        for r in rows
    ]


def query_candles(
    conn: sqlite3.Connection,
    instrument_id: int,
    *,
    interval: str,
    source_type: SourceType,
    start: datetime,
    end: datetime,
) -> list[Candle]:
    rows = conn.execute(
        "SELECT instrument_id, interval, source_type, open_ts, received_ts, open, high, low, close, "
        "volume, trades FROM candles WHERE instrument_id=? AND interval=? AND source_type=? "
        "AND open_ts BETWEEN ? AND ? ORDER BY open_ts",
        (instrument_id, interval, source_type.value, _ts(start), _ts(end)),
    ).fetchall()
    return [
        Candle(
            instrument_id=r[0], interval=r[1], source_type=SourceType(r[2]), open_ts=_parse_ts(r[3]),
            received_ts=_parse_ts(r[4]), open=Decimal(r[5]), high=Decimal(r[6]), low=Decimal(r[7]),
            close=Decimal(r[8]), volume=Decimal(r[9]), trades=r[10],
        )
        for r in rows
    ]


def query_book_spread_bps(
    conn: sqlite3.Connection, instrument_id: int, *, start: datetime, end: datetime
) -> list[tuple[datetime, Decimal]]:
    """Top-of-book spread in basis points per stored snapshot; snapshots missing a side are skipped."""
    rows = conn.execute(
        "SELECT exchange_ts, bids_json, asks_json FROM book_snapshots "
        "WHERE instrument_id=? AND exchange_ts BETWEEN ? AND ? ORDER BY exchange_ts",
        (instrument_id, _ts(start), _ts(end)),
    ).fetchall()
    out: list[tuple[datetime, Decimal]] = []
    for ts, bids_json, asks_json in rows:
        bids = json.loads(bids_json)
        asks = json.loads(asks_json)
        if not bids or not asks:
            continue
        best_bid = max(Decimal(l["price"]) for l in bids)
        best_ask = min(Decimal(l["price"]) for l in asks)
        mid = (best_bid + best_ask) / 2
        out.append((_parse_ts(ts), (best_ask - best_bid) / mid * Decimal(10_000)))
    return out
```

- [ ] **Step 5: Run the new tests and the whole suite**

Run: `.venv/Scripts/python -m pytest tests/test_storage_phase1.py tests/test_storage.py tests/test_gaps.py -v`
Expected: 5 new passed + 13 existing passed (the existing callers pass no `source_type`, so behaviour is unchanged)

- [ ] **Step 6: Commit**

```bash
git add polyperps/exchange/types.py polyperps/storage/db.py tests/test_storage_phase1.py
git commit -m "feat(phase1): fee schedule table, source-filtered queries, candles and book-spread queries"
```

---

### Task 3: `check_dataset` + `scripts/sufficiency.py`

**Files:**
- Modify: `polyperps/signal/sufficiency.py` (append)
- Create: `scripts/sufficiency.py`
- Test: `tests/test_sufficiency.py` (append)

**Interfaces:**
- Consumes: `query_funding(conn, iid, start=, end=, source_type=)` (Task 2), `dataset_meets_bar`.
- Produces: `check_dataset(conn, instrument_id: int, source_type: SourceType, *, now: datetime, bar: SufficiencyBar = BAR) -> SufficiencyReport` — `days` = (last funding ts − first funding ts) / 1 day as `Decimal` quantised to 0.01, `funding_periods` = row count; empty dataset → `days=0`, `periods=0`.

- [ ] **Step 1: Append failing tests to `tests/test_sufficiency.py`**

```python
from datetime import datetime, timedelta, timezone

from polyperps.exchange.types import FundingObservation
from polyperps.signal.sufficiency import check_dataset
from polyperps.storage.db import connect, insert_funding

UTC = timezone.utc
T0 = datetime(2026, 9, 3, 0, 0, tzinfo=UTC)


def _load(conn, hours, st):
    for i in range(hours):
        ts = T0 + timedelta(hours=i)
        insert_funding(conn, FundingObservation(instrument_id=6, funding_rate=Decimal("0.0001"),
                                                exchange_ts=ts, received_ts=ts, source_type=st))


def test_check_dataset_reports_shortfall_on_ten_days():
    conn = connect(":memory:")
    _load(conn, 10 * 24, SourceType.POLYMARKET_REST)
    r = check_dataset(conn, 6, SourceType.POLYMARKET_REST, now=T0 + timedelta(days=10))
    assert r.met is False
    assert r.funding_periods == 240
    assert r.days == Decimal("9.96")  # (239 hours) / 24
    assert set(r.shortfall) == {"days", "funding_periods"}


def test_check_dataset_met_on_sixty_one_days_native():
    conn = connect(":memory:")
    _load(conn, 61 * 24 + 1, SourceType.POLYMARKET_REST)
    r = check_dataset(conn, 6, SourceType.POLYMARKET_REST, now=T0 + timedelta(days=62))
    assert r.met is True


def test_check_dataset_ignores_other_sources():
    conn = connect(":memory:")
    _load(conn, 61 * 24 + 1, SourceType.PROXY_HYPERLIQUID)
    r = check_dataset(conn, 6, SourceType.POLYMARKET_REST, now=T0 + timedelta(days=62))
    assert r.funding_periods == 0 and r.days == Decimal("0")


def test_check_dataset_empty():
    conn = connect(":memory:")
    r = check_dataset(conn, 6, SourceType.POLYMARKET_REST, now=T0)
    assert r.met is False and r.funding_periods == 0
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_sufficiency.py -v`
Expected: 4 FAIL with `ImportError: cannot import name 'check_dataset'`

- [ ] **Step 3: Append to `polyperps/signal/sufficiency.py`**

```python
import sqlite3
from datetime import datetime, timedelta

from polyperps.storage.db import query_funding

_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


def check_dataset(
    conn: sqlite3.Connection,
    instrument_id: int,
    source_type: SourceType,
    *,
    now: datetime,
    bar: SufficiencyBar = BAR,
) -> SufficiencyReport:
    rows = query_funding(conn, instrument_id, start=_EPOCH, end=now, source_type=source_type)
    if not rows:
        return dataset_meets_bar(days=Decimal("0"), funding_periods=0, source_type=source_type, bar=bar)
    span = rows[-1].exchange_ts - rows[0].exchange_ts
    days = (Decimal(span.total_seconds()) / Decimal(86_400)).quantize(Decimal("0.01"))
    return dataset_meets_bar(days=days, funding_periods=len(rows), source_type=source_type, bar=bar)
```

(Add `from datetime import timezone` to the imports at the top of the file.)

- [ ] **Step 4: Write `scripts/sufficiency.py`**

```python
"""Spec 1.0 re-check: does the stored dataset meet the pre-registered bar?

    POLYPERPS_INSTRUMENT_IDS=6,7 .venv/Scripts/python scripts/sufficiency.py

Native data cannot meet the Strict bar before ~2026-11-02 (perps launched
2026-09-03). Run monthly (see docs/ops/eligibility-checklist.md). No network.
"""

from __future__ import annotations

from datetime import datetime, timezone

from polyperps.config import load_settings
from polyperps.exchange.types import SourceType
from polyperps.signal.sufficiency import BAR, check_dataset
from polyperps.storage import db


def main() -> None:
    settings = load_settings()
    conn = db.connect(settings.db_path)
    now = datetime.now(timezone.utc)
    print(f"bar: native_only={BAR.native_only} min_days={BAR.min_days} "
          f"min_funding_periods={BAR.min_funding_periods}")
    try:
        for iid in settings.instrument_ids:
            for st in (SourceType.POLYMARKET_REST, SourceType.PROXY_HYPERLIQUID):
                r = check_dataset(conn, iid, st, now=now)
                status = "MET" if r.met else "not met"
                print(f"{iid} {st.value:<18} days={r.days:<8} periods={r.funding_periods:<6} {status}")
                for k, v in r.shortfall.items():
                    print(f"    {k}: {v}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run tests and the script**

Run: `.venv/Scripts/python -m pytest tests/test_sufficiency.py -v`
Expected: 11 passed

Run: `POLYPERPS_INSTRUMENT_IDS=6,7 .venv/Scripts/python scripts/sufficiency.py`
Expected: four lines, all `not met`, native ones showing the current day count (~8) and `funding_periods` (~190).

- [ ] **Step 6: Commit**

```bash
git add polyperps/signal/sufficiency.py scripts/sufficiency.py tests/test_sufficiency.py
git commit -m "feat(phase1): check_dataset against the bar and the sufficiency re-check script"
```

---

### Task 4: Fee schedule from the exchange

**Files:**
- Modify: `polyperps/exchange/client.py` (converter + method)
- Create: `scripts/store_fees.py`
- Test: `tests/test_fees.py`

**Interfaces:**
- Consumes: SDK `AsyncPublicClient.fetch_perps_fees() -> tuple[PerpsFeeScheduleEntry, ...]` with fields `category`, `taker_fee_rate`, `maker_fee_rate`, `tiers` (verified in Phase 0 against py-sdk `models/perps/market.py`); `FeeSchedule`, `insert_fee`.
- Produces: `fee_from_rest(entry, fetched_at: datetime) -> FeeSchedule`; `PolymarketPerpsClient.fetch_fees() -> tuple[FeeSchedule, ...]` (goes through the limiter). `ExchangeClient` Protocol gains `async def fetch_fees(self) -> tuple[FeeSchedule, ...]`.

- [ ] **Step 1: Write the failing tests**

`tests/test_fees.py`:

```python
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from polyperps.exchange.client import PolymarketPerpsClient, fee_from_rest
from polyperps.exchange.rate_limiter import TokenBucket
from polyperps.exchange.types import FeeSchedule

T0 = datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc)


def test_fee_from_rest():
    entry = SimpleNamespace(category="crypto", taker_fee_rate=Decimal("0.0005"),
                            maker_fee_rate=Decimal("0.0002"), tiers=())
    assert fee_from_rest(entry, T0) == FeeSchedule(category="crypto", taker_fee_rate=Decimal("0.0005"),
                                                  maker_fee_rate=Decimal("0.0002"), fetched_at=T0)


class CountingBucket(TokenBucket):
    def __init__(self):
        super().__init__(rate_per_sec=1000, burst=1000)
        self.acquired = 0

    async def acquire(self):
        self.acquired += 1
        return 0.0


class FakeSdk:
    async def fetch_perps_fees(self):
        return (SimpleNamespace(category="crypto", taker_fee_rate=Decimal("0.0005"),
                                maker_fee_rate=Decimal("0.0002"), tiers=()),
                SimpleNamespace(category="equity", taker_fee_rate=Decimal("0.001"),
                                maker_fee_rate=Decimal("0.0005"), tiers=()))


async def test_fetch_fees_goes_through_limiter():
    bucket = CountingBucket()
    c = PolymarketPerpsClient(FakeSdk(), limiter=bucket, clock=lambda: T0)
    fees = await c.fetch_fees()
    assert [f.category for f in fees] == ["crypto", "equity"]
    assert all(f.fetched_at == T0 for f in fees)
    assert bucket.acquired == 1
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_fees.py -v`
Expected: FAIL with `ImportError: cannot import name 'fee_from_rest'`

- [ ] **Step 3: Extend `polyperps/exchange/client.py`**

Add `FeeSchedule` to the `from polyperps.exchange.types import (...)` list. After `candle_from_rest` add:

```python
def fee_from_rest(entry: Any, fetched_at: datetime) -> FeeSchedule:
    return FeeSchedule(
        category=str(entry.category),
        taker_fee_rate=entry.taker_fee_rate,
        maker_fee_rate=entry.maker_fee_rate,
        fetched_at=fetched_at,
    )
```

In the `ExchangeClient` Protocol add `async def fetch_fees(self) -> tuple[FeeSchedule, ...]: ...`. In `PolymarketPerpsClient` after `fetch_candles` add:

```python
    async def fetch_fees(self) -> tuple[FeeSchedule, ...]:
        await self._limiter.acquire()
        raw = await self._sdk.fetch_perps_fees()
        now = self._clock()
        return tuple(fee_from_rest(e, now) for e in raw)
```

Add `fetch_perps_fees` to the docstring's list of SDK methods relied on.

- [ ] **Step 4: Write `scripts/store_fees.py`**

```python
"""Store the exchange's current fee schedule so backtests are reproducible
and spec 3.3's "did fees change?" re-check has a baseline.

    .venv/Scripts/python scripts/store_fees.py

No credentials. One REST call.
"""

from __future__ import annotations

import asyncio

from polyperps.config import load_settings
from polyperps.exchange.client import PolymarketPerpsClient
from polyperps.storage import db


async def main() -> None:
    settings = load_settings()
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = db.connect(settings.db_path)
    client = PolymarketPerpsClient.create_public(
        rate_per_sec=settings.rest_rate_per_sec, burst=settings.rest_burst
    )
    try:
        for fee in await client.fetch_fees():
            written = db.insert_fee(conn, fee)
            print(f"{fee.category:<10} taker={fee.taker_fee_rate} maker={fee.maker_fee_rate} "
                  f"fetched_at={fee.fetched_at.isoformat()} {'stored' if written else 'duplicate'}")
    finally:
        await client.close()
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 5: Run tests, then the script once live**

Run: `.venv/Scripts/python -m pytest tests/test_fees.py tests/test_client.py -v`
Expected: 2 new + 12 existing passed

Run: `POLYPERPS_INSTRUMENT_IDS=6 .venv/Scripts/python scripts/store_fees.py`
Expected: at least a `crypto` line with a non-zero taker rate, `stored`. Record the taker rate in the report — it is the number every backtest will use.

- [ ] **Step 6: Commit**

```bash
git add polyperps/exchange/client.py scripts/store_fees.py tests/test_fees.py
git commit -m "feat(phase1): fetch and store the perps fee schedule"
```

---

### Task 5: Hyperliquid proxy ingest (spec §4.1)

**Files:**
- Modify: `pyproject.toml` (add `"httpx>=0.27,<1"`)
- Create: `polyperps/data_ingest/hyperliquid.py`, `scripts/backfill_hyperliquid.py`
- Test: `tests/test_hyperliquid.py`

**Interfaces:**
- Consumes: `FundingObservation`, `Candle`, `SourceType.PROXY_HYPERLIQUID`, `TokenBucket`, `insert_funding`, `insert_candle`.
- Produces:
  - `class TransientProxyError(RuntimeError)` — raised for HTTP 429/5xx and transport errors; carries `retry_after: float | None`.
  - `parse_funding_history(items: list[dict], *, instrument_id: int, received_ts: datetime) -> list[FundingObservation]` — item shape **UNVERIFIED**: `{"coin": "BTC", "fundingRate": "0.0000125", "premium": "...", "time": 1757548800000}` (`time` = epoch ms).
  - `parse_candles(items: list[dict], *, instrument_id: int, interval: str, received_ts: datetime) -> list[Candle]` — item shape **UNVERIFIED**: `{"t": 1757548800000, "T": ..., "s": "BTC", "i": "1h", "o": "..", "c": "..", "h": "..", "l": "..", "v": "..", "n": 12}`.
  - `class HyperliquidClient` with `__init__(self, *, limiter: TokenBucket, base_url: str = "https://api.hyperliquid.xyz", transport: httpx.AsyncBaseTransport | None = None, clock=_utcnow)`, `async funding_history(coin: str, *, start: datetime, end: datetime, instrument_id: int) -> list[FundingObservation]`, `async candles(coin: str, *, interval: str, start: datetime, end: datetime, instrument_id: int) -> list[Candle]`, `async close()`.
  - Request bodies (**UNVERIFIED**): `{"type": "fundingHistory", "coin": coin, "startTime": ms, "endTime": ms}` and `{"type": "candleSnapshot", "req": {"coin": coin, "interval": interval, "startTime": ms, "endTime": ms}}`, POSTed to `{base_url}/info`.

- [ ] **Step 1: Write the failing tests**

`tests/test_hyperliquid.py`:

```python
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import httpx
import pytest

from polyperps.data_ingest.hyperliquid import (
    HyperliquidClient, TransientProxyError, parse_candles, parse_funding_history,
)
from polyperps.exchange.rate_limiter import TokenBucket
from polyperps.exchange.types import SourceType

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 0, 0, tzinfo=UTC)
T0_MS = int(T0.timestamp() * 1000)
RX = T0 + timedelta(hours=5)

FUNDING_ITEMS = [
    {"coin": "BTC", "fundingRate": "0.0000125", "premium": "0.0001", "time": T0_MS},
    {"coin": "BTC", "fundingRate": "-0.00002", "premium": "-0.0001", "time": T0_MS + 3_600_000},
]
CANDLE_ITEMS = [
    {"t": T0_MS, "T": T0_MS + 3_599_999, "s": "BTC", "i": "1h", "o": "100.0", "c": "101.5",
     "h": "102", "l": "99", "v": "12.5", "n": 40},
]


def test_parse_funding_history_tags_proxy_and_utc():
    out = parse_funding_history(FUNDING_ITEMS, instrument_id=6, received_ts=RX)
    assert [f.funding_rate for f in out] == [Decimal("0.0000125"), Decimal("-0.00002")]
    assert out[0].exchange_ts == T0 and out[1].exchange_ts == T0 + timedelta(hours=1)
    assert all(f.source_type is SourceType.PROXY_HYPERLIQUID and f.instrument_id == 6 for f in out)
    assert out[0].received_ts == RX


def test_parse_candles():
    (c,) = parse_candles(CANDLE_ITEMS, instrument_id=6, interval="1h", received_ts=RX)
    assert c.open_ts == T0 and c.close == Decimal("101.5") and c.trades == 40
    assert c.interval == "1h" and c.source_type is SourceType.PROXY_HYPERLIQUID


def test_parse_rejects_unexpected_shape():
    with pytest.raises(KeyError):
        parse_funding_history([{"coin": "BTC", "rate": "0.1"}], instrument_id=6, received_ts=RX)


class CountingBucket(TokenBucket):
    def __init__(self):
        super().__init__(rate_per_sec=1000, burst=1000)
        self.acquired = 0

    async def acquire(self):
        self.acquired += 1
        return 0.0


def make_client(handler, bucket=None):
    transport = httpx.MockTransport(handler)
    return HyperliquidClient(limiter=bucket or CountingBucket(), transport=transport, clock=lambda: RX)


async def test_funding_history_posts_expected_body_and_parses():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json=FUNDING_ITEMS)

    bucket = CountingBucket()
    c = make_client(handler, bucket)
    out = await c.funding_history("BTC", start=T0, end=T0 + timedelta(hours=2), instrument_id=6)
    await c.close()
    assert seen["url"] == "https://api.hyperliquid.xyz/info"
    assert seen["body"] == {"type": "fundingHistory", "coin": "BTC", "startTime": T0_MS,
                            "endTime": T0_MS + 7_200_000}
    assert len(out) == 2 and bucket.acquired == 1


async def test_candles_posts_expected_body():
    seen = {}

    def handler(request):
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json=CANDLE_ITEMS)

    c = make_client(handler)
    out = await c.candles("BTC", interval="1h", start=T0, end=T0 + timedelta(hours=1), instrument_id=6)
    await c.close()
    assert seen["body"] == {"type": "candleSnapshot", "req": {"coin": "BTC", "interval": "1h",
                                                               "startTime": T0_MS, "endTime": T0_MS + 3_600_000}}
    assert len(out) == 1


async def test_429_raises_transient_with_retry_after():
    def handler(request):
        return httpx.Response(429, headers={"Retry-After": "7"}, json={"error": "rate limited"})

    c = make_client(handler)
    with pytest.raises(TransientProxyError) as exc:
        await c.funding_history("BTC", start=T0, end=T0 + timedelta(hours=1), instrument_id=6)
    await c.close()
    assert exc.value.retry_after == 7.0


async def test_400_is_not_transient():
    def handler(request):
        return httpx.Response(400, json={"error": "bad coin"})

    c = make_client(handler)
    with pytest.raises(httpx.HTTPStatusError):
        await c.funding_history("XXX", start=T0, end=T0 + timedelta(hours=1), instrument_id=6)
    await c.close()
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_hyperliquid.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.data_ingest.hyperliquid'`

- [ ] **Step 3: Add `httpx` to `pyproject.toml`**

In `[project] dependencies` add the line `"httpx>=0.27,<1",` after `"keyring>=25",`. Run `.venv/Scripts/python -m pip install -e ".[dev]"` (no new download expected).

- [ ] **Step 4: Write `polyperps/data_ingest/hyperliquid.py`**

```python
"""Hyperliquid public market-data client (proxy source, spec 1.1).

Proxy data is for SCREENING hypotheses only. Rows are tagged
SourceType.PROXY_HYPERLIQUID and stored under the Polymarket instrument id of
the same asset; polyperps.signal.sufficiency refuses to let a proxy dataset
meet the bar.

UNVERIFIED (written from memory, confirmed by one live call in Task 5 Step 7):
  POST {base_url}/info
    {"type": "fundingHistory", "coin": "BTC", "startTime": ms, "endTime": ms}
      -> [{"coin","fundingRate","premium","time"}]
    {"type": "candleSnapshot", "req": {"coin","interval","startTime","endTime"}}
      -> [{"t","T","s","i","o","c","h","l","v","n"}]
If the live shapes differ, fix the parsers and the fixtures in
tests/test_hyperliquid.py together; do not special-case in callers.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal

import httpx

from polyperps.exchange.rate_limiter import TokenBucket
from polyperps.exchange.types import Candle, FundingObservation, SourceType

DEFAULT_BASE_URL = "https://api.hyperliquid.xyz"
_TIMEOUT_S = 30.0


class TransientProxyError(RuntimeError):
    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def parse_funding_history(
    items: list[dict], *, instrument_id: int, received_ts: datetime
) -> list[FundingObservation]:
    return [
        FundingObservation(
            instrument_id=instrument_id,
            funding_rate=Decimal(str(item["fundingRate"])),
            exchange_ts=_from_ms(int(item["time"])),
            received_ts=received_ts,
            source_type=SourceType.PROXY_HYPERLIQUID,
        )
        for item in items
    ]


def parse_candles(
    items: list[dict], *, instrument_id: int, interval: str, received_ts: datetime
) -> list[Candle]:
    return [
        Candle(
            instrument_id=instrument_id,
            interval=interval,
            open_ts=_from_ms(int(item["t"])),
            open=Decimal(str(item["o"])),
            high=Decimal(str(item["h"])),
            low=Decimal(str(item["l"])),
            close=Decimal(str(item["c"])),
            volume=Decimal(str(item["v"])),
            trades=int(item["n"]),
            received_ts=received_ts,
            source_type=SourceType.PROXY_HYPERLIQUID,
        )
        for item in items
    ]


class HyperliquidClient:
    def __init__(
        self,
        *,
        limiter: TokenBucket,
        base_url: str = DEFAULT_BASE_URL,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._limiter = limiter
        self._clock = clock
        self._http = httpx.AsyncClient(base_url=base_url, timeout=_TIMEOUT_S, transport=transport)

    async def _info(self, body: dict) -> list[dict]:
        await self._limiter.acquire()
        try:
            resp = await self._http.post("/info", json=body)
        except httpx.TransportError as exc:
            raise TransientProxyError(f"transport error: {type(exc).__name__}") from exc
        if resp.status_code == 429 or resp.status_code >= 500:
            ra = resp.headers.get("Retry-After")
            raise TransientProxyError(
                f"HTTP {resp.status_code}", retry_after=float(ra) if ra else None
            )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            raise ValueError(f"expected a JSON list from /info, got {type(data).__name__}")
        return data

    async def funding_history(
        self, coin: str, *, start: datetime, end: datetime, instrument_id: int
    ) -> list[FundingObservation]:
        items = await self._info(
            {"type": "fundingHistory", "coin": coin, "startTime": _ms(start), "endTime": _ms(end)}
        )
        return parse_funding_history(items, instrument_id=instrument_id, received_ts=self._clock())

    async def candles(
        self, coin: str, *, interval: str, start: datetime, end: datetime, instrument_id: int
    ) -> list[Candle]:
        items = await self._info(
            {"type": "candleSnapshot",
             "req": {"coin": coin, "interval": interval, "startTime": _ms(start), "endTime": _ms(end)}}
        )
        return parse_candles(items, instrument_id=instrument_id, interval=interval, received_ts=self._clock())

    async def close(self) -> None:
        await self._http.aclose()
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_hyperliquid.py -v`
Expected: 7 passed

- [ ] **Step 6: Write `scripts/backfill_hyperliquid.py`**

```python
"""Spec 1.1: backfill Hyperliquid funding + candles as PROXY_HYPERLIQUID.

    .venv/Scripts/python scripts/backfill_hyperliquid.py --days 400 --map 6=BTC,7=ETH

Pulls 1h funding, 1h candles and 1m candles in 24h windows. Transient errors
(429/5xx/transport) are retried honouring Retry-After; anything else aborts.
Rows land in the same tables as native data, distinguished by source_type.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone

from polyperps.config import load_settings
from polyperps.data_ingest.hyperliquid import HyperliquidClient, TransientProxyError
from polyperps.exchange.rate_limiter import TokenBucket
from polyperps.storage import db

_MAX_ATTEMPTS = 3
_RETRY_SLEEP_S = 60


def parse_map(text: str) -> dict[int, str]:
    out: dict[int, str] = {}
    for part in text.split(","):
        iid, coin = part.strip().split("=")
        out[int(iid)] = coin.strip().upper()
    return out


async def with_retries(coro_factory, label: str):
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            return await coro_factory()
        except TransientProxyError as exc:
            if attempt == _MAX_ATTEMPTS:
                print(f"  {label}: giving up after {_MAX_ATTEMPTS} attempts ({exc})")
                raise
            wait = exc.retry_after or _RETRY_SLEEP_S
            print(f"  {label}: {exc}; retry {attempt}/{_MAX_ATTEMPTS} in {wait:.0f}s")
            await asyncio.sleep(wait)
    raise AssertionError("unreachable")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, required=True)
    ap.add_argument("--map", required=True, help="instrument_id=COIN pairs, e.g. 6=BTC,7=ETH")
    ap.add_argument("--no-minutes", action="store_true", help="skip 1m candles (faster)")
    args = ap.parse_args()

    settings = load_settings()
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = db.connect(settings.db_path)
    client = HyperliquidClient(
        limiter=TokenBucket(rate_per_sec=settings.rest_rate_per_sec, burst=settings.rest_burst)
    )
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=args.days)
    try:
        for iid, coin in parse_map(args.map).items():
            n_f = n_h = n_m = 0
            failed: list[tuple[datetime, datetime]] = []
            w_start = start
            while w_start < end:
                w_end = min(w_start + timedelta(days=1), end)
                label = f"{coin} {w_start:%Y-%m-%d}"
                try:
                    for f in await with_retries(
                        lambda: client.funding_history(coin, start=w_start, end=w_end, instrument_id=iid), label
                    ):
                        n_f += db.insert_funding(conn, f)
                    for c in await with_retries(
                        lambda: client.candles(coin, interval="1h", start=w_start, end=w_end, instrument_id=iid), label
                    ):
                        n_h += db.insert_candle(conn, c)
                    if not args.no_minutes:
                        for c in await with_retries(
                            lambda: client.candles(coin, interval="1m", start=w_start, end=w_end, instrument_id=iid), label
                        ):
                            n_m += db.insert_candle(conn, c)
                except TransientProxyError:
                    failed.append((w_start, w_end))
                w_start = w_end
            print(f"{iid} {coin}: +{n_f} funding, +{n_h} 1h candles, +{n_m} 1m candles")
            if failed:
                print(f"  windows NOT backfilled ({len(failed)}):")
                for a, b in failed:
                    print(f"    {a.isoformat()} -> {b.isoformat()}")
    finally:
        await client.close()
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 7: Verify the UNVERIFIED shapes with one live call, then a short backfill**

Run: `POLYPERPS_INSTRUMENT_IDS=6 .venv/Scripts/python scripts/backfill_hyperliquid.py --days 2 --map 6=BTC --no-minutes`
Expected: `6 BTC: +48 funding, +48 1h candles, +0 1m candles` (±1 at window edges). If the request is rejected (HTTP 4xx) or parsing raises `KeyError`, the shapes differ from memory: capture the real response (`print(resp.text[:500])` temporarily), fix the parsers **and** the fixtures in `tests/test_hyperliquid.py` to the real shape, re-run tests, and note the correction in the report. Do not proceed past this step with a guessed shape.

Then, if the shapes held: `POLYPERPS_INSTRUMENT_IDS=6,7 .venv/Scripts/python scripts/backfill_hyperliquid.py --days 400 --map 6=BTC,7=ETH` (this is ~1,200 windows × 3 calls at 2 req/s ≈ 30 minutes; run it in the background and record the final counts). 1m candles for 400 days is ~576k rows per coin — acceptable in SQLite; if the API caps `candleSnapshot` rows per call (a known possibility), the 1m pull will return partial windows: detect by `+1m candles` being far below `days × 1440` and report it; do not silently accept.

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml polyperps/data_ingest/hyperliquid.py scripts/backfill_hyperliquid.py tests/test_hyperliquid.py
git commit -m "feat(phase1): Hyperliquid proxy ingest and backfill script"
```

---

### Task 6: Hourly bars (spec §4.2)

**Files:**
- Create: `polyperps/backtest/__init__.py` (empty), `polyperps/backtest/bars.py`
- Test: `tests/test_bars.py`

**Interfaces:**
- Consumes: `query_candles`, `query_funding`, `query_ticks`, `query_book_spread_bps` (Task 2), `NATIVE_SOURCES`, `BAR.proxy_spread_bps`.
- Produces:
  - `Bar` frozen kw-only: `instrument_id: int`, `source_type: SourceType`, `open_ts: datetime`, `open: Decimal | None`, `high: Decimal | None`, `low: Decimal | None`, `close: Decimal | None`, `index_close: Decimal | None`, `funding_rate: Decimal | None`, `spread_bps: Decimal`, `complete: bool`. `complete` is `True` iff the 1h candle and the funding rate for `open_ts + 1h` are both present.
  - `floor_hour(dt) -> datetime`, `floor_minute(dt) -> datetime`, `is_native(source_type) -> bool`.
  - `build_bars(conn, instrument_id, source_type, *, start, end, proxy_spread_bps: Decimal = BAR.proxy_spread_bps) -> list[Bar]` — one bar per hour for `floor_hour(start) <= open_ts < end`. Funding for a bar is the row whose `floor_hour(exchange_ts) == open_ts + 1h`. `index_close` = `index_price` of the last native tick with `open_ts <= exchange_ts < open_ts+1h` (any native tick source); `None` on proxy. `spread_bps` = median of `query_book_spread_bps` in the hour for native (falls back to `proxy_spread_bps` when there are no snapshots that hour, and marks nothing — spread is an assumption either way); constant `proxy_spread_bps` on proxy.
  - `align_pair(native: list[Bar], proxy: list[Bar]) -> list[tuple[Bar, Bar]]` — pairs by `open_ts`, both `complete`.
  - `load_minute_closes(conn, instrument_id, source_type, *, start, end) -> dict[datetime, Decimal]` — `{open_ts: close}` from 1m candles.

- [ ] **Step 1: Write the failing tests**

`tests/test_bars.py`:

```python
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.backtest.bars import (
    Bar, align_pair, build_bars, floor_hour, floor_minute, is_native, load_minute_closes,
)
from polyperps.exchange.types import (
    BookLevel, BookSnapshot, Candle, FundingObservation, SourceType, Tick,
)
from polyperps.storage.db import connect, insert_book, insert_candle, insert_funding, insert_tick

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)
H = timedelta(hours=1)
NATIVE = SourceType.POLYMARKET_REST
PROXY = SourceType.PROXY_HYPERLIQUID


def candle(ts, st, interval="1h", close="100"):
    return Candle(instrument_id=6, interval=interval, open_ts=ts, open=Decimal("99"), high=Decimal("101"),
                  low=Decimal("98"), close=Decimal(close), volume=Decimal("1"), trades=1,
                  received_ts=ts, source_type=st)


def funding(ts, st, rate="0.0001"):
    return FundingObservation(instrument_id=6, funding_rate=Decimal(rate), exchange_ts=ts,
                              received_ts=ts, source_type=st)


def tick(ts, index="100.5"):
    return Tick(instrument_id=6, mark_price=Decimal("100"), index_price=Decimal(index),
                last_price=Decimal("100"), funding_rate=Decimal("0"), next_funding=ts,
                exchange_ts=ts, received_ts=ts, source_type=SourceType.POLYMARKET_WS, sequence=int(ts.timestamp()))


def book(ts, bid="99.5", ask="100.5"):
    return BookSnapshot(instrument_id=6, bids=(BookLevel(price=Decimal(bid), quantity=Decimal(1)),),
                        asks=(BookLevel(price=Decimal(ask), quantity=Decimal(1)),),
                        exchange_ts=ts, received_ts=ts, source_type=NATIVE)


def test_floor_helpers_and_is_native():
    t = datetime(2026, 9, 11, 12, 34, 56, 789, tzinfo=UTC)
    assert floor_hour(t) == datetime(2026, 9, 11, 12, 0, tzinfo=UTC)
    assert floor_minute(t) == datetime(2026, 9, 11, 12, 34, tzinfo=UTC)
    assert is_native(SourceType.POLYMARKET_WS) and is_native(SourceType.POLYMARKET_REST)
    assert not is_native(PROXY)


def test_build_bars_native_complete_bar():
    conn = connect(":memory:")
    insert_candle(conn, candle(T0, NATIVE, close="100"))
    insert_funding(conn, funding(T0 + H, NATIVE, "0.0002"))       # settled at end of the bar
    insert_tick(conn, tick(T0 + timedelta(minutes=10), index="100.1"))
    insert_tick(conn, tick(T0 + timedelta(minutes=50), index="100.9"))  # last in hour wins
    insert_book(conn, book(T0 + timedelta(minutes=5)))               # 100 bps
    insert_book(conn, book(T0 + timedelta(minutes=35), bid="99.9", ask="100.1"))  # 20 bps
    bars = build_bars(conn, 6, NATIVE, start=T0, end=T0 + H)
    assert len(bars) == 1
    b = bars[0]
    assert b.complete is True and b.close == Decimal("100") and b.funding_rate == Decimal("0.0002")
    assert b.index_close == Decimal("100.9")
    assert b.spread_bps == Decimal("60")  # median of 100 and 20
    assert b.source_type is NATIVE


def test_build_bars_marks_incomplete_when_candle_or_funding_missing():
    conn = connect(":memory:")
    insert_candle(conn, candle(T0, NATIVE))                     # bar 0: candle, no funding
    insert_funding(conn, funding(T0 + 2 * H, NATIVE))           # bar 1: funding, no candle
    bars = build_bars(conn, 6, NATIVE, start=T0, end=T0 + 2 * H)
    assert [b.complete for b in bars] == [False, False]
    assert bars[0].close == Decimal("100") and bars[0].funding_rate is None
    assert bars[1].close is None and bars[1].funding_rate == Decimal("0.0001")


def test_build_bars_proxy_uses_constant_spread_and_no_index():
    conn = connect(":memory:")
    insert_candle(conn, candle(T0, PROXY))
    insert_funding(conn, funding(T0 + H, PROXY))
    insert_tick(conn, tick(T0 + timedelta(minutes=10)))  # native tick must be ignored for proxy bars
    (b,) = build_bars(conn, 6, PROXY, start=T0, end=T0 + H, proxy_spread_bps=Decimal("7"))
    assert b.complete and b.index_close is None and b.spread_bps == Decimal("7")


def test_build_bars_range_is_hour_aligned_and_end_exclusive():
    conn = connect(":memory:")
    for i in range(3):
        insert_candle(conn, candle(T0 + i * H, NATIVE))
        insert_funding(conn, funding(T0 + (i + 1) * H, NATIVE))
    bars = build_bars(conn, 6, NATIVE, start=T0 + timedelta(minutes=20), end=T0 + 2 * H)
    assert [b.open_ts for b in bars] == [T0, T0 + H]


def test_align_pair_keeps_only_hours_complete_in_both():
    mk = lambda ts, st, complete: Bar(instrument_id=6, source_type=st, open_ts=ts, open=Decimal(1), high=Decimal(1),
                                      low=Decimal(1), close=Decimal(1), index_close=None, funding_rate=Decimal(0),
                                      spread_bps=Decimal(5), complete=complete)
    native = [mk(T0, NATIVE, True), mk(T0 + H, NATIVE, False), mk(T0 + 2 * H, NATIVE, True)]
    proxy = [mk(T0, PROXY, True), mk(T0 + H, PROXY, True), mk(T0 + 3 * H, PROXY, True)]
    pairs = align_pair(native, proxy)
    assert [(a.open_ts, b.open_ts) for a, b in pairs] == [(T0, T0)]


def test_load_minute_closes():
    conn = connect(":memory:")
    insert_candle(conn, candle(T0, NATIVE, interval="1m", close="100.1"))
    insert_candle(conn, candle(T0 + timedelta(minutes=1), NATIVE, interval="1m", close="100.2"))
    insert_candle(conn, candle(T0, NATIVE, interval="1h", close="999"))
    closes = load_minute_closes(conn, 6, NATIVE, start=T0, end=T0 + H)
    assert closes == {T0: Decimal("100.1"), T0 + timedelta(minutes=1): Decimal("100.2")}
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_bars.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.backtest'`

- [ ] **Step 3: Write `polyperps/backtest/__init__.py`** (empty) and `polyperps/backtest/bars.py`

```python
"""Hourly funding-period bars built from the Phase 0 tables.

A Bar never fabricates: a missing candle or funding row leaves the field None
and sets complete=False. The harness refuses to hold a position across an
incomplete bar, so gaps cannot be traded through silently.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from statistics import median

from polyperps.exchange.types import SourceType
from polyperps.signal.sufficiency import BAR, NATIVE_SOURCES
from polyperps.storage.db import query_book_spread_bps, query_candles, query_funding, query_ticks

HOUR = timedelta(hours=1)


@dataclass(frozen=True, slots=True, kw_only=True)
class Bar:
    instrument_id: int
    source_type: SourceType
    open_ts: datetime
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    index_close: Decimal | None
    funding_rate: Decimal | None
    spread_bps: Decimal
    complete: bool


def floor_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def floor_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def is_native(source_type: SourceType) -> bool:
    return source_type in NATIVE_SOURCES


def build_bars(
    conn: sqlite3.Connection,
    instrument_id: int,
    source_type: SourceType,
    *,
    start: datetime,
    end: datetime,
    proxy_spread_bps: Decimal = BAR.proxy_spread_bps,
) -> list[Bar]:
    first = floor_hour(start)
    if first >= end:
        return []
    last_open = floor_hour(end - timedelta(microseconds=1))
    native = is_native(source_type)

    candles = {
        c.open_ts: c
        for c in query_candles(conn, instrument_id, interval="1h", source_type=source_type,
                               start=first, end=last_open)
    }
    funding = {
        floor_hour(f.exchange_ts): f.funding_rate
        for f in query_funding(conn, instrument_id, start=first + HOUR, end=last_open + HOUR,
                               source_type=source_type)
    }
    index_by_hour: dict[datetime, Decimal] = {}
    spreads_by_hour: dict[datetime, list[Decimal]] = {}
    if native:
        for t in query_ticks(conn, instrument_id, start=first, end=last_open + HOUR - timedelta(microseconds=1)):
            if is_native(t.source_type):
                index_by_hour[floor_hour(t.exchange_ts)] = t.index_price  # ordered by ts: last wins
        for ts, bps in query_book_spread_bps(conn, instrument_id, start=first,
                                             end=last_open + HOUR - timedelta(microseconds=1)):
            spreads_by_hour.setdefault(floor_hour(ts), []).append(bps)

    bars: list[Bar] = []
    open_ts = first
    while open_ts <= last_open:
        c = candles.get(open_ts)
        rate = funding.get(open_ts + HOUR)
        if native:
            spreads = spreads_by_hour.get(open_ts)
            spread = median(spreads) if spreads else proxy_spread_bps
        else:
            spread = proxy_spread_bps
        bars.append(
            Bar(
                instrument_id=instrument_id,
                source_type=source_type,
                open_ts=open_ts,
                open=c.open if c else None,
                high=c.high if c else None,
                low=c.low if c else None,
                close=c.close if c else None,
                index_close=index_by_hour.get(open_ts) if native else None,
                funding_rate=rate,
                spread_bps=Decimal(spread),
                complete=c is not None and rate is not None,
            )
        )
        open_ts += HOUR
    return bars


def align_pair(native: list[Bar], proxy: list[Bar]) -> list[tuple[Bar, Bar]]:
    by_ts = {b.open_ts: b for b in proxy if b.complete}
    return [(n, by_ts[n.open_ts]) for n in native if n.complete and n.open_ts in by_ts]


def load_minute_closes(
    conn: sqlite3.Connection,
    instrument_id: int,
    source_type: SourceType,
    *,
    start: datetime,
    end: datetime,
) -> dict[datetime, Decimal]:
    return {
        c.open_ts: c.close
        for c in query_candles(conn, instrument_id, interval="1m", source_type=source_type,
                               start=start, end=end)
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_bars.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add polyperps/backtest tests/test_bars.py
git commit -m "feat(phase1): hourly bars with gap marking, index close, spread, and pair alignment"
```

---

### Task 7: Cost model and statistics (spec §5.3, §5.4)

**Files:**
- Create: `polyperps/backtest/costs.py`, `polyperps/backtest/stats.py`
- Test: `tests/test_costs.py`, `tests/test_stats.py`

**Interfaces:**
- Produces:
  - `fill_cost(*, notional_delta: Decimal, notional: Decimal, spread_bps: Decimal, taker_fee_rate: Decimal, impact_bps: Decimal) -> Decimal` = `notional_delta × (taker_fee_rate + spread_bps/20_000 + impact_bps/10_000 × notional_delta/notional)`.
  - `sharpe(returns: Sequence[Decimal], *, periods_per_year: int = 24 * 365) -> float` — mean/stdev × sqrt(periods); `0.0` if fewer than 2 returns or stdev is 0.
  - `max_drawdown(equity: Sequence[Decimal]) -> Decimal` — largest peak-to-trough drop, ≥ 0.
  - `hit_rate(trade_pnls: Sequence[Decimal]) -> float` — fraction > 0; `0.0` if empty.
  - `turnover(fill_notionals: Sequence[Decimal], *, notional: Decimal) -> Decimal` — sum / notional.
  - `block_bootstrap_ci(returns: Sequence[Decimal], *, block_len: int, resamples: int, ci: float, seed: int) -> tuple[float, float]` — circular block bootstrap of the mean; raises `ValueError` if `len(returns) < 2 * block_len`.
  - `chronological_split(items: Sequence[T], *, holdout_fraction: Decimal) -> tuple[list[T], list[T]]` — holdout = last `ceil(n × fraction)` items.

- [ ] **Step 1: Write the failing tests**

`tests/test_costs.py`:

```python
from decimal import Decimal

from polyperps.backtest.costs import fill_cost


def test_fill_cost_hand_computed():
    # delta 50 of notional 100: fee 0.0005*50 = 0.025; half-spread 100bps/2 -> 0.005*50 = 0.25;
    # impact 5bps * (50/100) = 2.5bps -> 0.00025*50 = 0.0125; total 0.2875
    c = fill_cost(notional_delta=Decimal("50"), notional=Decimal("100"), spread_bps=Decimal("100"),
                  taker_fee_rate=Decimal("0.0005"), impact_bps=Decimal("5"))
    assert c == Decimal("0.2875")


def test_fill_cost_zero_delta_is_free():
    assert fill_cost(notional_delta=Decimal("0"), notional=Decimal("100"), spread_bps=Decimal("100"),
                     taker_fee_rate=Decimal("0.0005"), impact_bps=Decimal("5")) == 0


def test_fill_cost_full_turnover_pays_full_impact():
    c = fill_cost(notional_delta=Decimal("100"), notional=Decimal("100"), spread_bps=Decimal("0"),
                  taker_fee_rate=Decimal("0"), impact_bps=Decimal("5"))
    assert c == Decimal("0.05")
```

`tests/test_stats.py`:

```python
import random
from decimal import Decimal

import pytest

from polyperps.backtest.stats import (
    block_bootstrap_ci, chronological_split, hit_rate, max_drawdown, sharpe, turnover,
)


def test_sharpe_constant_positive_returns_is_zero_stdev_guard():
    assert sharpe([Decimal("0.001")] * 10) == 0.0


def test_sharpe_alternating_returns():
    r = [Decimal("0.01"), Decimal("-0.005")] * 50
    s = sharpe(r)
    assert 20 < s < 40  # mean .0025, stdev ~.0075 -> .33 * sqrt(8760) ~ 31


def test_max_drawdown():
    eq = [Decimal(x) for x in (100, 110, 105, 120, 90, 95)]
    assert max_drawdown(eq) == Decimal("30")
    assert max_drawdown([Decimal(1), Decimal(2), Decimal(3)]) == Decimal("0")


def test_hit_rate_and_turnover():
    assert hit_rate([Decimal(1), Decimal(-1), Decimal(2), Decimal(0)]) == 0.5
    assert hit_rate([]) == 0.0
    assert turnover([Decimal(50), Decimal(100)], notional=Decimal(100)) == Decimal("1.5")


def test_bootstrap_iid_noise_contains_zero():
    rng = random.Random(1)
    r = [Decimal(str(round(rng.gauss(0, 0.01), 6))) for _ in range(500)]
    lo, hi = block_bootstrap_ci(r, block_len=24, resamples=500, ci=0.95, seed=7)
    assert lo < 0 < hi


def test_bootstrap_positive_drift_excludes_zero():
    rng = random.Random(2)
    r = [Decimal(str(round(0.005 + rng.gauss(0, 0.002), 6))) for _ in range(500)]
    lo, hi = block_bootstrap_ci(r, block_len=24, resamples=500, ci=0.95, seed=7)
    assert lo > 0


def test_bootstrap_is_deterministic_for_seed():
    r = [Decimal(str(i % 7 - 3)) for i in range(200)]
    a = block_bootstrap_ci(r, block_len=24, resamples=100, ci=0.95, seed=3)
    b = block_bootstrap_ci(r, block_len=24, resamples=100, ci=0.95, seed=3)
    assert a == b


def test_bootstrap_too_short_raises():
    with pytest.raises(ValueError, match="insufficient"):
        block_bootstrap_ci([Decimal(1)] * 40, block_len=24, resamples=10, ci=0.95, seed=1)


def test_chronological_split():
    train, hold = chronological_split(list(range(10)), holdout_fraction=Decimal("0.30"))
    assert train == [0, 1, 2, 3, 4, 5, 6] and hold == [7, 8, 9]
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_costs.py tests/test_stats.py -v`
Expected: FAIL with `ModuleNotFoundError` for both modules

- [ ] **Step 3: Write `polyperps/backtest/costs.py`**

```python
"""Execution cost model (spec 5.3). Pure Decimal arithmetic.

impact_bps is a pre-registered assumption (BAR.impact_bps), not a measurement.
"""

from __future__ import annotations

from decimal import Decimal

_BPS = Decimal(10_000)


def fill_cost(
    *,
    notional_delta: Decimal,
    notional: Decimal,
    spread_bps: Decimal,
    taker_fee_rate: Decimal,
    impact_bps: Decimal,
) -> Decimal:
    """Cost of changing exposure by notional_delta (absolute USD)."""
    if notional_delta == 0:
        return Decimal(0)
    turnover_fraction = notional_delta / notional
    rate = taker_fee_rate + spread_bps / (2 * _BPS) + impact_bps / _BPS * turnover_fraction
    return notional_delta * rate
```

- [ ] **Step 4: Write `polyperps/backtest/stats.py`**

```python
"""Performance statistics (spec 5.4). Floats are fine here - nothing is money."""

from __future__ import annotations

import math
import random
from collections.abc import Sequence
from decimal import Decimal
from statistics import mean, stdev
from typing import TypeVar

T = TypeVar("T")


def sharpe(returns: Sequence[Decimal], *, periods_per_year: int = 24 * 365) -> float:
    if len(returns) < 2:
        return 0.0
    xs = [float(r) for r in returns]
    sd = stdev(xs)
    if sd == 0.0:
        return 0.0
    return mean(xs) / sd * math.sqrt(periods_per_year)


def max_drawdown(equity: Sequence[Decimal]) -> Decimal:
    peak = None
    worst = Decimal(0)
    for e in equity:
        if peak is None or e > peak:
            peak = e
        dd = peak - e
        if dd > worst:
            worst = dd
    return worst


def hit_rate(trade_pnls: Sequence[Decimal]) -> float:
    if not trade_pnls:
        return 0.0
    return sum(1 for p in trade_pnls if p > 0) / len(trade_pnls)


def turnover(fill_notionals: Sequence[Decimal], *, notional: Decimal) -> Decimal:
    return sum(fill_notionals, Decimal(0)) / notional


def block_bootstrap_ci(
    returns: Sequence[Decimal],
    *,
    block_len: int,
    resamples: int,
    ci: float,
    seed: int,
) -> tuple[float, float]:
    """Circular block bootstrap CI for the mean return (funding is autocorrelated)."""
    n = len(returns)
    if n < 2 * block_len:
        raise ValueError(f"insufficient for block bootstrap: {n} returns < 2 x block_len {block_len}")
    xs = [float(r) for r in returns]
    rng = random.Random(seed)
    n_blocks = math.ceil(n / block_len)
    means: list[float] = []
    for _ in range(resamples):
        sample: list[float] = []
        for _ in range(n_blocks):
            start = rng.randrange(n)
            sample.extend(xs[(start + k) % n] for k in range(block_len))
        means.append(mean(sample[:n]))
    means.sort()
    alpha = (1.0 - ci) / 2.0
    lo = means[int(alpha * (resamples - 1))]
    hi = means[int((1.0 - alpha) * (resamples - 1))]
    return lo, hi


def chronological_split(items: Sequence[T], *, holdout_fraction: Decimal) -> tuple[list[T], list[T]]:
    n = len(items)
    k = math.ceil(n * float(holdout_fraction))
    return list(items[: n - k]), list(items[n - k :])
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_costs.py tests/test_stats.py -v`
Expected: 12 passed

- [ ] **Step 6: Commit**

```bash
git add polyperps/backtest/costs.py polyperps/backtest/stats.py tests/test_costs.py tests/test_stats.py
git commit -m "feat(phase1): cost model and statistics with block bootstrap"
```

---

### Task 8: Strategy protocol and the harness (spec §5.1, §5.2)

**Files:**
- Create: `polyperps/backtest/strategy.py`, `polyperps/backtest/harness.py`
- Test: `tests/test_harness.py`

**Interfaces:**
- Consumes: `Bar`, `floor_minute`, `fill_cost`, `BAR`.
- Produces:
  - `Strategy` Protocol: attributes `name: str`, `params: Mapping[str, Decimal | int]`; method `target(self, history: Sequence[Bar]) -> Decimal`. `clamp_target(x: Decimal) -> Decimal` into `[-1, 1]`.
  - `LedgerRow(ts: datetime, kind: Literal["funding","fill","fill_unavailable","gap_flatten","mark"], position: Decimal, price: Decimal | None, cash_delta: Decimal, equity: Decimal, note: str = "")` frozen.
  - `BacktestResult(ledger: list[LedgerRow], equity: list[tuple[datetime, Decimal]], returns: list[Decimal], trade_pnls: list[Decimal], fill_notionals: list[Decimal], params: dict[str, str], bars_total: int, bars_complete: int, fills: int, fills_unavailable: int)`.
  - `run_backtest(bars: Sequence[Bar], strategy: Strategy, *, minute_closes: Mapping[datetime, Decimal], taker_fee_rate: Decimal, warmup: int, latency_s: int = BAR.latency_s, impact_bps: Decimal = BAR.impact_bps, notional: Decimal = BAR.notional_usd) -> BacktestResult`.
  - Semantics exactly as spec §5.2; `returns[i] = (equity[i] − equity[i−1]) / notional`; `trade_pnls` = realised PnL each time a non-zero position is reduced/closed/flipped (per fill).

- [ ] **Step 1: Write the failing tests**

`tests/test_harness.py`:

```python
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.backtest.bars import Bar
from polyperps.backtest.harness import run_backtest
from polyperps.backtest.strategy import clamp_target
from polyperps.exchange.types import SourceType

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 0, 0, tzinfo=UTC)
H = timedelta(hours=1)
FEE = Decimal("0.0005")


def bar(i, close="100", funding="0", complete=True):
    ts = T0 + i * H
    c = Decimal(close) if complete else None
    return Bar(instrument_id=6, source_type=SourceType.POLYMARKET_REST, open_ts=ts, open=c, high=c, low=c,
               close=c, index_close=None, funding_rate=Decimal(funding) if complete else None,
               spread_bps=Decimal("10"), complete=complete)


def minutes(bars, price_by_hour=None):
    """1m closes for every minute of every bar = that bar's close (or an override)."""
    out = {}
    for b in bars:
        px = (price_by_hour or {}).get(b.open_ts, b.close)
        if px is None:
            continue
        for m in range(60):
            out[b.open_ts + timedelta(minutes=m)] = px
    return out


class Const:
    name = "const"
    params = {}

    def __init__(self, x):
        self.x = Decimal(x)

    def target(self, history):
        return self.x


class Recorder:
    name = "rec"
    params = {}

    def __init__(self):
        self.seen = []

    def target(self, history):
        self.seen.append((len(history), history[-1].open_ts))
        return Decimal(0)


def test_clamp():
    assert clamp_target(Decimal("1.7")) == 1 and clamp_target(Decimal("-3")) == -1 and clamp_target(Decimal("0.2")) == Decimal("0.2")


def test_strategy_sees_exactly_bars_up_to_t():
    bars = [bar(i) for i in range(6)]
    rec = Recorder()
    run_backtest(bars, rec, minute_closes=minutes(bars), taker_fee_rate=FEE, warmup=2)
    assert rec.seen == [(3, T0 + 2 * H), (4, T0 + 3 * H), (5, T0 + 4 * H)]


def test_long_pays_positive_funding_exactly():
    bars = [bar(0), bar(1, funding="0.001"), bar(2, funding="0.001"), bar(3)]
    res = run_backtest(bars, Const(1), minute_closes=minutes(bars), taker_fee_rate=Decimal(0), warmup=0)
    # enters at bar1 open (fill at t0+1h+2s), pays funding on bars 1 and 2 -> 2 * 100 * 0.001 = 0.2
    funding_rows = [r for r in res.ledger if r.kind == "funding"]
    assert sum(r.cash_delta for r in funding_rows) == Decimal("-0.2")
    assert res.fills == 1


def test_fill_pays_fee_spread_and_impact():
    bars = [bar(0), bar(1)]
    res = run_backtest(bars, Const(1), minute_closes=minutes(bars), taker_fee_rate=FEE, warmup=0)
    (fill,) = [r for r in res.ledger if r.kind == "fill"]
    # delta 100: fee .05 + half-spread 10bps/2*100 = .05 + impact 5bps*1*100 = .05 -> 0.15
    assert fill.cash_delta == Decimal("-0.15")
    assert res.fill_notionals == [Decimal("100")]


def test_fill_uses_minute_close_after_latency():
    bars = [bar(0, close="100"), bar(1, close="100")]
    mc = minutes(bars)
    mc[T0 + H] = Decimal("101")  # the minute containing open+2s
    res = run_backtest(bars, Const(1), minute_closes=mc, taker_fee_rate=Decimal(0), warmup=0, impact_bps=Decimal(0))
    (fill,) = [r for r in res.ledger if r.kind == "fill"]
    assert fill.price == Decimal("101")


def test_missing_minute_candle_refuses_fill():
    bars = [bar(0), bar(1), bar(2)]
    res = run_backtest(bars, Const(1), minute_closes={}, taker_fee_rate=FEE, warmup=0)
    assert res.fills == 0 and res.fills_unavailable == 2
    assert all(r.position == 0 for r in res.ledger)


def test_gap_forces_flatten_and_blocks_reentry_until_complete():
    bars = [bar(0), bar(1), bar(2), bar(3, complete=False), bar(4), bar(5)]
    res = run_backtest(bars, Const(1), minute_closes=minutes(bars), taker_fee_rate=Decimal(0), warmup=0)
    kinds = [(r.ts, r.kind) for r in res.ledger if r.kind in ("fill", "gap_flatten")]
    assert (T0 + 2 * H, "gap_flatten") in kinds          # flattened at bar 2 close before the gap at bar 3
    assert not any(ts == T0 + 3 * H and k == "fill" for ts, k in kinds)
    assert (T0 + 5 * H, "fill") in kinds                  # re-enters after the next complete bar
    assert res.bars_complete == 5


def test_equity_and_returns_track_price():
    bars = [bar(0, "100"), bar(1, "100"), bar(2, "110"), bar(3, "110")]
    res = run_backtest(bars, Const(1), minute_closes=minutes(bars), taker_fee_rate=Decimal(0), warmup=0,
                       impact_bps=Decimal(0))
    # enter at 100 (bar1 open), cost half-spread 10bps/2 * 100 = 0.05; mark at bar2 close 110 -> +10
    final = res.equity[-1][1]
    assert final == Decimal("9.95")
    assert sum(res.returns) == Decimal("0.0995")


def test_flip_realises_trade_pnl():
    bars = [bar(0, "100"), bar(1, "100"), bar(2, "105"), bar(3, "105"), bar(4, "105")]

    class Flip:
        name = "flip"; params = {}
        def target(self, history):
            return Decimal(1) if len(history) < 3 else Decimal(-1)

    res = run_backtest(bars, Flip(), minute_closes=minutes(bars), taker_fee_rate=Decimal(0), warmup=0,
                       impact_bps=Decimal(0))
    assert res.trade_pnls == [Decimal("5")]  # long from 100, flipped at 105
    assert res.fill_notionals == [Decimal("100"), Decimal("200")]
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_harness.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.backtest.harness'`

- [ ] **Step 3: Write `polyperps/backtest/strategy.py`**

```python
"""Strategy interface. The harness passes bars[:t+1] and nothing else."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Protocol

from polyperps.backtest.bars import Bar

ONE = Decimal(1)


class Strategy(Protocol):
    name: str
    params: Mapping[str, Decimal | int]

    def target(self, history: Sequence[Bar]) -> Decimal:
        """Desired position as a fraction of notional in [-1, +1], decided after history[-1] closed."""
        ...


def clamp_target(x: Decimal) -> Decimal:
    return max(-ONE, min(ONE, x))
```

- [ ] **Step 4: Write `polyperps/backtest/harness.py`**

```python
"""Event-driven hourly backtest (spec 5.2).

Point-in-time: the strategy receives bars[:t+1]. Fills happen at the 1-minute
close latency_s after the NEXT bar opens; no minute candle -> no fill.
Gaps: flatten before an incomplete bar, no re-entry until a complete one.
Fixed notional, 1x, no liquidation modelling (Phase 2 owns sizing/leverage).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal

from polyperps.backtest.bars import Bar, floor_minute
from polyperps.backtest.costs import fill_cost
from polyperps.backtest.strategy import Strategy, clamp_target
from polyperps.signal.sufficiency import BAR

Kind = Literal["funding", "fill", "fill_unavailable", "gap_flatten", "mark"]


@dataclass(frozen=True, slots=True, kw_only=True)
class LedgerRow:
    ts: datetime
    kind: Kind
    position: Decimal
    price: Decimal | None
    cash_delta: Decimal
    equity: Decimal
    note: str = ""


@dataclass(slots=True)
class BacktestResult:
    ledger: list[LedgerRow] = field(default_factory=list)
    equity: list[tuple[datetime, Decimal]] = field(default_factory=list)
    returns: list[Decimal] = field(default_factory=list)
    trade_pnls: list[Decimal] = field(default_factory=list)
    fill_notionals: list[Decimal] = field(default_factory=list)
    params: dict[str, str] = field(default_factory=dict)
    bars_total: int = 0
    bars_complete: int = 0
    fills: int = 0
    fills_unavailable: int = 0


class _Book:
    """Mutable position state for one run."""

    def __init__(self, notional: Decimal) -> None:
        self.notional = notional
        self.cash = Decimal(0)
        self.position = Decimal(0)
        self.entry = Decimal(0)

    def unrealised(self, price: Decimal) -> Decimal:
        if self.position == 0:
            return Decimal(0)
        return self.position * self.notional * (price / self.entry - 1)

    def equity(self, price: Decimal | None) -> Decimal:
        return self.cash + (self.unrealised(price) if price is not None else Decimal(0))


def run_backtest(
    bars: Sequence[Bar],
    strategy: Strategy,
    *,
    minute_closes: Mapping[datetime, Decimal],
    taker_fee_rate: Decimal,
    warmup: int,
    latency_s: int = BAR.latency_s,
    impact_bps: Decimal = BAR.impact_bps,
    notional: Decimal = BAR.notional_usd,
) -> BacktestResult:
    res = BacktestResult(
        params={"taker_fee_rate": str(taker_fee_rate), "latency_s": str(latency_s),
                "impact_bps": str(impact_bps), "notional": str(notional), "warmup": str(warmup),
                "strategy": strategy.name, **{k: str(v) for k, v in strategy.params.items()}},
        bars_total=len(bars),
        bars_complete=sum(1 for b in bars if b.complete),
    )
    book = _Book(notional)
    latency = timedelta(seconds=latency_s)
    last_equity: Decimal | None = None

    def log(ts: datetime, kind: Kind, price: Decimal | None, cash_delta: Decimal, note: str = "") -> None:
        res.ledger.append(LedgerRow(ts=ts, kind=kind, position=book.position, price=price,
                                    cash_delta=cash_delta, equity=book.equity(price), note=note))

    def trade_to(target: Decimal, price: Decimal, spread_bps: Decimal, ts: datetime, kind: Kind) -> None:
        delta = target - book.position
        if delta == 0:
            return
        if book.position != 0:
            realised = book.unrealised(price)
            book.cash += realised
            res.trade_pnls.append(realised)
        notional_delta = abs(delta) * notional
        cost = fill_cost(notional_delta=notional_delta, notional=notional, spread_bps=spread_bps,
                         taker_fee_rate=taker_fee_rate, impact_bps=impact_bps)
        book.cash -= cost
        book.position = target
        book.entry = price if target != 0 else Decimal(0)
        res.fill_notionals.append(notional_delta)
        res.fills += 1
        log(ts, kind, price, -cost)

    def mark(ts: datetime, price: Decimal | None) -> None:
        nonlocal last_equity
        eq = book.equity(price)
        res.equity.append((ts, eq))
        if last_equity is not None:
            res.returns.append((eq - last_equity) / notional)
        last_equity = eq
        log(ts, "mark", price, Decimal(0))

    for t in range(warmup, len(bars) - 1):
        bar, nxt = bars[t], bars[t + 1]

        if book.position != 0 and bar.funding_rate is not None:
            paid = -book.position * notional * bar.funding_rate
            book.cash += paid
            log(bar.open_ts, "funding", bar.close, paid)

        if not nxt.complete or not bar.complete:
            if book.position != 0 and bar.close is not None:
                trade_to(Decimal(0), bar.close, bar.spread_bps, bar.open_ts, "gap_flatten")
            mark(nxt.open_ts, nxt.close)
            continue

        target = clamp_target(strategy.target(bars[: t + 1]))
        if target != book.position:
            fill_ts = nxt.open_ts + latency
            price = minute_closes.get(floor_minute(fill_ts))
            if price is None:
                res.fills_unavailable += 1
                log(nxt.open_ts, "fill_unavailable", None, Decimal(0), f"no 1m candle at {fill_ts.isoformat()}")
            else:
                trade_to(target, price, bar.spread_bps, nxt.open_ts, "fill")

        mark(nxt.open_ts, nxt.close)

    return res
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_harness.py -v`
Expected: 9 passed. If `test_equity_and_returns_track_price` is off by the spread cost, recheck: entry fill at 100 with `spread_bps=10` costs `100 × 10/20000 = 0.05`; final equity `10 − 0.05 = 9.95`.

- [ ] **Step 6: Commit**

```bash
git add polyperps/backtest/strategy.py polyperps/backtest/harness.py tests/test_harness.py
git commit -m "feat(phase1): point-in-time hourly backtest harness with costs, funding, and gap rule"
```

---

### Task 9: The three strategies with pre-registered grids (spec §6)

**Files:**
- Create: `polyperps/strategies/__init__.py`, `polyperps/strategies/_zscore.py`, `polyperps/strategies/funding_reversion.py`, `polyperps/strategies/basis.py`, `polyperps/strategies/index_lag.py`
- Test: `tests/test_strategies.py`

**Interfaces:**
- Consumes: `Bar`, `Strategy`, `run_backtest`.
- Produces:
  - `zscore(values: Sequence[Decimal]) -> Decimal | None` — z of the last value vs the window; `None` if `< 3` values or stdev 0.
  - `FundingReversion(*, lookback: int, entry_z: Decimal, exit_z: Decimal)`; `name="h1_funding_reversion"`; `warmup = lookback`.
  - `Basis(*, lookback: int, entry_z: Decimal, proxy_close_by_hour: Mapping[datetime, Decimal])`; `name="h2_basis"`; `warmup = lookback`; looks up proxy closes only for `open_ts` values present in `history` (point-in-time by construction).
  - `IndexLag(*, entry_bps: Decimal, hold_bars: int)`; `name="h3_index_lag"`; `warmup = 1`; returns `0` when `history[-1].index_close is None`.
  - Each class exposes `warmup: int` and `params: dict`.
  - `GRIDS: dict[str, list[dict]]` with keys `"h1"`, `"h2"`, `"h3"` — exactly the Global Constraints grids (H1: 4 combos, H2: 4, H3: 4).
  - `build_strategy(hypothesis: str, params: Mapping, *, proxy_close_by_hour: Mapping[datetime, Decimal] | None = None) -> Strategy`.

- [ ] **Step 1: Write the failing tests**

`tests/test_strategies.py`:

```python
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from polyperps.backtest.bars import Bar
from polyperps.backtest.harness import run_backtest
from polyperps.exchange.types import SourceType
from polyperps.strategies import GRIDS, build_strategy
from polyperps.strategies._zscore import zscore
from polyperps.strategies.basis import Basis
from polyperps.strategies.funding_reversion import FundingReversion
from polyperps.strategies.index_lag import IndexLag

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 0, 0, tzinfo=UTC)
H = timedelta(hours=1)


def bar(i, close="100", funding="0", index=None, st=SourceType.POLYMARKET_REST):
    ts = T0 + i * H
    return Bar(instrument_id=6, source_type=st, open_ts=ts, open=Decimal(close), high=Decimal(close),
               low=Decimal(close), close=Decimal(close), index_close=Decimal(index) if index else None,
               funding_rate=Decimal(funding), spread_bps=Decimal("5"), complete=True)


def minutes(bars):
    return {b.open_ts + timedelta(minutes=m): b.close for b in bars for m in range(60)}


def test_zscore():
    assert zscore([Decimal(1)] * 5) is None
    assert zscore([Decimal(1), Decimal(2)]) is None
    z = zscore([Decimal(0)] * 9 + [Decimal(3)])
    assert z > 2


def test_grids_are_pre_registered():
    assert GRIDS["h1"] == [
        {"lookback": 48, "entry_z": Decimal("1.5"), "exit_z": Decimal("0.5")},
        {"lookback": 48, "entry_z": Decimal("2.0"), "exit_z": Decimal("0.5")},
        {"lookback": 168, "entry_z": Decimal("1.5"), "exit_z": Decimal("0.5")},
        {"lookback": 168, "entry_z": Decimal("2.0"), "exit_z": Decimal("0.5")},
    ]
    assert GRIDS["h2"] == [
        {"lookback": 24, "entry_z": Decimal("2.0")}, {"lookback": 24, "entry_z": Decimal("3.0")},
        {"lookback": 72, "entry_z": Decimal("2.0")}, {"lookback": 72, "entry_z": Decimal("3.0")},
    ]
    assert GRIDS["h3"] == [
        {"entry_bps": Decimal("10"), "hold_bars": 1}, {"entry_bps": Decimal("10"), "hold_bars": 3},
        {"entry_bps": Decimal("25"), "hold_bars": 1}, {"entry_bps": Decimal("25"), "hold_bars": 3},
    ]


def test_h1_shorts_extreme_positive_funding_and_profits_on_synthetic():
    # 47 calm hours, then funding spikes to +1%/h for 6 hours at flat price: a short collects 6% of notional.
    bars = [bar(i, funding="0.0001") for i in range(47)] + [bar(47 + j, funding="0.01") for j in range(8)]
    s = FundingReversion(lookback=48, entry_z=Decimal("1.5"), exit_z=Decimal("0.5"))
    assert s.warmup == 48
    res = run_backtest(bars, s, minute_closes=minutes(bars), taker_fee_rate=Decimal("0.0005"), warmup=s.warmup)
    assert res.fills >= 1
    (first_fill,) = [r for r in res.ledger if r.kind == "fill"][:1]
    assert first_fill.position == Decimal(-1)
    assert res.equity[-1][1] > Decimal("3")   # several 1%-of-notional funding receipts net of ~0.1 costs


def test_h1_never_trades_on_constant_funding():
    bars = [bar(i, funding="0.0001") for i in range(60)]
    s = FundingReversion(lookback=48, entry_z=Decimal("1.5"), exit_z=Decimal("0.5"))
    res = run_backtest(bars, s, minute_closes=minutes(bars), taker_fee_rate=Decimal("0.0005"), warmup=s.warmup)
    assert res.fills == 0


def test_h2_fades_rich_polymarket_basis():
    # PM flat at 100, HL flat at 100 for 24h, then PM jumps to 103 while HL stays -> basis z spikes -> short PM
    bars = [bar(i) for i in range(24)] + [bar(24 + j, close="103") for j in range(4)]
    hl = {b.open_ts: Decimal("100") for b in bars}
    s = Basis(lookback=24, entry_z=Decimal("2.0"), proxy_close_by_hour=hl)
    res = run_backtest(bars, s, minute_closes=minutes(bars), taker_fee_rate=Decimal("0.0005"), warmup=s.warmup)
    first = [r for r in res.ledger if r.kind == "fill"][0]
    assert first.position == Decimal(-1)


def test_h2_is_flat_when_proxy_hour_missing():
    bars = [bar(i) for i in range(30)]
    s = Basis(lookback=24, entry_z=Decimal("2.0"), proxy_close_by_hour={})
    assert s.target(bars[:25]) == 0


def test_h3_trades_toward_index_and_holds_then_exits():
    # mark 100 vs index 100.5 -> premium -50bps -> mark should catch up -> long; hold 1 bar then flat
    bars = [bar(0, index="100"), bar(1, index="100.5"), bar(2, index="100.5"), bar(3, index="100.5"), bar(4, index="100.5")]
    s = IndexLag(entry_bps=Decimal("25"), hold_bars=1)
    assert s.warmup == 1
    assert s.target(bars[:2]) == Decimal(1)
    res = run_backtest(bars, s, minute_closes=minutes(bars), taker_fee_rate=Decimal("0.0005"), warmup=1)
    positions = [r.position for r in res.ledger if r.kind == "fill"]
    assert positions[:2] == [Decimal(1), Decimal(0)]


def test_h3_returns_zero_on_proxy_bars():
    bars = [bar(i, st=SourceType.PROXY_HYPERLIQUID) for i in range(3)]
    s = IndexLag(entry_bps=Decimal("10"), hold_bars=1)
    assert s.target(bars) == 0


def test_build_strategy():
    assert build_strategy("h1", GRIDS["h1"][0]).name == "h1_funding_reversion"
    assert build_strategy("h3", GRIDS["h3"][0]).name == "h3_index_lag"
    assert build_strategy("h2", GRIDS["h2"][0], proxy_close_by_hour={}).name == "h2_basis"
    with pytest.raises(ValueError):
        build_strategy("h2", GRIDS["h2"][0])
    with pytest.raises(ValueError):
        build_strategy("h9", {})
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_strategies.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.strategies'`

- [ ] **Step 3: Write `polyperps/strategies/_zscore.py`**

```python
from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from statistics import mean, stdev


def zscore(values: Sequence[Decimal]) -> Decimal | None:
    """z of the LAST value against the whole window. None if the window is too short or flat."""
    if len(values) < 3:
        return None
    xs = [float(v) for v in values]
    sd = stdev(xs)
    if sd == 0.0:
        return None
    return Decimal(str((xs[-1] - mean(xs)) / sd))
```

- [ ] **Step 4: Write `polyperps/strategies/funding_reversion.py`** (H1)

```python
"""H1: funding-rate mean reversion. Extreme positive funding -> short (collect it);
extreme negative -> long. Flat once |z| falls under exit_z."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from polyperps.backtest.bars import Bar
from polyperps.strategies._zscore import zscore


class FundingReversion:
    name = "h1_funding_reversion"

    def __init__(self, *, lookback: int, entry_z: Decimal, exit_z: Decimal) -> None:
        self.lookback = lookback
        self.entry_z = entry_z
        self.exit_z = exit_z
        self.params = {"lookback": lookback, "entry_z": entry_z, "exit_z": exit_z}
        self.warmup = lookback
        self._position = Decimal(0)

    def target(self, history: Sequence[Bar]) -> Decimal:
        window = [b.funding_rate for b in history[-self.lookback:] if b.funding_rate is not None]
        z = zscore(window)
        if z is None:
            return self._position
        if z >= self.entry_z:
            self._position = Decimal(-1)
        elif z <= -self.entry_z:
            self._position = Decimal(1)
        elif abs(z) < self.exit_z:
            self._position = Decimal(0)
        return self._position
```

- [ ] **Step 5: Write `polyperps/strategies/basis.py`** (H2)

```python
"""H2: cross-venue basis. basis = pm_close / hl_close - 1. Fade a stretched basis by
trading the Polymarket leg. Proxy closes are looked up ONLY for open_ts values present in
history, so the strategy cannot see a proxy hour the harness has not yet reached."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal

from polyperps.backtest.bars import Bar
from polyperps.strategies._zscore import zscore

_EXIT_Z = Decimal("0.5")


class Basis:
    name = "h2_basis"

    def __init__(self, *, lookback: int, entry_z: Decimal, proxy_close_by_hour: Mapping[datetime, Decimal]) -> None:
        self.lookback = lookback
        self.entry_z = entry_z
        self.params = {"lookback": lookback, "entry_z": entry_z}
        self.warmup = lookback
        self._proxy = proxy_close_by_hour
        self._position = Decimal(0)

    def target(self, history: Sequence[Bar]) -> Decimal:
        window: list[Decimal] = []
        for b in history[-self.lookback:]:
            hl = self._proxy.get(b.open_ts)
            if b.close is None or hl is None or hl == 0:
                continue
            window.append(b.close / hl - 1)
        if len(window) < self.lookback or self._proxy.get(history[-1].open_ts) is None:
            return Decimal(0)
        z = zscore(window)
        if z is None:
            return self._position
        if z >= self.entry_z:
            self._position = Decimal(-1)
        elif z <= -self.entry_z:
            self._position = Decimal(1)
        elif abs(z) < _EXIT_Z:
            self._position = Decimal(0)
        return self._position
```

- [ ] **Step 6: Write `polyperps/strategies/index_lag.py`** (H3)

```python
"""H3: mark-vs-index lag. If the mark sits far from its own index, bet it catches up;
hold hold_bars bars, then flat. Native-only: returns 0 when index_close is None."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from polyperps.backtest.bars import Bar

_BPS = Decimal(10_000)


class IndexLag:
    name = "h3_index_lag"

    def __init__(self, *, entry_bps: Decimal, hold_bars: int) -> None:
        self.entry_bps = entry_bps
        self.hold_bars = hold_bars
        self.params = {"entry_bps": entry_bps, "hold_bars": hold_bars}
        self.warmup = 1
        self._position = Decimal(0)
        self._held = 0

    def target(self, history: Sequence[Bar]) -> Decimal:
        last = history[-1]
        if last.index_close is None or last.close is None or last.index_close == 0:
            self._position = Decimal(0)
            self._held = 0
            return self._position
        if self._position != 0:
            self._held += 1
            if self._held >= self.hold_bars:
                self._position = Decimal(0)
                self._held = 0
            return self._position
        premium_bps = (last.close / last.index_close - 1) * _BPS
        if premium_bps <= -self.entry_bps:
            self._position = Decimal(1)   # mark below index: expect it to rise
        elif premium_bps >= self.entry_bps:
            self._position = Decimal(-1)  # mark above index: expect it to fall
        self._held = 0
        return self._position
```

- [ ] **Step 7: Write `polyperps/strategies/__init__.py`**

```python
"""Pre-registered hypothesis strategies and their fixed grids (spec section 6).

Adding a grid point after seeing results is a spec amendment; record it in the
validation log's next record."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal

from polyperps.backtest.strategy import Strategy
from polyperps.strategies.basis import Basis
from polyperps.strategies.funding_reversion import FundingReversion
from polyperps.strategies.index_lag import IndexLag

GRIDS: dict[str, list[dict]] = {
    "h1": [
        {"lookback": lb, "entry_z": ez, "exit_z": Decimal("0.5")}
        for lb in (48, 168)
        for ez in (Decimal("1.5"), Decimal("2.0"))
    ],
    "h2": [
        {"lookback": lb, "entry_z": ez}
        for lb in (24, 72)
        for ez in (Decimal("2.0"), Decimal("3.0"))
    ],
    "h3": [
        {"entry_bps": eb, "hold_bars": hb}
        for eb in (Decimal("10"), Decimal("25"))
        for hb in (1, 3)
    ],
}


def build_strategy(
    hypothesis: str,
    params: Mapping,
    *,
    proxy_close_by_hour: Mapping[datetime, Decimal] | None = None,
) -> Strategy:
    if hypothesis == "h1":
        return FundingReversion(**params)
    if hypothesis == "h2":
        if proxy_close_by_hour is None:
            raise ValueError("h2 needs proxy_close_by_hour")
        return Basis(**params, proxy_close_by_hour=proxy_close_by_hour)
    if hypothesis == "h3":
        return IndexLag(**params)
    raise ValueError(f"unknown hypothesis {hypothesis!r}")
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_strategies.py -v`
Expected: 9 passed. If `test_h1_shorts_extreme_positive_funding_and_profits_on_synthetic` fails on the equity threshold, check the arithmetic: entry at bar 48's open after the first spike hour is seen; each subsequent spike bar pays `100 × 0.01 = 1.0` to the short; with ~5 such bars minus ~0.1 in costs, equity is ≈ 4.9.

- [ ] **Step 9: Commit**

```bash
git add polyperps/strategies tests/test_strategies.py
git commit -m "feat(phase1): H1 funding reversion, H2 basis, H3 index lag with pre-registered grids"
```

---

### Task 10: Validation log and `scripts/run_backtest.py` (spec §8.1)

**Files:**
- Create: `polyperps/signal/validation_log.py`, `polyperps/signal/validation_log.jsonl` (empty), `scripts/run_backtest.py`
- Modify: `pyproject.toml` (package-data)
- Test: `tests/test_validation_log.py`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `LOG_PATH = Path(__file__).with_name("validation_log.jsonl")`.
  - `make_run_id(ts: datetime, hypothesis: str, instrument_id: int, source_type: SourceType) -> str` → `f"{ts:%Y%m%dT%H%M%S}-{hypothesis}-{instrument_id}-{source_type.value}"`.
  - `append_record(record: dict, *, path: Path = LOG_PATH) -> None` — one JSON object per line, `default=str` for Decimals/datetimes.
  - `read_records(*, path: Path = LOG_PATH) -> list[dict]`; `read_passing(*, path=LOG_PATH) -> list[dict]` (records with `passed is True`).
  - `evaluate_run(*, source_type, sufficiency: SufficiencyReport, holdout_sharpe: float, ci_lo: float, ci_hi: float) -> tuple[bool, bool]` → `(screened, passed)` where `screened = stats_clear_bar(...)` and `passed = screened and source_type in NATIVE_SOURCES and sufficiency.met`.

- [ ] **Step 1: Write the failing tests**

`tests/test_validation_log.py`:

```python
import json
from datetime import datetime, timezone
from decimal import Decimal

from polyperps.exchange.types import SourceType
from polyperps.signal.sufficiency import SufficiencyReport
from polyperps.signal.validation_log import (
    append_record, evaluate_run, make_run_id, read_passing, read_records,
)

T0 = datetime(2026, 9, 11, 12, 0, 5, tzinfo=timezone.utc)


def test_make_run_id():
    assert make_run_id(T0, "h1", 6, SourceType.PROXY_HYPERLIQUID) == "20260911T120005-h1-6-proxy_hyperliquid"


def test_append_and_read_round_trip(tmp_path):
    p = tmp_path / "log.jsonl"
    append_record({"run_id": "a", "x": Decimal("1.5"), "ts": T0, "passed": False}, path=p)
    append_record({"run_id": "b", "passed": True}, path=p)
    recs = read_records(path=p)
    assert [r["run_id"] for r in recs] == ["a", "b"]
    assert recs[0]["x"] == "1.5" and recs[0]["ts"].startswith("2026-09-11T12:00:05")
    assert [r["run_id"] for r in read_passing(path=p)] == ["b"]
    assert read_records(path=tmp_path / "missing.jsonl") == []


def _suff(met, st):
    return SufficiencyReport(met=met, days=Decimal("61"), funding_periods=1464, source_type=st, shortfall={})


def test_evaluate_run_proxy_can_screen_but_never_pass():
    screened, passed = evaluate_run(source_type=SourceType.PROXY_HYPERLIQUID,
                                    sufficiency=_suff(False, SourceType.PROXY_HYPERLIQUID),
                                    holdout_sharpe=2.0, ci_lo=0.001, ci_hi=0.002)
    assert screened is True and passed is False


def test_evaluate_run_native_passes_only_with_sufficiency_and_stats():
    st = SourceType.POLYMARKET_REST
    assert evaluate_run(source_type=st, sufficiency=_suff(True, st), holdout_sharpe=1.5, ci_lo=0.001, ci_hi=0.002) == (True, True)
    assert evaluate_run(source_type=st, sufficiency=_suff(False, st), holdout_sharpe=1.5, ci_lo=0.001, ci_hi=0.002) == (True, False)
    assert evaluate_run(source_type=st, sufficiency=_suff(True, st), holdout_sharpe=0.5, ci_lo=0.001, ci_hi=0.002) == (False, False)
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_validation_log.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.signal.validation_log'`

- [ ] **Step 3: Write `polyperps/signal/validation_log.py`** and create the empty `polyperps/signal/validation_log.jsonl`

```python
"""Append-only record of every backtest run, pass or fail (spec 8.1). Committed.

`passed` is True only for a native-source run whose dataset met the bar AND
whose holdout statistics cleared it. Proxy runs can only be `screened`.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from polyperps.exchange.types import SourceType
from polyperps.signal.sufficiency import NATIVE_SOURCES, SufficiencyReport, stats_clear_bar

LOG_PATH = Path(__file__).with_name("validation_log.jsonl")


def make_run_id(ts: datetime, hypothesis: str, instrument_id: int, source_type: SourceType) -> str:
    return f"{ts:%Y%m%dT%H%M%S}-{hypothesis}-{instrument_id}-{source_type.value}"


def append_record(record: dict, *, path: Path = LOG_PATH) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, default=str, sort_keys=True) + "\n")


def read_records(*, path: Path = LOG_PATH) -> list[dict]:
    if not path.exists():
        return []
    out: list[dict] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def read_passing(*, path: Path = LOG_PATH) -> list[dict]:
    return [r for r in read_records(path=path) if r.get("passed") is True]


def evaluate_run(
    *,
    source_type: SourceType,
    sufficiency: SufficiencyReport,
    holdout_sharpe: float,
    ci_lo: float,
    ci_hi: float,
) -> tuple[bool, bool]:
    screened = stats_clear_bar(oos_sharpe=holdout_sharpe, ci_lo=ci_lo, ci_hi=ci_hi)
    passed = screened and source_type in NATIVE_SOURCES and sufficiency.met
    return screened, passed
```

Create the log file empty: `: > polyperps/signal/validation_log.jsonl` (Bash) — it must exist and be committed so the first run appends rather than creates.

In `pyproject.toml` add:

```toml
[tool.setuptools.package-data]
polyperps = ["signal/*.jsonl", "signal/*.json"]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_validation_log.py -v`
Expected: 4 passed

- [ ] **Step 5: Write `scripts/run_backtest.py`**

```python
"""Spec 1.2: run one hypothesis on one instrument/source, log the result.

    POLYPERPS_INSTRUMENT_IDS=6 .venv/Scripts/python scripts/run_backtest.py \
        --hypothesis h1 --instrument 6 --source hyperliquid [--category crypto] [--seed 42]

Grid points are evaluated on the chronological train slice; the best by train
Sharpe runs ONCE on the holdout; the holdout gets a block-bootstrap CI. One JSON
record is appended to polyperps/signal/validation_log.jsonl whether the run
screened, passed, or failed. Refuses to run without a stored fee row.
h2 needs BOTH native and hyperliquid data loaded (it trades the native leg).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal

from polyperps.backtest.bars import Bar, build_bars, load_minute_closes
from polyperps.backtest.harness import run_backtest
from polyperps.backtest.stats import (
    block_bootstrap_ci, chronological_split, hit_rate, max_drawdown, sharpe, turnover,
)
from polyperps.config import load_settings
from polyperps.exchange.types import SourceType
from polyperps.signal.sufficiency import BAR, check_dataset
from polyperps.signal.validation_log import append_record, evaluate_run, make_run_id
from polyperps.storage import db
from polyperps.strategies import GRIDS, build_strategy

_SOURCES = {"native": SourceType.POLYMARKET_REST, "hyperliquid": SourceType.PROXY_HYPERLIQUID}
_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


def _stats(res, *, bootstrap: bool, seed: int) -> dict:
    out = {
        "sharpe": sharpe(res.returns),
        "max_dd": str(max_drawdown([e for _, e in res.equity])),
        "hit_rate": hit_rate(res.trade_pnls),
        "turnover": str(turnover(res.fill_notionals, notional=BAR.notional_usd)),
        "n": len(res.returns),
        "fills": res.fills,
        "fills_unavailable": res.fills_unavailable,
        "final_equity": str(res.equity[-1][1]) if res.equity else "0",
    }
    if bootstrap:
        lo, hi = block_bootstrap_ci(res.returns, block_len=BAR.block_len, resamples=BAR.resamples,
                                    ci=float(BAR.bootstrap_ci), seed=seed)
        out["ci_lo"], out["ci_hi"] = lo, hi
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hypothesis", choices=sorted(GRIDS), required=True)
    ap.add_argument("--instrument", type=int, required=True)
    ap.add_argument("--source", choices=sorted(_SOURCES), required=True)
    ap.add_argument("--category", default="crypto")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    settings = load_settings()
    conn = db.connect(settings.db_path)
    now = datetime.now(timezone.utc)
    source = _SOURCES[args.source]
    try:
        fee = db.latest_fee(conn, args.category)
        if fee is None:
            raise SystemExit(f"no fee row for category {args.category!r}; run scripts/store_fees.py first")

        bars = build_bars(conn, args.instrument, source, start=_EPOCH, end=now)
        bars = [b for b in bars if b.open_ts >= _first_complete(bars)] if bars else []
        if len(bars) < 4 * BAR.block_len:
            raise SystemExit(f"only {len(bars)} bars for {args.instrument}/{source.value}; nothing to test")
        minute_closes = load_minute_closes(conn, args.instrument, source, start=bars[0].open_ts, end=now)

        proxy_closes = None
        if args.hypothesis == "h2":
            if source is not SourceType.POLYMARKET_REST:
                raise SystemExit("h2 trades the native leg: use --source native")
            proxy_bars = build_bars(conn, args.instrument, SourceType.PROXY_HYPERLIQUID,
                                    start=bars[0].open_ts, end=now)
            proxy_closes = {b.open_ts: b.close for b in proxy_bars if b.complete and b.close is not None}

        train, holdout = chronological_split(bars, holdout_fraction=BAR.holdout_fraction)

        trials = []
        for params in GRIDS[args.hypothesis]:
            strat = build_strategy(args.hypothesis, params, proxy_close_by_hour=proxy_closes)
            res = run_backtest(train, strat, minute_closes=minute_closes, taker_fee_rate=fee.taker_fee_rate,
                               warmup=strat.warmup)
            trials.append((params, _stats(res, bootstrap=False, seed=args.seed)))
        best_params, best_train = max(trials, key=lambda t: t[1]["sharpe"])

        strat = build_strategy(args.hypothesis, best_params, proxy_close_by_hour=proxy_closes)
        # holdout run is seeded with the tail of train so warm-up does not eat the holdout
        hold_input = train[-strat.warmup:] + holdout if strat.warmup else holdout
        hres = run_backtest(hold_input, strat, minute_closes=minute_closes, taker_fee_rate=fee.taker_fee_rate,
                            warmup=strat.warmup)
        hstats = _stats(hres, bootstrap=True, seed=args.seed)

        suff = check_dataset(conn, args.instrument, source, now=now)
        screened, passed = evaluate_run(source_type=source, sufficiency=suff,
                                        holdout_sharpe=hstats["sharpe"], ci_lo=hstats["ci_lo"], ci_hi=hstats["ci_hi"])
        record = {
            "run_id": make_run_id(now, args.hypothesis, args.instrument, source),
            "ts": now.isoformat(),
            "hypothesis": args.hypothesis,
            "instrument_id": args.instrument,
            "source_type": source.value,
            "params_chosen": best_params,
            "grid_tried": [p for p, _ in trials],
            "dataset": {"start": bars[0].open_ts.isoformat(), "end": bars[-1].open_ts.isoformat(),
                        "bars": len(bars), "complete_bars": sum(b.complete for b in bars),
                        "funding_periods": suff.funding_periods, "days": str(suff.days)},
            "fee_used": str(fee.taker_fee_rate), "fee_fetched_at": fee.fetched_at.isoformat(),
            "latency_s": BAR.latency_s, "impact_bps": str(BAR.impact_bps), "seed": args.seed,
            "train": best_train, "holdout": hstats,
            "sufficiency": {"met": suff.met, "shortfall": suff.shortfall},
            "screened": screened, "passed": passed,
        }
        append_record(record)
        print(f"{record['run_id']}: params={best_params} holdout_sharpe={hstats['sharpe']:.2f} "
              f"ci=({hstats['ci_lo']:.5f},{hstats['ci_hi']:.5f}) screened={screened} passed={passed}")
        if suff.shortfall:
            print(f"  sufficiency shortfall: {suff.shortfall}")
    finally:
        conn.close()


def _first_complete(bars: list[Bar]) -> datetime:
    for b in bars:
        if b.complete:
            return b.open_ts
    return bars[-1].open_ts


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Run a real screening on Hyperliquid (requires Task 5's 400-day backfill and Task 4's fee row)**

Run: `POLYPERPS_INSTRUMENT_IDS=6 .venv/Scripts/python scripts/run_backtest.py --hypothesis h1 --instrument 6 --source hyperliquid`
Expected: one line ending in `screened=<bool> passed=False`, and one new line in `polyperps/signal/validation_log.jsonl`. Whatever the result is, it is the result — record it. Then `--hypothesis h3 --source hyperliquid` (expect 0 fills: H3 is native-only; the record documents that), and `--hypothesis h2 --source native` (expect `SystemExit` about too few bars unless ≥ 96 native hours with both sources exist — record either outcome).

- [ ] **Step 7: Commit (including the log lines produced)**

```bash
git add polyperps/signal/validation_log.py polyperps/signal/validation_log.jsonl scripts/run_backtest.py pyproject.toml tests/test_validation_log.py
git commit -m "feat(phase1): validation log and run_backtest with pre-registered grid selection"
```

---

### Task 11: The two-key gate (spec §8.2) + docs

**Files:**
- Modify: `polyperps/signal/base.py`
- Create: `polyperps/signal/validated.json` (content `{}`)
- Modify: `README.md`, `docs/ops/eligibility-checklist.md`
- Test: `tests/test_signal_gate.py`

**Interfaces:**
- Consumes: `read_records(path=)`.
- Produces: `VALIDATED_PATH = Path(__file__).with_name("validated.json")`; `load_validated(*, validated_path: Path = VALIDATED_PATH, log_path: Path = LOG_PATH) -> bool`; `SIGNAL_VALIDATED: bool = load_validated()` at import. `generate_signal` unchanged.
- `load_validated` is `True` iff: file exists, parses as a JSON object, has non-empty string `run_id`, `approved_by`, `approved_at`, and `read_records(path=log_path)` contains a record with that `run_id` whose `passed is True`. Any exception → `False` with a `logging.warning` (never raise at import).

- [ ] **Step 1: Write the failing tests**

`tests/test_signal_gate.py`:

```python
import importlib
import json

import polyperps.signal.base as base
from polyperps.signal.base import load_validated
from polyperps.signal.validation_log import append_record


def _files(tmp_path, *, record=None, validated=None):
    log = tmp_path / "log.jsonl"
    val = tmp_path / "validated.json"
    if record is not None:
        append_record(record, path=log)
    if validated is not None:
        val.write_text(json.dumps(validated), encoding="utf-8")
    return log, val


PASSING = {"run_id": "r1", "passed": True, "source_type": "polymarket_rest"}
APPROVAL = {"run_id": "r1", "approved_by": "lockheng", "approved_at": "2026-11-05T00:00:00+00:00", "note": "ok"}


def test_default_module_flag_is_false():
    importlib.reload(base)
    assert base.SIGNAL_VALIDATED is False


def test_true_only_for_full_combination(tmp_path):
    log, val = _files(tmp_path, record=PASSING, validated=APPROVAL)
    assert load_validated(validated_path=val, log_path=log) is True


def test_missing_file_is_false(tmp_path):
    log, val = _files(tmp_path, record=PASSING)
    assert load_validated(validated_path=val, log_path=log) is False


def test_empty_object_is_false(tmp_path):
    log, val = _files(tmp_path, record=PASSING, validated={})
    assert load_validated(validated_path=val, log_path=log) is False


def test_unknown_run_id_is_false(tmp_path):
    log, val = _files(tmp_path, record=PASSING, validated={**APPROVAL, "run_id": "nope"})
    assert load_validated(validated_path=val, log_path=log) is False


def test_record_not_passed_is_false(tmp_path):
    log, val = _files(tmp_path, record={**PASSING, "passed": False}, validated=APPROVAL)
    assert load_validated(validated_path=val, log_path=log) is False


def test_screened_proxy_record_is_false(tmp_path):
    log, val = _files(tmp_path, record={"run_id": "r1", "passed": False, "screened": True,
                                        "source_type": "proxy_hyperliquid"}, validated=APPROVAL)
    assert load_validated(validated_path=val, log_path=log) is False


def test_no_approver_is_false(tmp_path):
    log, val = _files(tmp_path, record=PASSING, validated={**APPROVAL, "approved_by": ""})
    assert load_validated(validated_path=val, log_path=log) is False


def test_malformed_json_is_false_not_exception(tmp_path):
    log, val = _files(tmp_path, record=PASSING)
    val.write_text("{not json", encoding="utf-8")
    assert load_validated(validated_path=val, log_path=log) is False
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/Scripts/python -m pytest tests/test_signal_gate.py -v`
Expected: FAIL with `ImportError: cannot import name 'load_validated'`

- [ ] **Step 3: Rewrite `polyperps/signal/base.py`** and create `polyperps/signal/validated.json` containing exactly `{}`

```python
"""Signal interface and the Phase 1 gate.

SIGNAL_VALIDATED is the third conjunct of the live-order gate (polyperps.gates).
It is derived, never assigned by hand:

  True  iff  validated.json names a run_id
         AND that run_id exists in validation_log.jsonl with passed == True
             (which itself requires a native source and a met sufficiency bar)
         AND validated.json carries non-empty approved_by and approved_at.

Two keys: code writes the passing record; a human commits the approval.
Neither alone flips the flag. generate_signal stays unimplemented - choosing
which validated strategy runs live is a Phase 2 decision.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, NoReturn

from polyperps.signal.validation_log import LOG_PATH, read_records

log = logging.getLogger(__name__)

VALIDATED_PATH = Path(__file__).with_name("validated.json")


def load_validated(*, validated_path: Path = VALIDATED_PATH, log_path: Path = LOG_PATH) -> bool:
    try:
        if not validated_path.exists():
            return False
        approval = json.loads(validated_path.read_text(encoding="utf-8"))
        if not isinstance(approval, dict):
            return False
        run_id = approval.get("run_id")
        if not all(isinstance(approval.get(k), str) and approval.get(k) for k in ("run_id", "approved_by", "approved_at")):
            return False
        return any(r.get("run_id") == run_id and r.get("passed") is True for r in read_records(path=log_path))
    except Exception as exc:  # never crash an import over the gate file
        log.warning("validated.json unreadable (%s); SIGNAL_VALIDATED stays False", type(exc).__name__)
        return False


SIGNAL_VALIDATED: bool = load_validated()


def generate_signal(market_state: Any) -> NoReturn:
    """No strategy is wired to live execution. Phase 2 decides which validated one is."""
    raise NotImplementedError(
        "No validated signal is wired for execution. See polyperps/signal/validation_log.jsonl."
    )
```

- [ ] **Step 4: Run the gate tests and the existing gate tests**

Run: `.venv/Scripts/python -m pytest tests/test_signal_gate.py tests/test_gates.py -v`
Expected: 9 + 9 passed (`test_defaults_pass_when_flag_monkeypatched` still monkeypatches the module attribute, which still works).

- [ ] **Step 5: Update `README.md` and the checklist**

Append to `README.md`:

```markdown
## Phase 1 — signal research

Spec: `docs/superpowers/specs/2026-09-11-polyperps-phase1-design.md`. The
sufficiency bar is pre-registered in `polyperps/signal/sufficiency.py` (Strict:
native ≥60 days, ≥1,000 funding periods, 30 % holdout, OOS Sharpe ≥1.0 after
costs, bootstrap 95 % CI excluding zero). Native data cannot meet it before
~2026-11-02.

| Step | Command |
|------|---------|
| store fees (once, and after any fee change) | `scripts/store_fees.py` |
| proxy backfill | `scripts/backfill_hyperliquid.py --days 400 --map 6=BTC,7=ETH` |
| sufficiency re-check (monthly) | `scripts/sufficiency.py` |
| screen a hypothesis | `scripts/run_backtest.py --hypothesis h1 --instrument 6 --source hyperliquid` |
| confirm on native (after the bar is met) | `scripts/run_backtest.py --hypothesis h1 --instrument 6 --source native` |

Every run appends to `polyperps/signal/validation_log.jsonl` (committed).
`SIGNAL_VALIDATED` flips to `True` only when `polyperps/signal/validated.json`
names a `passed=True` native record **and** carries `approved_by`/`approved_at`
— a deliberate, reviewed commit by a human. No code path writes that file.
```

Append a row to the checklist's "Checks" list in `docs/ops/eligibility-checklist.md`:

```markdown
6. **Sufficiency re-check** - run `scripts/sufficiency.py`; record days/periods
   per instrument. Earliest possible native pass: ~2026-11-02.
```

- [ ] **Step 6: Full suite**

Run: `.venv/Scripts/python -m pytest -q`
Expected: 83 existing + 7+4+5+2+7+7+12+9+9+4+9 = 158 passed, no warnings. If the number differs, report what pytest prints; pass/fail is what matters.

- [ ] **Step 7: Commit**

```bash
git add polyperps/signal/base.py polyperps/signal/validated.json README.md docs/ops/eligibility-checklist.md tests/test_signal_gate.py
git commit -m "feat(phase1): two-key SIGNAL_VALIDATED gate derived from the validation log and a human approval"
```

---

## Phase 1 exit mapping

| Parent spec | Evidence |
|---|---|
| 1.0 bar written before 1.2 | Task 1 commit precedes Tasks 8–10 in history; `test_bar_values_are_pinned`. |
| 1.1 data pulled; shortfall logged not waived | Task 5 backfill counts; `scripts/sufficiency.py` output; `sufficiency.shortfall` in every log record. |
| 1.2 realistic execution, no look-ahead, OOS holdout, negatives kept | Harness tests (latency fill, costs, gap rule, `bars[:t+1]`); `chronological_split`; every run appended. |
| 1.3 stop if nothing clears | `passed=False` records and `validated.json == {}` mean `SIGNAL_VALIDATED` stays `False`; nothing ships. |
| Gate: code-enforced, native-only, manual | Task 11 `load_validated` + tests; `evaluate_run` refuses `passed` for proxy. |

## Self-review notes

- **Spec coverage:** §4.1→T5; §4.2→T6; §4.3→T2+T4; §5.1→T8 `strategy.py`; §5.2→T8 harness; §5.3→T7 costs; §5.4→T7 stats; §6→T9; §7→T1+T3; §8.1→T10; §8.2→T11; §9 error handling→T5 transient errors, T6 `complete=False`, T8 `fill_unavailable`, T7 bootstrap `ValueError`, T11 warning-not-raise; §10 tests→each task; §12 schedule→T3 script + T11 checklist row.
- **Type consistency:** `Bar` fields identical in T6/T8/T9 tests; `SufficiencyReport.shortfall` is `dict[str, str]` everywhere; `stats_clear_bar` takes floats and `evaluate_run` passes `hstats` floats; `run_backtest` kwargs match between T8, T9 tests and T10 script; `build_strategy` signature matches T9 and T10; `query_*` `source_type` kwarg matches T2/T3/T6.
- **Judgment calls recorded:** H2's proxy lookup is keyed by `history` timestamps (structural point-in-time); holdout runs are prefixed with the last `warmup` train bars so warm-up doesn't consume holdout; the `spread_bps` fallback on native hours without snapshots uses the proxy constant (an assumption either way, and stated); `run_backtest.py` trims leading incomplete bars only.
