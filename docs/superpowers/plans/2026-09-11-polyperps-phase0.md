# polyperps Phase 0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Phase 0 foundation of `polyperps` — a read-only, credential-free Polymarket perps market-data pipeline (client wrapper, rate limiter, filtered WS feed, SQLite storage, historical backfill), plus the structural safety gates and secret handling that every later phase depends on. No signal, no capital, no order placement.

**Architecture:** A thin async adapter (`exchange/client.py`) wraps `polymarket-client`'s `AsyncPublicClient` and converts SDK pydantic models into our own frozen dataclasses tagged with `source_type`. A pure-function filter layer rejects stale/insane ticks before anything is persisted. SQLite (polyweather's pattern) stores ticks, funding, books, candles, and feed health. `gates.py` implements the dual execution gate + `SIGNAL_VALIDATED` check from day one so Phase 2 cannot bypass it. `security/key_management.py` resolves secrets from systemd credentials → OS keyring → env (dev only), never plaintext files.

**Tech Stack:** Python 3.12, `polymarket-client==0.10.0` (pinned — perps API is experimental), `sqlite3` (stdlib), `keyring`, `pytest` + `pytest-asyncio`.

**Spec:** [`polyperps-implementation-plan.md`](../../../polyperps-implementation-plan.md) — Phase 0 rows 0.1–0.6 and "Non-negotiable conventions".

## Global Constraints

- Python `>=3.11` (SDK floor); develop on 3.12.
- `polymarket-client==0.10.0` pinned exactly. The README states: *"All Perps APIs are currently experimental … may change in any release"*. Bumps are a deliberate task, never `>=`.
- Dual execution gate: `EXECUTION_MODE` + `POLYMARKET_LIVE_TRADING=true`, no exceptions. Phase 0 adds the third check, `SIGNAL_VALIDATED`, structurally.
- No-fabrication: stubs raise `NotImplementedError`; unverified endpoints flagged explicitly; `source_type` provenance on every market-data record.
- Deployment target is EC2 via systemd, no Docker, no CloudWatch — secrets flow through systemd `LoadCredential=`, not the unit's `Environment=`.
- Signal logic, leverage ceilings, and asset kill-decisions never self-adjust.
- Secrets never appear in logs, `repr`, exceptions, or test fixtures.
- Prices/rates are `Decimal` end-to-end and stored as TEXT in SQLite — never `float`.
- Every datetime is timezone-aware UTC.

---

## Analysis of the roadmap (what changed and why)

These findings come from reading the SDK source at `Polymarket/py-sdk@main` (pushed 2026-09-10) and the `polyweather` reference code. Each one either corrects a roadmap assumption or fills a gap the roadmap left open.

| # | Roadmap says | What's actually true | Consequence in this plan |
|---|---|---|---|
| A1 | "Install `Polymarket/py-sdk`" | PyPI package is **`polymarket-client`** (0.10.0). The PyPI package named `polymarket` is an unrelated 2024 one-file script. | Task 1 pins `polymarket-client==0.10.0`. |
| A2 | 0.1 exit: "authenticated call to `/v1/account/balances`" | No such path is exposed. The SDK equivalent is `AsyncSecureClient.create(private_key=…)` → `open_perps_session()` → `PerpsSession.fetch_balances()`. | 0.1 exit criterion rewritten as "`scripts/check_auth.py` prints a balance tuple". Raw path flagged **unverified**. |
| A3 | 0.2: "rate limiter wrapping all calls" | The SDK has **no client-side limiter**. `rate_limit.py` only exposes `RateLimitUpdate` (server `Poly-RateLimit-*` headers, order/cancel responses only, with a `warning` flag for pre-enforcement mode). | Task 3 builds our own token bucket. Phase 2 must also register `on_rate_limit_update` and treat `warning=True` as a hard signal. |
| A4 | 0.5: "trading key scoped without withdrawal permission if API supports scoping" | Session keys (`SessionKeyKnownScope`) enumerate `ALL`, `CLOB`, `COMBOSRFQ` — **no perps scope**. Perps use **delegated credentials** (`PerpsCredentials`: proxy, private_key, secret, `expires_at`; default TTL 1 week; revocable via `revoke_perps_credentials`). `PerpsSession` has **no withdraw method** (`withdraw_from_perps` lives on `AsyncSecureClient`, which needs the wallet key). **But** resuming credentials via the public API still requires `AsyncSecureClient.create(private_key=…)`, so the wallet key must be on-host unless we construct `polymarket._internal.perps_session.PerpsSession` directly (private API). | Task 4 documents this as a **residual risk**, not a solved one. Mitigations: dedicated wallet holding only trading capital; key delivered via systemd `LoadCredential=`; short-TTL delegated creds. Constructing `PerpsSession` from stored creds only is recorded as a Phase 2 option, flagged experimental. |
| A5 | 0.3: staleness filtering on WS ticks | The WS ticker payload (`PerpsTickerUpdate`) carries **no timestamp**; the event envelope does (`event.timestamp`, `event.sequence`). REST `PerpsTicker.timestamp` is `Optional`. | `Tick` carries both `exchange_ts` (envelope) and `received_ts` (local clock). Staleness = `received_ts − exchange_ts`. REST ticks with no timestamp use `received_ts` and are distinguishable by `source_type=POLYMARKET_REST`. |
| A6 | 1.1: "thin native history … backfill from Hyperliquid/CEX" | Native history endpoints exist: `list_perps_funding_history`, `list_perps_candles` (intervals 1s…1w), `list_perps_trades`. All default to the **last 24h** unless `start`/`end` passed. | Task 9 backfills natively with explicit ranges. Proxy sources get their own `SourceType` values so a proxy-only edge can never be mistaken for a native one (spec Phase 1 gate). |
| A7 | 2.3: "exchange-side stop order at entry" | Supported: `PerpsSession.place_position_tp_sl()`, plus `arm_auto_cancel()` — a server-side **dead-man switch** that cancels orders if the bot stops heartbeating. | Not built in Phase 0; recorded as a Phase 2 requirement (auto-cancel is a stronger backstop than the roadmap assumed). |
| A8 | 2.2: "leverage cap below exchange max (20x/10x ceilings)" | `PerpsInstrument` exposes `max_leverage`, `risk_tiers`, `isolated_only`, `min_notional`, `funding_interval`. | `Instrument` type captures these now so Phase 2's guard reads exchange limits from the API rather than hardcoding. |
| A9 | 2.4: reconciliation after WS drop | `PerpsSession` emits `PerpsResyncEvent` on reconnect and tracks per-channel sequence gaps; public stream handles expose `dropped` counts. | Phase 2 reconciliation should consume these. Phase 0 feed logs `sequence` so gaps are visible in the data. |
| A10 | Phase 0 needs auth | Everything in 0.2–0.4 works on `AsyncPublicClient()` with **zero credentials**. Only 0.1 touches a key. | The 48h soak runs with no secret on the box. |
| A11 | polyweather "dual gate" | Implemented as a per-station `EXECUTION_MODE` dict in `executor.py` + `POLYMARKET_LIVE_TRADING` env check in `wallet_client.py`. | `gates.py` ports this per-instrument and adds `SIGNAL_VALIDATED` as the third conjunct. |
| A12 | polyweather "DB pattern" | `storage.py`: stdlib `sqlite3`, `CREATE TABLE IF NOT EXISTS` on connect, one file, composite primary keys, idempotent inserts. | Task 7 follows it exactly. |

**Out of scope for this plan (separate plans, in order):**
1. **Phase 1 research harness** — `backtest/harness.py`, sufficiency bar (1.0), hypothesis tests (1.2). Written after Phase 0 has ≥48h of native data.
2. **Phase 2 risk + execution** — gated on a passing Phase 1 result on record.
3. **Phase 2.5/3 deploy** — systemd unit, kill thresholds, live tracking.

Items that outlive this session and are handed to you: the 48h unattended soak (0.3), running `check_auth.py` with your key (0.1), and the jurisdiction check (0.6, checklist provided in Task 9).

---

## File structure

```
perpbot/                                  # repo root (git init here, branch phase-0)
├── pyproject.toml
├── .gitignore
├── README.md
├── docs/
│   ├── ops/eligibility-checklist.md      # 0.6 — recurring, human-run
│   └── superpowers/plans/…               # this plan
├── polyperps/
│   ├── __init__.py
│   ├── config.py                         # env-driven settings, no secrets
│   ├── gates.py                          # dual gate + SIGNAL_VALIDATED (all phases)
│   ├── exchange/
│   │   ├── __init__.py
│   │   ├── types.py                      # SourceType, Instrument, Tick, BookLevel, BookSnapshot, FundingObservation, Candle
│   │   ├── rate_limiter.py               # TokenBucket
│   │   └── client.py                     # ExchangeClient protocol + PolymarketPerpsClient (read-only)
│   ├── security/
│   │   ├── __init__.py
│   │   └── key_management.py             # systemd creds → keyring → env(dev); redaction
│   ├── data_ingest/
│   │   ├── __init__.py
│   │   ├── filters.py                    # SanityBounds, Rejection, check_tick (pure)
│   │   └── market_feed.py                # MarketFeed: stream → filter → callbacks → FeedHealth
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── db.py                         # schema + inserts + queries
│   │   └── gaps.py                       # find_gaps
│   └── signal/
│       ├── __init__.py
│       └── base.py                       # SIGNAL_VALIDATED = False; generate_signal raises
├── scripts/
│   ├── check_auth.py                     # 0.1 — you run this
│   ├── run_feed.py                       # 0.3 — 48h soak entrypoint
│   └── backfill.py                       # 0.4 — historical funding + candles
└── tests/
    ├── conftest.py
    ├── test_gates.py
    ├── test_types.py
    ├── test_rate_limiter.py
    ├── test_key_management.py
    ├── test_client.py
    ├── test_filters.py
    ├── test_storage.py
    ├── test_gaps.py
    └── test_market_feed.py
```

---

### Task 1: Project scaffold, execution gates, signal stub

**Files:**
- Create: `pyproject.toml`, `.gitignore`, `polyperps/__init__.py`, `polyperps/gates.py`, `polyperps/signal/__init__.py`, `polyperps/signal/base.py`, `tests/conftest.py`
- Test: `tests/test_gates.py`

**Interfaces:**
- Produces: `ExecutionMode` (StrEnum: `MANUAL_REVIEW`, `PAPER`, `AUTO`), `GateDecision(allowed: bool, reason: str)`, `live_orders_allowed(instrument_id: int, *, modes: Mapping[int, ExecutionMode], env: Mapping[str, str], signal_validated: bool) -> GateDecision`, `polyperps.signal.base.SIGNAL_VALIDATED: bool`, `generate_signal(market_state) -> NoReturn`.

- [ ] **Step 1: Initialise the repo and branch**

```bash
cd C:/Users/user/Downloads/perpbot
git init
git checkout -b phase-0
```

- [ ] **Step 2: Write `pyproject.toml`**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "polyperps"
version = "0.0.1"
description = "Polymarket perps trading bot - Phase 0 plumbing"
requires-python = ">=3.11"
dependencies = [
  # Perps APIs are experimental and may break in patch releases. Pin exactly.
  "polymarket-client==0.10.0",
  "keyring>=25",
]

[project.optional-dependencies]
dev = ["pytest>=8", "pytest-asyncio>=0.24"]

[tool.setuptools.packages.find]
include = ["polyperps*"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

- [ ] **Step 3: Write `.gitignore`**

```
__pycache__/
*.pyc
.venv/
*.egg-info/
data/
*.sqlite3
.env
.pytest_cache/
```

- [ ] **Step 4: Create the venv and install**

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -e ".[dev]"
```

Expected: `Successfully installed polymarket-client-0.10.0 …`. If `polymarket-client` fails to resolve, stop — do not substitute `polymarket`.

- [ ] **Step 5: Write the failing gate tests**

`tests/conftest.py`:

```python
import pytest


@pytest.fixture
def no_env():
    """An environment mapping with nothing set."""
    return {}
```

`tests/test_gates.py`:

```python
import pytest

from polyperps.gates import ExecutionMode, GateDecision, live_orders_allowed


def _decide(mode, env, validated):
    return live_orders_allowed(1, modes={1: mode}, env=env, signal_validated=validated)


def test_default_mode_is_manual_review_for_unknown_instrument():
    d = live_orders_allowed(
        99, modes={}, env={"POLYMARKET_LIVE_TRADING": "true"}, signal_validated=True
    )
    assert d.allowed is False
    assert "manual_review" in d.reason


def test_blocked_when_mode_not_auto():
    d = _decide(ExecutionMode.PAPER, {"POLYMARKET_LIVE_TRADING": "true"}, True)
    assert d.allowed is False
    assert "paper" in d.reason


def test_blocked_when_env_flag_missing():
    d = _decide(ExecutionMode.AUTO, {}, True)
    assert d.allowed is False
    assert "POLYMARKET_LIVE_TRADING" in d.reason


def test_blocked_when_env_flag_not_exactly_true():
    d = _decide(ExecutionMode.AUTO, {"POLYMARKET_LIVE_TRADING": "1"}, True)
    assert d.allowed is False


def test_blocked_when_signal_not_validated():
    d = _decide(ExecutionMode.AUTO, {"POLYMARKET_LIVE_TRADING": "true"}, False)
    assert d.allowed is False
    assert "SIGNAL_VALIDATED" in d.reason


def test_allowed_only_when_all_three_agree():
    d = _decide(ExecutionMode.AUTO, {"POLYMARKET_LIVE_TRADING": "true"}, True)
    assert d == GateDecision(allowed=True, reason="all gates passed")


def test_signal_stub_is_not_validated_and_raises():
    from polyperps.signal import base

    assert base.SIGNAL_VALIDATED is False
    with pytest.raises(NotImplementedError):
        base.generate_signal(market_state=None)
```

- [ ] **Step 6: Run tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_gates.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.gates'`

- [ ] **Step 7: Write `polyperps/__init__.py`** (empty file) and `polyperps/signal/__init__.py` (empty file).

- [ ] **Step 8: Write `polyperps/signal/base.py`**

```python
"""Signal interface. Phase 1 gate lives here.

SIGNAL_VALIDATED is the third conjunct of the live-order gate (see
polyperps.gates). It is False until a Phase 1 hypothesis passes the
pre-registered sufficiency bar *on native Polymarket data* and that result
is logged. Flipping it is a manual, reviewed change - never automated.
"""

from __future__ import annotations

from typing import Any, NoReturn

SIGNAL_VALIDATED: bool = False


def generate_signal(market_state: Any) -> NoReturn:
    """Phase 1 has not produced a validated signal. Nothing to run."""
    raise NotImplementedError(
        "No validated signal exists. Phase 1 must clear the sufficiency bar first."
    )
```

- [ ] **Step 9: Write `polyperps/gates.py`**

```python
"""Live-order gates. Ported from polyweather (executor.EXECUTION_MODE +
wallet_client._live_trading_enabled) with a third conjunct, SIGNAL_VALIDATED.

All three must agree before any real order is placed:
  1. per-instrument ExecutionMode == AUTO
  2. env POLYMARKET_LIVE_TRADING == "true" (exact, lowercase)
  3. polyperps.signal.base.SIGNAL_VALIDATED is True

Phase 0 has no order path; Phase 2's order_router must call
live_orders_allowed() and refuse on any False.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum


class ExecutionMode(StrEnum):
    MANUAL_REVIEW = "manual_review"
    PAPER = "paper"
    AUTO = "auto"


@dataclass(frozen=True, slots=True)
class GateDecision:
    allowed: bool
    reason: str


LIVE_ENV_VAR = "POLYMARKET_LIVE_TRADING"


def live_orders_allowed(
    instrument_id: int,
    *,
    modes: Mapping[int, ExecutionMode],
    env: Mapping[str, str],
    signal_validated: bool,
) -> GateDecision:
    mode = modes.get(instrument_id, ExecutionMode.MANUAL_REVIEW)
    if mode is not ExecutionMode.AUTO:
        return GateDecision(False, f"instrument {instrument_id} mode is {mode.value}, not auto")
    if env.get(LIVE_ENV_VAR) != "true":
        return GateDecision(False, f"{LIVE_ENV_VAR} is not exactly 'true'")
    if not signal_validated:
        return GateDecision(False, "SIGNAL_VALIDATED is False - Phase 1 has not cleared")
    return GateDecision(True, "all gates passed")
```

- [ ] **Step 10: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_gates.py -v`
Expected: 7 passed

- [ ] **Step 11: Commit**

```bash
git add pyproject.toml .gitignore polyperps tests
git commit -m "feat: scaffold polyperps with dual gate + SIGNAL_VALIDATED stub"
```

---

### Task 2: Exchange data types with provenance

**Files:**
- Create: `polyperps/exchange/__init__.py`, `polyperps/exchange/types.py`
- Test: `tests/test_types.py`

**Interfaces:**
- Produces:
  - `SourceType` (StrEnum): `POLYMARKET_WS`, `POLYMARKET_REST`, `PROXY_HYPERLIQUID`, `PROXY_CEX`, `SYNTHETIC_TEST`
  - `Instrument(instrument_id: int, symbol: str, category: str, funding_interval: str, max_leverage: int, price_decimals: int, quantity_decimals: int, min_notional: Decimal, isolated_only: bool)`
  - `Tick(instrument_id: int, mark_price: Decimal, index_price: Decimal, last_price: Decimal, funding_rate: Decimal, next_funding: datetime, exchange_ts: datetime, received_ts: datetime, source_type: SourceType, sequence: int | None = None)`
  - `BookLevel(price: Decimal, quantity: Decimal)`
  - `BookSnapshot(instrument_id: int, bids: tuple[BookLevel, ...], asks: tuple[BookLevel, ...], exchange_ts: datetime, received_ts: datetime, source_type: SourceType, sequence: int | None = None)`
  - `FundingObservation(instrument_id: int, funding_rate: Decimal, exchange_ts: datetime, received_ts: datetime, source_type: SourceType)`
  - `Candle(instrument_id: int, interval: str, open_ts: datetime, open: Decimal, high: Decimal, low: Decimal, close: Decimal, volume: Decimal, trades: int, received_ts: datetime, source_type: SourceType)`
  - All are `@dataclass(frozen=True, slots=True, kw_only=True)`; every datetime must be tz-aware or `ValueError` is raised.

- [ ] **Step 1: Write the failing tests**

`tests/test_types.py`:

```python
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from polyperps.exchange.types import (
    BookLevel,
    BookSnapshot,
    Candle,
    FundingObservation,
    SourceType,
    Tick,
)

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


def make_tick(**over):
    base = dict(
        instrument_id=1,
        mark_price=Decimal("100.5"),
        index_price=Decimal("100.4"),
        last_price=Decimal("100.6"),
        funding_rate=Decimal("0.0001"),
        next_funding=T0,
        exchange_ts=T0,
        received_ts=T0,
        source_type=SourceType.POLYMARKET_WS,
    )
    base.update(over)
    return Tick(**base)


def test_tick_is_frozen_and_keyword_only():
    t = make_tick()
    with pytest.raises(AttributeError):
        t.mark_price = Decimal("1")  # type: ignore[misc]
    with pytest.raises(TypeError):
        Tick(1)  # positional not allowed


def test_naive_datetime_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        make_tick(exchange_ts=datetime(2026, 9, 11, 12, 0))


def test_source_type_is_required_and_enum():
    with pytest.raises(TypeError):
        make_tick(source_type=None)
    assert SourceType.POLYMARKET_WS.value == "polymarket_ws"
    assert SourceType.PROXY_HYPERLIQUID.value == "proxy_hyperliquid"


def test_book_snapshot_levels_are_tuples():
    snap = BookSnapshot(
        instrument_id=1,
        bids=(BookLevel(price=Decimal("99"), quantity=Decimal("1")),),
        asks=(BookLevel(price=Decimal("101"), quantity=Decimal("2")),),
        exchange_ts=T0,
        received_ts=T0,
        source_type=SourceType.POLYMARKET_REST,
    )
    assert snap.bids[0].price == Decimal("99")
    assert snap.sequence is None


def test_funding_and_candle_construct():
    f = FundingObservation(
        instrument_id=1,
        funding_rate=Decimal("-0.0002"),
        exchange_ts=T0,
        received_ts=T0,
        source_type=SourceType.POLYMARKET_REST,
    )
    c = Candle(
        instrument_id=1,
        interval="1m",
        open_ts=T0,
        open=Decimal("1"),
        high=Decimal("2"),
        low=Decimal("0.5"),
        close=Decimal("1.5"),
        volume=Decimal("10"),
        trades=3,
        received_ts=T0,
        source_type=SourceType.POLYMARKET_REST,
    )
    assert f.funding_rate < 0
    assert c.close == Decimal("1.5")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_types.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.exchange'`

- [ ] **Step 3: Write `polyperps/exchange/__init__.py`** (empty) and `polyperps/exchange/types.py`

```python
"""Exchange-neutral market data types.

Every record carries source_type (provenance) and two timestamps:
  exchange_ts  - what the exchange said (WS envelope / REST field)
  received_ts  - our wall clock at receipt
Staleness is received_ts - exchange_ts. REST tickers may lack an exchange
timestamp; the adapter then sets exchange_ts = received_ts, and the
POLYMARKET_REST source_type is how a consumer knows that happened.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


class SourceType(StrEnum):
    POLYMARKET_WS = "polymarket_ws"
    POLYMARKET_REST = "polymarket_rest"
    PROXY_HYPERLIQUID = "proxy_hyperliquid"  # Phase 1 backfill only; never confirms an edge
    PROXY_CEX = "proxy_cex"  # Phase 1 backfill only; never confirms an edge
    SYNTHETIC_TEST = "synthetic_test"


def _require_aware(obj: object) -> None:
    for f in fields(obj):  # type: ignore[arg-type]
        v = getattr(obj, f.name)
        if isinstance(v, datetime) and (v.tzinfo is None or v.utcoffset() is None):
            raise ValueError(f"{type(obj).__name__}.{f.name} must be timezone-aware")


def _require_source_type(obj: object) -> None:
    st = getattr(obj, "source_type", None)
    if not isinstance(st, SourceType):
        raise TypeError(f"{type(obj).__name__}.source_type must be a SourceType, got {st!r}")


@dataclass(frozen=True, slots=True, kw_only=True)
class Instrument:
    instrument_id: int
    symbol: str
    category: str
    funding_interval: str
    max_leverage: int
    price_decimals: int
    quantity_decimals: int
    min_notional: Decimal
    isolated_only: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class Tick:
    instrument_id: int
    mark_price: Decimal
    index_price: Decimal
    last_price: Decimal
    funding_rate: Decimal
    next_funding: datetime
    exchange_ts: datetime
    received_ts: datetime
    source_type: SourceType
    sequence: int | None = None

    def __post_init__(self) -> None:
        _require_aware(self)
        _require_source_type(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class BookLevel:
    price: Decimal
    quantity: Decimal


@dataclass(frozen=True, slots=True, kw_only=True)
class BookSnapshot:
    instrument_id: int
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    exchange_ts: datetime
    received_ts: datetime
    source_type: SourceType
    sequence: int | None = None

    def __post_init__(self) -> None:
        _require_aware(self)
        _require_source_type(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class FundingObservation:
    instrument_id: int
    funding_rate: Decimal
    exchange_ts: datetime
    received_ts: datetime
    source_type: SourceType

    def __post_init__(self) -> None:
        _require_aware(self)
        _require_source_type(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class Candle:
    instrument_id: int
    interval: str
    open_ts: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trades: int
    received_ts: datetime
    source_type: SourceType

    def __post_init__(self) -> None:
        _require_aware(self)
        _require_source_type(self)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_types.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add polyperps/exchange tests/test_types.py
git commit -m "feat: exchange data types with source_type provenance"
```

---

### Task 3: Async token-bucket rate limiter

**Files:**
- Create: `polyperps/exchange/rate_limiter.py`
- Test: `tests/test_rate_limiter.py`

**Interfaces:**
- Produces: `TokenBucket(*, rate_per_sec: float, burst: int, clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], Awaitable[None]] = asyncio.sleep)` with `async acquire() -> float` (seconds waited).
- Rationale (A3): the SDK reports server-side rate-limit headers but does not throttle. Sustained polling in 0.2's exit criterion needs client-side pacing.

- [ ] **Step 1: Write the failing tests**

`tests/test_rate_limiter.py`:

```python
import pytest

from polyperps.exchange.rate_limiter import TokenBucket


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def make_sleep(clock, log):
    async def _sleep(seconds):
        log.append(seconds)
        clock.t += seconds

    return _sleep


async def test_burst_is_free():
    clock, log = FakeClock(), []
    b = TokenBucket(rate_per_sec=1.0, burst=2, clock=clock, sleep=make_sleep(clock, log))
    assert await b.acquire() == 0.0
    assert await b.acquire() == 0.0
    assert log == []


async def test_third_call_waits_for_refill():
    clock, log = FakeClock(), []
    b = TokenBucket(rate_per_sec=1.0, burst=2, clock=clock, sleep=make_sleep(clock, log))
    await b.acquire()
    await b.acquire()
    waited = await b.acquire()
    assert waited == pytest.approx(1.0)
    assert log == [pytest.approx(1.0)]


async def test_tokens_refill_with_time():
    clock, log = FakeClock(), []
    b = TokenBucket(rate_per_sec=2.0, burst=1, clock=clock, sleep=make_sleep(clock, log))
    await b.acquire()
    clock.t += 0.5  # one token refilled at 2/s
    assert await b.acquire() == 0.0


def test_rejects_bad_config():
    with pytest.raises(ValueError):
        TokenBucket(rate_per_sec=0, burst=1)
    with pytest.raises(ValueError):
        TokenBucket(rate_per_sec=1, burst=0)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_rate_limiter.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.exchange.rate_limiter'`

- [ ] **Step 3: Write `polyperps/exchange/rate_limiter.py`**

```python
"""Client-side token bucket.

polymarket-client does not throttle requests; it only surfaces server
Poly-RateLimit-* headers on order/cancel responses (polymarket.rate_limit).
This bucket paces *all* our calls so sustained polling never trips the server.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable


class TokenBucket:
    def __init__(
        self,
        *,
        rate_per_sec: float,
        burst: int,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if rate_per_sec <= 0:
            raise ValueError("rate_per_sec must be > 0")
        if burst < 1:
            raise ValueError("burst must be >= 1")
        self._rate = float(rate_per_sec)
        self._burst = float(burst)
        self._clock = clock
        self._sleep = sleep
        self._tokens = self._burst
        self._last = clock()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self._clock()
        self._tokens = min(self._burst, self._tokens + (now - self._last) * self._rate)
        self._last = now

    async def acquire(self) -> float:
        """Take one token, sleeping if none is available. Returns seconds waited."""
        async with self._lock:
            self._refill()
            waited = 0.0
            if self._tokens < 1.0:
                waited = (1.0 - self._tokens) / self._rate
                await self._sleep(waited)
                self._refill()
            self._tokens -= 1.0
            return waited
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_rate_limiter.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add polyperps/exchange/rate_limiter.py tests/test_rate_limiter.py
git commit -m "feat: async token-bucket rate limiter"
```

---

### Task 4: Secret resolution (systemd credentials → keyring → env)

**Files:**
- Create: `polyperps/security/__init__.py`, `polyperps/security/key_management.py`
- Test: `tests/test_key_management.py`

**Interfaces:**
- Produces:
  - `class SecretUnavailable(RuntimeError)`
  - `load_secret(name: str, *, env: Mapping[str, str] = os.environ, keyring_backend: KeyringLike | None = None) -> str`
  - `store_secret(name: str, value: str, *, keyring_backend: KeyringLike | None = None) -> None`
  - `redact(value: str) -> str` → `"<redacted:len=N>"`
  - `KeyringLike` Protocol: `get_password(service, name) -> str | None`, `set_password(service, name, value) -> None`
  - `SERVICE = "polyperps"`, `ALLOW_ENV_VAR = "POLYPERPS_ALLOW_ENV_SECRETS"`
- Resolution order: (1) file `$CREDENTIALS_DIRECTORY/<name>` (systemd `LoadCredential=`), (2) keyring, (3) `env[name]` **only if** `env["POLYPERPS_ALLOW_ENV_SECRETS"] == "1"`. Otherwise `SecretUnavailable`.

- [ ] **Step 1: Write the failing tests**

`tests/test_key_management.py`:

```python
import pytest

from polyperps.security.key_management import (
    ALLOW_ENV_VAR,
    SERVICE,
    SecretUnavailable,
    load_secret,
    redact,
    store_secret,
)


class FakeKeyring:
    def __init__(self):
        self.store = {}

    def get_password(self, service, name):
        return self.store.get((service, name))

    def set_password(self, service, name, value):
        self.store[(service, name)] = value


def test_systemd_credentials_dir_wins(tmp_path):
    (tmp_path / "POLYMARKET_PRIVATE_KEY").write_text("0xfromsystemd\n")
    kr = FakeKeyring()
    kr.set_password(SERVICE, "POLYMARKET_PRIVATE_KEY", "0xfromkeyring")
    env = {"CREDENTIALS_DIRECTORY": str(tmp_path), "POLYMARKET_PRIVATE_KEY": "0xfromenv"}
    assert load_secret("POLYMARKET_PRIVATE_KEY", env=env, keyring_backend=kr) == "0xfromsystemd"


def test_keyring_used_when_no_credentials_dir():
    kr = FakeKeyring()
    kr.set_password(SERVICE, "POLYMARKET_PRIVATE_KEY", "0xfromkeyring")
    assert load_secret("POLYMARKET_PRIVATE_KEY", env={}, keyring_backend=kr) == "0xfromkeyring"


def test_env_refused_unless_explicitly_allowed():
    env = {"POLYMARKET_PRIVATE_KEY": "0xfromenv"}
    with pytest.raises(SecretUnavailable) as exc:
        load_secret("POLYMARKET_PRIVATE_KEY", env=env, keyring_backend=FakeKeyring())
    assert "0xfromenv" not in str(exc.value)


def test_env_allowed_with_dev_flag():
    env = {"POLYMARKET_PRIVATE_KEY": "0xfromenv", ALLOW_ENV_VAR: "1"}
    assert load_secret("POLYMARKET_PRIVATE_KEY", env=env, keyring_backend=FakeKeyring()) == "0xfromenv"


def test_missing_everywhere_raises_without_leaking():
    with pytest.raises(SecretUnavailable, match="POLYMARKET_PRIVATE_KEY"):
        load_secret("POLYMARKET_PRIVATE_KEY", env={}, keyring_backend=FakeKeyring())


def test_store_secret_round_trip():
    kr = FakeKeyring()
    store_secret("X", "secret-value", keyring_backend=kr)
    assert load_secret("X", env={}, keyring_backend=kr) == "secret-value"


def test_redact_never_echoes_value():
    out = redact("0xdeadbeef")
    assert "dead" not in out
    assert out == "<redacted:len=10>"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_key_management.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.security'`

- [ ] **Step 3: Write `polyperps/security/__init__.py`** (empty) and `polyperps/security/key_management.py`

```python
"""Secret resolution. Nothing here ever logs, reprs, or raises a secret value.

Resolution order:
  1. $CREDENTIALS_DIRECTORY/<name>   - systemd LoadCredential= (EC2 target).
     Put secrets in /etc/credstore/<name> (root:root 0600) and add
     LoadCredential=<name>:/etc/credstore/<name> to the unit. They never
     appear in the unit file, the environment, or `systemctl show`.
  2. OS keyring (service "polyperps")  - developer workstations.
  3. Environment variable               - ONLY if POLYPERPS_ALLOW_ENV_SECRETS=1.
     Dev convenience. Never set that flag on the trading host.

RESIDUAL RISK (spec 0.5) - read before deploying:
  Polymarket perps auth uses delegated credentials (polymarket.models.perps
  PerpsCredentials: proxy/private_key/secret, default TTL 1 week, revocable
  via AsyncSecureClient.revoke_perps_credentials). The PerpsSession object
  has no withdrawal method, so a leaked *delegated* credential can trade
  but not withdraw. However the public SDK API only opens/resumes a session
  through AsyncSecureClient.create(private_key=<wallet key>), so the wallet
  key is still on-host. Mitigations, in order of importance:
    a. Dedicated wallet that holds only the capital you are willing to lose.
    b. Deliver the wallet key via systemd LoadCredential=, never env/.env.
    c. Short-TTL delegated credentials (expires_in=timedelta(days=1)).
  Phase 2 may evaluate constructing polymarket._internal.perps_session.
  PerpsSession directly from stored PerpsCredentials so the runtime process
  holds no wallet key at all - that is private SDK API and must be re-verified
  on every SDK bump. There is NO perps-scoped session key in the SDK today
  (SessionKeyKnownScope = ALL | CLOB | COMBOSRFQ).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Protocol

SERVICE = "polyperps"
ALLOW_ENV_VAR = "POLYPERPS_ALLOW_ENV_SECRETS"
CREDENTIALS_DIR_VAR = "CREDENTIALS_DIRECTORY"


class SecretUnavailable(RuntimeError):
    pass


class KeyringLike(Protocol):
    def get_password(self, service: str, name: str) -> str | None: ...
    def set_password(self, service: str, name: str, value: str) -> None: ...


def redact(value: str) -> str:
    return f"<redacted:len={len(value)}>"


def _default_keyring() -> KeyringLike:
    import keyring  # imported lazily so tests never touch a real backend

    return keyring


def _from_credentials_dir(name: str, env: Mapping[str, str]) -> str | None:
    directory = env.get(CREDENTIALS_DIR_VAR)
    if not directory:
        return None
    path = Path(directory) / name
    if not path.is_file():
        return None
    return path.read_text(encoding="utf-8").strip()


def load_secret(
    name: str,
    *,
    env: Mapping[str, str] = os.environ,
    keyring_backend: KeyringLike | None = None,
) -> str:
    value = _from_credentials_dir(name, env)
    if value:
        return value

    kr = keyring_backend if keyring_backend is not None else _default_keyring()
    try:
        value = kr.get_password(SERVICE, name)
    except Exception:  # no backend available on a headless host is normal
        value = None
    if value:
        return value

    if env.get(ALLOW_ENV_VAR) == "1" and env.get(name):
        return env[name]

    raise SecretUnavailable(
        f"secret {name!r} not found in {CREDENTIALS_DIR_VAR}, keyring service "
        f"{SERVICE!r}, or env (env requires {ALLOW_ENV_VAR}=1)"
    )


def store_secret(name: str, value: str, *, keyring_backend: KeyringLike | None = None) -> None:
    kr = keyring_backend if keyring_backend is not None else _default_keyring()
    kr.set_password(SERVICE, name, value)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_key_management.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add polyperps/security tests/test_key_management.py
git commit -m "feat: secret resolution via systemd creds, keyring, or dev env"
```

---

### Task 5: Read-only exchange client adapter + auth check script

**Files:**
- Create: `polyperps/exchange/client.py`, `scripts/check_auth.py`
- Test: `tests/test_client.py`

**Interfaces:**
- Consumes: `TokenBucket.acquire()`, types from Task 2, `load_secret` from Task 4.
- Produces:
  - `class ExchangeClient(Protocol)`: `fetch_instruments() -> tuple[Instrument, ...]`, `fetch_ticker(instrument_id: int) -> Tick`, `fetch_book(instrument_id: int, *, depth: int = 100) -> BookSnapshot`, `fetch_funding_history(instrument_id: int, *, start: datetime, end: datetime) -> list[FundingObservation]`, `fetch_candles(instrument_id: int, *, interval: str, start: datetime, end: datetime) -> list[Candle]`, `stream_ticks(instrument_ids: Sequence[int]) -> AsyncIterator[Tick]`, `close() -> None`. All `async` except `stream_ticks` which is an async generator.
  - `class PolymarketPerpsClient(ExchangeClient)`: `__init__(sdk: Any, *, limiter: TokenBucket, clock: Callable[[], datetime] = _utcnow)`; `@classmethod create_public(*, rate_per_sec: float = 5.0, burst: int = 10) -> PolymarketPerpsClient`.
  - Pure converters (tested directly): `tick_from_rest(t, received_ts) -> Tick`, `tick_from_event(event, received_ts) -> Tick`, `book_from_rest(b, received_ts) -> BookSnapshot`, `funding_from_rest(instrument_id, fr, received_ts) -> FundingObservation`, `candle_from_rest(instrument_id, interval, c, received_ts) -> Candle`, `instrument_from_rest(i) -> Instrument`.
- SDK facts used (verified against `Polymarket/py-sdk@main`, 2026-09-10): `AsyncPublicClient.fetch_perps_instruments()`, `.fetch_perps_ticker(instrument_id=)`, `.fetch_perps_book(instrument_id=, depth=)` (depth ∈ {10,100,500,1000}), `.list_perps_funding_history(instrument_id=, start=, end=)` and `.list_perps_candles(instrument_id=, interval=, start=, end=)` returning `AsyncPaginator` with `.iter_items()`, `.subscribe([PerpsTickersSpec(instrument_id=…)])` returning an async-iterable handle of `PerpsTickerEvent(topic="perps.tickers", timestamp, sequence, payload=PerpsTickerUpdate)`. `PerpsInstrument.id` is the instrument id (aliased from `instrument_id`).

- [ ] **Step 1: Write the failing tests**

`tests/test_client.py`:

```python
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from polyperps.exchange.client import (
    PolymarketPerpsClient,
    book_from_rest,
    candle_from_rest,
    funding_from_rest,
    instrument_from_rest,
    tick_from_event,
    tick_from_rest,
)
from polyperps.exchange.rate_limiter import TokenBucket
from polyperps.exchange.types import SourceType

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)
RX = T0 + timedelta(milliseconds=50)


def rest_ticker(ts=T0):
    return SimpleNamespace(
        instrument_id=7, symbol="BTC-PERP", index_price=Decimal("100"), mark_price=Decimal("101"),
        last_price=Decimal("100.5"), mid_price=Decimal("100.5"), open_interest=Decimal("5"),
        funding_rate=Decimal("0.0001"), next_funding=T0 + timedelta(hours=1), timestamp=ts,
    )


def ws_event(seq=10):
    payload = SimpleNamespace(
        instrument_id=7, index_price=Decimal("100"), mark_price=Decimal("101"),
        last_price=Decimal("100.5"), mid_price=Decimal("100.5"), open_interest=Decimal("5"),
        funding_rate=Decimal("0.0001"), next_funding=T0 + timedelta(hours=1),
    )
    return SimpleNamespace(topic="perps.tickers", type="ticker", channel="c", timestamp=T0,
                           sequence=seq, payload=payload)


def test_tick_from_rest_uses_exchange_timestamp():
    t = tick_from_rest(rest_ticker(), RX)
    assert t.instrument_id == 7
    assert t.exchange_ts == T0 and t.received_ts == RX
    assert t.source_type is SourceType.POLYMARKET_REST
    assert t.sequence is None


def test_tick_from_rest_without_timestamp_falls_back_to_received():
    t = tick_from_rest(rest_ticker(ts=None), RX)
    assert t.exchange_ts == RX


def test_tick_from_event_uses_envelope_timestamp_and_sequence():
    t = tick_from_event(ws_event(seq=42), RX)
    assert t.exchange_ts == T0 and t.sequence == 42
    assert t.source_type is SourceType.POLYMARKET_WS
    assert t.mark_price == Decimal("101")


def test_book_from_rest():
    b = SimpleNamespace(
        instrument_id=7,
        bids=(SimpleNamespace(price=Decimal("99"), quantity=Decimal("1")),),
        asks=(SimpleNamespace(price=Decimal("102"), quantity=Decimal("3")),),
        timestamp=T0, sequence=5,
    )
    snap = book_from_rest(b, RX)
    assert snap.bids[0].quantity == Decimal("1") and snap.asks[0].price == Decimal("102")
    assert snap.sequence == 5 and snap.source_type is SourceType.POLYMARKET_REST


def test_funding_and_candle_and_instrument_from_rest():
    f = funding_from_rest(7, SimpleNamespace(funding_rate=Decimal("-0.0003"), timestamp=T0), RX)
    assert f.instrument_id == 7 and f.funding_rate == Decimal("-0.0003")
    c = candle_from_rest(7, "1m", SimpleNamespace(
        timestamp=T0, open=Decimal("1"), high=Decimal("2"), low=Decimal("0.5"),
        close=Decimal("1.5"), volume=Decimal("9"), trades=4), RX)
    assert c.interval == "1m" and c.open_ts == T0 and c.trades == 4
    i = instrument_from_rest(SimpleNamespace(
        id=7, symbol="BTC-PERP", category="crypto", funding_interval="1h", max_leverage=20,
        price_decimals=2, quantity_decimals=4, min_notional=Decimal("10"), isolated_only=False))
    assert i.instrument_id == 7 and i.max_leverage == 20


class FakePaginator:
    def __init__(self, items):
        self._items = items

    async def iter_items(self):
        for it in self._items:
            yield it


class FakeHandle:
    def __init__(self, events):
        self._events = events
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.closed = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._events:
            raise StopAsyncIteration
        return self._events.pop(0)


class FakeSdk:
    def __init__(self):
        self.calls = []
        self.handle = FakeHandle([ws_event(1), SimpleNamespace(topic="perps.bbo"), ws_event(2)])
        self.closed = False

    async def fetch_perps_instruments(self):
        self.calls.append("instruments")
        return (SimpleNamespace(id=7, symbol="BTC-PERP", category="crypto", funding_interval="1h",
                                max_leverage=20, price_decimals=2, quantity_decimals=4,
                                min_notional=Decimal("10"), isolated_only=False),)

    async def fetch_perps_ticker(self, *, instrument_id):
        self.calls.append(("ticker", instrument_id))
        return rest_ticker()

    async def fetch_perps_book(self, *, instrument_id, depth):
        self.calls.append(("book", instrument_id, depth))
        return SimpleNamespace(instrument_id=instrument_id, bids=(), asks=(), timestamp=T0, sequence=1)

    def list_perps_funding_history(self, *, instrument_id, start, end):
        self.calls.append(("funding", instrument_id, start, end))
        return FakePaginator([SimpleNamespace(funding_rate=Decimal("0.0001"), timestamp=T0)])

    def list_perps_candles(self, *, instrument_id, interval, start, end):
        self.calls.append(("candles", instrument_id, interval))
        return FakePaginator([SimpleNamespace(timestamp=T0, open=Decimal("1"), high=Decimal("1"),
                                              low=Decimal("1"), close=Decimal("1"),
                                              volume=Decimal("0"), trades=0)])

    async def subscribe(self, specs):
        self.calls.append(("subscribe", [s.instrument_id for s in specs]))
        return self.handle

    async def close(self):
        self.closed = True


class CountingBucket(TokenBucket):
    def __init__(self):
        super().__init__(rate_per_sec=1000, burst=1000)
        self.acquired = 0

    async def acquire(self):
        self.acquired += 1
        return 0.0


@pytest.fixture
def client():
    sdk = FakeSdk()
    bucket = CountingBucket()
    return PolymarketPerpsClient(sdk, limiter=bucket, clock=lambda: RX), sdk, bucket


async def test_rest_calls_go_through_limiter_and_convert(client):
    c, sdk, bucket = client
    inst = await c.fetch_instruments()
    tick = await c.fetch_ticker(7)
    book = await c.fetch_book(7, depth=10)
    fund = await c.fetch_funding_history(7, start=T0 - timedelta(days=1), end=T0)
    candles = await c.fetch_candles(7, interval="1m", start=T0 - timedelta(hours=1), end=T0)
    assert inst[0].symbol == "BTC-PERP"
    assert tick.received_ts == RX
    assert book.instrument_id == 7 and ("book", 7, 10) in sdk.calls
    assert fund[0].source_type is SourceType.POLYMARKET_REST
    assert candles[0].interval == "1m"
    assert bucket.acquired == 5


async def test_stream_ticks_filters_to_ticker_events_and_closes(client):
    c, sdk, _ = client
    ticks = [t async for t in c.stream_ticks([7])]
    assert [t.sequence for t in ticks] == [1, 2]
    assert ("subscribe", [7]) in sdk.calls
    assert sdk.handle.closed is True


async def test_close_closes_sdk(client):
    c, sdk, _ = client
    await c.close()
    assert sdk.closed is True


def test_no_trading_methods_exist_in_phase0():
    for name in ("place_order", "cancel_order", "update_leverage", "withdraw"):
        assert not hasattr(PolymarketPerpsClient, name)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_client.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.exchange.client'`

- [ ] **Step 3: Write `polyperps/exchange/client.py`**

```python
"""Read-only adapter over polymarket-client's AsyncPublicClient.

Clean interface boundary: nothing outside this module imports `polymarket`.
Swapping the venue (or a future NautilusTrader adapter, spec 2.0) means
re-implementing ExchangeClient here and nowhere else.

Phase 0 is read-only by construction: there are no order/cancel/leverage/
withdraw methods on this class. Phase 2 adds a separate authenticated
trading client whose every write path calls polyperps.gates first.

SDK surface used (verified against Polymarket/py-sdk main, 2026-09-10; all
Perps APIs are marked experimental - re-verify on every version bump):
  AsyncPublicClient.fetch_perps_instruments / fetch_perps_ticker /
  fetch_perps_book / list_perps_funding_history / list_perps_candles /
  subscribe(PerpsTickersSpec)
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Sequence
from datetime import datetime, timezone
from typing import Any, Protocol

from polyperps.exchange.rate_limiter import TokenBucket
from polyperps.exchange.types import (
    BookLevel,
    BookSnapshot,
    Candle,
    FundingObservation,
    Instrument,
    SourceType,
    Tick,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# --- pure converters (SDK model -> our types) -------------------------------


def instrument_from_rest(i: Any) -> Instrument:
    return Instrument(
        instrument_id=int(i.id),
        symbol=i.symbol,
        category=str(i.category),
        funding_interval=i.funding_interval,
        max_leverage=int(i.max_leverage),
        price_decimals=int(i.price_decimals),
        quantity_decimals=int(i.quantity_decimals),
        min_notional=i.min_notional,
        isolated_only=bool(i.isolated_only),
    )


def tick_from_rest(t: Any, received_ts: datetime) -> Tick:
    # PerpsTicker.timestamp is Optional. If absent we record received_ts;
    # source_type=POLYMARKET_REST tells consumers the exchange_ts may be local.
    return Tick(
        instrument_id=int(t.instrument_id),
        mark_price=t.mark_price,
        index_price=t.index_price,
        last_price=t.last_price,
        funding_rate=t.funding_rate,
        next_funding=t.next_funding,
        exchange_ts=t.timestamp if t.timestamp is not None else received_ts,
        received_ts=received_ts,
        source_type=SourceType.POLYMARKET_REST,
        sequence=None,
    )


def tick_from_event(event: Any, received_ts: datetime) -> Tick:
    # PerpsTickerUpdate (payload) has no timestamp; the envelope does.
    p = event.payload
    return Tick(
        instrument_id=int(p.instrument_id),
        mark_price=p.mark_price,
        index_price=p.index_price,
        last_price=p.last_price,
        funding_rate=p.funding_rate,
        next_funding=p.next_funding,
        exchange_ts=event.timestamp,
        received_ts=received_ts,
        source_type=SourceType.POLYMARKET_WS,
        sequence=int(event.sequence),
    )


def book_from_rest(b: Any, received_ts: datetime) -> BookSnapshot:
    return BookSnapshot(
        instrument_id=int(b.instrument_id),
        bids=tuple(BookLevel(price=lvl.price, quantity=lvl.quantity) for lvl in b.bids),
        asks=tuple(BookLevel(price=lvl.price, quantity=lvl.quantity) for lvl in b.asks),
        exchange_ts=b.timestamp,
        received_ts=received_ts,
        source_type=SourceType.POLYMARKET_REST,
        sequence=int(b.sequence) if b.sequence is not None else None,
    )


def funding_from_rest(instrument_id: int, fr: Any, received_ts: datetime) -> FundingObservation:
    return FundingObservation(
        instrument_id=instrument_id,
        funding_rate=fr.funding_rate,
        exchange_ts=fr.timestamp,
        received_ts=received_ts,
        source_type=SourceType.POLYMARKET_REST,
    )


def candle_from_rest(instrument_id: int, interval: str, c: Any, received_ts: datetime) -> Candle:
    return Candle(
        instrument_id=instrument_id,
        interval=interval,
        open_ts=c.timestamp,
        open=c.open,
        high=c.high,
        low=c.low,
        close=c.close,
        volume=c.volume,
        trades=int(c.trades),
        received_ts=received_ts,
        source_type=SourceType.POLYMARKET_REST,
    )


# --- interface ---------------------------------------------------------------


class ExchangeClient(Protocol):
    async def fetch_instruments(self) -> tuple[Instrument, ...]: ...
    async def fetch_ticker(self, instrument_id: int) -> Tick: ...
    async def fetch_book(self, instrument_id: int, *, depth: int = 100) -> BookSnapshot: ...
    async def fetch_funding_history(
        self, instrument_id: int, *, start: datetime, end: datetime
    ) -> list[FundingObservation]: ...
    async def fetch_candles(
        self, instrument_id: int, *, interval: str, start: datetime, end: datetime
    ) -> list[Candle]: ...
    def stream_ticks(self, instrument_ids: Sequence[int]) -> AsyncIterator[Tick]: ...
    async def close(self) -> None: ...


# --- Polymarket implementation ----------------------------------------------


class PolymarketPerpsClient:
    def __init__(
        self,
        sdk: Any,
        *,
        limiter: TokenBucket,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._sdk = sdk
        self._limiter = limiter
        self._clock = clock

    @classmethod
    def create_public(cls, *, rate_per_sec: float = 5.0, burst: int = 10) -> PolymarketPerpsClient:
        from polymarket import AsyncPublicClient

        return cls(AsyncPublicClient(), limiter=TokenBucket(rate_per_sec=rate_per_sec, burst=burst))

    async def fetch_instruments(self) -> tuple[Instrument, ...]:
        await self._limiter.acquire()
        raw = await self._sdk.fetch_perps_instruments()
        return tuple(instrument_from_rest(i) for i in raw)

    async def fetch_ticker(self, instrument_id: int) -> Tick:
        await self._limiter.acquire()
        raw = await self._sdk.fetch_perps_ticker(instrument_id=instrument_id)
        return tick_from_rest(raw, self._clock())

    async def fetch_book(self, instrument_id: int, *, depth: int = 100) -> BookSnapshot:
        await self._limiter.acquire()
        raw = await self._sdk.fetch_perps_book(instrument_id=instrument_id, depth=depth)
        return book_from_rest(raw, self._clock())

    async def fetch_funding_history(
        self, instrument_id: int, *, start: datetime, end: datetime
    ) -> list[FundingObservation]:
        await self._limiter.acquire()
        pager = self._sdk.list_perps_funding_history(instrument_id=instrument_id, start=start, end=end)
        now = self._clock()
        return [funding_from_rest(instrument_id, fr, now) async for fr in pager.iter_items()]

    async def fetch_candles(
        self, instrument_id: int, *, interval: str, start: datetime, end: datetime
    ) -> list[Candle]:
        await self._limiter.acquire()
        pager = self._sdk.list_perps_candles(
            instrument_id=instrument_id, interval=interval, start=start, end=end
        )
        now = self._clock()
        return [candle_from_rest(instrument_id, interval, c, now) async for c in pager.iter_items()]

    async def stream_ticks(self, instrument_ids: Sequence[int]) -> AsyncIterator[Tick]:
        from polymarket.streams import PerpsTickersSpec

        specs = [PerpsTickersSpec(instrument_id=i) for i in instrument_ids]
        handle = await self._sdk.subscribe(specs)
        async with handle:
            async for event in handle:
                if getattr(event, "topic", None) != "perps.tickers":
                    continue
                yield tick_from_event(event, self._clock())

    async def close(self) -> None:
        await self._sdk.close()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_client.py -v`
Expected: 10 passed

- [ ] **Step 5: Write `scripts/check_auth.py`** (spec 0.1 — the user runs this; it is not part of the test suite and places no orders)

```python
"""Spec 0.1: prove an authenticated perps call works.

Run by a human with their own key. Places no orders. Prints balances and
the delegated credential's expiry; never prints any secret.

    POLYPERPS_ALLOW_ENV_SECRETS=1 POLYMARKET_PRIVATE_KEY=0x... \
        .venv/Scripts/python scripts/check_auth.py

The spec names "/v1/account/balances"; that raw path is UNVERIFIED. The
SDK-level equivalent used here is PerpsSession.fetch_balances().
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from polyperps.security.key_management import load_secret, redact


async def main() -> None:
    from polymarket import AsyncSecureClient

    pk = load_secret("POLYMARKET_PRIVATE_KEY")
    print(f"loaded POLYMARKET_PRIVATE_KEY {redact(pk)}")
    client = await AsyncSecureClient.create(private_key=pk)
    try:
        session = await client.open_perps_session(
            expires_in=timedelta(hours=1), label="polyperps-auth-check"
        )
        try:
            creds = session.credentials
            print(f"delegated proxy={creds.proxy} expires_at={creds.expires_at.isoformat()}")
            balances = await session.fetch_balances()
            print(f"fetch_balances -> {len(balances)} entries")
            for b in balances:
                print(f"  {b!r}")
        finally:
            await session.close()
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 6: Commit**

```bash
git add polyperps/exchange/client.py scripts/check_auth.py tests/test_client.py
git commit -m "feat: read-only Polymarket perps client adapter and auth check script"
```

---

### Task 6: Tick sanity and staleness filters

**Files:**
- Create: `polyperps/data_ingest/__init__.py`, `polyperps/data_ingest/filters.py`
- Test: `tests/test_filters.py`

**Interfaces:**
- Consumes: `Tick` from Task 2.
- Produces:
  - `SanityBounds(*, max_staleness: timedelta, max_abs_funding_rate: Decimal, max_mark_index_divergence: Decimal, max_jump: Decimal)` — divergence/jump are fractions (`Decimal("0.05")` = 5%).
  - `Rejection(reason: str, detail: str)` with reasons: `"stale"`, `"non_positive_price"`, `"mark_index_divergence"`, `"funding_out_of_bounds"`, `"price_jump"`, `"out_of_order"`.
  - `check_tick(tick: Tick, *, bounds: SanityBounds, previous: Tick | None) -> Rejection | None` — `None` means accept.
  - `DEFAULT_BOUNDS = SanityBounds(max_staleness=timedelta(seconds=5), max_abs_funding_rate=Decimal("0.01"), max_mark_index_divergence=Decimal("0.05"), max_jump=Decimal("0.10"))`

- [ ] **Step 1: Write the failing tests**

`tests/test_filters.py`:

```python
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.data_ingest.filters import DEFAULT_BOUNDS, SanityBounds, check_tick
from polyperps.exchange.types import SourceType, Tick

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


def tick(**over):
    base = dict(
        instrument_id=1, mark_price=Decimal("100"), index_price=Decimal("100"),
        last_price=Decimal("100"), funding_rate=Decimal("0.0001"), next_funding=T0,
        exchange_ts=T0, received_ts=T0, source_type=SourceType.POLYMARKET_WS, sequence=1,
    )
    base.update(over)
    return Tick(**base)


def test_clean_tick_accepted():
    assert check_tick(tick(), bounds=DEFAULT_BOUNDS, previous=None) is None


def test_stale_tick_rejected():
    r = check_tick(tick(received_ts=T0 + timedelta(seconds=6)), bounds=DEFAULT_BOUNDS, previous=None)
    assert r is not None and r.reason == "stale"


def test_non_positive_price_rejected():
    r = check_tick(tick(mark_price=Decimal("0")), bounds=DEFAULT_BOUNDS, previous=None)
    assert r is not None and r.reason == "non_positive_price"


def test_mark_index_divergence_rejected():
    r = check_tick(tick(mark_price=Decimal("110")), bounds=DEFAULT_BOUNDS, previous=None)
    assert r is not None and r.reason == "mark_index_divergence"


def test_funding_out_of_bounds_rejected():
    r = check_tick(tick(funding_rate=Decimal("0.5")), bounds=DEFAULT_BOUNDS, previous=None)
    assert r is not None and r.reason == "funding_out_of_bounds"


def test_price_jump_vs_previous_rejected():
    prev = tick(sequence=1)
    nxt = tick(mark_price=Decimal("120"), index_price=Decimal("120"), sequence=2,
               exchange_ts=T0 + timedelta(seconds=1), received_ts=T0 + timedelta(seconds=1))
    r = check_tick(nxt, bounds=DEFAULT_BOUNDS, previous=prev)
    assert r is not None and r.reason == "price_jump"


def test_out_of_order_sequence_rejected():
    prev = tick(sequence=5)
    r = check_tick(tick(sequence=4), bounds=DEFAULT_BOUNDS, previous=prev)
    assert r is not None and r.reason == "out_of_order"


def test_sequence_not_compared_across_sources():
    prev = tick(sequence=5)
    rest = tick(sequence=None, source_type=SourceType.POLYMARKET_REST)
    assert check_tick(rest, bounds=DEFAULT_BOUNDS, previous=prev) is None


def test_custom_bounds_respected():
    loose = SanityBounds(max_staleness=timedelta(seconds=60), max_abs_funding_rate=Decimal("1"),
                         max_mark_index_divergence=Decimal("1"), max_jump=Decimal("1"))
    assert check_tick(tick(received_ts=T0 + timedelta(seconds=30)), bounds=loose, previous=None) is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_filters.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.data_ingest'`

- [ ] **Step 3: Write `polyperps/data_ingest/__init__.py`** (empty) and `polyperps/data_ingest/filters.py`

```python
"""Pure tick validation. No I/O, no clocks - everything comes from the Tick.

Order of checks matters: the first failing rule names the rejection, so a
tick that is both stale and insane is reported as "stale".
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal

from polyperps.exchange.types import Tick


@dataclass(frozen=True, slots=True, kw_only=True)
class SanityBounds:
    max_staleness: timedelta
    max_abs_funding_rate: Decimal
    max_mark_index_divergence: Decimal  # fraction of index, e.g. 0.05 = 5%
    max_jump: Decimal  # fraction of previous mark, e.g. 0.10 = 10%


@dataclass(frozen=True, slots=True)
class Rejection:
    reason: str
    detail: str


DEFAULT_BOUNDS = SanityBounds(
    max_staleness=timedelta(seconds=5),
    max_abs_funding_rate=Decimal("0.01"),
    max_mark_index_divergence=Decimal("0.05"),
    max_jump=Decimal("0.10"),
)


def check_tick(tick: Tick, *, bounds: SanityBounds, previous: Tick | None) -> Rejection | None:
    age = tick.received_ts - tick.exchange_ts
    if age > bounds.max_staleness:
        return Rejection("stale", f"age={age.total_seconds():.3f}s > {bounds.max_staleness.total_seconds()}s")

    for name in ("mark_price", "index_price", "last_price"):
        if getattr(tick, name) <= 0:
            return Rejection("non_positive_price", f"{name}={getattr(tick, name)}")

    divergence = abs(tick.mark_price - tick.index_price) / tick.index_price
    if divergence > bounds.max_mark_index_divergence:
        return Rejection("mark_index_divergence", f"{divergence:.4f} > {bounds.max_mark_index_divergence}")

    if abs(tick.funding_rate) > bounds.max_abs_funding_rate:
        return Rejection("funding_out_of_bounds", f"|{tick.funding_rate}| > {bounds.max_abs_funding_rate}")

    if previous is not None:
        same_stream = (
            previous.source_type is tick.source_type
            and previous.sequence is not None
            and tick.sequence is not None
        )
        if same_stream and tick.sequence <= previous.sequence:
            return Rejection("out_of_order", f"sequence {tick.sequence} <= previous {previous.sequence}")
        jump = abs(tick.mark_price - previous.mark_price) / previous.mark_price
        if jump > bounds.max_jump:
            return Rejection("price_jump", f"{jump:.4f} > {bounds.max_jump} vs previous mark {previous.mark_price}")

    return None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_filters.py -v`
Expected: 9 passed

- [ ] **Step 5: Commit**

```bash
git add polyperps/data_ingest tests/test_filters.py
git commit -m "feat: tick staleness and sanity filters"
```

---

### Task 7: SQLite storage (polyweather pattern) and gap check

**Files:**
- Create: `polyperps/storage/__init__.py`, `polyperps/storage/db.py`, `polyperps/storage/gaps.py`
- Test: `tests/test_storage.py`, `tests/test_gaps.py`

**Interfaces:**
- Consumes: `Tick`, `BookSnapshot`, `FundingObservation`, `Candle`, `Rejection`.
- Produces (`polyperps.storage.db`):
  - `connect(path: str | Path) -> sqlite3.Connection` — creates schema if missing; `":memory:"` allowed.
  - `insert_tick(conn, tick) -> bool`, `insert_funding(conn, obs) -> bool`, `insert_book(conn, snap, *, max_levels: int = 10) -> bool`, `insert_candle(conn, candle) -> bool`, `insert_rejection(conn, *, instrument_id: int, reason: str, detail: str, at: datetime) -> None`. Inserts are idempotent (`INSERT OR IGNORE`); return `True` if a row was written.
  - `query_ticks(conn, instrument_id: int, *, start: datetime, end: datetime) -> list[Tick]`
  - `query_funding(conn, instrument_id: int, *, start: datetime, end: datetime) -> list[FundingObservation]`
  - `count_rejections(conn, instrument_id: int) -> dict[str, int]`
- Produces (`polyperps.storage.gaps`): `find_gaps(conn, instrument_id: int, *, table: Literal["ticks", "funding_rates"], max_gap: timedelta, start: datetime, end: datetime) -> list[tuple[datetime, datetime]]` — every adjacent pair of `exchange_ts` further apart than `max_gap`, plus leading/trailing gaps against `start`/`end`.
- Storage rules: `Decimal` → TEXT via `str()`; datetimes → ISO-8601 TEXT with `+00:00`; `source_type` → TEXT.

- [ ] **Step 1: Write the failing storage tests**

`tests/test_storage.py`:

```python
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.exchange.types import (
    BookLevel, BookSnapshot, Candle, FundingObservation, SourceType, Tick,
)
from polyperps.storage.db import (
    connect, count_rejections, insert_book, insert_candle, insert_funding,
    insert_rejection, insert_tick, query_funding, query_ticks,
)

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


def tick(ts=T0, seq=1, mark="100.123456789"):
    return Tick(instrument_id=1, mark_price=Decimal(mark), index_price=Decimal("100"),
                last_price=Decimal("100"), funding_rate=Decimal("0.0001"), next_funding=T0,
                exchange_ts=ts, received_ts=ts, source_type=SourceType.POLYMARKET_WS, sequence=seq)


def test_schema_created_and_tick_round_trips_decimal_exactly():
    conn = connect(":memory:")
    assert insert_tick(conn, tick()) is True
    rows = query_ticks(conn, 1, start=T0 - timedelta(minutes=1), end=T0 + timedelta(minutes=1))
    assert rows == [tick()]
    assert rows[0].mark_price == Decimal("100.123456789")
    assert rows[0].exchange_ts.tzinfo is not None


def test_insert_tick_is_idempotent():
    conn = connect(":memory:")
    assert insert_tick(conn, tick()) is True
    assert insert_tick(conn, tick()) is False


def test_query_ticks_respects_range_and_order():
    conn = connect(":memory:")
    for i in (3, 1, 2):
        insert_tick(conn, tick(ts=T0 + timedelta(seconds=i), seq=i))
    rows = query_ticks(conn, 1, start=T0 + timedelta(seconds=2), end=T0 + timedelta(seconds=3))
    assert [r.sequence for r in rows] == [2, 3]


def test_funding_round_trip():
    conn = connect(":memory:")
    obs = FundingObservation(instrument_id=1, funding_rate=Decimal("-0.00025"), exchange_ts=T0,
                             received_ts=T0, source_type=SourceType.POLYMARKET_REST)
    assert insert_funding(conn, obs) is True
    assert query_funding(conn, 1, start=T0, end=T0) == [obs]


def test_book_stores_top_levels_only():
    conn = connect(":memory:")
    levels = tuple(BookLevel(price=Decimal(100 - i), quantity=Decimal(1)) for i in range(20))
    snap = BookSnapshot(instrument_id=1, bids=levels, asks=levels, exchange_ts=T0, received_ts=T0,
                        source_type=SourceType.POLYMARKET_REST, sequence=9)
    assert insert_book(conn, snap, max_levels=5) is True
    (bids_json,) = conn.execute("SELECT bids_json FROM book_snapshots").fetchone()
    assert bids_json.count('"price"') == 5


def test_candle_insert_idempotent():
    conn = connect(":memory:")
    c = Candle(instrument_id=1, interval="1m", open_ts=T0, open=Decimal(1), high=Decimal(2),
               low=Decimal(1), close=Decimal(2), volume=Decimal(3), trades=1, received_ts=T0,
               source_type=SourceType.POLYMARKET_REST)
    assert insert_candle(conn, c) is True
    assert insert_candle(conn, c) is False


def test_rejections_counted_by_reason():
    conn = connect(":memory:")
    insert_rejection(conn, instrument_id=1, reason="stale", detail="x", at=T0)
    insert_rejection(conn, instrument_id=1, reason="stale", detail="y", at=T0 + timedelta(seconds=1))
    insert_rejection(conn, instrument_id=1, reason="price_jump", detail="z", at=T0)
    assert count_rejections(conn, 1) == {"stale": 2, "price_jump": 1}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_storage.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.storage'`

- [ ] **Step 3: Write `polyperps/storage/__init__.py`** (empty) and `polyperps/storage/db.py`

```python
"""SQLite persistence, following polyweather/storage.py: stdlib sqlite3,
CREATE TABLE IF NOT EXISTS on connect, composite primary keys, idempotent
inserts. Decimals are stored as TEXT (exact); datetimes as ISO-8601 UTC TEXT.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from polyperps.exchange.types import (
    BookLevel,
    BookSnapshot,
    Candle,
    FundingObservation,
    SourceType,
    Tick,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks (
    instrument_id INTEGER NOT NULL,
    source_type   TEXT NOT NULL,
    exchange_ts   TEXT NOT NULL,
    received_ts   TEXT NOT NULL,
    sequence      INTEGER,
    mark_price    TEXT NOT NULL,
    index_price   TEXT NOT NULL,
    last_price    TEXT NOT NULL,
    funding_rate  TEXT NOT NULL,
    next_funding  TEXT NOT NULL,
    PRIMARY KEY (instrument_id, source_type, exchange_ts)
);
CREATE INDEX IF NOT EXISTS ticks_by_time ON ticks (instrument_id, exchange_ts);

CREATE TABLE IF NOT EXISTS funding_rates (
    instrument_id INTEGER NOT NULL,
    source_type   TEXT NOT NULL,
    exchange_ts   TEXT NOT NULL,
    received_ts   TEXT NOT NULL,
    funding_rate  TEXT NOT NULL,
    PRIMARY KEY (instrument_id, source_type, exchange_ts)
);

CREATE TABLE IF NOT EXISTS book_snapshots (
    instrument_id INTEGER NOT NULL,
    source_type   TEXT NOT NULL,
    exchange_ts   TEXT NOT NULL,
    received_ts   TEXT NOT NULL,
    sequence      INTEGER,
    bids_json     TEXT NOT NULL,
    asks_json     TEXT NOT NULL,
    PRIMARY KEY (instrument_id, source_type, exchange_ts)
);

CREATE TABLE IF NOT EXISTS candles (
    instrument_id INTEGER NOT NULL,
    interval      TEXT NOT NULL,
    source_type   TEXT NOT NULL,
    open_ts       TEXT NOT NULL,
    received_ts   TEXT NOT NULL,
    open TEXT NOT NULL, high TEXT NOT NULL, low TEXT NOT NULL, close TEXT NOT NULL,
    volume TEXT NOT NULL, trades INTEGER NOT NULL,
    PRIMARY KEY (instrument_id, interval, source_type, open_ts)
);

CREATE TABLE IF NOT EXISTS rejections (
    instrument_id INTEGER NOT NULL,
    at            TEXT NOT NULL,
    reason        TEXT NOT NULL,
    detail        TEXT NOT NULL
);
"""


def _ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s)


def connect(path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.executescript(_SCHEMA)
    return conn


def insert_tick(conn: sqlite3.Connection, t: Tick) -> bool:
    cur = conn.execute(
        "INSERT OR IGNORE INTO ticks VALUES (?,?,?,?,?,?,?,?,?,?)",
        (t.instrument_id, t.source_type.value, _ts(t.exchange_ts), _ts(t.received_ts), t.sequence,
         str(t.mark_price), str(t.index_price), str(t.last_price), str(t.funding_rate),
         _ts(t.next_funding)),
    )
    conn.commit()
    return cur.rowcount == 1


def insert_funding(conn: sqlite3.Connection, f: FundingObservation) -> bool:
    cur = conn.execute(
        "INSERT OR IGNORE INTO funding_rates VALUES (?,?,?,?,?)",
        (f.instrument_id, f.source_type.value, _ts(f.exchange_ts), _ts(f.received_ts),
         str(f.funding_rate)),
    )
    conn.commit()
    return cur.rowcount == 1


def _levels_json(levels: tuple[BookLevel, ...], max_levels: int) -> str:
    return json.dumps([{"price": str(l.price), "quantity": str(l.quantity)} for l in levels[:max_levels]])


def insert_book(conn: sqlite3.Connection, b: BookSnapshot, *, max_levels: int = 10) -> bool:
    cur = conn.execute(
        "INSERT OR IGNORE INTO book_snapshots VALUES (?,?,?,?,?,?,?)",
        (b.instrument_id, b.source_type.value, _ts(b.exchange_ts), _ts(b.received_ts), b.sequence,
         _levels_json(b.bids, max_levels), _levels_json(b.asks, max_levels)),
    )
    conn.commit()
    return cur.rowcount == 1


def insert_candle(conn: sqlite3.Connection, c: Candle) -> bool:
    cur = conn.execute(
        "INSERT OR IGNORE INTO candles VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (c.instrument_id, c.interval, c.source_type.value, _ts(c.open_ts), _ts(c.received_ts),
         str(c.open), str(c.high), str(c.low), str(c.close), str(c.volume), c.trades),
    )
    conn.commit()
    return cur.rowcount == 1


def insert_rejection(
    conn: sqlite3.Connection, *, instrument_id: int, reason: str, detail: str, at: datetime
) -> None:
    conn.execute(
        "INSERT INTO rejections VALUES (?,?,?,?)", (instrument_id, _ts(at), reason, detail)
    )
    conn.commit()


def query_ticks(
    conn: sqlite3.Connection, instrument_id: int, *, start: datetime, end: datetime
) -> list[Tick]:
    rows = conn.execute(
        "SELECT instrument_id, source_type, exchange_ts, received_ts, sequence, mark_price, "
        "index_price, last_price, funding_rate, next_funding FROM ticks "
        "WHERE instrument_id=? AND exchange_ts BETWEEN ? AND ? ORDER BY exchange_ts, sequence",
        (instrument_id, _ts(start), _ts(end)),
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
    conn: sqlite3.Connection, instrument_id: int, *, start: datetime, end: datetime
) -> list[FundingObservation]:
    rows = conn.execute(
        "SELECT instrument_id, source_type, exchange_ts, received_ts, funding_rate "
        "FROM funding_rates WHERE instrument_id=? AND exchange_ts BETWEEN ? AND ? "
        "ORDER BY exchange_ts",
        (instrument_id, _ts(start), _ts(end)),
    ).fetchall()
    return [
        FundingObservation(
            instrument_id=r[0], source_type=SourceType(r[1]), exchange_ts=_parse_ts(r[2]),
            received_ts=_parse_ts(r[3]), funding_rate=Decimal(r[4]),
        )
        for r in rows
    ]


def count_rejections(conn: sqlite3.Connection, instrument_id: int) -> dict[str, int]:
    rows = conn.execute(
        "SELECT reason, COUNT(*) FROM rejections WHERE instrument_id=? GROUP BY reason",
        (instrument_id,),
    ).fetchall()
    return {reason: n for reason, n in rows}
```

- [ ] **Step 4: Run storage tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_storage.py -v`
Expected: 7 passed

- [ ] **Step 5: Write the failing gap tests**

`tests/test_gaps.py`:

```python
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.exchange.types import FundingObservation, SourceType, Tick
from polyperps.storage.db import connect, insert_funding, insert_tick
from polyperps.storage.gaps import find_gaps

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)
S = timedelta(seconds=1)


def tick(ts, seq):
    return Tick(instrument_id=1, mark_price=Decimal(1), index_price=Decimal(1), last_price=Decimal(1),
                funding_rate=Decimal(0), next_funding=T0, exchange_ts=ts, received_ts=ts,
                source_type=SourceType.POLYMARKET_WS, sequence=seq)


def test_no_gaps_when_dense():
    conn = connect(":memory:")
    for i in range(5):
        insert_tick(conn, tick(T0 + i * S, i))
    assert find_gaps(conn, 1, table="ticks", max_gap=2 * S, start=T0, end=T0 + 4 * S) == []


def test_internal_gap_detected():
    conn = connect(":memory:")
    for i in (0, 1, 2, 10, 11):
        insert_tick(conn, tick(T0 + i * S, i))
    gaps = find_gaps(conn, 1, table="ticks", max_gap=2 * S, start=T0, end=T0 + 11 * S)
    assert gaps == [(T0 + 2 * S, T0 + 10 * S)]


def test_leading_and_trailing_gaps_against_range():
    conn = connect(":memory:")
    insert_tick(conn, tick(T0 + 5 * S, 1))
    gaps = find_gaps(conn, 1, table="ticks", max_gap=2 * S, start=T0, end=T0 + 10 * S)
    assert gaps == [(T0, T0 + 5 * S), (T0 + 5 * S, T0 + 10 * S)]


def test_empty_table_is_one_whole_gap():
    conn = connect(":memory:")
    assert find_gaps(conn, 1, table="ticks", max_gap=S, start=T0, end=T0 + 10 * S) == [(T0, T0 + 10 * S)]


def test_funding_table_supported():
    conn = connect(":memory:")
    for i in (0, 1):
        insert_funding(conn, FundingObservation(instrument_id=1, funding_rate=Decimal(0),
                                                exchange_ts=T0 + i * timedelta(hours=1),
                                                received_ts=T0, source_type=SourceType.POLYMARKET_REST))
    assert find_gaps(conn, 1, table="funding_rates", max_gap=timedelta(hours=1, minutes=5),
                     start=T0, end=T0 + timedelta(hours=1)) == []
```

- [ ] **Step 6: Run gap tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_gaps.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.storage.gaps'`

- [ ] **Step 7: Write `polyperps/storage/gaps.py`**

```python
"""Gap check for stored time series (spec 0.4 exit criterion)."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Literal

from polyperps.storage.db import _parse_ts, _ts

_TIME_COLUMN = {"ticks": "exchange_ts", "funding_rates": "exchange_ts"}


def find_gaps(
    conn: sqlite3.Connection,
    instrument_id: int,
    *,
    table: Literal["ticks", "funding_rates"],
    max_gap: timedelta,
    start: datetime,
    end: datetime,
) -> list[tuple[datetime, datetime]]:
    col = _TIME_COLUMN[table]  # whitelisted; never interpolate caller strings
    rows = conn.execute(
        f"SELECT {col} FROM {table} WHERE instrument_id=? AND {col} BETWEEN ? AND ? ORDER BY {col}",
        (instrument_id, _ts(start), _ts(end)),
    ).fetchall()
    stamps = [_parse_ts(r[0]) for r in rows]
    if not stamps:
        return [(start, end)]

    gaps: list[tuple[datetime, datetime]] = []
    if stamps[0] - start > max_gap:
        gaps.append((start, stamps[0]))
    for a, b in zip(stamps, stamps[1:]):
        if b - a > max_gap:
            gaps.append((a, b))
    if end - stamps[-1] > max_gap:
        gaps.append((stamps[-1], end))
    return gaps
```

- [ ] **Step 8: Run all storage tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_storage.py tests/test_gaps.py -v`
Expected: 12 passed

- [ ] **Step 9: Commit**

```bash
git add polyperps/storage tests/test_storage.py tests/test_gaps.py
git commit -m "feat: sqlite storage for ticks/funding/books/candles and gap check"
```

---

### Task 8: Market feed loop, config, and the 48h soak entrypoint

**Files:**
- Create: `polyperps/config.py`, `polyperps/data_ingest/market_feed.py`, `scripts/run_feed.py`
- Test: `tests/test_market_feed.py`

**Interfaces:**
- Consumes: `check_tick`, `SanityBounds`, `Rejection`, `Tick`, `ExchangeClient`, storage inserts.
- Produces:
  - `FeedHealth` dataclass (mutable): `received: int = 0`, `accepted: int = 0`, `rejected: dict[str, int]` (Counter by reason), `last_accepted: dict[int, datetime]` (per instrument), `last_event_wallclock: datetime | None`.
  - `MarketFeed(*, ticks: AsyncIterator[Tick], bounds: SanityBounds, on_accept: Callable[[Tick], None], on_reject: Callable[[Tick, Rejection], None] | None = None, clock: Callable[[], datetime] = _utcnow)` with `async run(*, max_events: int | None = None) -> FeedHealth` and property `health`.
  - `polyperps.config.Settings(db_path: Path, instrument_ids: tuple[int, ...], bounds: SanityBounds, book_snapshot_interval_s: float, health_log_interval_s: float, rest_rate_per_sec: float, rest_burst: int)` and `load_settings(env: Mapping[str, str] = os.environ) -> Settings` from `POLYPERPS_DB_PATH` (default `data/polyperps.sqlite3`), `POLYPERPS_INSTRUMENT_IDS` (required, comma-separated ints), `POLYPERPS_MAX_STALENESS_S` (default 5), `POLYPERPS_BOOK_INTERVAL_S` (default 5), `POLYPERPS_HEALTH_LOG_S` (default 60), `POLYPERPS_REST_RATE` (default 5), `POLYPERPS_REST_BURST` (default 10).

- [ ] **Step 1: Write the failing tests**

`tests/test_market_feed.py`:

```python
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from polyperps.config import load_settings
from polyperps.data_ingest.filters import DEFAULT_BOUNDS
from polyperps.data_ingest.market_feed import MarketFeed
from polyperps.exchange.types import SourceType, Tick

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)


def tick(i, seq, ts=T0, mark="100", instrument_id=1, received=None):
    return Tick(instrument_id=instrument_id, mark_price=Decimal(mark), index_price=Decimal("100"),
                last_price=Decimal("100"), funding_rate=Decimal("0.0001"), next_funding=T0,
                exchange_ts=ts,
                received_ts=received if received is not None else ts + timedelta(milliseconds=i),
                source_type=SourceType.POLYMARKET_WS, sequence=seq)


async def gen(items):
    for it in items:
        yield it


async def test_accepts_clean_ticks_and_tracks_health():
    accepted = []
    feed = MarketFeed(ticks=gen([tick(0, 1), tick(1, 2)]), bounds=DEFAULT_BOUNDS,
                      on_accept=accepted.append)
    health = await feed.run()
    assert [t.sequence for t in accepted] == [1, 2]
    assert health.received == 2 and health.accepted == 2 and health.rejected == {}
    assert health.last_accepted[1] == T0


async def test_rejects_and_counts_by_reason_without_updating_previous():
    accepted, rejected = [], []
    # exchange said T0-10s, we received it at T0 -> 10s old, over the 5s bound
    stale = tick(0, 2, ts=T0 - timedelta(seconds=10), received=T0)
    ticks = [tick(0, 1), stale, tick(2, 3)]
    feed = MarketFeed(ticks=gen(ticks), bounds=DEFAULT_BOUNDS, on_accept=accepted.append,
                      on_reject=lambda t, r: rejected.append((t.sequence, r.reason)))
    health = await feed.run()
    assert [t.sequence for t in accepted] == [1, 3]
    assert rejected == [(2, "stale")]
    assert health.rejected == {"stale": 1}


async def test_previous_is_tracked_per_instrument():
    accepted = []
    ticks = [tick(0, 5, instrument_id=1), tick(1, 1, instrument_id=2), tick(2, 6, instrument_id=1)]
    feed = MarketFeed(ticks=gen(ticks), bounds=DEFAULT_BOUNDS, on_accept=accepted.append)
    health = await feed.run()
    assert health.accepted == 3  # instrument 2's seq 1 is not "out of order" vs instrument 1's 5


async def test_max_events_stops_early():
    feed = MarketFeed(ticks=gen([tick(i, i + 1) for i in range(10)]), bounds=DEFAULT_BOUNDS,
                      on_accept=lambda t: None)
    health = await feed.run(max_events=3)
    assert health.received == 3


def test_load_settings_parses_env(tmp_path):
    s = load_settings({
        "POLYPERPS_DB_PATH": str(tmp_path / "x.sqlite3"),
        "POLYPERPS_INSTRUMENT_IDS": "7, 9",
        "POLYPERPS_MAX_STALENESS_S": "2.5",
    })
    assert s.instrument_ids == (7, 9)
    assert s.bounds.max_staleness == timedelta(seconds=2.5)
    assert s.book_snapshot_interval_s == 5.0


def test_load_settings_requires_instruments():
    with pytest.raises(ValueError, match="POLYPERPS_INSTRUMENT_IDS"):
        load_settings({})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_market_feed.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'polyperps.config'`

- [ ] **Step 3: Write `polyperps/config.py`**

```python
"""Non-secret runtime settings from environment. Secrets never live here -
see polyperps.security.key_management."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from polyperps.data_ingest.filters import DEFAULT_BOUNDS, SanityBounds


@dataclass(frozen=True, slots=True, kw_only=True)
class Settings:
    db_path: Path
    instrument_ids: tuple[int, ...]
    bounds: SanityBounds
    book_snapshot_interval_s: float
    health_log_interval_s: float
    rest_rate_per_sec: float
    rest_burst: int


def load_settings(env: Mapping[str, str] = os.environ) -> Settings:
    raw_ids = env.get("POLYPERPS_INSTRUMENT_IDS", "").strip()
    if not raw_ids:
        raise ValueError("POLYPERPS_INSTRUMENT_IDS is required (comma-separated instrument ids)")
    ids = tuple(int(x.strip()) for x in raw_ids.split(",") if x.strip())

    bounds = SanityBounds(
        max_staleness=timedelta(seconds=float(env.get("POLYPERPS_MAX_STALENESS_S", "5"))),
        max_abs_funding_rate=Decimal(env.get("POLYPERPS_MAX_ABS_FUNDING", str(DEFAULT_BOUNDS.max_abs_funding_rate))),
        max_mark_index_divergence=Decimal(env.get("POLYPERPS_MAX_MARK_INDEX_DIV", str(DEFAULT_BOUNDS.max_mark_index_divergence))),
        max_jump=Decimal(env.get("POLYPERPS_MAX_JUMP", str(DEFAULT_BOUNDS.max_jump))),
    )
    return Settings(
        db_path=Path(env.get("POLYPERPS_DB_PATH", "data/polyperps.sqlite3")),
        instrument_ids=ids,
        bounds=bounds,
        book_snapshot_interval_s=float(env.get("POLYPERPS_BOOK_INTERVAL_S", "5")),
        health_log_interval_s=float(env.get("POLYPERPS_HEALTH_LOG_S", "60")),
        rest_rate_per_sec=float(env.get("POLYPERPS_REST_RATE", "5")),
        rest_burst=int(env.get("POLYPERPS_REST_BURST", "10")),
    )
```

- [ ] **Step 4: Write `polyperps/data_ingest/market_feed.py`**

```python
"""Consume a Tick stream, filter it, and hand accepted ticks downstream.

Rejected ticks never update `previous` - a bad tick must not become the
baseline for the next jump check.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone

from polyperps.data_ingest.filters import Rejection, SanityBounds, check_tick
from polyperps.exchange.types import Tick


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class FeedHealth:
    received: int = 0
    accepted: int = 0
    rejected: Counter[str] = field(default_factory=Counter)
    last_accepted: dict[int, datetime] = field(default_factory=dict)
    last_event_wallclock: datetime | None = None


class MarketFeed:
    def __init__(
        self,
        *,
        ticks: AsyncIterator[Tick],
        bounds: SanityBounds,
        on_accept: Callable[[Tick], None],
        on_reject: Callable[[Tick, Rejection], None] | None = None,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._ticks = ticks
        self._bounds = bounds
        self._on_accept = on_accept
        self._on_reject = on_reject
        self._clock = clock
        self._previous: dict[int, Tick] = {}
        self._health = FeedHealth()

    @property
    def health(self) -> FeedHealth:
        return self._health

    async def run(self, *, max_events: int | None = None) -> FeedHealth:
        h = self._health
        async for tick in self._ticks:
            h.received += 1
            h.last_event_wallclock = self._clock()
            rejection = check_tick(
                tick, bounds=self._bounds, previous=self._previous.get(tick.instrument_id)
            )
            if rejection is None:
                self._previous[tick.instrument_id] = tick
                h.accepted += 1
                h.last_accepted[tick.instrument_id] = tick.exchange_ts
                self._on_accept(tick)
            else:
                h.rejected[rejection.reason] += 1
                if self._on_reject is not None:
                    self._on_reject(tick, rejection)
            if max_events is not None and h.received >= max_events:
                break
        return h
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/Scripts/python -m pytest tests/test_market_feed.py -v`
Expected: 6 passed

- [ ] **Step 6: Write `scripts/run_feed.py`** (spec 0.3 — the 48h soak; no credentials required)

```python
"""Spec 0.3: continuous market feed with persistence and health logging.

    POLYPERPS_INSTRUMENT_IDS=<id,id> .venv/Scripts/python scripts/run_feed.py

Discover ids first:
    .venv/Scripts/python scripts/run_feed.py --list-instruments

Runs until Ctrl-C. WS ticks (mark/index/last/funding) go through the
sanity filter into `ticks`; rejections into `rejections`; a REST book
snapshot per instrument every POLYPERPS_BOOK_INTERVAL_S into
`book_snapshots`. A health line is logged every POLYPERPS_HEALTH_LOG_S.
The SDK reconnects the WS internally; if the stream ends anyway, this
loop restarts it with backoff so a 48h soak survives transient failures.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timezone

from polyperps.config import load_settings
from polyperps.data_ingest.market_feed import MarketFeed
from polyperps.exchange.client import PolymarketPerpsClient
from polyperps.storage import db

log = logging.getLogger("polyperps.feed")


async def list_instruments() -> None:
    client = PolymarketPerpsClient.create_public()
    try:
        for i in await client.fetch_instruments():
            print(f"{i.instrument_id:>6}  {i.symbol:<14} {i.category:<10} "
                  f"funding={i.funding_interval} max_lev={i.max_leverage}x")
    finally:
        await client.close()


async def book_snapshots(client, conn, settings, stop: asyncio.Event) -> None:
    while not stop.is_set():
        for iid in settings.instrument_ids:
            try:
                db.insert_book(conn, await client.fetch_book(iid, depth=10))
            except Exception:
                log.exception("book snapshot failed for %s", iid)
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.book_snapshot_interval_s)
        except asyncio.TimeoutError:
            pass


async def health_logger(feed: MarketFeed, settings, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.health_log_interval_s)
        except asyncio.TimeoutError:
            h = feed.health
            silent = (
                (datetime.now(timezone.utc) - h.last_event_wallclock).total_seconds()
                if h.last_event_wallclock else None
            )
            log.info("health received=%d accepted=%d rejected=%s silent_for_s=%s last=%s",
                     h.received, h.accepted, dict(h.rejected), silent,
                     {k: v.isoformat() for k, v in h.last_accepted.items()})


async def run_once(settings) -> None:
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = db.connect(settings.db_path)
    client = PolymarketPerpsClient.create_public(
        rate_per_sec=settings.rest_rate_per_sec, burst=settings.rest_burst
    )
    stop = asyncio.Event()

    def on_reject(tick, rej):
        db.insert_rejection(conn, instrument_id=tick.instrument_id, reason=rej.reason,
                            detail=rej.detail, at=tick.received_ts)
        log.warning("rejected %s seq=%s: %s %s", tick.instrument_id, tick.sequence, rej.reason, rej.detail)

    feed = MarketFeed(
        ticks=client.stream_ticks(settings.instrument_ids),
        bounds=settings.bounds,
        on_accept=lambda t: db.insert_tick(conn, t),
        on_reject=on_reject,
    )
    tasks = [
        asyncio.create_task(book_snapshots(client, conn, settings, stop)),
        asyncio.create_task(health_logger(feed, settings, stop)),
    ]
    try:
        await feed.run()
    finally:
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await client.close()
        conn.close()


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if "--list-instruments" in sys.argv:
        await list_instruments()
        return
    settings = load_settings()
    backoff = 1.0
    while True:
        started = asyncio.get_running_loop().time()
        try:
            await run_once(settings)
            log.warning("feed stream ended cleanly; restarting")
        except (KeyboardInterrupt, asyncio.CancelledError):
            raise
        except Exception:
            log.exception("feed crashed; restarting in %.0fs", backoff)
        ran_for = asyncio.get_running_loop().time() - started
        backoff = 1.0 if ran_for > 300 else min(backoff * 2, 60.0)
        await asyncio.sleep(backoff)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
```

- [ ] **Step 7: Smoke-test against the live public API (no credentials)**

Run: `.venv/Scripts/python scripts/run_feed.py --list-instruments`
Expected: a table of instruments with integer ids. If this raises, the SDK surface has drifted from what Task 5 assumed — stop and report the traceback; do not patch around it.

Then, with two ids from that list:

Run: `POLYPERPS_INSTRUMENT_IDS=<a>,<b> POLYPERPS_HEALTH_LOG_S=10 .venv/Scripts/python scripts/run_feed.py` for ~60 seconds, then Ctrl-C.
Expected: at least one `health received=N accepted=M` line with N > 0, and `data/polyperps.sqlite3` containing rows in `ticks` and `book_snapshots`. Record N, M, and `rejected` in the commit message.

- [ ] **Step 8: Commit**

```bash
git add polyperps/config.py polyperps/data_ingest/market_feed.py scripts/run_feed.py tests/test_market_feed.py
git commit -m "feat: filtered market feed loop, settings, and 48h soak entrypoint"
```

---

### Task 9: Historical backfill, gap report, eligibility checklist, README

**Files:**
- Create: `scripts/backfill.py`, `docs/ops/eligibility-checklist.md`, `README.md`

**Interfaces:**
- Consumes: `PolymarketPerpsClient.fetch_funding_history / fetch_candles`, `db.insert_funding / insert_candle`, `find_gaps`, `load_settings`.
- Produces: CLI `scripts/backfill.py --days N [--interval 1m]` writing to the configured DB and printing a gap report per instrument.

- [ ] **Step 1: Write `scripts/backfill.py`** (spec 0.4)

```python
"""Spec 0.4: backfill native funding-rate and candle history, then gap-check.

    POLYPERPS_INSTRUMENT_IDS=<id,id> .venv/Scripts/python scripts/backfill.py --days 7 --interval 1m

Native only. Proxy sources (Hyperliquid/CEX) belong to Phase 1 and must be
tagged SourceType.PROXY_*; they are deliberately not wired here so this
table can never silently mix provenance.

SDK note: list_perps_* default to the last 24h; explicit start/end are
always passed. History is pulled in 24h windows to keep pages small.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone

from polyperps.config import load_settings
from polyperps.exchange.client import PolymarketPerpsClient
from polyperps.storage import db
from polyperps.storage.gaps import find_gaps

_INTERVAL_TD = {"1m": timedelta(minutes=1), "5m": timedelta(minutes=5), "15m": timedelta(minutes=15),
                "1h": timedelta(hours=1), "4h": timedelta(hours=4), "1d": timedelta(days=1)}


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, required=True)
    ap.add_argument("--interval", default="1m", choices=sorted(_INTERVAL_TD))
    args = ap.parse_args()

    settings = load_settings()
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = db.connect(settings.db_path)
    client = PolymarketPerpsClient.create_public(
        rate_per_sec=settings.rest_rate_per_sec, burst=settings.rest_burst
    )
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=args.days)
    try:
        instruments = {i.instrument_id: i for i in await client.fetch_instruments()}
        for iid in settings.instrument_ids:
            inst = instruments.get(iid)
            if inst is None:
                print(f"{iid}: not in fetch_instruments() - skipped")
                continue
            n_f = n_c = 0
            w_start = start
            while w_start < end:
                w_end = min(w_start + timedelta(days=1), end)
                for f in await client.fetch_funding_history(iid, start=w_start, end=w_end):
                    n_f += db.insert_funding(conn, f)
                for c in await client.fetch_candles(iid, interval=args.interval, start=w_start, end=w_end):
                    n_c += db.insert_candle(conn, c)
                w_start = w_end
            print(f"{iid} {inst.symbol}: +{n_f} funding rows, +{n_c} {args.interval} candles "
                  f"(funding_interval={inst.funding_interval})")

            # Gap check on funding: allow one missed interval before flagging.
            fi = _INTERVAL_TD.get(inst.funding_interval, timedelta(hours=1))
            gaps = find_gaps(conn, iid, table="funding_rates", max_gap=2 * fi, start=start, end=end)
            if gaps:
                print(f"  funding gaps ({len(gaps)}):")
                for a, b in gaps:
                    print(f"    {a.isoformat()} -> {b.isoformat()}  ({(b - a)})")
            else:
                print("  funding: no gaps")
    finally:
        await client.close()
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Run the backfill for a short window**

Run: `POLYPERPS_INSTRUMENT_IDS=<a> .venv/Scripts/python scripts/backfill.py --days 2 --interval 1h`
Expected: `+N funding rows, +M 1h candles` with N, M > 0 and a gap report. Perps launched 2026-09-03, so `--days 30` will legitimately report a leading gap — that is a correct shortfall to log per spec 1.1, not a bug.

- [ ] **Step 3: Write `docs/ops/eligibility-checklist.md`** (spec 0.6 — human-run, recurring)

```markdown
# Eligibility / jurisdiction checklist (spec 0.6)

Re-run on the 1st of every month and after any Polymarket terms/product
announcement. Record each pass in the log table. This is never "done".

## Checks

1. **Geo access** - From the trading host's egress IP, load the perps UI and
   run `scripts/run_feed.py --list-instruments`. Both must succeed without a
   geo-block notice. Record the egress IP and region.
2. **Terms of use** - Re-read Polymarket's Terms and any perps-specific
   addendum for changes to eligible jurisdictions, leverage limits, or
   automated-trading clauses. Record the document date.
3. **Product status** - Confirm perps are still live and not in a
   restricted/beta state that excludes API trading. Note SDK version and
   whether `polymarket-client` marks perps APIs experimental (it does at
   0.10.0).
4. **Fee / funding schedule** - `fetch_perps_fees()` output compared to what
   `risk/sizing.py` assumes (Phase 2+). Any change is a Phase 3.3 re-check
   trigger.
5. **Personal eligibility** - Your own residency/tax status has not changed
   in a way that alters the above.

## Log

| Date | Egress IP / region | Terms date | SDK ver | Result | Notes |
|------|--------------------|------------|---------|--------|-------|
|      |                    |            |         |        |       |
```

- [ ] **Step 4: Write `README.md`**

```markdown
# polyperps

Polymarket perps trading bot. **Phase 0 only**: read-only market data,
storage, and safety plumbing. No signal, no capital, no order path exists
in this codebase.

Plan: `polyperps-implementation-plan.md` (roadmap) and
`docs/superpowers/plans/2026-09-11-polyperps-phase0.md` (this phase).

## Setup

    python -m venv .venv
    .venv/Scripts/python -m pip install -e ".[dev]"
    .venv/Scripts/python -m pytest

## Phase 0 runbook

| Spec | Command | Needs a key? |
|------|---------|--------------|
| 0.1 auth check | `scripts/check_auth.py` | yes - see `polyperps/security/key_management.py` docstring |
| 0.2 client | covered by `tests/test_client.py` + `run_feed.py --list-instruments` | no |
| 0.3 48h soak | `POLYPERPS_INSTRUMENT_IDS=a,b scripts/run_feed.py` | no |
| 0.4 backfill + gaps | `scripts/backfill.py --days 7` | no |
| 0.5 key mgmt | `tests/test_key_management.py`; residual risk documented in module docstring | - |
| 0.6 eligibility | `docs/ops/eligibility-checklist.md` (human, monthly) | - |

## Gates

Three conjuncts, all required for any live order (`polyperps/gates.py`):
per-instrument `ExecutionMode.AUTO`, env `POLYMARKET_LIVE_TRADING=true`,
and `polyperps.signal.base.SIGNAL_VALIDATED`. The last is `False` and is
flipped only by a manual, reviewed change after Phase 1 clears on native data.

## Dependency pin

`polymarket-client==0.10.0`. All perps APIs are marked experimental by the
SDK; bumping is a deliberate task that re-verifies `exchange/client.py`.
```

- [ ] **Step 5: Run the full suite**

Run: `.venv/Scripts/python -m pytest -v`
Expected: 60 passed (7 gates + 5 types + 4 limiter + 7 key mgmt + 10 client + 9 filters + 7 storage + 5 gaps + 6 feed), 0 failed.

- [ ] **Step 6: Commit**

```bash
git add scripts/backfill.py docs/ops/eligibility-checklist.md README.md
git commit -m "feat: native history backfill with gap report, eligibility checklist, README"
```

---

## Phase 0 exit gate (from the spec, mapped to evidence)

| Spec | Evidence that it is done |
|------|--------------------------|
| 0.1 | User runs `scripts/check_auth.py`; output shows `fetch_balances -> N entries`. |
| 0.2 | `tests/test_client.py` green; `run_feed.py --list-instruments` succeeds live; limiter counted on every REST call. |
| 0.3 | `run_feed.py` left running 48h; final health line shows `silent_for_s` small, `rejected` dict inspected, `find_gaps(table="ticks", max_gap=…)` over the window returns `[]` or only explained gaps. |
| 0.4 | `backfill.py` populates `funding_rates`/`candles`; gap report reviewed. |
| 0.5 | `key_management.py` tests green; residual risk in docstring acknowledged by user before Phase 3. |
| 0.6 | First row filled in `docs/ops/eligibility-checklist.md`. |

## Self-review notes

- **Spec coverage:** 0.1→Task 5 script; 0.2→Tasks 3+5; 0.3→Tasks 6+8; 0.4→Tasks 7+9; 0.5→Task 4; 0.6→Task 9 checklist. Conventions: dual gate→Task 1; no-fabrication→`NotImplementedError` in `signal/base.py`, `UNVERIFIED` flag on the raw balances path, `source_type` on every record (Task 2); EC2/systemd→`CREDENTIALS_DIRECTORY` support (Task 4). "Never self-adjust"→no code path writes to `SIGNAL_VALIDATED` or `ExecutionMode`.
- **Type consistency:** `Tick.sequence: int | None`; `check_tick` compares sequences only when both non-None and same `source_type`; `MarketFeed` keys `previous` by `instrument_id`; `insert_*` return `bool` and `backfill.py` sums them; `find_gaps` imports `_ts/_parse_ts` from `db` (private-but-shared within the package, acceptable).
- **Known judgment calls:** order-book depth is captured by periodic REST snapshot rather than WS deltas (no local book-building in Phase 0 — YAGNI until Phase 1 needs it). REST tickers without an exchange timestamp fall back to `received_ts` rather than being dropped, distinguishable by `source_type`.
