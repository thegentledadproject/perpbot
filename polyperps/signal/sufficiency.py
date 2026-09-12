"""Spec 1.0: the sufficiency bar, pre-registered before any hypothesis is tested.

BAR is frozen and pinned by tests/test_sufficiency.py. Every number here was
chosen on 2026-09-11 before any backtest ran. Changing one is a spec
amendment: edit the spec, edit this file, edit the test, and say so in the
validation log's next record. Never adjust it to make a result pass.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

from polyperps.exchange.types import SourceType
from polyperps.storage.db import query_funding

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
