from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from polyperps.exchange.types import FundingObservation, SourceType
from polyperps.signal.sufficiency import (
    BAR,
    NATIVE_SOURCES,
    SufficiencyBar,
    check_dataset,
    dataset_meets_bar,
    stats_clear_bar,
)
from polyperps.storage.db import connect, insert_funding

UTC = timezone.utc
T0 = datetime(2026, 9, 3, 0, 0, tzinfo=UTC)


def _load(conn, hours, st):
    for i in range(hours):
        ts = T0 + timedelta(hours=i)
        insert_funding(conn, FundingObservation(instrument_id=6, funding_rate=Decimal("0.0001"),
                                                exchange_ts=ts, received_ts=ts, source_type=st))


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
