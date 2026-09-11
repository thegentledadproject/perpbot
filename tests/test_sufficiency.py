from decimal import Decimal

import pytest

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
