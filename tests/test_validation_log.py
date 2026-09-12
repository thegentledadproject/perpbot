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
    assert make_run_id(T0, "h1", 6, SourceType.PROXY_HYPERLIQUID) == "20260911T120005000000-h1-6-proxy_hyperliquid"


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


def _eval(st, *, met=True, sharpe=1.5, ci_lo=0.001, ci_hi=0.002, fallback_fills=0, tested_days=Decimal("61")):
    return evaluate_run(source_type=st, sufficiency=_suff(met, st), holdout_sharpe=sharpe, ci_lo=ci_lo, ci_hi=ci_hi,
                        holdout_fills_at_hourly_open=fallback_fills, tested_days=tested_days)


def test_evaluate_run_proxy_can_screen_but_never_pass():
    screened, passed = _eval(SourceType.PROXY_HYPERLIQUID, met=False, sharpe=2.0)
    assert screened is True and passed is False


def test_evaluate_run_native_passes_only_with_sufficiency_and_stats():
    st = SourceType.POLYMARKET_REST
    assert _eval(st) == (True, True)
    assert _eval(st, met=False) == (True, False)
    assert _eval(st, sharpe=0.5) == (False, False)


def test_evaluate_run_native_and_met_but_one_fallback_fill_is_not_passed():
    # Amendment 2026-09-12: a single holdout fill priced off the hourly open blocks `passed`
    # (it still screens -- the statistics themselves are untouched).
    st = SourceType.POLYMARKET_REST
    assert _eval(st, fallback_fills=1) == (True, False)
    assert _eval(st, fallback_fills=0) == (True, True)


def test_evaluate_run_native_and_met_but_short_tested_span_is_not_passed():
    # Amendment 2026-09-12: the span actually backtested must reach BAR.min_days (60), not just
    # the span stored in the DB that check_dataset measures.
    st = SourceType.POLYMARKET_REST
    assert _eval(st, tested_days=Decimal("30")) == (True, False)
    assert _eval(st, tested_days=Decimal("59.99")) == (True, False)
    assert _eval(st, tested_days=Decimal("60")) == (True, True)


def test_evaluate_run_no_ci_can_neither_screen_nor_pass():
    st = SourceType.POLYMARKET_REST
    assert _eval(st, sharpe=2.0, ci_lo=None, ci_hi=0.002) == (False, False)
    assert _eval(st, sharpe=2.0, ci_lo=0.001, ci_hi=None) == (False, False)
    assert _eval(st, sharpe=2.0, ci_lo=None, ci_hi=None) == (False, False)
