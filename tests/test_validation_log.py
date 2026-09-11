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
