import importlib
import json

import polyperps.signal.base as base
from polyperps.backtest.harness import HARNESS_VERSION
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


PASSING = {"run_id": "r1", "passed": True, "source_type": "polymarket_rest", "harness_version": HARNESS_VERSION,
           "sufficiency": {"met": True, "shortfall": {}}, "holdout": {"fills_at_hourly_open": 0},
           "robust_screened": True, "hypothesis": "h1", "instrument_id": 6, "end": "2026-10-12T00:00:00+00:00"}
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


def test_passed_flag_on_proxy_record_is_false(tmp_path):
    # defence in depth: a hand-edited passed=True on a proxy record does not open the gate
    log, val = _files(tmp_path, record={**PASSING, "source_type": "proxy_hyperliquid"}, validated=APPROVAL)
    assert load_validated(validated_path=val, log_path=log) is False


def test_passed_flag_with_fallback_fills_is_false(tmp_path):
    log, val = _files(tmp_path, record={**PASSING, "holdout": {"fills_at_hourly_open": 3}}, validated=APPROVAL)
    assert load_validated(validated_path=val, log_path=log) is False


def test_passed_flag_without_sufficiency_met_is_false(tmp_path):
    log, val = _files(tmp_path, record={**PASSING, "sufficiency": {"met": False, "shortfall": {"days": "30 < 60"}}},
                      validated=APPROVAL)
    assert load_validated(validated_path=val, log_path=log) is False


def test_passed_flag_with_missing_keys_is_false(tmp_path):
    cases = [{k: v for k, v in PASSING.items() if k != missing} for missing in ("source_type", "sufficiency", "holdout")]
    # a holdout block that lacks the fill count, or carries a non-int, is also False
    cases += [{**PASSING, "holdout": bad} for bad in
              ({}, {"fills_at_hourly_open": None}, {"fills_at_hourly_open": False}, {"fills_at_hourly_open": "0"})]
    for i, record in enumerate(cases):
        d = tmp_path / str(i)
        d.mkdir()
        log, val = _files(d, record=record, validated=APPROVAL)
        assert load_validated(validated_path=val, log_path=log) is False, record


def test_no_approver_is_false(tmp_path):
    log, val = _files(tmp_path, record=PASSING, validated={**APPROVAL, "approved_by": ""})
    assert load_validated(validated_path=val, log_path=log) is False


def test_whitespace_only_approver_is_false(tmp_path):
    log, val = _files(tmp_path, record=PASSING, validated={**APPROVAL, "approved_by": "  \n"})
    assert load_validated(validated_path=val, log_path=log) is False


def test_malformed_json_is_false_not_exception(tmp_path):
    log, val = _files(tmp_path, record=PASSING)
    val.write_text("{not json", encoding="utf-8")
    assert load_validated(validated_path=val, log_path=log) is False


def test_record_from_an_older_harness_is_rejected(tmp_path):
    cases = [{k: v for k, v in PASSING.items() if k != "harness_version"},   # Phase 1 records have none
             {**PASSING, "harness_version": HARNESS_VERSION - 1},
             {**PASSING, "harness_version": str(HARNESS_VERSION)}]
    for n, bad in enumerate(cases):
        d = tmp_path / f"case{n}"
        d.mkdir()
        log, val = _files(d, record=bad, validated=APPROVAL)
        assert load_validated(validated_path=val, log_path=log) is False


def test_harness_version_pinned():
    assert HARNESS_VERSION == 3


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
