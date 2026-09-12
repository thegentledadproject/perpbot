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


def test_whitespace_only_approver_is_false(tmp_path):
    log, val = _files(tmp_path, record=PASSING, validated={**APPROVAL, "approved_by": "  \n"})
    assert load_validated(validated_path=val, log_path=log) is False


def test_malformed_json_is_false_not_exception(tmp_path):
    log, val = _files(tmp_path, record=PASSING)
    val.write_text("{not json", encoding="utf-8")
    assert load_validated(validated_path=val, log_path=log) is False
