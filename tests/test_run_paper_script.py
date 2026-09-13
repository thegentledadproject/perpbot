import importlib.util
import sys

import pytest


def load():
    spec = importlib.util.spec_from_file_location("run_paper", "scripts/run_paper.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_live_executor_refused_in_phase_2a(monkeypatch, tmp_path):
    monkeypatch.setenv("POLYPERPS_INSTRUMENT_IDS", "6")
    monkeypatch.setenv("POLYPERPS_DB_PATH", str(tmp_path / "t.sqlite3"))
    monkeypatch.setattr(sys, "argv", ["run_paper.py", "--executor", "live", "--hypothesis", "h1"])
    mod = load()
    with pytest.raises(SystemExit) as e:
        mod.main()
    assert e.value.code == 2


def test_parser_defaults():
    mod = load()
    args = mod.build_parser().parse_args(["--executor", "sim", "--hypothesis", "h1"])
    assert args.equity == "1000" and args.fee_category == "equity" and args.grid_index == 0
