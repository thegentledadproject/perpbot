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


def test_constants_pinned():
    mod = load()
    assert mod.HEARTBEAT_S == 20 and mod.RECONCILE_S == 60


def test_run_id_minted_once_across_supervised_restarts(monkeypatch, tmp_path):
    """C1: a supervised restart must reopen the SAME paper account, so run_id is chosen once
    in main() and handed down to every run_once() - not re-minted per attempt."""
    monkeypatch.setenv("POLYPERPS_INSTRUMENT_IDS", "6")
    monkeypatch.setenv("POLYPERPS_DB_PATH", str(tmp_path / "t.sqlite3"))
    monkeypatch.setattr(sys, "argv", ["run_paper.py", "--executor", "sim", "--hypothesis", "h1"])
    mod = load()
    seen = []

    async def fake_run_once(args, settings):
        seen.append(args.run_id)
        if len(seen) == 1:
            raise RuntimeError("crash once -> supervisor restarts")
        raise SystemExit(0)   # second attempt: stop the supervisor

    monkeypatch.setattr(mod, "run_once", fake_run_once)
    monkeypatch.setattr(mod, "INITIAL_BACKOFF_S", 0.0)
    monkeypatch.setattr(mod, "MAX_BACKOFF_S", 0.0)
    with pytest.raises(SystemExit):
        mod.main()
    assert len(seen) == 2 and seen[0] == seen[1]
    assert seen[0].startswith("paper-")
