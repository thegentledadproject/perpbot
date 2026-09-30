import importlib.util
from datetime import datetime, timedelta, timezone

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
UP = {"ActiveState": "active", "NRestarts": "0"}


def load():
    spec = importlib.util.spec_from_file_location("healthcheck", "scripts/healthcheck.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def run(mod, state=None, **kw):
    args = dict(now=NOW, disk_pct=50.0, last_ticks={6: NOW, 7: NOW},
                units={"polyperps-feed.service": UP, "polyperps-paper.service": UP},
                prune_result="success", state=state or {})
    args.update(kw)
    return mod.evaluate(**args)


def kinds(alerts):
    return [(a.level, a.kind) for a in alerts]


def test_healthy_box_is_silent():
    alerts, state = run(load())
    assert alerts == [] and state["open"] == []


def test_disk_alert_once_then_recovered():
    mod = load()
    alerts, state = run(mod, disk_pct=96.0)
    assert kinds(alerts) == [("CRITICAL", "disk_full")]
    alerts, state = run(mod, state=state, disk_pct=96.5)
    assert alerts == []                      # still full: no repeat every 5 minutes
    alerts, state = run(mod, state=state, disk_pct=80.0)
    assert kinds(alerts) == [("INFO", "recovered")] and state["open"] == []


def test_stale_ticks_alert_per_instrument():
    alerts, _ = run(load(), last_ticks={6: NOW - timedelta(minutes=10), 7: NOW})
    assert kinds(alerts) == [("CRITICAL", "ticks_stale")]
    assert alerts[0].instrument_id == 6


def test_missing_ticks_count_as_stale():
    alerts, _ = run(load(), last_ticks={6: None, 7: NOW})
    assert kinds(alerts) == [("CRITICAL", "ticks_stale")]


def test_unit_down_and_prune_failed():
    alerts, _ = run(load(), units={"polyperps-feed.service": {"ActiveState": "failed", "NRestarts": "0"},
                                   "polyperps-paper.service": UP},
                    prune_result="exit-code")
    assert sorted(kinds(alerts)) == [("CRITICAL", "prune_failed"), ("CRITICAL", "unit_down")]


def test_restart_alerts_on_each_increase_not_on_first_sight():
    mod = load()
    units = {"polyperps-feed.service": UP, "polyperps-paper.service": {"ActiveState": "active", "NRestarts": "4"}}
    alerts, state = run(mod, units=units)
    assert alerts == []                      # first run: no baseline yet
    units["polyperps-paper.service"] = {"ActiveState": "active", "NRestarts": "6"}
    alerts, state = run(mod, state=state, units=units)
    assert kinds(alerts) == [("CRITICAL", "unit_restarted")]
    assert alerts[0].detail == {"unit": "polyperps-paper.service", "restarts": 6, "new": 2}
    alerts, _ = run(mod, state=state, units=units)
    assert alerts == []


def test_restart_counter_reset_is_silent():
    mod = load()
    _, state = run(mod, units={"polyperps-feed.service": {"ActiveState": "active", "NRestarts": "9"},
                               "polyperps-paper.service": UP})
    alerts, state = run(mod, state=state)    # deploy restarted the unit: counter back to 0
    assert alerts == [] and state["restarts"]["polyperps-feed.service"] == 0


def test_load_state_missing_or_corrupt_is_empty(tmp_path):
    mod = load()
    assert mod.load_state(tmp_path / "nope.json") == {}
    bad = tmp_path / "bad.json"
    bad.write_text('{"open": [', encoding="utf-8")
    assert mod.load_state(bad) == {}


def test_disk_pct_matches_df_not_total():
    from types import SimpleNamespace
    pct = load().disk_used_pct(SimpleNamespace(total=100, used=90, free=5))   # 5 blocks are root-reserved
    assert round(pct, 1) == 94.7
