"""Static checks on the EC2/systemd deploy files (deploy/). No network, no
subprocess against a real host - these tests only read text files and repo
paths."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
DEPLOY_DIR = REPO_ROOT / "deploy"

UNIT_FILES = ["polyperps-feed.service", "polyperps-paper.service", "polyperps-dashboard.service"]
SHELL_FILES = ["bootstrap.sh", "update.sh"]

# All deploy files must be plain LF text, deploy.ps1 included - PowerShell
# 5.1 does not require CRLF, and the repo standardizes on LF everywhere.
LF_ONLY_FILES = UNIT_FILES + ["env.example"] + SHELL_FILES + ["deploy.ps1", "polyperps-health.service", "polyperps-health.timer",
                                                                              "polyperps-prune.service", "polyperps-prune.timer"]

LIVE_EXECUTOR_RE = re.compile(r"--executor\s+live")

EXEC_START_RE = re.compile(r"^ExecStart=\S*?/python\s+(\S+\.py)", re.MULTILINE)


def _read(name: str) -> str:
    path = DEPLOY_DIR / name
    assert path.is_file(), f"missing deploy file: {path}"
    return path.read_text(encoding="utf-8")


@pytest.mark.parametrize("name", UNIT_FILES)
def test_unit_file_exists_and_has_required_directives(name):
    text = _read(name)
    assert "User=polyperps" in text
    assert "KillSignal=SIGTERM" in text


@pytest.mark.parametrize("name", UNIT_FILES)
def test_unit_file_never_enables_live_trading(name):
    text = _read(name)
    assert not LIVE_EXECUTOR_RE.search(text)
    assert "POLYMARKET_LIVE_TRADING" not in text


@pytest.mark.parametrize("name", UNIT_FILES)
def test_unit_file_exec_start_scripts_exist(name):
    text = _read(name)
    matches = EXEC_START_RE.findall(text)
    assert matches, f"no ExecStart=.../python <script>.py line found in {name}"
    for script in matches:
        assert (REPO_ROOT / script).is_file(), f"{script} referenced by ExecStart= does not exist"


def test_env_example_exists():
    _read("env.example")


def test_env_example_never_sets_live_trading_on_a_live_line():
    text = _read("env.example")
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or not stripped:
            continue
        assert "POLYMARKET_LIVE_TRADING" not in stripped


@pytest.mark.parametrize("name", SHELL_FILES)
def test_shell_script_shebang_and_strict_mode(name):
    text = _read(name)
    assert text.startswith("#!/usr/bin/env bash\n")
    assert "set -euo pipefail" in text


@pytest.mark.parametrize("name", SHELL_FILES)
def test_shell_script_never_enables_live_trading(name):
    text = _read(name)
    assert not LIVE_EXECUTOR_RE.search(text)
    assert "POLYMARKET_LIVE_TRADING" not in text


@pytest.mark.parametrize("name", LF_ONLY_FILES)
def test_no_carriage_returns(name):
    data = (DEPLOY_DIR / name).read_bytes()
    assert b"\r" not in data


def test_dashboard_unit_binds_port_80_without_root():
    text = (DEPLOY_DIR / "polyperps-dashboard.service").read_text(encoding="utf-8")
    assert "AmbientCapabilities=CAP_NET_BIND_SERVICE" in text
    assert "CapabilityBoundingSet=CAP_NET_BIND_SERVICE" in text
    assert "User=polyperps" in text
    assert "run_dashboard.py" in text


def test_deploy_scripts_know_the_dashboard_unit():
    for name in ("bootstrap.sh", "update.sh"):
        assert "polyperps-dashboard" in (DEPLOY_DIR / name).read_text(encoding="utf-8")
    assert "polyperps-dashboard" in (DEPLOY_DIR / "deploy.ps1").read_text(encoding="utf-8")
    assert "POLYPERPS_DASHBOARD_BIND=0.0.0.0:80" in (DEPLOY_DIR / "env.example").read_text(encoding="utf-8")


@pytest.mark.parametrize("name", ["polyperps-feed.service", "polyperps-paper.service"])
def test_long_running_units_are_supervised_by_systemd(name):
    # The scripts no longer restart themselves; systemd must, even after a clean exit.
    text = _read(name)
    assert "Restart=always" in text
    assert "RestartMaxDelaySec=" in text


def test_health_check_is_wired():
    health = _read("polyperps-health.service")
    assert "User=polyperps" in health
    assert "ImportCredential=TELEGRAM_BOT_TOKEN" in health
    for script in EXEC_START_RE.findall(health):
        assert (REPO_ROOT / script).is_file()
    assert "OnUnitActiveSec=5min" in _read("polyperps-health.timer")
    assert "OnFailure=polyperps-health.service" in _read("polyperps-prune.service")
    assert "ImportCredential=TELEGRAM_BOT_TOKEN" in _read("polyperps-paper.service")
    for name in ("bootstrap.sh", "update.sh"):
        assert "polyperps-health.timer" in _read(name)
