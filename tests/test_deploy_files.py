"""Static checks on the EC2/systemd deploy files (deploy/). No network, no
subprocess against a real host - these tests only read text files and repo
paths."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
DEPLOY_DIR = REPO_ROOT / "deploy"

UNIT_FILES = ["polyperps-feed.service", "polyperps-paper.service"]
SHELL_FILES = ["bootstrap.sh", "update.sh"]

# The five deploy files that must be plain LF text (deploy.ps1 is Windows
# PowerShell and is exempt from the no-\r check).
LF_ONLY_FILES = UNIT_FILES + ["env.example"] + SHELL_FILES

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
