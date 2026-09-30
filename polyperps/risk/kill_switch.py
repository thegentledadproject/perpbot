"""Spec 2.6 mechanism plus the Part A §5 hard loss limit.

Divergence: THRESHOLDS are None until Phase 2b Part B sets them from a passing native record.
With None, a live run evaluates to "pause" (cannot start) and a paper run to "run". An undefined
divergence (missing sharpe input, or backtest_sharpe == 0) falls back to the same mode default.

Loss limit (fixed in code, never self-adjusting): equity at or below -5 % of start equity pauses
new entries; at or below -10 % shuts down (flatten everything, halt every router). Clearing it is
a human decision (--clear-halt). The stricter of the two checks wins."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from polyperps.monitor.alerts import ALERT_THRESHOLDS

Mode = Literal["paper", "live"]
Action = Literal["run", "pause", "shutdown"]
_RANK: dict[str, int] = {"run": 0, "pause": 1, "shutdown": 2}


@dataclass(frozen=True, slots=True, kw_only=True)
class KillThresholds:
    pause: Decimal | None
    shutdown: Decimal | None


THRESHOLDS = KillThresholds(pause=None, shutdown=None)


def divergence(live: float | None, backtest: float | None) -> Decimal | None:
    if live is None or backtest is None or backtest == 0.0:
        return None
    return Decimal(str(abs(live - backtest) / abs(backtest)))


def _divergence_action(live_sharpe: float | None, backtest_sharpe: float | None, mode: Mode,
                       thresholds: KillThresholds) -> Action:
    if thresholds.pause is None or thresholds.shutdown is None:
        return "pause" if mode == "live" else "run"
    d = divergence(live_sharpe, backtest_sharpe)
    if d is None:
        return "pause" if mode == "live" else "run"
    if d >= thresholds.shutdown:
        return "shutdown"
    if d >= thresholds.pause:
        return "pause"
    return "run"


def loss_limit(equity: Decimal | None, start_equity: Decimal | None) -> Action:
    if equity is None or start_equity is None or start_equity <= 0:
        return "run"
    drawdown = equity / start_equity - 1
    if drawdown <= ALERT_THRESHOLDS.pnl_critical:
        return "shutdown"
    if drawdown <= ALERT_THRESHOLDS.pnl_warn:
        return "pause"
    return "run"


def evaluate(
    *,
    live_sharpe: float | None,
    backtest_sharpe: float | None,
    mode: Mode,
    equity: Decimal | None = None,
    start_equity: Decimal | None = None,
    thresholds: KillThresholds = THRESHOLDS,
) -> Action:
    return max(_divergence_action(live_sharpe, backtest_sharpe, mode, thresholds),
               loss_limit(equity, start_equity), key=_RANK.__getitem__)
