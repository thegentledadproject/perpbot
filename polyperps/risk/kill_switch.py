"""Spec 2.6 mechanism. THRESHOLDS are None in Phase 2a: with None, a live run
evaluates to "pause" (cannot start) and a paper run to "run". The numbers are
set in Phase 2b from a passing native validation record - never here.

An undefined divergence (missing sharpe input, or backtest_sharpe == 0) falls
back to the same mode default as None thresholds: "pause" for live, "run" for
paper - never a bare "pause" regardless of mode."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

Mode = Literal["paper", "live"]
Action = Literal["run", "pause", "shutdown"]


@dataclass(frozen=True, slots=True, kw_only=True)
class KillThresholds:
    pause: Decimal | None
    shutdown: Decimal | None


THRESHOLDS = KillThresholds(pause=None, shutdown=None)


def divergence(live: float | None, backtest: float | None) -> Decimal | None:
    if live is None or backtest is None or backtest == 0.0:
        return None
    return Decimal(str(abs(live - backtest) / abs(backtest)))


def evaluate(
    *,
    live_sharpe: float | None,
    backtest_sharpe: float | None,
    mode: Mode,
    thresholds: KillThresholds = THRESHOLDS,
) -> Action:
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
