"""Live-order gates. Ported from polyweather (executor.EXECUTION_MODE +
wallet_client._live_trading_enabled) with a third conjunct, SIGNAL_VALIDATED.

All three must agree before any real order is placed:
  1. per-instrument ExecutionMode == AUTO
  2. env POLYMARKET_LIVE_TRADING == "true" (exact, lowercase)
  3. polyperps.signal.base.SIGNAL_VALIDATED is True

The honest call is the two-argument one - live_orders_allowed(instrument_id,
modes=...) - which reads env and SIGNAL_VALIDATED live from os.environ and
polyperps.signal.base at call time. Passing an explicit signal_validated=
(or env=) is a deliberate override for tests; it is grep-able so a caller
that hardcodes signal_validated=True in production code is easy to find
and flag in review.

Phase 0 has no order path; Phase 2's order_router must call
live_orders_allowed() and refuse on any False.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum


class ExecutionMode(StrEnum):
    MANUAL_REVIEW = "manual_review"
    PAPER = "paper"
    AUTO = "auto"


@dataclass(frozen=True, slots=True)
class GateDecision:
    allowed: bool
    reason: str


LIVE_ENV_VAR = "POLYMARKET_LIVE_TRADING"


def live_orders_allowed(
    instrument_id: int,
    *,
    modes: Mapping[int, ExecutionMode],
    env: Mapping[str, str] | None = None,
    signal_validated: bool | None = None,
) -> GateDecision:
    if env is None:
        env = os.environ
    if signal_validated is None:
        import polyperps.signal.base as signal_base

        signal_validated = signal_base.SIGNAL_VALIDATED

    mode = modes.get(instrument_id, ExecutionMode.MANUAL_REVIEW)
    if mode is not ExecutionMode.AUTO:
        return GateDecision(False, f"instrument {instrument_id} mode is {mode.value}, not auto")
    if env.get(LIVE_ENV_VAR) != "true":
        return GateDecision(False, f"{LIVE_ENV_VAR} is not exactly 'true'")
    if not signal_validated:
        return GateDecision(False, "SIGNAL_VALIDATED is False - Phase 1 has not cleared")
    return GateDecision(True, "all gates passed")
