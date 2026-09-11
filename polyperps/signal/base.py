"""Signal interface. Phase 1 gate lives here.

SIGNAL_VALIDATED is the third conjunct of the live-order gate (see
polyperps.gates). It is False until a Phase 1 hypothesis passes the
pre-registered sufficiency bar *on native Polymarket data* and that result
is logged. Flipping it is a manual, reviewed change - never automated.
"""

from __future__ import annotations

from typing import Any, NoReturn

SIGNAL_VALIDATED: bool = False


def generate_signal(market_state: Any) -> NoReturn:
    """Phase 1 has not produced a validated signal. Nothing to run."""
    raise NotImplementedError(
        "No validated signal exists. Phase 1 must clear the sufficiency bar first."
    )
