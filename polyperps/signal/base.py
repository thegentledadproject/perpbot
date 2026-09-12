"""Signal interface and the Phase 1 gate.

SIGNAL_VALIDATED is the third conjunct of the live-order gate (polyperps.gates).
It is derived, never assigned by hand:

  True  iff  validated.json names a run_id
         AND that run_id exists in validation_log.jsonl with passed == True
             (which itself requires a native source and a met sufficiency bar)
         AND, re-checked from the record itself (defence in depth, spec 8.3):
             source_type is native, sufficiency.met is True, and
             holdout.fills_at_hourly_open == 0
         AND validated.json carries non-empty approved_by and approved_at.

Two keys: code writes the passing record; a human commits the approval.
Neither alone flips the flag. generate_signal stays unimplemented - choosing
which validated strategy runs live is a Phase 2 decision.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, NoReturn

from polyperps.signal.sufficiency import NATIVE_SOURCES
from polyperps.signal.validation_log import LOG_PATH, read_records

log = logging.getLogger(__name__)

VALIDATED_PATH = Path(__file__).with_name("validated.json")
_NATIVE_VALUES = frozenset(s.value for s in NATIVE_SOURCES)


def _record_passes(record: dict, run_id: str) -> bool:
    """`passed` is necessary but not sufficient: the gate re-derives the pre-registered
    conditions from the record so a hand-edited or stale `passed` flag cannot open it.
    Any missing key is a False."""
    if record.get("run_id") != run_id or record.get("passed") is not True:
        return False
    if record.get("source_type") not in _NATIVE_VALUES:
        return False
    sufficiency = record.get("sufficiency")
    if not isinstance(sufficiency, dict) or sufficiency.get("met") is not True:
        return False
    holdout = record.get("holdout")
    if not isinstance(holdout, dict):
        return False
    fallback_fills = holdout.get("fills_at_hourly_open")
    # exact int 0 only: JSON false/None/"0" must not read as zero fills
    return type(fallback_fills) is int and fallback_fills == 0


def load_validated(*, validated_path: Path = VALIDATED_PATH, log_path: Path = LOG_PATH) -> bool:
    try:
        if not validated_path.exists():
            return False
        approval = json.loads(validated_path.read_text(encoding="utf-8"))
        if not isinstance(approval, dict):
            return False
        if not all(
            isinstance(approval.get(k), str) and approval.get(k).strip()
            for k in ("run_id", "approved_by", "approved_at")
        ):
            return False
        run_id = approval["run_id"].strip()
        return any(_record_passes(r, run_id) for r in read_records(path=log_path))
    except Exception as exc:  # never crash an import over the gate file
        log.warning("validated.json unreadable (%s); SIGNAL_VALIDATED stays False", type(exc).__name__)
        return False


SIGNAL_VALIDATED: bool = load_validated()


def generate_signal(market_state: Any) -> NoReturn:
    """No strategy is wired to live execution. Phase 2 decides which validated one is."""
    raise NotImplementedError(
        "No validated signal is wired for execution. See polyperps/signal/validation_log.jsonl."
    )
