"""Signal interface and the Phase 1 gate.

SIGNAL_VALIDATED is the third conjunct of the live-order gate (polyperps.gates).
It is derived, never assigned by hand:

  True  iff  validated.json names a run_id
         AND that run_id exists in validation_log.jsonl with passed == True
             (which itself requires a native source and a met sufficiency bar)
         AND, re-checked from the record itself (defence in depth, spec 8.3):
             source_type is native, sufficiency.met is True, and
             holdout.fills_at_hourly_open == 0
             and robust_screened is True,
             and harness_version == the current HARNESS_VERSION (Part A §6.6)
         AND no later-window native record for the same hypothesis/instrument at this harness failed (provisional pass, spec 8.4).
         AND validated.json carries non-empty approved_by and approved_at.

Two keys: code writes the passing record; a human commits the approval.
Neither alone flips the flag. generate_signal stays unimplemented - choosing
which validated strategy runs live is a Phase 2 decision.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, NoReturn

from polyperps.backtest.harness import HARNESS_VERSION
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
    version = record.get("harness_version")
    if type(version) is not int or version != HARNESS_VERSION:
        return False   # a result from older trading rules says nothing about the code that trades
    if record.get("source_type") not in _NATIVE_VALUES:
        return False
    sufficiency = record.get("sufficiency")
    if not isinstance(sufficiency, dict) or sufficiency.get("met") is not True:
        return False
    if record.get("robust_screened") is not True:
        return False   # amendment B: must also clear with last-trade fills at the hourly open
    if not (isinstance(record.get("end"), str) and isinstance(record.get("hypothesis"), str)
            and type(record.get("instrument_id")) is int):
        return False   # cannot be placed in time, so revocation could not be checked
    end, ts = _aware(record["end"]), _aware(record.get("ts"))
    if end is None or ts is None or end > ts:
        return False   # unplaceable end, or a window ending after the run itself: later runs could never revoke it
    holdout = record.get("holdout")
    if not isinstance(holdout, dict):
        return False
    fallback_fills = holdout.get("fills_at_hourly_open")
    # exact int 0 only: JSON false/None/"0" must not read as zero fills
    return type(fallback_fills) is int and fallback_fills == 0


def _aware(s: Any) -> datetime | None:
    """Parsed tz-aware datetime, or None for anything missing, unparseable or naive."""
    try:
        dt = datetime.fromisoformat(s)
    except (TypeError, ValueError):
        return None
    return dt if dt.tzinfo is not None else None


def _revokes(later: dict, approved: dict) -> bool:
    """Amendment A (spec 8.4): a pass is provisional. A native record at the current harness for the
    same hypothesis and instrument, on a LATER data window, that did not pass, closes the gate."""
    if not (later.get("hypothesis") is not None and str(later["hypothesis"]).lower() == approved["hypothesis"].lower()
            and str(later.get("instrument_id")) == str(approved["instrument_id"])
            and str(later.get("harness_version")) == str(HARNESS_VERSION)
            and str(later.get("source_type")).lower() in _NATIVE_VALUES
            and later.get("passed") is not True):
        return False
    end = _aware(later.get("end"))
    return end is None or end > _aware(approved["end"])   # a failure we cannot place in time fails closed


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
        records = read_records(path=log_path)
        approved = next((r for r in records if _record_passes(r, run_id)), None)
        return approved is not None and not any(_revokes(r, approved) for r in records)
    except Exception as exc:  # never crash an import over the gate file
        log.warning("validated.json unreadable (%s); SIGNAL_VALIDATED stays False", type(exc).__name__)
        return False


SIGNAL_VALIDATED: bool = load_validated()


def generate_signal(market_state: Any) -> NoReturn:
    """No strategy is wired to live execution. Phase 2 decides which validated one is."""
    raise NotImplementedError(
        "No validated signal is wired for execution. See polyperps/signal/validation_log.jsonl."
    )
