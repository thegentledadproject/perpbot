"""Append-only record of every backtest run, pass or fail (spec 8.1). Committed.

`passed` is True only for a native-source run whose dataset met the bar AND
whose holdout statistics cleared it. Proxy runs can only be `screened`.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from polyperps.exchange.types import SourceType
from polyperps.signal.sufficiency import NATIVE_SOURCES, SufficiencyReport, stats_clear_bar

LOG_PATH = Path(__file__).with_name("validation_log.jsonl")


def make_run_id(ts: datetime, hypothesis: str, instrument_id: int, source_type: SourceType) -> str:
    return f"{ts:%Y%m%dT%H%M%S%f}-{hypothesis}-{instrument_id}-{source_type.value}"


def _json_default(o: object) -> str:
    return o.isoformat() if isinstance(o, datetime) else str(o)


def append_record(record: dict, *, path: Path = LOG_PATH) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, default=_json_default, sort_keys=True) + "\n")


def read_records(*, path: Path = LOG_PATH) -> list[dict]:
    if not path.exists():
        return []
    out: list[dict] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def read_passing(*, path: Path = LOG_PATH) -> list[dict]:
    return [r for r in read_records(path=path) if r.get("passed") is True]


def evaluate_run(
    *,
    source_type: SourceType,
    sufficiency: SufficiencyReport,
    holdout_sharpe: float,
    ci_lo: float | None,
    ci_hi: float | None,
) -> tuple[bool, bool]:
    if ci_lo is None or ci_hi is None:
        # A run without a CI (too few holdout returns for the block bootstrap) can
        # neither screen nor pass -- there is nothing to judge significance against.
        return False, False
    screened = stats_clear_bar(oos_sharpe=holdout_sharpe, ci_lo=ci_lo, ci_hi=ci_hi)
    passed = screened and source_type in NATIVE_SOURCES and sufficiency.met
    return screened, passed
