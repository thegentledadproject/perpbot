"""Gap check for stored time series (spec 0.4 exit criterion)."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Literal

from polyperps.storage.db import _parse_ts, _ts

_TIME_COLUMN = {"ticks": "exchange_ts", "funding_rates": "exchange_ts"}


def find_gaps(
    conn: sqlite3.Connection,
    instrument_id: int,
    *,
    table: Literal["ticks", "funding_rates"],
    max_gap: timedelta,
    start: datetime,
    end: datetime,
) -> list[tuple[datetime, datetime]]:
    col = _TIME_COLUMN[table]  # whitelisted; never interpolate caller strings
    rows = conn.execute(
        f"SELECT {col} FROM {table} WHERE instrument_id=? AND {col} BETWEEN ? AND ? ORDER BY {col}",
        (instrument_id, _ts(start), _ts(end)),
    ).fetchall()
    stamps = [_parse_ts(r[0]) for r in rows]
    if not stamps:
        return [(start, end)]

    gaps: list[tuple[datetime, datetime]] = []
    if stamps[0] - start > max_gap:
        gaps.append((start, stamps[0]))
    for a, b in zip(stamps, stamps[1:]):
        if b - a > max_gap:
            gaps.append((a, b))
    if end - stamps[-1] > max_gap:
        gaps.append((stamps[-1], end))
    return gaps
