"""Gap check for stored time series (spec 0.4 exit criterion).

Supports ticks, funding_rates, and candles. candles is keyed on
(instrument_id, interval, source_type, open_ts), so unlike the other two
tables an `interval` is required - without it rows for every interval
would be pooled together and gaps would be meaningless.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Literal

from polyperps.storage.db import _parse_ts, _ts

_TIME_COLUMN = {"ticks": "exchange_ts", "funding_rates": "exchange_ts", "candles": "open_ts"}


def find_gaps(
    conn: sqlite3.Connection,
    instrument_id: int,
    *,
    table: Literal["ticks", "funding_rates", "candles"],
    max_gap: timedelta,
    start: datetime,
    end: datetime,
    interval: str | None = None,
) -> list[tuple[datetime, datetime]]:
    col = _TIME_COLUMN[table]  # whitelisted; never interpolate caller strings
    if table == "candles":
        if interval is None:
            raise ValueError("interval is required when table='candles'")
        rows = conn.execute(
            f"SELECT {col} FROM {table} WHERE instrument_id=? AND interval=? "
            f"AND {col} BETWEEN ? AND ? ORDER BY {col}",
            (instrument_id, interval, _ts(start), _ts(end)),
        ).fetchall()
    else:
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
