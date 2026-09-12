"""Hourly funding-period bars built from the Phase 0 tables.

A Bar never fabricates: a missing candle or funding row leaves the field None
and sets complete=False. The harness refuses to hold a position across an
incomplete bar, so gaps cannot be traded through silently.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from statistics import median
from typing import Literal

from polyperps.exchange.types import SourceType
from polyperps.signal.sufficiency import BAR, NATIVE_SOURCES
from polyperps.storage.db import query_book_spread_bps, query_candles, query_funding, query_ticks

HOUR = timedelta(hours=1)


@dataclass(frozen=True, slots=True, kw_only=True)
class Bar:
    instrument_id: int
    source_type: SourceType
    open_ts: datetime
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    index_close: Decimal | None
    funding_rate: Decimal | None
    spread_bps: Decimal
    spread_source: Literal["book", "constant"]  # "book" = median of stored snapshots; "constant" = pre-registered fallback
    complete: bool


def floor_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def floor_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def is_native(source_type: SourceType) -> bool:
    return source_type in NATIVE_SOURCES


def build_bars(
    conn: sqlite3.Connection,
    instrument_id: int,
    source_type: SourceType,
    *,
    start: datetime,
    end: datetime,
    proxy_spread_bps: Decimal = BAR.proxy_spread_bps,
) -> list[Bar]:
    first = floor_hour(start)
    if first >= end:
        return []
    last_open = floor_hour(end - timedelta(microseconds=1))
    native = is_native(source_type)

    candles = {
        c.open_ts: c
        for c in query_candles(conn, instrument_id, interval="1h", source_type=source_type,
                               start=first, end=last_open)
    }
    funding = {
        floor_hour(f.exchange_ts): f.funding_rate
        for f in query_funding(conn, instrument_id, start=first + HOUR,
                               end=last_open + 2 * HOUR - timedelta(microseconds=1),
                               source_type=source_type)
    }
    index_by_hour: dict[datetime, Decimal] = {}
    spreads_by_hour: dict[datetime, list[Decimal]] = {}
    if native:
        for t in query_ticks(conn, instrument_id, start=first, end=last_open + HOUR - timedelta(microseconds=1)):
            if is_native(t.source_type):
                index_by_hour[floor_hour(t.exchange_ts)] = t.index_price  # ordered by ts: last wins
        for ts, bps in query_book_spread_bps(conn, instrument_id, start=first,
                                             end=last_open + HOUR - timedelta(microseconds=1)):
            spreads_by_hour.setdefault(floor_hour(ts), []).append(bps)

    bars: list[Bar] = []
    open_ts = first
    while open_ts <= last_open:
        c = candles.get(open_ts)
        rate = funding.get(open_ts + HOUR)
        spreads = spreads_by_hour.get(open_ts) if native else None
        if spreads:
            spread, spread_source = median(spreads), "book"
        else:
            spread, spread_source = proxy_spread_bps, "constant"
        bars.append(
            Bar(
                instrument_id=instrument_id,
                source_type=source_type,
                open_ts=open_ts,
                open=c.open if c else None,
                high=c.high if c else None,
                low=c.low if c else None,
                close=c.close if c else None,
                index_close=index_by_hour.get(open_ts) if native else None,
                funding_rate=rate,
                spread_bps=spread,
                spread_source=spread_source,
                complete=c is not None and rate is not None,
            )
        )
        open_ts += HOUR
    return bars


def align_pair(native: list[Bar], proxy: list[Bar]) -> list[tuple[Bar, Bar]]:
    by_ts = {b.open_ts: b for b in proxy if b.complete}
    return [(n, by_ts[n.open_ts]) for n in native if n.complete and n.open_ts in by_ts]


def load_minute_closes(
    conn: sqlite3.Connection,
    instrument_id: int,
    source_type: SourceType,
    *,
    start: datetime,
    end: datetime,
) -> dict[datetime, Decimal]:
    return {
        c.open_ts: c.close
        for c in query_candles(conn, instrument_id, interval="1m", source_type=source_type,
                               start=start, end=end)
    }
