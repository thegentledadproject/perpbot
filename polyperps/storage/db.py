"""SQLite persistence, following polyweather/storage.py: stdlib sqlite3,
CREATE TABLE IF NOT EXISTS on connect, composite primary keys, idempotent
inserts. Decimals are stored as TEXT (exact); datetimes as ISO-8601 UTC TEXT.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from polyperps.exchange.types import (
    BookLevel,
    BookSnapshot,
    Candle,
    FundingObservation,
    SourceType,
    Tick,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks (
    instrument_id INTEGER NOT NULL,
    source_type   TEXT NOT NULL,
    exchange_ts   TEXT NOT NULL,
    received_ts   TEXT NOT NULL,
    sequence      INTEGER,
    mark_price    TEXT NOT NULL,
    index_price   TEXT NOT NULL,
    last_price    TEXT NOT NULL,
    funding_rate  TEXT NOT NULL,
    next_funding  TEXT NOT NULL,
    PRIMARY KEY (instrument_id, source_type, exchange_ts)
);
CREATE INDEX IF NOT EXISTS ticks_by_time ON ticks (instrument_id, exchange_ts);

CREATE TABLE IF NOT EXISTS funding_rates (
    instrument_id INTEGER NOT NULL,
    source_type   TEXT NOT NULL,
    exchange_ts   TEXT NOT NULL,
    received_ts   TEXT NOT NULL,
    funding_rate  TEXT NOT NULL,
    PRIMARY KEY (instrument_id, source_type, exchange_ts)
);

CREATE TABLE IF NOT EXISTS book_snapshots (
    instrument_id INTEGER NOT NULL,
    source_type   TEXT NOT NULL,
    exchange_ts   TEXT NOT NULL,
    received_ts   TEXT NOT NULL,
    sequence      INTEGER,
    bids_json     TEXT NOT NULL,
    asks_json     TEXT NOT NULL,
    PRIMARY KEY (instrument_id, source_type, exchange_ts)
);

CREATE TABLE IF NOT EXISTS candles (
    instrument_id INTEGER NOT NULL,
    interval      TEXT NOT NULL,
    source_type   TEXT NOT NULL,
    open_ts       TEXT NOT NULL,
    received_ts   TEXT NOT NULL,
    open TEXT NOT NULL, high TEXT NOT NULL, low TEXT NOT NULL, close TEXT NOT NULL,
    volume TEXT NOT NULL, trades INTEGER NOT NULL,
    PRIMARY KEY (instrument_id, interval, source_type, open_ts)
);

CREATE TABLE IF NOT EXISTS rejections (
    instrument_id INTEGER NOT NULL,
    at            TEXT NOT NULL,
    reason        TEXT NOT NULL,
    detail        TEXT NOT NULL
);
"""


def _ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s)


def connect(path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    # WAL: cheaper commits, readers never block the writer (feed + backfill share the file).
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_SCHEMA)
    return conn


def insert_tick(conn: sqlite3.Connection, t: Tick) -> bool:
    cur = conn.execute(
        "INSERT OR IGNORE INTO ticks VALUES (?,?,?,?,?,?,?,?,?,?)",
        (t.instrument_id, t.source_type.value, _ts(t.exchange_ts), _ts(t.received_ts), t.sequence,
         str(t.mark_price), str(t.index_price), str(t.last_price), str(t.funding_rate),
         _ts(t.next_funding)),
    )
    conn.commit()
    return cur.rowcount == 1


def insert_funding(conn: sqlite3.Connection, f: FundingObservation) -> bool:
    cur = conn.execute(
        "INSERT OR IGNORE INTO funding_rates VALUES (?,?,?,?,?)",
        (f.instrument_id, f.source_type.value, _ts(f.exchange_ts), _ts(f.received_ts),
         str(f.funding_rate)),
    )
    conn.commit()
    return cur.rowcount == 1


def _levels_json(levels: tuple[BookLevel, ...], max_levels: int) -> str:
    return json.dumps([{"price": str(l.price), "quantity": str(l.quantity)} for l in levels[:max_levels]])


def insert_book(conn: sqlite3.Connection, b: BookSnapshot, *, max_levels: int = 10) -> bool:
    cur = conn.execute(
        "INSERT OR IGNORE INTO book_snapshots VALUES (?,?,?,?,?,?,?)",
        (b.instrument_id, b.source_type.value, _ts(b.exchange_ts), _ts(b.received_ts), b.sequence,
         _levels_json(b.bids, max_levels), _levels_json(b.asks, max_levels)),
    )
    conn.commit()
    return cur.rowcount == 1


def insert_candle(conn: sqlite3.Connection, c: Candle) -> bool:
    cur = conn.execute(
        "INSERT OR IGNORE INTO candles VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (c.instrument_id, c.interval, c.source_type.value, _ts(c.open_ts), _ts(c.received_ts),
         str(c.open), str(c.high), str(c.low), str(c.close), str(c.volume), c.trades),
    )
    conn.commit()
    return cur.rowcount == 1


def insert_rejection(
    conn: sqlite3.Connection, *, instrument_id: int, reason: str, detail: str, at: datetime
) -> None:
    conn.execute(
        "INSERT INTO rejections VALUES (?,?,?,?)", (instrument_id, _ts(at), reason, detail)
    )
    conn.commit()


def query_ticks(
    conn: sqlite3.Connection, instrument_id: int, *, start: datetime, end: datetime
) -> list[Tick]:
    rows = conn.execute(
        "SELECT instrument_id, source_type, exchange_ts, received_ts, sequence, mark_price, "
        "index_price, last_price, funding_rate, next_funding FROM ticks "
        "WHERE instrument_id=? AND exchange_ts BETWEEN ? AND ? ORDER BY exchange_ts, sequence",
        (instrument_id, _ts(start), _ts(end)),
    ).fetchall()
    return [
        Tick(
            instrument_id=r[0], source_type=SourceType(r[1]), exchange_ts=_parse_ts(r[2]),
            received_ts=_parse_ts(r[3]), sequence=r[4], mark_price=Decimal(r[5]),
            index_price=Decimal(r[6]), last_price=Decimal(r[7]), funding_rate=Decimal(r[8]),
            next_funding=_parse_ts(r[9]),
        )
        for r in rows
    ]


def query_funding(
    conn: sqlite3.Connection, instrument_id: int, *, start: datetime, end: datetime
) -> list[FundingObservation]:
    rows = conn.execute(
        "SELECT instrument_id, source_type, exchange_ts, received_ts, funding_rate "
        "FROM funding_rates WHERE instrument_id=? AND exchange_ts BETWEEN ? AND ? "
        "ORDER BY exchange_ts",
        (instrument_id, _ts(start), _ts(end)),
    ).fetchall()
    return [
        FundingObservation(
            instrument_id=r[0], source_type=SourceType(r[1]), exchange_ts=_parse_ts(r[2]),
            received_ts=_parse_ts(r[3]), funding_rate=Decimal(r[4]),
        )
        for r in rows
    ]


def count_rejections(conn: sqlite3.Connection, instrument_id: int) -> dict[str, int]:
    rows = conn.execute(
        "SELECT reason, COUNT(*) FROM rejections WHERE instrument_id=? GROUP BY reason",
        (instrument_id,),
    ).fetchall()
    return {reason: n for reason, n in rows}
