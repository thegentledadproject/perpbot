"""SQLite persistence, following polyweather/storage.py: stdlib sqlite3,
CREATE TABLE IF NOT EXISTS on connect, composite primary keys, idempotent
inserts. Decimals are stored as TEXT (exact); datetimes as ISO-8601 UTC TEXT.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from polyperps.exchange.types import (
    BookLevel,
    BookSnapshot,
    Candle,
    FeeSchedule,
    FundingObservation,
    SourceType,
    Tick,
)
from polyperps.execution.types import DecisionRow, Intent, OrderRow, PositionLocalRow, State

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

CREATE TABLE IF NOT EXISTS fee_schedule (
    category       TEXT NOT NULL,
    taker_fee_rate TEXT NOT NULL,
    maker_fee_rate TEXT NOT NULL,
    fetched_at     TEXT NOT NULL,
    PRIMARY KEY (category, fetched_at)
);

CREATE TABLE IF NOT EXISTS decisions (
    run_id TEXT NOT NULL, instrument_id INTEGER NOT NULL, seq INTEGER NOT NULL, ts TEXT NOT NULL,
    state_before TEXT NOT NULL, target TEXT, verdicts_json TEXT NOT NULL, intent_json TEXT,
    client_order_id TEXT, note TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (run_id, instrument_id, seq)
);
CREATE TABLE IF NOT EXISTS orders (
    client_order_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, instrument_id INTEGER NOT NULL, side TEXT NOT NULL,
    quantity TEXT NOT NULL, reduce_only INTEGER NOT NULL, status TEXT NOT NULL, exchange_order_id TEXT,
    filled_quantity TEXT NOT NULL, avg_price TEXT, submitted_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS positions_local (
    run_id TEXT NOT NULL, instrument_id INTEGER NOT NULL, state TEXT NOT NULL, size TEXT NOT NULL,
    entry_price TEXT, stop_trigger TEXT, stop_order_id TEXT, cumulative_funding TEXT NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, instrument_id)
);
CREATE TABLE IF NOT EXISTS sim_account (run_id TEXT PRIMARY KEY, json TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS alerts (
    ts TEXT NOT NULL, run_id TEXT NOT NULL, level TEXT NOT NULL, kind TEXT NOT NULL,
    instrument_id INTEGER, detail_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recovery (run_id TEXT NOT NULL, ts TEXT NOT NULL, findings_json TEXT NOT NULL);
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


def insert_fee(conn: sqlite3.Connection, fee: FeeSchedule) -> bool:
    cur = conn.execute(
        "INSERT OR IGNORE INTO fee_schedule VALUES (?,?,?,?)",
        (fee.category, str(fee.taker_fee_rate), str(fee.maker_fee_rate), _ts(fee.fetched_at)),
    )
    conn.commit()
    return cur.rowcount == 1


def latest_fee(conn: sqlite3.Connection, category: str) -> FeeSchedule | None:
    row = conn.execute(
        "SELECT category, taker_fee_rate, maker_fee_rate, fetched_at FROM fee_schedule "
        "WHERE category=? ORDER BY fetched_at DESC LIMIT 1",
        (category,),
    ).fetchone()
    if row is None:
        return None
    return FeeSchedule(category=row[0], taker_fee_rate=Decimal(row[1]),
                       maker_fee_rate=Decimal(row[2]), fetched_at=_parse_ts(row[3]))


def _source_clause(source_type: SourceType | None) -> tuple[str, tuple[str, ...]]:
    if source_type is None:
        return "", ()
    return " AND source_type=?", (source_type.value,)


def query_ticks(
    conn: sqlite3.Connection,
    instrument_id: int,
    *,
    start: datetime,
    end: datetime,
    source_type: SourceType | None = None,
) -> list[Tick]:
    clause, extra = _source_clause(source_type)
    rows = conn.execute(
        "SELECT instrument_id, source_type, exchange_ts, received_ts, sequence, mark_price, "
        "index_price, last_price, funding_rate, next_funding FROM ticks "
        f"WHERE instrument_id=? AND exchange_ts BETWEEN ? AND ?{clause} ORDER BY exchange_ts, sequence",
        (instrument_id, _ts(start), _ts(end), *extra),
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
    conn: sqlite3.Connection,
    instrument_id: int,
    *,
    start: datetime,
    end: datetime,
    source_type: SourceType | None = None,
) -> list[FundingObservation]:
    clause, extra = _source_clause(source_type)
    rows = conn.execute(
        "SELECT instrument_id, source_type, exchange_ts, received_ts, funding_rate "
        f"FROM funding_rates WHERE instrument_id=? AND exchange_ts BETWEEN ? AND ?{clause} "
        "ORDER BY exchange_ts",
        (instrument_id, _ts(start), _ts(end), *extra),
    ).fetchall()
    return [
        FundingObservation(
            instrument_id=r[0], source_type=SourceType(r[1]), exchange_ts=_parse_ts(r[2]),
            received_ts=_parse_ts(r[3]), funding_rate=Decimal(r[4]),
        )
        for r in rows
    ]


def query_candles(
    conn: sqlite3.Connection,
    instrument_id: int,
    *,
    interval: str,
    source_type: SourceType,
    start: datetime,
    end: datetime,
) -> list[Candle]:
    rows = conn.execute(
        "SELECT instrument_id, interval, source_type, open_ts, received_ts, open, high, low, close, "
        "volume, trades FROM candles WHERE instrument_id=? AND interval=? AND source_type=? "
        "AND open_ts BETWEEN ? AND ? ORDER BY open_ts",
        (instrument_id, interval, source_type.value, _ts(start), _ts(end)),
    ).fetchall()
    return [
        Candle(
            instrument_id=r[0], interval=r[1], source_type=SourceType(r[2]), open_ts=_parse_ts(r[3]),
            received_ts=_parse_ts(r[4]), open=Decimal(r[5]), high=Decimal(r[6]), low=Decimal(r[7]),
            close=Decimal(r[8]), volume=Decimal(r[9]), trades=r[10],
        )
        for r in rows
    ]


def query_book_spread_bps(
    conn: sqlite3.Connection, instrument_id: int, *, start: datetime, end: datetime
) -> list[tuple[datetime, Decimal]]:
    """Top-of-book spread in basis points per stored snapshot; snapshots missing a side are skipped."""
    rows = conn.execute(
        "SELECT exchange_ts, bids_json, asks_json FROM book_snapshots "
        "WHERE instrument_id=? AND exchange_ts BETWEEN ? AND ? ORDER BY exchange_ts",
        (instrument_id, _ts(start), _ts(end)),
    ).fetchall()
    out: list[tuple[datetime, Decimal]] = []
    for ts, bids_json, asks_json in rows:
        bps = _spread_bps(bids_json, asks_json)
        if bps is not None:
            out.append((_parse_ts(ts), bps))
    return out


def _floor_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


# Mirrors polyperps.signal.sufficiency.NATIVE_SOURCES (which imports this module, so it
# cannot be imported here); tests/test_storage_phase1.py pins the two equal.
_NATIVE_TICK_SOURCES = (SourceType.POLYMARKET_WS.value, SourceType.POLYMARKET_REST.value)


def query_last_index_by_hour(
    conn: sqlite3.Connection, instrument_id: int, *, start: datetime, end: datetime
) -> dict[datetime, Decimal]:
    """Last native index_price per hour in [start, end]. Streams the cursor: one row in
    memory at a time, no Tick objects -- weeks of ticks must not be materialised for bars."""
    cur = conn.execute(
        "SELECT exchange_ts, index_price FROM ticks WHERE instrument_id=? AND source_type IN (?, ?) "
        "AND exchange_ts BETWEEN ? AND ? ORDER BY exchange_ts, sequence",
        (instrument_id, *_NATIVE_TICK_SOURCES, _ts(start), _ts(end)),
    )
    out: dict[datetime, Decimal] = {}
    for ts, index_price in cur:
        out[_floor_hour(_parse_ts(ts))] = Decimal(index_price)  # ordered by ts: last wins
    return out


def _spread_bps(bids_json: str, asks_json: str) -> Decimal | None:
    bids = json.loads(bids_json)
    asks = json.loads(asks_json)
    if not bids or not asks:
        return None
    best_bid = max(Decimal(l["price"]) for l in bids)
    best_ask = min(Decimal(l["price"]) for l in asks)
    mid = (best_bid + best_ask) / 2
    return (best_ask - best_bid) / mid * Decimal(10_000)


def query_book_spread_bps_by_hour(
    conn: sqlite3.Connection, instrument_id: int, *, start: datetime, end: datetime
) -> dict[datetime, list[Decimal]]:
    """Top-of-book spreads (bps) grouped by hour in [start, end]; snapshots missing a side are
    skipped. Streams the cursor like query_last_index_by_hour."""
    cur = conn.execute(
        "SELECT exchange_ts, bids_json, asks_json FROM book_snapshots "
        "WHERE instrument_id=? AND exchange_ts BETWEEN ? AND ? ORDER BY exchange_ts",
        (instrument_id, _ts(start), _ts(end)),
    )
    out: dict[datetime, list[Decimal]] = {}
    for ts, bids_json, asks_json in cur:
        bps = _spread_bps(bids_json, asks_json)
        if bps is not None:
            out.setdefault(_floor_hour(_parse_ts(ts)), []).append(bps)
    return out


def count_rejections(conn: sqlite3.Connection, instrument_id: int) -> dict[str, int]:
    rows = conn.execute(
        "SELECT reason, COUNT(*) FROM rejections WHERE instrument_id=? GROUP BY reason",
        (instrument_id,),
    ).fetchall()
    return {reason: n for reason, n in rows}


# --- Phase 2: execution / paper-trading tables --------------------------------


def _dec(s: str | None) -> Decimal | None:
    return Decimal(s) if s is not None else None


def _intent_json(i: Intent | None) -> str | None:
    if i is None:
        return None
    return json.dumps({"instrument_id": i.instrument_id, "side": i.side, "quantity": str(i.quantity),
                       "notional": str(i.notional), "reduce_only": i.reduce_only, "reason": i.reason})


def _intent_from_json(s: str | None) -> Intent | None:
    if s is None:
        return None
    d = json.loads(s)
    return Intent(instrument_id=d["instrument_id"], side=d["side"], quantity=Decimal(d["quantity"]),
                  notional=Decimal(d["notional"]), reduce_only=d["reduce_only"], reason=d["reason"])


def insert_decision(conn: sqlite3.Connection, row: DecisionRow) -> None:
    conn.execute(
        "INSERT INTO decisions VALUES (?,?,?,?,?,?,?,?,?,?)",
        (row.run_id, row.instrument_id, row.seq, _ts(row.ts), row.state_before.value,
         str(row.target) if row.target is not None else None, json.dumps(row.verdicts, sort_keys=True),
         _intent_json(row.intent), row.client_order_id, row.note),
    )
    conn.commit()


def list_decisions(conn: sqlite3.Connection, run_id: str, instrument_id: int) -> list[DecisionRow]:
    rows = conn.execute(
        "SELECT run_id, instrument_id, seq, ts, state_before, target, verdicts_json, intent_json, client_order_id, note "
        "FROM decisions WHERE run_id=? AND instrument_id=? ORDER BY seq", (run_id, instrument_id)).fetchall()
    return [DecisionRow(run_id=r[0], instrument_id=r[1], seq=r[2], ts=_parse_ts(r[3]), state_before=State(r[4]),
                        target=_dec(r[5]), verdicts=json.loads(r[6]), intent=_intent_from_json(r[7]),
                        client_order_id=r[8], note=r[9]) for r in rows]


def upsert_order(conn: sqlite3.Connection, o: OrderRow) -> None:
    conn.execute(
        "INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(client_order_id) DO UPDATE SET "
        "status=excluded.status, exchange_order_id=excluded.exchange_order_id, filled_quantity=excluded.filled_quantity, "
        "avg_price=excluded.avg_price, updated_at=excluded.updated_at, reason=excluded.reason",
        (o.client_order_id, o.run_id, o.instrument_id, o.side, str(o.quantity), int(o.reduce_only), o.status,
         o.exchange_order_id, str(o.filled_quantity), str(o.avg_price) if o.avg_price is not None else None,
         _ts(o.submitted_at), _ts(o.updated_at), o.reason),
    )
    conn.commit()


def _order_from_row(r) -> OrderRow:
    return OrderRow(client_order_id=r[0], run_id=r[1], instrument_id=r[2], side=r[3], quantity=Decimal(r[4]),
                    reduce_only=bool(r[5]), status=r[6], exchange_order_id=r[7], filled_quantity=Decimal(r[8]),
                    avg_price=_dec(r[9]), submitted_at=_parse_ts(r[10]), updated_at=_parse_ts(r[11]), reason=r[12])


_ORDER_COLS = ("client_order_id, run_id, instrument_id, side, quantity, reduce_only, status, exchange_order_id, "
               "filled_quantity, avg_price, submitted_at, updated_at, reason")


def get_order(conn: sqlite3.Connection, client_order_id: str) -> OrderRow | None:
    r = conn.execute(f"SELECT {_ORDER_COLS} FROM orders WHERE client_order_id=?", (client_order_id,)).fetchone()
    return _order_from_row(r) if r else None


def list_orders(conn: sqlite3.Connection, run_id: str, *, status: str | None = None) -> list[OrderRow]:
    if status is None:
        rows = conn.execute(f"SELECT {_ORDER_COLS} FROM orders WHERE run_id=? ORDER BY submitted_at", (run_id,)).fetchall()
    else:
        rows = conn.execute(f"SELECT {_ORDER_COLS} FROM orders WHERE run_id=? AND status=? ORDER BY submitted_at",
                            (run_id, status)).fetchall()
    return [_order_from_row(r) for r in rows]


def upsert_position_local(conn: sqlite3.Connection, p: PositionLocalRow) -> None:
    conn.execute(
        "INSERT INTO positions_local VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(run_id, instrument_id) DO UPDATE SET "
        "state=excluded.state, size=excluded.size, entry_price=excluded.entry_price, stop_trigger=excluded.stop_trigger, "
        "stop_order_id=excluded.stop_order_id, cumulative_funding=excluded.cumulative_funding, updated_at=excluded.updated_at",
        (p.run_id, p.instrument_id, p.state.value, str(p.size), str(p.entry_price) if p.entry_price is not None else None,
         str(p.stop_trigger) if p.stop_trigger is not None else None, p.stop_order_id, str(p.cumulative_funding),
         _ts(p.updated_at)),
    )
    conn.commit()


def get_positions_local(conn: sqlite3.Connection, run_id: str) -> dict[int, PositionLocalRow]:
    rows = conn.execute(
        "SELECT run_id, instrument_id, state, size, entry_price, stop_trigger, stop_order_id, cumulative_funding, updated_at "
        "FROM positions_local WHERE run_id=?", (run_id,)).fetchall()
    return {r[1]: PositionLocalRow(run_id=r[0], instrument_id=r[1], state=State(r[2]), size=Decimal(r[3]),
                                   entry_price=_dec(r[4]), stop_trigger=_dec(r[5]), stop_order_id=r[6],
                                   cumulative_funding=Decimal(r[7]), updated_at=_parse_ts(r[8])) for r in rows}


def save_sim_account(conn: sqlite3.Connection, run_id: str, json_text: str) -> None:
    conn.execute("INSERT INTO sim_account VALUES (?,?,?) ON CONFLICT(run_id) DO UPDATE SET json=excluded.json, "
                 "updated_at=excluded.updated_at", (run_id, json_text, _ts(datetime.now(timezone.utc))))
    conn.commit()


def load_sim_account(conn: sqlite3.Connection, run_id: str) -> str | None:
    r = conn.execute("SELECT json FROM sim_account WHERE run_id=?", (run_id,)).fetchone()
    return r[0] if r else None


def insert_alert(conn: sqlite3.Connection, *, run_id: str, level: str, kind: str, instrument_id: int | None,
                 detail_json: str, ts: datetime) -> None:
    conn.execute("INSERT INTO alerts VALUES (?,?,?,?,?,?)", (_ts(ts), run_id, level, kind, instrument_id, detail_json))
    conn.commit()


def list_alerts(conn: sqlite3.Connection, run_id: str) -> list[tuple[datetime, str, str, int | None, dict]]:
    rows = conn.execute("SELECT ts, level, kind, instrument_id, detail_json FROM alerts WHERE run_id=? ORDER BY ts",
                        (run_id,)).fetchall()
    return [(_parse_ts(r[0]), r[1], r[2], r[3], json.loads(r[4])) for r in rows]


def insert_recovery(conn: sqlite3.Connection, *, run_id: str, ts: datetime, findings_json: str) -> None:
    conn.execute("INSERT INTO recovery VALUES (?,?,?)", (run_id, _ts(ts), findings_json))
    conn.commit()


def list_recovery(conn: sqlite3.Connection, run_id: str) -> list[tuple[datetime, dict]]:
    rows = conn.execute("SELECT ts, findings_json FROM recovery WHERE run_id=? ORDER BY ts", (run_id,)).fetchall()
    return [(_parse_ts(r[0]), json.loads(r[1])) for r in rows]
