"""SQLite persistence, following polyweather/storage.py: stdlib sqlite3,
CREATE TABLE IF NOT EXISTS on connect, composite primary keys, idempotent
inserts. Decimals are stored as TEXT (exact); datetimes as ISO-8601 UTC TEXT.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
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
from polyperps.execution.types import AccountSnapshot, DecisionRow, Intent, OrderRow, PositionLocalRow, PositionView, State

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
CREATE TABLE IF NOT EXISTS hourly_rollup (
    instrument_id INTEGER NOT NULL,
    hour_ts       TEXT NOT NULL,      -- hour open, _ts() format (UTC ISO)
    index_close   TEXT,               -- last native tick index_price in the hour (NULL if no native tick)
    spread_bps    TEXT,               -- median top-of-book spread bps in the hour (NULL if no two-sided book)
    open_mark     TEXT,               -- mark_price of the last native tick at or before hour_ts + 2 s, searched back at most 1 h; NULL if none
    PRIMARY KEY (instrument_id, hour_ts)
);

CREATE TABLE IF NOT EXISTS account_snapshots (
    run_id TEXT PRIMARY KEY, ts TEXT NOT NULL, executor TEXT NOT NULL, start_equity TEXT NOT NULL, json TEXT NOT NULL
);
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
    # timeout=30: feed, paper, prune and healthcheck share this file; the 5 s default made
    # writers give up while prune held the lock.
    conn = sqlite3.connect(str(path), timeout=30)
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


_HOUR = timedelta(hours=1)
# Mirrors polyperps.signal.sufficiency.BAR.latency_s (2 s); that module imports this one, so it
# cannot be imported here. tests/test_storage_phase1.py pins the two equal.
_FILL_LATENCY = timedelta(seconds=2)


def _rollup_hours(
    conn: sqlite3.Connection, instrument_id: int, column: str, start: datetime, end: datetime
) -> dict[datetime, str]:
    """hourly_rollup values (non-NULL `column`) for hours lying fully inside [start, end]."""
    rows = conn.execute(
        f"SELECT hour_ts, {column} FROM hourly_rollup "  # noqa: S608 - column is a literal from the callers
        f"WHERE instrument_id=? AND hour_ts BETWEEN ? AND ? AND {column} IS NOT NULL",
        (instrument_id, _ts(start), _ts(end)),
    )
    out = {}
    for ts, v in rows:
        h = _parse_ts(ts)
        if h >= start and h + _HOUR - timedelta(microseconds=1) <= end:
            out[h] = v
    return out


def query_last_index_by_hour(
    conn: sqlite3.Connection, instrument_id: int, *, start: datetime, end: datetime
) -> dict[datetime, Decimal]:
    """Last native index_price per hour in [start, end]: one seek per hour on ticks_by_time
    (the hour's newest tick), never a read of every tick. Reading all rows -- in Python or with
    GROUP BY -- took ~3 min per instrument at every paper restart on the box (I/O-bound,
    ~3.3M rows); the seeks take ~0.2 s for 72 hours. Hours fully inside [start, end] that have an
    hourly_rollup row (kept after prune deletes the raw ticks) come from it; partially covered hours
    always come from ticks."""
    # ponytail: ties on the hour's newest exchange_ts pick either row (no sequence tie-break,
    # which would force reading the whole hour); add one if same-timestamp ticks ever disagree.
    out: dict[datetime, Decimal] = {
        h: Decimal(v) for h, v in _rollup_hours(conn, instrument_id, "index_close", start, end).items()
    }
    hour = _floor_hour(start)
    while hour <= end:
        if hour in out:
            hour += _HOUR
            continue
        lo, hi = max(hour, start), min(hour + timedelta(hours=1) - timedelta(microseconds=1), end)
        row = conn.execute(
            "SELECT index_price FROM ticks INDEXED BY ticks_by_time "
            "WHERE instrument_id=? AND exchange_ts BETWEEN ? AND ? AND source_type IN (?, ?) "
            "ORDER BY exchange_ts DESC LIMIT 1",
            (instrument_id, _ts(lo), _ts(hi), *_NATIVE_TICK_SOURCES),
        ).fetchone()
        if row is not None:
            out[hour] = Decimal(row[0])
        hour += timedelta(hours=1)
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
    skipped. Streams the cursor like query_last_index_by_hour. Hours fully inside the window with
    an hourly_rollup row come from it as a one-element list (their median); book rows are read
    only for the remaining stretches."""
    out: dict[datetime, list[Decimal]] = {
        h: [Decimal(v)] for h, v in _rollup_hours(conn, instrument_id, "spread_bps", start, end).items()
    }
    # Contiguous runs of hours not taken from the rollup: one book query per run.
    runs: list[list[datetime]] = []
    hour = _floor_hour(start)
    while hour <= end:
        if hour not in out:
            if runs and runs[-1][1] == hour:
                runs[-1][1] = hour + _HOUR
            else:
                runs.append([hour, hour + _HOUR])
        hour += _HOUR
    for first, stop in runs:
        cur = conn.execute(
            "SELECT exchange_ts, bids_json, asks_json FROM book_snapshots "
            "WHERE instrument_id=? AND exchange_ts BETWEEN ? AND ? ORDER BY exchange_ts",
            (instrument_id, _ts(max(first, start)), _ts(min(stop - timedelta(microseconds=1), end))),
        )
        for ts, bids_json, asks_json in cur:
            bps = _spread_bps(bids_json, asks_json)
            if bps is not None:
                out.setdefault(_floor_hour(_parse_ts(ts)), []).append(bps)
    return out


def _median(xs: list[Decimal]) -> Decimal | None:
    xs = sorted(xs)
    n = len(xs)
    if not n:
        return None
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


def rollup_hours(conn: sqlite3.Connection, *, now: datetime) -> int:
    """Write one hourly_rollup row per instrument per closed hour (hour + 1 h <= now) that has
    none yet, from the raw ticks/books; returns rows inserted. Resumes after the instrument's
    newest rollup hour. Run before prune deletes the raw rows."""
    inserted = 0
    ids = {r[0] for r in conn.execute("SELECT DISTINCT instrument_id FROM ticks")}
    ids |= {r[0] for r in conn.execute("SELECT DISTINCT instrument_id FROM book_snapshots")}
    for iid in sorted(ids):
        last = conn.execute("SELECT MAX(hour_ts) FROM hourly_rollup WHERE instrument_id=?", (iid,)).fetchone()[0]
        if last is not None:
            hour = _parse_ts(last) + _HOUR
        else:
            firsts = [r[0] for r in (
                conn.execute("SELECT MIN(exchange_ts) FROM ticks WHERE instrument_id=?", (iid,)).fetchone(),
                conn.execute("SELECT MIN(exchange_ts) FROM book_snapshots WHERE instrument_id=?", (iid,)).fetchone(),
            ) if r[0] is not None]
            hour = _floor_hour(_parse_ts(min(firsts)))
        while hour + _HOUR <= now:
            lo, hi = hour, hour + _HOUR - timedelta(microseconds=1)
            idx = query_last_index_by_hour(conn, iid, start=lo, end=hi).get(hour)
            spread = _median(query_book_spread_bps_by_hour(conn, iid, start=lo, end=hi).get(hour, []))
            mark = conn.execute(
                "SELECT mark_price FROM ticks INDEXED BY ticks_by_time "
                "WHERE instrument_id=? AND exchange_ts BETWEEN ? AND ? AND source_type IN (?, ?) "
                "ORDER BY exchange_ts DESC LIMIT 1",
                (iid, _ts(hour - _HOUR), _ts(hour + _FILL_LATENCY), *_NATIVE_TICK_SOURCES),
            ).fetchone()
            if idx is not None or spread is not None or mark is not None:
                inserted += conn.execute(
                    "INSERT OR IGNORE INTO hourly_rollup VALUES (?,?,?,?,?)",
                    (iid, _ts(hour), None if idx is None else str(idx),
                     None if spread is None else str(spread), None if mark is None else mark[0]),
                ).rowcount
            hour += _HOUR
    conn.commit()
    return inserted


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


def save_account_snapshot(conn: sqlite3.Connection, run_id: str, snap: AccountSnapshot, *,
                          start_equity: Decimal, executor: str) -> None:
    """Part A §3.4: the runner's latest fast-loop snapshot, one row per run (sim, shadow and live alike)."""
    blob = json.dumps({
        "equity": str(snap.equity), "in_liquidation": snap.in_liquidation, "open_orders": list(snap.open_orders),
        "stops": {str(i): str(t) for i, t in snap.stops.items()},
        "positions": [{"instrument_id": p.instrument_id, "size": str(p.size), "entry_price": str(p.entry_price),
                       "notional": str(p.notional), "leverage": p.leverage,
                       "liquidation_price": str(p.liquidation_price) if p.liquidation_price is not None else None,
                       "unrealised_pnl": str(p.unrealised_pnl), "cumulative_funding": str(p.cumulative_funding)}
                      for p in snap.positions],
    })
    conn.execute("INSERT INTO account_snapshots VALUES (?,?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET "
                 "ts=excluded.ts, executor=excluded.executor, start_equity=excluded.start_equity, json=excluded.json",
                 (run_id, _ts(snap.ts), executor, str(start_equity), blob))
    conn.commit()


def load_account_snapshot(conn: sqlite3.Connection, run_id: str) -> tuple[AccountSnapshot, Decimal, str] | None:
    r = conn.execute("SELECT ts, executor, start_equity, json FROM account_snapshots WHERE run_id=?",
                     (run_id,)).fetchone()
    if r is None:
        return None
    d = json.loads(r[3])
    positions = tuple(
        PositionView(instrument_id=p["instrument_id"], size=Decimal(p["size"]), entry_price=Decimal(p["entry_price"]),
                     notional=Decimal(p["notional"]), leverage=p["leverage"],
                     liquidation_price=_dec(p["liquidation_price"]), unrealised_pnl=Decimal(p["unrealised_pnl"]),
                     cumulative_funding=Decimal(p["cumulative_funding"]))
        for p in d["positions"])
    snap = AccountSnapshot(equity=Decimal(d["equity"]), positions=positions, open_orders=tuple(d["open_orders"]),
                           stops={int(i): Decimal(t) for i, t in d["stops"].items()},
                           in_liquidation=d["in_liquidation"], ts=_parse_ts(r[0]))
    return snap, Decimal(r[2]), r[1]


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
