import sqlite3
from datetime import datetime, timedelta, timezone

from polyperps.storage.db import connect
from scripts.prune import prune


def _tick(conn: sqlite3.Connection, ts: datetime) -> None:
    conn.execute(
        "INSERT INTO ticks VALUES (6,'polymarket_rest',?,?,1,'100','100','100','0.0000125',?)",
        (ts.isoformat(), ts.isoformat(), ts.isoformat()),
    )


def test_prune_keeps_the_retention_window(tmp_path):
    conn = connect(tmp_path / "t.sqlite3")
    now = datetime.now(timezone.utc)
    _tick(conn, now - timedelta(days=5))
    _tick(conn, now - timedelta(days=1))
    conn.commit()

    deleted = prune(conn, now - timedelta(days=3))

    assert deleted["ticks"] == 1
    assert conn.execute("SELECT COUNT(*) FROM ticks").fetchone()[0] == 1
