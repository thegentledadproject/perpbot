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
    _tick(conn, now - timedelta(days=4))
    _tick(conn, now - timedelta(days=1))
    conn.commit()

    deleted = prune(conn, now - timedelta(days=3), batch=1)

    assert deleted["ticks"] == 2
    assert conn.execute("SELECT COUNT(*) FROM ticks").fetchone()[0] == 1


def test_run_rolls_up_before_pruning(tmp_path):
    from scripts.prune import run
    conn = connect(tmp_path / "t.sqlite3")
    now = datetime.now(timezone.utc)
    _tick(conn, now - timedelta(days=5))
    conn.commit()
    rolled, deleted = run(conn, now, 3)
    # the tick's own hour plus the next hour (open_mark searches back 1 h)
    assert rolled == 2 and deleted["ticks"] == 1
    assert conn.execute("SELECT COUNT(*) FROM hourly_rollup WHERE index_close='100'").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM ticks").fetchone()[0] == 0


def test_run_deletes_nothing_when_rollup_fails(tmp_path, monkeypatch):
    import pytest
    import scripts.prune as p
    conn = connect(tmp_path / "t.sqlite3")
    now = datetime.now(timezone.utc)
    _tick(conn, now - timedelta(days=5))
    conn.commit()

    def boom(*a, **k):
        raise RuntimeError("rollup failed")
    monkeypatch.setattr(p, "rollup_hours", boom)
    with pytest.raises(RuntimeError):
        p.run(conn, now, 3)
    assert conn.execute("SELECT COUNT(*) FROM ticks").fetchone()[0] == 1
