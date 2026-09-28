"""Delete ticks and book snapshots older than --days.

Tick retention was unbounded: 1.4M rows/day (~370 MB) filled the 6.7 GB EC2
disk in 10 days and crash-looped the feed with "database or disk is full".
Candles, funding, decisions, orders and alerts are small and never pruned.

DELETE alone leaves the file at its high-water mark and SQLite reuses the freed
pages, which is what you want for the daily timer. --vacuum additionally
rewrites the file to shrink it on disk; it needs free space for the new copy,
so it is a one-time repair step, not part of the timer.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
from datetime import datetime, timedelta, timezone

from polyperps.config import load_settings

TABLES = ("ticks", "book_snapshots")


def prune(conn: sqlite3.Connection, cutoff: datetime) -> dict[str, int]:
    """Delete rows with exchange_ts strictly before cutoff. Returns rows deleted per table."""
    iso = cutoff.isoformat()
    deleted = {}
    for table in TABLES:
        deleted[table] = conn.execute(
            f"DELETE FROM {table} WHERE exchange_ts < ?", (iso,)  # noqa: S608 - table names are literals above
        ).rowcount
    conn.commit()
    return deleted


def vacuum_into(conn: sqlite3.Connection, db_path: str) -> None:
    """Rewrite the database to a temp file and swap it in, reclaiming freed pages."""
    tmp = db_path + ".vacuum"
    if os.path.exists(tmp):
        os.remove(tmp)
    conn.execute("VACUUM INTO ?", (tmp,))
    conn.close()
    os.replace(tmp, db_path)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    # RETAIN_DAYS from the env, not the unit's ExecStart: an unset var there became `--days ''` and failed every run.
    days = int(os.environ.get("RETAIN_DAYS") or 3)
    ap.add_argument("--days", type=int, default=days, help="retain this many days of ticks (default $RETAIN_DAYS or 3)")
    ap.add_argument("--vacuum", action="store_true", help="rewrite the file to shrink it (needs free disk)")
    args = ap.parse_args()

    db_path = str(load_settings().db_path)
    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    conn = sqlite3.connect(db_path)
    try:
        deleted = prune(conn, cutoff)
        print(f"cutoff {cutoff.isoformat()}: " + ", ".join(f"{t} -{n}" for t, n in deleted.items()))
        if args.vacuum:
            before = os.path.getsize(db_path)
            vacuum_into(conn, db_path)
            print(f"vacuum: {before // 1048576} MB -> {os.path.getsize(db_path) // 1048576} MB")
    finally:
        try:
            conn.close()
        except sqlite3.ProgrammingError:
            pass  # vacuum_into already closed it


if __name__ == "__main__":
    main()
