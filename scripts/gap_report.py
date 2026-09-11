"""Spec 0.3 exit-evidence command: summarize gaps and rejections after a soak.

    POLYPERPS_INSTRUMENT_IDS=<id,id> .venv/Scripts/python scripts/gap_report.py --hours 48

Reads the DB written by run_feed.py / backfill.py over the window
[now - hours, now] and prints, per configured instrument:
  - tick gaps (max_gap=--tick-gap-s, default 30s - roomier than the feed's
    own health-log cadence so normal jitter doesn't show up as a "gap")
  - funding gaps (max_gap=--funding-gap-h, default 2h - the real funding
    interval isn't known without a REST call, which this script
    deliberately never makes, so the default is a flat, documented
    fallback rather than a guess dressed up as fact)
  - rejection counts by reason (polyperps.storage.db.count_rejections)

Entirely offline: no exchange client, no network calls, no event loop.
Safe to run against a live DB file while run_feed.py is still writing to
it (WAL mode - see polyperps/storage/db.py connect()).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from polyperps.config import load_settings
from polyperps.storage import db
from polyperps.storage.gaps import find_gaps


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, required=True)
    ap.add_argument("--tick-gap-s", type=float, default=30.0)
    ap.add_argument("--funding-gap-h", type=float, default=2.0)
    args = ap.parse_args()

    settings = load_settings()
    conn = db.connect(settings.db_path)
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(hours=args.hours)
        print(f"window: {start.isoformat()} -> {end.isoformat()}")

        for iid in settings.instrument_ids:
            print(f"\ninstrument {iid}")

            tick_gaps = find_gaps(
                conn, iid, table="ticks", max_gap=timedelta(seconds=args.tick_gap_s),
                start=start, end=end,
            )
            if tick_gaps:
                print(f"  ticks gaps ({len(tick_gaps)}):")
                for a, b in tick_gaps:
                    print(f"    {a.isoformat()} -> {b.isoformat()}  ({(b - a)})")
            else:
                print("  ticks: no gaps")

            funding_gaps = find_gaps(
                conn, iid, table="funding_rates", max_gap=timedelta(hours=args.funding_gap_h),
                start=start, end=end,
            )
            if funding_gaps:
                print(f"  funding gaps ({len(funding_gaps)}):")
                for a, b in funding_gaps:
                    print(f"    {a.isoformat()} -> {b.isoformat()}  ({(b - a)})")
            else:
                print("  funding: no gaps")

            rejections = db.count_rejections(conn, iid)
            if rejections:
                print(f"  rejections: {rejections}")
            else:
                print("  rejections: none")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
