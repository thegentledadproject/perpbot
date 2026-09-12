"""Spec 1.0 re-check: does the stored dataset meet the pre-registered bar?

    POLYPERPS_INSTRUMENT_IDS=6,7 .venv/Scripts/python scripts/sufficiency.py

Native data cannot meet the Strict bar before ~2026-10-11 (60 days after the
first stored native funding row, 2026-08-12; re-check with this script). Run
monthly (see docs/ops/eligibility-checklist.md). No network.
"""

from __future__ import annotations

from datetime import datetime, timezone

from polyperps.config import load_settings
from polyperps.exchange.types import SourceType
from polyperps.signal.sufficiency import BAR, check_dataset
from polyperps.storage import db


def main() -> None:
    settings = load_settings()
    conn = db.connect(settings.db_path)
    now = datetime.now(timezone.utc)
    print(f"bar: native_only={BAR.native_only} min_days={BAR.min_days} "
          f"min_funding_periods={BAR.min_funding_periods}")
    try:
        for iid in settings.instrument_ids:
            for st in (SourceType.POLYMARKET_REST, SourceType.PROXY_HYPERLIQUID):
                r = check_dataset(conn, iid, st, now=now)
                status = "MET" if r.met else "not met"
                print(f"{iid} {st.value:<18} days={r.days:<8} periods={r.funding_periods:<6} {status}")
                for k, v in r.shortfall.items():
                    print(f"    {k}: {v}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
