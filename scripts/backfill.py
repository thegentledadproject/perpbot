"""Spec 0.4: backfill native funding-rate and candle history, then gap-check.

    POLYPERPS_INSTRUMENT_IDS=<id,id> .venv/Scripts/python scripts/backfill.py --days 7 --interval 1m

Native only. Proxy sources (Hyperliquid/CEX) belong to Phase 1 and must be
tagged SourceType.PROXY_*; they are deliberately not wired here so this
table can never silently mix provenance.

SDK note: list_perps_* default to the last 24h; explicit start/end are
always passed. History is pulled in 24h windows to keep pages small.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone

from polyperps.config import load_settings
from polyperps.exchange.client import PolymarketPerpsClient
from polyperps.storage import db
from polyperps.storage.gaps import find_gaps

_INTERVAL_TD = {"1m": timedelta(minutes=1), "5m": timedelta(minutes=5), "15m": timedelta(minutes=15),
                "1h": timedelta(hours=1), "4h": timedelta(hours=4), "1d": timedelta(days=1)}


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, required=True)
    ap.add_argument("--interval", default="1m", choices=sorted(_INTERVAL_TD))
    args = ap.parse_args()

    settings = load_settings()
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = db.connect(settings.db_path)
    client = PolymarketPerpsClient.create_public(
        rate_per_sec=settings.rest_rate_per_sec, burst=settings.rest_burst
    )
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=args.days)
    try:
        instruments = {i.instrument_id: i for i in await client.fetch_instruments()}
        for iid in settings.instrument_ids:
            inst = instruments.get(iid)
            if inst is None:
                print(f"{iid}: not in fetch_instruments() - skipped")
                continue
            n_f = n_c = 0
            w_start = start
            while w_start < end:
                w_end = min(w_start + timedelta(days=1), end)
                for f in await client.fetch_funding_history(iid, start=w_start, end=w_end):
                    n_f += db.insert_funding(conn, f)
                for c in await client.fetch_candles(iid, interval=args.interval, start=w_start, end=w_end):
                    n_c += db.insert_candle(conn, c)
                w_start = w_end
            print(f"{iid} {inst.symbol}: +{n_f} funding rows, +{n_c} {args.interval} candles "
                  f"(funding_interval={inst.funding_interval})")

            # Gap check on funding: allow one missed interval before flagging.
            fi = _INTERVAL_TD.get(inst.funding_interval, timedelta(hours=1))
            gaps = find_gaps(conn, iid, table="funding_rates", max_gap=2 * fi, start=start, end=end)
            if gaps:
                print(f"  funding gaps ({len(gaps)}):")
                for a, b in gaps:
                    print(f"    {a.isoformat()} -> {b.isoformat()}  ({(b - a)})")
            else:
                print("  funding: no gaps")
    finally:
        await client.close()
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
