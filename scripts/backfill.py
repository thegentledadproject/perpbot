"""Spec 0.4: backfill native funding-rate and candle history, then gap-check.

    POLYPERPS_INSTRUMENT_IDS=<id,id> .venv/Scripts/python scripts/backfill.py --days 7 --interval 1m

Native only. Proxy sources (Hyperliquid/CEX) belong to Phase 1 and must be
tagged SourceType.PROXY_*; they are deliberately not wired here so this
table can never silently mix provenance.

SDK note: list_perps_* default to the last 24h; explicit start/end are
always passed. History is pulled in 24h windows to keep pages small.

Only TRANSIENT_ERRORS (rate limit / timeout / transport failure) are
retried, with a cooldown - a rate-limit error's server-suggested
retry_after is honored when present, else _RETRY_SLEEP_S. A window that
still fails after _MAX_ATTEMPTS is skipped and listed at the end, so the
gap report is read with that caveat rather than being mistaken for a
complete pull. Any other exception (bad request, auth failure, ...) is
deterministic - retrying it would just burn the retry budget on every
later window too, so it propagates immediately and aborts the run for
that instrument with a clear message; the client/DB connection are still
closed via the outer `finally`.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone

from polyperps.config import load_settings
from polyperps.data_ingest.intervals import parse_interval
from polyperps.exchange.client import TRANSIENT_ERRORS, PolymarketPerpsClient, retry_after_seconds
from polyperps.storage import db
from polyperps.storage.gaps import find_gaps

# SDK-supported candle intervals, for the --interval argparse choices only;
# actual gap-check tolerance comes from parse_interval() on the instrument's
# own funding_interval, never from this table.
_INTERVAL_CHOICES = ("1m", "5m", "15m", "1h", "4h", "1d")

_MAX_ATTEMPTS = 3
_RETRY_SLEEP_S = 60


async def _fetch_window(
    client: PolymarketPerpsClient,
    conn,
    iid: int,
    interval: str,
    w_start: datetime,
    w_end: datetime,
) -> tuple[int, int]:
    """Fetch+insert one window's funding and candles, retrying transient failures only."""
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            n_f = sum(
                db.insert_funding(conn, f)
                for f in await client.fetch_funding_history(iid, start=w_start, end=w_end)
            )
            n_c = sum(
                db.insert_candle(conn, c)
                for c in await client.fetch_candles(iid, interval=interval, start=w_start, end=w_end)
            )
            return n_f, n_c
        except TRANSIENT_ERRORS as exc:
            if attempt == _MAX_ATTEMPTS:
                print(f"  window {w_start.isoformat()}..{w_end.isoformat()} "
                      f"giving up after {_MAX_ATTEMPTS} attempts")
                break
            sleep_s = retry_after_seconds(exc) or _RETRY_SLEEP_S
            print(
                f"  window {w_start.isoformat()}..{w_end.isoformat()} failed "
                f"({type(exc).__name__}); retry {attempt}/{_MAX_ATTEMPTS} in {sleep_s}s"
            )
            await asyncio.sleep(sleep_s)
    raise RuntimeError(f"window {w_start.isoformat()}..{w_end.isoformat()} exhausted retries")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, required=True)
    ap.add_argument("--interval", default="1m", choices=_INTERVAL_CHOICES)
    args = ap.parse_args()

    settings = load_settings()
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = db.connect(settings.db_path)
    client = PolymarketPerpsClient.create_public(
        rate_per_sec=settings.rest_rate_per_sec, burst=settings.rest_burst
    )
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=args.days)
    aborted = False
    try:
        instruments = {i.instrument_id: i for i in await client.fetch_instruments()}
        for iid in settings.instrument_ids:
            inst = instruments.get(iid)
            if inst is None:
                print(f"{iid}: not in fetch_instruments() - skipped")
                continue
            n_f = n_c = 0
            failed_windows: list[tuple[datetime, datetime]] = []
            w_start = start
            while w_start < end:
                w_end = min(w_start + timedelta(days=1), end)
                try:
                    wf, wc = await _fetch_window(client, conn, iid, args.interval, w_start, w_end)
                    n_f += wf
                    n_c += wc
                except RuntimeError:
                    failed_windows.append((w_start, w_end))
                except Exception as exc:
                    print(f"aborting: {type(exc).__name__} on {iid} window "
                          f"{w_start.isoformat()}..{w_end.isoformat()} - not a transient error, "
                          f"fix and rerun")
                    aborted = True
                    break
                w_start = w_end
            if aborted:
                break
            print(f"{iid} {inst.symbol}: +{n_f} funding rows, +{n_c} {args.interval} candles "
                  f"(funding_interval={inst.funding_interval})")

            # Gap check on funding: allow one missed interval before flagging.
            fi = parse_interval(inst.funding_interval)
            if fi is None:
                print(f"  funding interval {inst.funding_interval!r} not parseable - gap check skipped")
            else:
                gaps = find_gaps(conn, iid, table="funding_rates", max_gap=2 * fi, start=start, end=end)
                if gaps:
                    print(f"  funding gaps ({len(gaps)}):")
                    for a, b in gaps:
                        print(f"    {a.isoformat()} -> {b.isoformat()}  ({(b - a)})")
                else:
                    print("  funding: no gaps")

            # Gap check on candles: allow one missed bar before flagging.
            candle_interval = parse_interval(args.interval)
            candle_gaps = find_gaps(
                conn, iid, table="candles", interval=args.interval,
                max_gap=2 * candle_interval, start=start, end=end,
            )
            if candle_gaps:
                print(f"  candles gaps ({len(candle_gaps)}):")
                for a, b in candle_gaps:
                    print(f"    {a.isoformat()} -> {b.isoformat()}  ({(b - a)})")
            else:
                print("  candles: no gaps")

            if failed_windows:
                print(f"  windows NOT backfilled ({len(failed_windows)}):")
                for a, b in failed_windows:
                    print(f"    {a.isoformat()} -> {b.isoformat()}")
    finally:
        await client.close()
        conn.close()
    if aborted:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
