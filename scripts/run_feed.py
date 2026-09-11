"""Spec 0.3: continuous market feed with persistence and health logging.

    POLYPERPS_INSTRUMENT_IDS=<id,id> .venv/Scripts/python scripts/run_feed.py

Discover ids first:
    .venv/Scripts/python scripts/run_feed.py --list-instruments

Runs until Ctrl-C. WS ticks (mark/index/last/funding) go through the
sanity filter into `ticks`; rejections into `rejections`; a REST book
snapshot per instrument every POLYPERPS_BOOK_INTERVAL_S into
`book_snapshots`. A health line is logged every POLYPERPS_HEALTH_LOG_S.
The SDK reconnects the WS internally; if the stream ends anyway, this
loop restarts it with backoff so a 48h soak survives transient failures.

Before subscribing, run_once() validates every id in
POLYPERPS_INSTRUMENT_IDS against fetch_instruments() and logs the
resolved id -> symbol map at INFO; an unknown id logs an error and exits
via SystemExit(2) rather than silently subscribing to nothing useful for
48 hours. SystemExit is a BaseException, so main()'s restart loop does
not treat it as a transient crash to retry.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import sys
from datetime import datetime, timezone

from polyperps.config import load_settings
from polyperps.data_ingest.market_feed import MarketFeed
from polyperps.exchange.client import PolymarketPerpsClient
from polyperps.storage import db

log = logging.getLogger("polyperps.feed")


async def list_instruments() -> None:
    client = PolymarketPerpsClient.create_public()
    try:
        for i in await client.fetch_instruments():
            print(f"{i.instrument_id:>6}  {i.symbol:<14} {i.category:<10} "
                  f"funding={i.funding_interval} max_lev={i.max_leverage}x")
    finally:
        await client.close()


async def book_snapshots(client, conn, settings, stop: asyncio.Event) -> None:
    while not stop.is_set():
        for iid in settings.instrument_ids:
            try:
                db.insert_book(conn, await client.fetch_book(iid, depth=10))
            except Exception:
                log.exception("book snapshot failed for %s", iid)
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.book_snapshot_interval_s)
        except asyncio.TimeoutError:
            pass


async def health_logger(feed: MarketFeed, settings, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.health_log_interval_s)
        except asyncio.TimeoutError:
            h = feed.health
            silent = (
                (datetime.now(timezone.utc) - h.last_event_wallclock).total_seconds()
                if h.last_event_wallclock else None
            )
            log.info("health received=%d accepted=%d rejected=%s silent_for_s=%s last=%s",
                     h.received, h.accepted, dict(h.rejected), silent,
                     {k: v.isoformat() for k, v in h.last_accepted.items()})


async def run_once(settings) -> None:
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = db.connect(settings.db_path)
    client = PolymarketPerpsClient.create_public(
        rate_per_sec=settings.rest_rate_per_sec, burst=settings.rest_burst
    )

    known = {i.instrument_id: i.symbol for i in await client.fetch_instruments()}
    unknown = [iid for iid in settings.instrument_ids if iid not in known]
    if unknown:
        log.error("unknown instrument id(s) in POLYPERPS_INSTRUMENT_IDS: %s", unknown)
        await client.close()
        conn.close()
        raise SystemExit(2)
    log.info("resolved instruments: %s",
              {iid: known[iid] for iid in settings.instrument_ids})

    stop = asyncio.Event()

    def on_reject(tick, rej):
        db.insert_rejection(conn, instrument_id=tick.instrument_id, reason=rej.reason,
                            detail=rej.detail, at=tick.received_ts)
        log.warning("rejected %s seq=%s: %s %s", tick.instrument_id, tick.sequence, rej.reason, rej.detail)

    ticks = client.stream_ticks(settings.instrument_ids)
    feed = MarketFeed(
        ticks=ticks,
        bounds=settings.bounds,
        on_accept=lambda t: db.insert_tick(conn, t),
        on_reject=on_reject,
    )
    tasks = [
        asyncio.create_task(book_snapshots(client, conn, settings, stop)),
        asyncio.create_task(health_logger(feed, settings, stop)),
    ]
    try:
        await feed.run()
    finally:
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        with contextlib.suppress(Exception):
            await ticks.aclose()
        await client.close()
        conn.close()


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if "--list-instruments" in sys.argv:
        await list_instruments()
        return
    settings = load_settings()
    backoff = 1.0
    while True:
        started = asyncio.get_running_loop().time()
        try:
            await run_once(settings)
            log.warning("feed stream ended cleanly; restarting")
        except (KeyboardInterrupt, asyncio.CancelledError, SystemExit):
            # SystemExit is a BaseException already (not caught by `except Exception`
            # below); listed explicitly so an unknown-instrument-id abort from
            # run_once() is never mistaken for a transient crash and retried.
            raise
        except Exception:
            log.exception("feed crashed; restarting in %.0fs", backoff)
        ran_for = asyncio.get_running_loop().time() - started
        backoff = 1.0 if ran_for > 300 else min(backoff * 2, 60.0)
        await asyncio.sleep(backoff)


def _raise_keyboard_interrupt(signum, frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
