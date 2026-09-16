"""Serve the read-only dashboard for a paper run.

Reads the same /etc/polyperps/env as the other units. Never writes the DB.
Binds POLYPERPS_DASHBOARD_BIND (default 127.0.0.1:8080 so a local run never
listens on all interfaces by accident; the box sets 0.0.0.0:80).
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import threading

from polyperps.config import load_settings
from polyperps.dashboard.server import DashboardServer
from polyperps.exchange.client import PolymarketPerpsClient

log = logging.getLogger("run_dashboard")
REFRESH_S = 3600


class InstrumentCache:
    """Best-effort instrument names/categories, refreshed hourly in a thread."""

    def __init__(self, *, rate_per_sec: float, burst: int) -> None:
        self._rate, self._burst = rate_per_sec, burst
        self._value = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="instruments", daemon=True)

    def get(self):
        return self._value

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    async def _fetch(self):
        client = PolymarketPerpsClient.create_public(rate_per_sec=self._rate, burst=self._burst)
        try:
            return {i.instrument_id: i for i in await client.fetch_instruments()}
        finally:
            await client.close()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._value = asyncio.run(self._fetch())
                log.info("instruments refreshed: %d", len(self._value))
            except Exception as e:   # best-effort by design: the page falls back to ids
                log.warning("instrument fetch failed (names fall back to ids): %s", e)
            self._stop.wait(REFRESH_S)


def parse_bind(text: str) -> tuple[str, int]:
    host, _, port = text.rpartition(":")
    if not host or not port.isdigit():
        raise SystemExit(f"POLYPERPS_DASHBOARD_BIND must be host:port, got {text!r}")
    return host, int(port)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    settings = load_settings()
    run_id = os.environ.get("PAPER_RUN_ID")
    if not run_id:
        raise SystemExit("PAPER_RUN_ID is required (the paper account to show)")
    hypothesis = os.environ.get("PAPER_HYPOTHESIS", "?")
    bind = parse_bind(os.environ.get("POLYPERPS_DASHBOARD_BIND", "127.0.0.1:8080"))

    cache = InstrumentCache(rate_per_sec=settings.rest_rate_per_sec, burst=settings.rest_burst)
    server = DashboardServer(
        bind=bind, db_path=settings.db_path, run_id=run_id,
        instrument_ids=settings.instrument_ids, hypothesis=hypothesis,
        instruments_provider=cache.get,
    )
    cache.start()
    log.info("dashboard on http://%s:%d for run %s (db %s)", bind[0], server.port, run_id, settings.db_path)
    try:
        server.serve_forever()
    finally:
        cache.stop()
        server.server_close()
        log.info("dashboard stopped")


def _raise_keyboard_interrupt(signum, frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)
    try:
        main()
    except KeyboardInterrupt:
        pass
