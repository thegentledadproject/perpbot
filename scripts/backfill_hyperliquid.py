"""Spec 1.1: backfill Hyperliquid funding + candles as PROXY_HYPERLIQUID.

    .venv/Scripts/python scripts/backfill_hyperliquid.py --days 400 --map 6=BTC,7=ETH

Pulls 1h funding, 1h candles and 1m candles in 24h windows. Transient errors
(429/5xx/transport) are retried honouring Retry-After; anything else aborts.
Rows land in the same tables as native data, distinguished by source_type.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone

from polyperps.config import load_settings
from polyperps.data_ingest.hyperliquid import HyperliquidClient, TransientProxyError
from polyperps.exchange.rate_limiter import TokenBucket
from polyperps.storage import db

_MAX_ATTEMPTS = 3
_RETRY_SLEEP_S = 60


def parse_map(text: str) -> dict[int, str]:
    out: dict[int, str] = {}
    for part in text.split(","):
        iid, coin = part.strip().split("=")
        out[int(iid)] = coin.strip().upper()
    return out


async def with_retries(coro_factory, label: str):
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            return await coro_factory()
        except TransientProxyError as exc:
            if attempt == _MAX_ATTEMPTS:
                print(f"  {label}: giving up after {_MAX_ATTEMPTS} attempts ({exc})")
                raise
            wait = exc.retry_after or _RETRY_SLEEP_S
            print(f"  {label}: {exc}; retry {attempt}/{_MAX_ATTEMPTS} in {wait:.0f}s")
            await asyncio.sleep(wait)
    raise AssertionError("unreachable")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, required=True)
    ap.add_argument("--map", required=True, help="instrument_id=COIN pairs, e.g. 6=BTC,7=ETH")
    ap.add_argument("--no-minutes", action="store_true", help="skip 1m candles (faster)")
    args = ap.parse_args()

    settings = load_settings()
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = db.connect(settings.db_path)
    client = HyperliquidClient(
        limiter=TokenBucket(rate_per_sec=settings.rest_rate_per_sec, burst=settings.rest_burst)
    )
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=args.days)
    try:
        for iid, coin in parse_map(args.map).items():
            n_f = n_h = n_m = 0
            failed: list[tuple[datetime, datetime]] = []
            w_start = start
            while w_start < end:
                w_end = min(w_start + timedelta(days=1), end)
                label = f"{coin} {w_start:%Y-%m-%d}"
                try:
                    for f in await with_retries(
                        lambda: client.funding_history(coin, start=w_start, end=w_end, instrument_id=iid), label
                    ):
                        n_f += db.insert_funding(conn, f)
                    for c in await with_retries(
                        lambda: client.candles(coin, interval="1h", start=w_start, end=w_end, instrument_id=iid), label
                    ):
                        n_h += db.insert_candle(conn, c)
                    if not args.no_minutes:
                        for c in await with_retries(
                            lambda: client.candles(coin, interval="1m", start=w_start, end=w_end, instrument_id=iid), label
                        ):
                            n_m += db.insert_candle(conn, c)
                except TransientProxyError:
                    failed.append((w_start, w_end))
                w_start = w_end
            print(f"{iid} {coin}: +{n_f} funding, +{n_h} 1h candles, +{n_m} 1m candles")
            if failed:
                print(f"  windows NOT backfilled ({len(failed)}):")
                for a, b in failed:
                    print(f"    {a.isoformat()} -> {b.isoformat()}")
    finally:
        await client.close()
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
