"""Store the exchange's current fee schedule so backtests are reproducible
and spec 3.3's "did fees change?" re-check has a baseline.

    .venv/Scripts/python scripts/store_fees.py

No credentials. One REST call.
"""

from __future__ import annotations

import asyncio

from polyperps.config import load_settings
from polyperps.exchange.client import PolymarketPerpsClient
from polyperps.storage import db


async def main() -> None:
    settings = load_settings()
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = db.connect(settings.db_path)
    client = PolymarketPerpsClient.create_public(
        rate_per_sec=settings.rest_rate_per_sec, burst=settings.rest_burst
    )
    try:
        for fee in await client.fetch_fees():
            written = db.insert_fee(conn, fee)
            print(f"{fee.category:<10} taker={fee.taker_fee_rate} maker={fee.maker_fee_rate} "
                  f"fetched_at={fee.fetched_at.isoformat()} {'stored' if written else 'duplicate'}")
    finally:
        await client.close()
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
