import asyncio
import importlib.util
import sqlite3
import sys
from datetime import datetime, timedelta
from decimal import Decimal

from polyperps.exchange.types import Candle, SourceType


def load():
    spec = importlib.util.spec_from_file_location("backfill_hyperliquid", "scripts/backfill_hyperliquid.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _FakeHL:
    """Every candles() call returns the last closed bar plus the one still open at `end`."""

    def __init__(self, **_):
        pass

    async def funding_history(self, coin, *, start, end, instrument_id):
        return []

    async def candles(self, coin, *, interval, start, end, instrument_id):
        step = timedelta(hours=1) if interval == "1h" else timedelta(minutes=1)
        open_now = end.replace(second=0, microsecond=0)  # end is the script's own "now"
        if interval == "1h":
            open_now = open_now.replace(minute=0)
        return [
            Candle(instrument_id=instrument_id, interval=interval, open_ts=ts, open=Decimal(1), high=Decimal(1),
                   low=Decimal(1), close=Decimal(1), volume=Decimal(1), trades=1, received_ts=end,
                   source_type=SourceType.PROXY_HYPERLIQUID)
            for ts in (open_now - step, open_now)
        ]

    async def close(self):
        pass


def test_still_open_proxy_candles_are_not_stored(monkeypatch, tmp_path):
    """INSERT OR IGNORE would freeze the open bar half-built; only the closed one may land."""
    db_path = tmp_path / "t.sqlite3"
    mod = load()
    monkeypatch.setattr(mod, "HyperliquidClient", _FakeHL)
    monkeypatch.setenv("POLYPERPS_DB_PATH", str(db_path))
    monkeypatch.setenv("POLYPERPS_INSTRUMENT_IDS", "6")
    monkeypatch.setattr(sys, "argv", ["x", "--days", "1", "--map", "6=BTC"])
    asyncio.run(mod.main())
    rows = sqlite3.connect(db_path).execute(
        "select interval, open_ts, received_ts from candles order by interval").fetchall()
    assert [r[0] for r in rows] == ["1h", "1m"]
    for interval, open_ts, received_ts in rows:
        step = timedelta(hours=1) if interval == "1h" else timedelta(minutes=1)
        assert datetime.fromisoformat(open_ts) + step <= datetime.fromisoformat(received_ts)
