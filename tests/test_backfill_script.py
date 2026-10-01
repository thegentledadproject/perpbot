import importlib.util
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.exchange.types import Candle, SourceType

T0 = datetime(2026, 10, 1, 14, 0, tzinfo=timezone.utc)
H = timedelta(hours=1)


def load():
    spec = importlib.util.spec_from_file_location("backfill", "scripts/backfill.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _c(open_ts, interval="1h"):
    return Candle(instrument_id=6, interval=interval, open_ts=open_ts, open=Decimal(1), high=Decimal(1),
                  low=Decimal(1), close=Decimal(1), volume=Decimal(1), trades=1, received_ts=open_ts,
                  source_type=SourceType.POLYMARKET_REST)


def test_open_candle_is_not_stored():
    """The 14:00 candle is still open at 14:05; INSERT OR IGNORE would freeze it half-built."""
    mod = load()
    out = mod.closed([_c(T0 - H), _c(T0)], now=T0 + timedelta(minutes=5))
    assert [c.open_ts for c in out] == [T0 - H]


def test_candle_closing_exactly_now_is_kept():
    mod = load()
    out = mod.closed([_c(T0 - H)], now=T0)
    assert len(out) == 1


def test_minute_candles_use_their_own_interval():
    mod = load()
    out = mod.closed([_c(T0, "1m"), _c(T0 + timedelta(minutes=1), "1m")], now=T0 + timedelta(minutes=1, seconds=30))
    assert [c.open_ts for c in out] == [T0]
