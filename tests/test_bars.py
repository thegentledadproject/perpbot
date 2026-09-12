from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.backtest.bars import (
    Bar, align_pair, build_bars, floor_hour, floor_minute, is_native, load_minute_closes,
)
from polyperps.exchange.types import (
    BookLevel, BookSnapshot, Candle, FundingObservation, SourceType, Tick,
)
from polyperps.storage.db import connect, insert_book, insert_candle, insert_funding, insert_tick

UTC = timezone.utc
T0 = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)
H = timedelta(hours=1)
NATIVE = SourceType.POLYMARKET_REST
PROXY = SourceType.PROXY_HYPERLIQUID


def candle(ts, st, interval="1h", close="100"):
    return Candle(instrument_id=6, interval=interval, open_ts=ts, open=Decimal("99"), high=Decimal("101"),
                  low=Decimal("98"), close=Decimal(close), volume=Decimal("1"), trades=1,
                  received_ts=ts, source_type=st)


def funding(ts, st, rate="0.0001"):
    return FundingObservation(instrument_id=6, funding_rate=Decimal(rate), exchange_ts=ts,
                              received_ts=ts, source_type=st)


def tick(ts, index="100.5"):
    return Tick(instrument_id=6, mark_price=Decimal("100"), index_price=Decimal(index),
                last_price=Decimal("100"), funding_rate=Decimal("0"), next_funding=ts,
                exchange_ts=ts, received_ts=ts, source_type=SourceType.POLYMARKET_WS, sequence=int(ts.timestamp()))


def book(ts, bid="99.5", ask="100.5"):
    return BookSnapshot(instrument_id=6, bids=(BookLevel(price=Decimal(bid), quantity=Decimal(1)),),
                        asks=(BookLevel(price=Decimal(ask), quantity=Decimal(1)),),
                        exchange_ts=ts, received_ts=ts, source_type=NATIVE)


def test_floor_helpers_and_is_native():
    t = datetime(2026, 9, 11, 12, 34, 56, 789, tzinfo=UTC)
    assert floor_hour(t) == datetime(2026, 9, 11, 12, 0, tzinfo=UTC)
    assert floor_minute(t) == datetime(2026, 9, 11, 12, 34, tzinfo=UTC)
    assert is_native(SourceType.POLYMARKET_WS) and is_native(SourceType.POLYMARKET_REST)
    assert not is_native(PROXY)


def test_build_bars_native_complete_bar():
    conn = connect(":memory:")
    insert_candle(conn, candle(T0, NATIVE, close="100"))
    insert_funding(conn, funding(T0 + H, NATIVE, "0.0002"))       # settled at end of the bar
    insert_tick(conn, tick(T0 + timedelta(minutes=10), index="100.1"))
    insert_tick(conn, tick(T0 + timedelta(minutes=50), index="100.9"))  # last in hour wins
    insert_book(conn, book(T0 + timedelta(minutes=5)))               # 100 bps
    insert_book(conn, book(T0 + timedelta(minutes=35), bid="99.9", ask="100.1"))  # 20 bps
    bars = build_bars(conn, 6, NATIVE, start=T0, end=T0 + H)
    assert len(bars) == 1
    b = bars[0]
    assert b.complete is True and b.close == Decimal("100") and b.funding_rate == Decimal("0.0002")
    assert b.index_close == Decimal("100.9")
    assert b.spread_bps == Decimal("60")  # median of 100 and 20
    assert b.spread_source == "book"
    assert b.source_type is NATIVE


def test_build_bars_native_without_snapshots_labels_spread_constant():
    conn = connect(":memory:")
    insert_candle(conn, candle(T0, NATIVE))
    insert_funding(conn, funding(T0 + H, NATIVE))
    (b,) = build_bars(conn, 6, NATIVE, start=T0, end=T0 + H)
    assert b.complete and b.spread_bps == Decimal("5") and b.spread_source == "constant"


def test_build_bars_marks_incomplete_when_candle_or_funding_missing():
    conn = connect(":memory:")
    insert_candle(conn, candle(T0, NATIVE))                     # bar 0: candle, no funding
    insert_funding(conn, funding(T0 + 2 * H, NATIVE))           # bar 1: funding, no candle
    bars = build_bars(conn, 6, NATIVE, start=T0, end=T0 + 2 * H)
    assert [b.complete for b in bars] == [False, False]
    assert bars[0].close == Decimal("100") and bars[0].funding_rate is None
    assert bars[1].close is None and bars[1].funding_rate == Decimal("0.0001")


def test_last_bar_funding_matched_when_timestamp_is_off_the_hour():
    conn = connect(":memory:")
    insert_candle(conn, candle(T0, NATIVE))
    insert_funding(conn, funding(T0 + H + timedelta(minutes=5), NATIVE))
    (b,) = build_bars(conn, 6, NATIVE, start=T0, end=T0 + H)
    assert b.funding_rate == Decimal("0.0001") and b.complete is True


def test_build_bars_proxy_uses_constant_spread_and_no_index():
    conn = connect(":memory:")
    insert_candle(conn, candle(T0, PROXY))
    insert_funding(conn, funding(T0 + H, PROXY))
    insert_tick(conn, tick(T0 + timedelta(minutes=10)))  # native tick must be ignored for proxy bars
    (b,) = build_bars(conn, 6, PROXY, start=T0, end=T0 + H, proxy_spread_bps=Decimal("7"))
    assert b.complete and b.index_close is None and b.spread_bps == Decimal("7")
    assert b.spread_source == "constant"


def test_build_bars_range_is_hour_aligned_and_end_exclusive():
    conn = connect(":memory:")
    for i in range(3):
        insert_candle(conn, candle(T0 + i * H, NATIVE))
        insert_funding(conn, funding(T0 + (i + 1) * H, NATIVE))
    bars = build_bars(conn, 6, NATIVE, start=T0 + timedelta(minutes=20), end=T0 + 2 * H)
    assert [b.open_ts for b in bars] == [T0, T0 + H]


def test_align_pair_keeps_only_hours_complete_in_both():
    mk = lambda ts, st, complete: Bar(instrument_id=6, source_type=st, open_ts=ts, open=Decimal(1), high=Decimal(1),
                                      low=Decimal(1), close=Decimal(1), index_close=None, funding_rate=Decimal(0),
                                      spread_bps=Decimal(5), spread_source="constant", complete=complete)
    native = [mk(T0, NATIVE, True), mk(T0 + H, NATIVE, False), mk(T0 + 2 * H, NATIVE, True)]
    proxy = [mk(T0, PROXY, True), mk(T0 + H, PROXY, True), mk(T0 + 3 * H, PROXY, True)]
    pairs = align_pair(native, proxy)
    assert [(a.open_ts, b.open_ts) for a, b in pairs] == [(T0, T0)]


def test_load_minute_closes():
    conn = connect(":memory:")
    insert_candle(conn, candle(T0, NATIVE, interval="1m", close="100.1"))
    insert_candle(conn, candle(T0 + timedelta(minutes=1), NATIVE, interval="1m", close="100.2"))
    insert_candle(conn, candle(T0, NATIVE, interval="1h", close="999"))
    closes = load_minute_closes(conn, 6, NATIVE, start=T0, end=T0 + H)
    assert closes == {T0: Decimal("100.1"), T0 + timedelta(minutes=1): Decimal("100.2")}
