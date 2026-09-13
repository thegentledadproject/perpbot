import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from polyperps.exchange.types import Candle, FundingObservation, SourceType
from polyperps.execution.live_bars import LiveBarBuilder
from polyperps.storage.db import connect, insert_candle, insert_funding

T0 = datetime(2026, 9, 12, 0, 0, tzinfo=timezone.utc)
H = timedelta(hours=1)
NATIVE = SourceType.POLYMARKET_REST


def load():
    spec = importlib.util.spec_from_file_location("run_paper", "scripts/run_paper.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_live_executor_refused_in_phase_2a(monkeypatch, tmp_path):
    monkeypatch.setenv("POLYPERPS_INSTRUMENT_IDS", "6")
    monkeypatch.setenv("POLYPERPS_DB_PATH", str(tmp_path / "t.sqlite3"))
    monkeypatch.setattr(sys, "argv", ["run_paper.py", "--executor", "live", "--hypothesis", "h1"])
    mod = load()
    with pytest.raises(SystemExit) as e:
        mod.main()
    assert e.value.code == 2


def test_parser_defaults():
    mod = load()
    args = mod.build_parser().parse_args(["--executor", "sim", "--hypothesis", "h1"])
    assert args.equity == "1000" and args.fee_category == "equity" and args.grid_index == 0


def test_constants_pinned():
    mod = load()
    assert mod.HEARTBEAT_S == 20 and mod.RECONCILE_S == 60


def test_run_id_minted_once_across_supervised_restarts(monkeypatch, tmp_path):
    """C1: a supervised restart must reopen the SAME paper account, so run_id is chosen once
    in main() and handed down to every run_once() - not re-minted per attempt."""
    monkeypatch.setenv("POLYPERPS_INSTRUMENT_IDS", "6")
    monkeypatch.setenv("POLYPERPS_DB_PATH", str(tmp_path / "t.sqlite3"))
    monkeypatch.setattr(sys, "argv", ["run_paper.py", "--executor", "sim", "--hypothesis", "h1"])
    mod = load()
    seen = []

    async def fake_run_once(args, settings):
        seen.append(args.run_id)
        if len(seen) == 1:
            raise RuntimeError("crash once -> supervisor restarts")
        raise SystemExit(0)   # second attempt: stop the supervisor

    monkeypatch.setattr(mod, "run_once", fake_run_once)
    monkeypatch.setattr(mod, "INITIAL_BACKOFF_S", 0.0)
    monkeypatch.setattr(mod, "MAX_BACKOFF_S", 0.0)
    with pytest.raises(SystemExit):
        mod.main()
    assert len(seen) == 2 and seen[0] == seen[1]
    assert seen[0].startswith("paper-")


def _candle(ts, close="100"):
    return Candle(instrument_id=6, interval="1h", open_ts=ts, open=Decimal("99"), high=Decimal("101"),
                  low=Decimal("98"), close=Decimal(close), volume=Decimal("1"), trades=1, received_ts=ts,
                  source_type=NATIVE)


def _funding(ts, rate="0.0001"):
    return FundingObservation(instrument_id=6, funding_rate=Decimal(rate), exchange_ts=ts, received_ts=ts,
                              source_type=NATIVE)


def test_seed_history_loads_closed_complete_bars_from_stored_candles():
    """C3: the strategy's warm-up is paid from stored 1h candles instead of waiting `lookback`
    hours of live ticks. Only complete (candle + funding) bars strictly before the current hour
    count; spread is the constant proxy the live builder also stamps."""
    mod = load()
    conn = connect(":memory:")
    now = T0 + 6 * H + timedelta(minutes=20)              # hour 6 is open: must not be seeded
    for h in range(7):
        insert_candle(conn, _candle(T0 + h * H, close=str(100 + h)))
        if h != 2:                                         # hour 2 has no settlement row -> incomplete
            insert_funding(conn, _funding(T0 + (h + 1) * H))
    builder = LiveBarBuilder()
    counts = mod.seed_history(conn, builder, {6: 4, 7: 4}, now=now, source_type=NATIVE)
    hist = builder.history(6)
    assert counts == {6: 4, 7: 0} and len(hist) == 4
    assert [b.open_ts for b in hist] == [T0 + h * H for h in (1, 3, 4, 5)]
    assert all(b.complete and b.spread_source == "constant" for b in hist)
    assert hist[-1].close == Decimal(105) and hist[-1].funding_rate == Decimal("0.0001")
    assert builder.history(7) == []


def test_seed_history_zero_bars_is_fine():
    mod = load()
    conn = connect(":memory:")
    builder = LiveBarBuilder()
    assert mod.seed_history(conn, builder, {6: 48}, now=T0, source_type=NATIVE) == {6: 0}
    assert builder.history(6) == []


def test_recover_strategies_tells_open_routers_their_side():
    """I4: after recover(), a router carrying a position tells its strategy which side it is
    on, so the strategy's internal _position matches the book instead of restarting at 0."""
    mod = load()

    class Recording:
        def __init__(self): self.calls = []
        def on_recover(self, sign): self.calls.append(sign)

    class NoHook:
        pass

    class R:
        def __init__(self, size, strategy): self.size, self.strategy = Decimal(size), strategy

    long_s, short_s, flat_s, plain = Recording(), Recording(), Recording(), NoHook()
    routers = {6: R("1", long_s), 7: R("-2", short_s), 8: R("0", flat_s), 9: R("1", plain)}
    mod._recover_strategies(routers)
    assert long_s.calls == [1] and short_s.calls == [-1] and flat_s.calls == []
