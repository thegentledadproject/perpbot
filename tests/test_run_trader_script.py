import asyncio
import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from polyperps.exchange.types import Candle, FeeSchedule, FundingObservation, Instrument, SourceType, Tick
from polyperps.execution.executor import GateClosed
from polyperps.execution.live_bars import LiveBarBuilder
from polyperps.execution.live_executor import ShadowExecutor
from polyperps.execution.order_router import Portfolio
from polyperps.execution.sim_executor import SimExecutor
from polyperps.execution.types import AccountSnapshot, State
from polyperps.monitor.alerts import Alerter, SqliteSink
from polyperps.storage.db import (
    connect, get_positions_local, insert_candle, insert_fee, insert_funding, list_alerts, list_decisions,
    load_account_snapshot, save_account_snapshot,
)

T0 = datetime(2026, 9, 12, 0, 0, tzinfo=timezone.utc)
H = timedelta(hours=1)
NATIVE = SourceType.POLYMARKET_REST


def load():
    spec = importlib.util.spec_from_file_location("run_trader", "scripts/run_trader.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeAccount:
    """The authenticated perps session, read side only: a flat account, no orders, a quiet event stream."""
    def __init__(self, equity="1000"):
        self.equity = Decimal(equity)
        self.closed = False

    async def fetch_portfolio(self):
        return SimpleNamespace(positions=(), margin=SimpleNamespace(total_account_value=self.equity),
                               in_liquidation=False)

    async def fetch_open_orders(self):
        return ()

    def __aiter__(self):
        async def gen():
            await asyncio.Event().wait()
            yield None  # never reached
        return gen()

    async def close(self):
        self.closed = True


class EndedStream(FakeAccount):
    def __aiter__(self):
        async def gen():
            return
            yield None  # makes this an async generator
        return gen()


class BrokenStream(FakeAccount):
    def __aiter__(self):
        async def gen():
            raise ConnectionError("socket closed")
            yield None
        return gen()


def test_live_exits_2_before_touching_the_wallet_while_any_lock_is_closed(monkeypatch, tmp_path):
    monkeypatch.setenv("POLYPERPS_INSTRUMENT_IDS", "6")
    monkeypatch.setenv("POLYPERPS_DB_PATH", str(tmp_path / "t.sqlite3"))
    monkeypatch.setattr(sys, "argv", ["run_trader.py", "--executor", "live", "--hypothesis", "h1"])
    mod = load()

    async def no_session(label):
        raise AssertionError("the wallet key must not be loaded while a lock is closed")

    monkeypatch.setattr(mod, "open_session", no_session)
    with pytest.raises(SystemExit) as e:
        mod.main()
    assert e.value.code == 2


def test_parser_defaults_and_modes():
    mod = load()
    args = mod.build_parser().parse_args(["--executor", "sim", "--hypothesis", "h1"])
    assert args.equity == "1000" and args.fee_category == "equity" and args.grid_index == 0
    for mode in ("sim", "shadow", "live"):
        assert mod.build_parser().parse_args(["--executor", mode, "--hypothesis", "h1"]).executor == mode
    assert mod.MODES == {}


def test_constants_pinned():
    mod = load()
    assert mod.HEARTBEAT_S == 20 and mod.RECONCILE_S == 60


async def test_build_executor_builds_each_mode_with_fakes():
    mod = load()
    conn = connect(":memory:")
    kw = dict(run_id="t", conn=conn, fee_rate=Decimal("0.0004"), equity=Decimal(1000), instrument_ids=[6])
    sim = await mod.build_executor("sim", **kw)
    assert isinstance(sim, SimExecutor) and sim.start_equity == Decimal(1000)
    shadow = await mod.build_executor("shadow", session=FakeAccount("1234"), **kw)
    assert isinstance(shadow, ShadowExecutor) and shadow.start_equity == Decimal(1234)
    with pytest.raises(GateClosed):                      # the constructor is still the real lock
        await mod.build_executor("live", session=FakeAccount(), **kw)


async def test_build_executor_keeps_the_loss_baseline_across_restarts():
    """Review focus 2: a restart after a -9 % day must not make the post-loss equity the new start."""
    mod = load()
    conn = connect(":memory:")
    save_account_snapshot(conn, "t", AccountSnapshot(equity=Decimal(910), positions=(), open_orders=(), stops={},
                                                     in_liquidation=False, ts=T0),
                          start_equity=Decimal(1000), executor="shadow")
    shadow = await mod.build_executor("shadow", run_id="t", conn=conn, fee_rate=Decimal("0.0004"),
                                      equity=Decimal(1000), instrument_ids=[6], session=FakeAccount("910"))
    assert shadow.start_equity == Decimal(1000)


def test_main_runs_once_and_lets_a_crash_exit(monkeypatch, tmp_path):
    """systemd (Restart=always) is the supervisor: a crash must leave the process so systemd
    counts it, not loop inside it. run_id is still minted once in main()."""
    monkeypatch.setenv("POLYPERPS_INSTRUMENT_IDS", "6")
    monkeypatch.setenv("POLYPERPS_DB_PATH", str(tmp_path / "t.sqlite3"))
    monkeypatch.setattr(sys, "argv", ["run_trader.py", "--executor", "sim", "--hypothesis", "h1"])
    mod = load()
    seen = []

    async def fake_run_once(args, settings):
        seen.append(args.run_id)
        raise RuntimeError("database is locked")

    monkeypatch.setattr(mod, "run_once", fake_run_once)
    with pytest.raises(RuntimeError, match="locked"):
        mod.main()
    assert len(seen) == 1 and seen[0].startswith("sim-")


def test_run_until_first_exits_propagates_a_dead_loop_and_cancels_the_rest():
    mod = load()
    cancelled = []

    async def forever():
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    async def dies():
        raise RuntimeError("database is locked")

    with pytest.raises(RuntimeError, match="locked"):
        asyncio.run(mod.run_until_first_exits(forever(), dies()))
    assert cancelled == [True]


def test_run_until_first_exits_returns_when_one_finishes_cleanly():
    mod = load()

    async def forever():
        await asyncio.sleep(3600)

    async def ends():
        return None

    asyncio.run(asyncio.wait_for(mod.run_until_first_exits(forever(), ends()), 5))


def test_event_stream_end_or_error_ends_the_run():
    """§4.10: when the live event stream ends or raises, the pump returns (or raises) and the
    whole run ends, so systemd restarts it and recovery runs. Regression pin: this already held
    before run_trader.py (run_until_first_exits came from PR #2); it guards the design."""
    mod = load()

    async def forever():
        await asyncio.sleep(3600)

    def pump(session):
        pf = Portfolio(run_id="t", executor=ShadowExecutor(session), conn=connect(":memory:"),
                       alerter=Alerter("t", []), routers={})
        return pf.run_event_pump()

    asyncio.run(asyncio.wait_for(mod.run_until_first_exits(forever(), pump(EndedStream())), 5))
    with pytest.raises(ConnectionError):
        asyncio.run(asyncio.wait_for(mod.run_until_first_exits(forever(), pump(BrokenStream())), 5))


# --- run_once end to end with fakes ------------------------------------------------------------

T1 = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


def _tick(minutes, mark="100"):
    ts = T1 + timedelta(minutes=minutes)
    return Tick(instrument_id=6, mark_price=Decimal(mark), index_price=Decimal(mark), last_price=Decimal(mark),
                funding_rate=Decimal("0.0000125"), next_funding=ts, exchange_ts=ts, received_ts=ts,
                source_type=SourceType.POLYMARKET_WS, sequence=minutes)


class FakePublicClient:
    """Public market data: instrument 6 and three ticks that close one hourly bar, then the stream ends."""
    async def fetch_instruments(self):
        return (Instrument(instrument_id=6, symbol="BTC", category="crypto", funding_interval="1h", max_leverage=20,
                           price_decimals=2, quantity_decimals=4, min_notional=Decimal("1"), isolated_only=True),)

    async def stream_ticks(self, ids):
        for m in (1, 30, 61):
            yield _tick(m)
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.2)     # let the bar and fast loops see the closed bar before the run ends

    async def close(self):
        pass


def _setup(monkeypatch, tmp_path, executor):
    monkeypatch.setenv("POLYPERPS_INSTRUMENT_IDS", "6")
    db_path = tmp_path / "t.sqlite3"
    monkeypatch.setenv("POLYPERPS_DB_PATH", str(db_path))
    conn = connect(db_path)
    insert_fee(conn, FeeSchedule(category="equity", taker_fee_rate=Decimal("0.0004"), maker_fee_rate=Decimal(0),
                                 fetched_at=T1))
    conn.close()
    mod = load()
    monkeypatch.setattr(mod, "PolymarketPerpsClient", SimpleNamespace(create_public=lambda **kw: FakePublicClient()))
    monkeypatch.setattr(mod, "_alerter", lambda run_id, conn: Alerter(run_id, [SqliteSink(conn)]))
    monkeypatch.setattr(mod, "HEARTBEAT_S", 0.01)
    monkeypatch.setattr(mod, "RECONCILE_S", 0.01)
    args = mod.build_parser().parse_args(["--executor", executor, "--hypothesis", "h1", "--run-id", "t"])
    return mod, args, db_path


def test_run_once_sim_wires_hooks_bars_and_account_snapshots(monkeypatch, tmp_path):
    mod, args, db_path = _setup(monkeypatch, tmp_path, "sim")
    asyncio.run(mod.run_once(args, mod.load_settings()))
    conn = connect(db_path)
    snap, start_equity, executor = load_account_snapshot(conn, "t")
    assert executor == "sim" and start_equity == Decimal(1000) and snap.equity == Decimal(1000)
    assert [d.note for d in list_decisions(conn, "t", 6)] == ["skip:warmup"]   # the closed bar reached the router


def test_run_once_shadow_reads_the_account_and_trips_the_loss_limit(monkeypatch, tmp_path):
    mod, args, db_path = _setup(monkeypatch, tmp_path, "shadow")
    conn = connect(db_path)
    save_account_snapshot(conn, "t", AccountSnapshot(equity=Decimal(1000), positions=(), open_orders=(), stops={},
                                                     in_liquidation=False, ts=T1),
                          start_equity=Decimal(1000), executor="shadow")
    conn.close()
    session = FakeAccount("890")                           # -11 % against the persisted baseline
    sdk = SimpleNamespace(closed=False)

    async def sdk_close():
        sdk.closed = True

    sdk.close = sdk_close

    async def fake_open_session(label):
        return sdk, session

    monkeypatch.setattr(mod, "open_session", fake_open_session)
    asyncio.run(mod.run_once(args, mod.load_settings()))
    conn = connect(db_path)
    snap, start_equity, executor = load_account_snapshot(conn, "t")
    assert executor == "shadow" and start_equity == Decimal(1000) and snap.equity == Decimal(890)
    assert ("CRITICAL", "loss_limit") in [(a[1], a[2]) for a in list_alerts(conn, "t")]
    assert get_positions_local(conn, "t")[6].state is State.HALTED
    assert session.closed and sdk.closed


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
