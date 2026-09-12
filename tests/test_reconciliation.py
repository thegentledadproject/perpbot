from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.backtest.bars import Bar
from polyperps.execution.order_router import InstrumentRouter, Portfolio
from polyperps.execution.reconciliation import Mismatch, diff
from polyperps.execution.sim_executor import SimExecutor
from polyperps.execution.types import AccountSnapshot, PositionLocalRow, PositionView, State
from polyperps.exchange.types import SourceType
from polyperps.monitor.alerts import Alerter, SqliteSink
from polyperps.storage.db import connect, list_alerts

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


def local(iid, state, size, stop=None):
    return PositionLocalRow(run_id="r", instrument_id=iid, state=state, size=Decimal(size), entry_price=Decimal(100),
                            stop_trigger=Decimal(stop) if stop else None, stop_order_id=None,
                            cumulative_funding=Decimal(0), updated_at=T0)


def remote(positions=(), open_orders=(), stops=None):
    return AccountSnapshot(equity=Decimal(1000), positions=tuple(positions), open_orders=tuple(open_orders),
                           stops=stops or {}, in_liquidation=False, ts=T0)


def pv(iid, size):
    return PositionView(instrument_id=iid, size=Decimal(size), entry_price=Decimal(100), notional=abs(Decimal(size)) * 100,
                        leverage=3, liquidation_price=Decimal(70), unrealised_pnl=Decimal(0), cumulative_funding=Decimal(0))


def test_diff_clean():
    assert diff(local={6: local(6, State.OPEN, "1", "85")}, remote=remote([pv(6, "1")], stops={6: Decimal(85)}),
                run_id="r", known_orders=set()) == []


def test_diff_each_kind():
    ms = diff(local={6: local(6, State.OPEN, "1", "85"), 7: local(7, State.FLAT, "0")},
              remote=remote([pv(6, "2")], open_orders=("r-6-9", "alien-1"), stops={7: Decimal(50), 6: Decimal(70)}),
              run_id="r", known_orders={"r-6-9"})
    kinds = sorted(m.kind for m in ms)
    assert kinds == ["size", "stop_without_position", "unknown_order"]   # size mismatch on 6 short-circuits stop checks for 6
    assert next(m for m in ms if m.kind == "unknown_order").remote == "alien-1"


def test_diff_stop_drift_and_missing_stop_and_unknown_position():
    ms = diff(local={6: local(6, State.OPEN, "1", "85")}, remote=remote([pv(6, "1")], stops={6: Decimal(70)}),
              run_id="r", known_orders=set())
    assert [m.kind for m in ms] == ["stop_drift"]
    ms = diff(local={6: local(6, State.OPEN, "1")}, remote=remote([pv(6, "1"), pv(8, "3")]), run_id="r", known_orders=set())
    assert [m.kind for m in ms] == ["missing_stop", "size"]
    assert ms[1].instrument_id == 8 and ms[1].local == "0"


class Strat:
    name = "t"; params = {}
    def target(self, h): return Decimal(1)
    def on_flatten(self): pass


def bar(i):
    c = Decimal(100)
    return Bar(instrument_id=6, source_type=SourceType.POLYMARKET_WS, open_ts=T0 + i * timedelta(hours=1), open=c, high=c,
               low=c, close=c, index_close=None, funding_rate=Decimal("0.0001"), spread_bps=Decimal(5),
               spread_source="constant", complete=True)


async def make_open():
    conn = connect(":memory:")
    ex = SimExecutor("r", equity=Decimal(1000), taker_fee_rate=Decimal("0.0004"), clock=lambda: T0)
    ex.update_mark(6, Decimal(100))
    alerter = Alerter("r", [SqliteSink(conn)])
    router = InstrumentRouter(run_id="r", instrument_id=6, category="crypto", strategy=Strat(), executor=ex, conn=conn,
                              alerter=alerter, categories={6: "crypto"}, clock=lambda: T0)
    pf = Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter, routers={6: router})
    await pf.on_bar({6: [bar(0)]}, "run")
    for ev in ex.drain_events():
        await pf.dispatch(ev)
    assert router.state is State.OPEN
    return conn, ex, router, pf


async def test_reconcile_replaces_missing_stop():
    conn, ex, router, pf = await make_open()
    await ex.cancel_stop(6)                       # simulate the venue losing our stop
    ms = await pf.reconcile_now()
    assert [m.kind for m in ms] == ["missing_stop"]
    assert (await ex.snapshot()).stops == {6: router.stop_trigger}
    assert "stop_missing" in [a[2] for a in list_alerts(conn, "r")]


async def test_reconcile_size_mismatch_halts():
    conn, ex, router, pf = await make_open()
    ex.update_mark(6, Decimal(50))
    ex.check_triggers()                           # stop fires on the venue; we never process the event
    ms = await pf.reconcile_now()
    assert [m.kind for m in ms] == ["size"]
    assert router.state is State.HALTED and any(a[1] == "CRITICAL" for a in list_alerts(conn, "r"))


def test_diff_skips_size_and_stop_checks_for_pending_local_rows():
    # An order is in flight (ENTRY_PENDING): the persisted size is still pre-fill (0) while the
    # remote already reflects the fill. That's not a reconciliation problem - the fill handler
    # owns this row's transition - so diff() must not raise size or missing_stop for it.
    ms = diff(local={6: local(6, State.ENTRY_PENDING, "0")}, remote=remote([pv(6, "1")]),
              run_id="r", known_orders=set())
    assert ms == []


async def test_reconcile_size_mismatch_halt_is_idempotent():
    conn, ex, router, pf = await make_open()
    ex.update_mark(6, Decimal(50))
    ex.check_triggers()                           # stop fires on the venue; we never process the event
    await pf.reconcile_now()
    assert router.state is State.HALTED
    ms2 = await pf.reconcile_now()                # size mismatch still present on the second pass
    assert [m.kind for m in ms2] == ["size"]
    assert router.state is State.HALTED
    halted_alerts = [a for a in list_alerts(conn, "r") if a[2] == "halted"]
    assert len(halted_alerts) == 1
