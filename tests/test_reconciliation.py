from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from polyperps.backtest.bars import Bar
from polyperps.execution.order_router import InstrumentRouter, Portfolio
from polyperps.execution.live_executor import ShadowExecutor
from polyperps.execution.reconciliation import Mismatch, diff
from polyperps.execution.sim_executor import SimExecutor
from polyperps.execution.types import AccountSnapshot, OrderRequest, PositionLocalRow, PositionView, State
from polyperps.exchange.types import SourceType
from polyperps.monitor.alerts import Alerter, SqliteSink
from polyperps.risk.liquidation_guard import stop_price
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
    assert [(m.kind, m.instrument_id) for m in ms] == [("missing_stop", 6), ("missing_stop", 8), ("size", 8)]
    assert ms[2].local == "0"


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


def test_diff_skips_size_checks_for_pending_rows_but_never_the_stop():
    # An order is in flight (ENTRY_PENDING): the persisted size is still pre-fill (0) while the
    # remote already reflects the fill. The size is the fill handler's business - but a venue
    # position without a venue stop is a mismatch whatever the local state (Part A §4).
    ms = diff(local={6: local(6, State.ENTRY_PENDING, "0")}, remote=remote([pv(6, "1")]),
              run_id="r", known_orders=set())
    assert [m.kind for m in ms] == ["missing_stop"]
    assert diff(local={6: local(6, State.ENTRY_PENDING, "0")}, remote=remote([pv(6, "1")], stops={6: Decimal(85)}),
                run_id="r", known_orders=set()) == []


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


class AlienOrderAccount:
    """A real account (as the perps session sees it) with one order this run never placed."""
    async def fetch_portfolio(self):
        return SimpleNamespace(positions=(), margin=SimpleNamespace(total_account_value=Decimal(1000)),
                               in_liquidation=False)

    async def fetch_open_orders(self):
        return (SimpleNamespace(client_order_id="alien-1", id=5, tp_sl=None, instrument_id=6),)


async def test_reconcile_in_shadow_records_refused_writes_and_keeps_running():
    conn = connect(":memory:")
    ex = ShadowExecutor(AlienOrderAccount(), clock=lambda: T0)
    alerter = Alerter("r", [SqliteSink(conn)])
    pf = Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter, routers={})
    ms = await pf.reconcile_now()             # must not raise: the reconcile loop keeps running
    assert [m.kind for m in ms] == ["unknown_order"]
    kinds = [a[2] for a in list_alerts(conn, "r")]
    assert "shadow_refused" in kinds and "unknown_order" not in kinds


STATES = [State.OPEN, State.HALTED, State.LIQUIDATED, State.ENTRY_PENDING, State.EXIT_PENDING, State.FLAT]


@pytest.mark.parametrize("state", STATES + [None])
def test_missing_stop_is_raised_for_every_local_state(state):
    loc = {} if state is None else {6: local(6, state, "1")}
    ms = diff(local=loc, remote=remote([pv(6, "1")]), run_id="r", known_orders=set())
    assert [m.kind for m in ms if m.instrument_id == 6].count("missing_stop") == 1


@pytest.mark.parametrize("state", STATES)
async def test_reconcile_restores_the_stop_from_the_exchange_entry_for_every_state(state):
    conn, ex, router, pf = await make_open()
    await ex.cancel_stop(6)                                   # the venue lost our stop
    router.state = state
    router._persist()
    ms = await pf.reconcile_now()
    assert "missing_stop" in [m.kind for m in ms]
    entry = (await ex.snapshot()).position(6).entry_price
    assert (await ex.snapshot()).stops == {6: stop_price(side="long", entry=entry)}


async def test_reconcile_restores_the_stop_for_a_position_with_no_router():
    conn, ex, router, pf = await make_open()
    ex.update_mark(7, Decimal(50))
    await ex.submit(OrderRequest(client_order_id="elsewhere", instrument_id=7, side="sell", quantity=Decimal(2),
                                 reduce_only=False, ts=T0))
    ex.drain_events()
    await pf.reconcile_now()
    entry7 = (await ex.snapshot()).position(7).entry_price
    assert (await ex.snapshot()).stops[7] == stop_price(side="short", entry=entry7)
    # Controller ruling F12: every stop reconciliation places is announced as INFO stop_placed.
    assert [(a[1], a[4]["trigger"]) for a in list_alerts(conn, "r") if a[2] == "stop_placed" and a[3] == 7] == [
        ("INFO", str(stop_price(side="short", entry=entry7)))]


class UnguardedPositionAccount(AlienOrderAccount):
    """A real account holding a position on 6 with no stop order."""
    async def fetch_portfolio(self):
        p = SimpleNamespace(instrument_id=6, size=Decimal(1), entry_price=Decimal(100), position_value=Decimal(100),
                            leverage=3, liquidation_price=Decimal(70), unrealized_pnl=Decimal(0),
                            cumulative_funding=Decimal(0))
        return SimpleNamespace(positions=(p,), margin=SimpleNamespace(total_account_value=Decimal(1000)),
                               in_liquidation=False)

    async def fetch_open_orders(self):
        return ()


async def test_reconcile_in_shadow_records_a_refused_missing_stop():
    """T5: shadow cannot place the missing stop; it records shadow_refused and never claims stop_placed."""
    conn = connect(":memory:")
    ex = ShadowExecutor(UnguardedPositionAccount(), clock=lambda: T0)
    alerter = Alerter("r", [SqliteSink(conn)])
    pf = Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter, routers={})
    ms = await pf.reconcile_now()
    assert "missing_stop" in [m.kind for m in ms]
    alerts = [(a[1], a[2]) for a in list_alerts(conn, "r")]
    assert ("WARN", "shadow_refused") in alerts and "stop_placed" not in [k for _, k in alerts]
