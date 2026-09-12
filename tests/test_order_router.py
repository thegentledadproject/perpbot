from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.backtest.bars import Bar
from polyperps.execution.order_router import InstrumentRouter, Portfolio, apply_guards
from polyperps.execution.sim_executor import SimExecutor
from polyperps.execution.types import Intent, OrderRequest, State
from polyperps.exchange.types import SourceType
from polyperps.monitor.alerts import Alerter, SqliteSink
from polyperps.monitor.decision_trail import reconstruct
from polyperps.risk.liquidation_guard import Allow, Reject, Resize
from polyperps.storage.db import connect, get_order, get_positions_local, list_alerts, list_decisions

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)
H = timedelta(hours=1)
FEE = Decimal("0.0004")
CATS = {6: "crypto", 7: "crypto"}


class Strat:
    name = "t"; params = {}
    def __init__(self, target=1):
        self.t = Decimal(target); self.flattened = 0
    def target(self, history):
        return self.t
    def on_flatten(self):
        self.flattened += 1


def bar(i, close="100", funding="0.0001"):
    ts = T0 + i * H
    c = Decimal(close)
    return Bar(instrument_id=6, source_type=SourceType.POLYMARKET_WS, open_ts=ts, open=c, high=c, low=c, close=c,
               index_close=None, funding_rate=Decimal(funding), spread_bps=Decimal(5), spread_source="constant",
               complete=True)


class Ticker:
    """A clock that advances by one second on every call. Shared between the router and the
    sim so their row timestamps interleave in true call order instead of tying at a frozen T0 -
    used only by the decision-trail test, which needs causal (not just kind-grouped) ordering."""
    def __init__(self, start=T0, step=timedelta(seconds=1)):
        self.n = -1
        self.start, self.step = start, step

    def __call__(self):
        self.n += 1
        return self.start + self.n * self.step


def make(target=1, equity="1000", clock=lambda: T0):
    conn = connect(":memory:")
    ex = SimExecutor("r", equity=Decimal(equity), taker_fee_rate=FEE, clock=clock)
    ex.update_mark(6, Decimal(100)); ex.update_mark(7, Decimal(100))
    alerter = Alerter("r", [SqliteSink(conn)])
    strat = Strat(target)
    router = InstrumentRouter(run_id="r", instrument_id=6, category="crypto", strategy=strat, executor=ex, conn=conn,
                              alerter=alerter, categories=CATS, clock=clock)
    pf = Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter, routers={6: router})
    return conn, ex, strat, router, pf


async def pump(pf, ex):
    for ev in ex.drain_events():
        await pf.dispatch(ev)


def kinds(conn):
    return [a[2] for a in list_alerts(conn, "r")]


def test_apply_guards():
    i = Intent(instrument_id=6, side="buy", quantity=Decimal(1), notional=Decimal(100))
    assert apply_guards(i, [("a", Allow()), ("b", Allow())]) == (i, {"a": "allow", "b": "allow"})
    out, labels = apply_guards(i, [("a", Resize(quantity=Decimal("0.5"))), ("b", Resize(quantity=Decimal("0.25")))])
    assert out.quantity == Decimal("0.25") and out.notional == Decimal("25.00") and labels["b"] == "resize:0.25"
    assert apply_guards(i, [("a", Allow()), ("b", Reject(reason="x"))])[0] is None


async def test_entry_then_open_with_stop_and_rows():
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "run")
    assert router.state is State.ENTRY_PENDING
    order = get_order(conn, "r-6-1")
    assert order.status == "accepted" and order.exchange_order_id == "sim-1"
    d = list_decisions(conn, "r", 6)[0]
    assert d.client_order_id == "r-6-1" and d.verdicts == {"vet_entry": "allow", "vet_exposure": "allow"}
    await pump(pf, ex)
    assert router.state is State.OPEN and router.size == 1 and router.entry == Decimal("100.08")
    assert router.stop_trigger == Decimal("85.07")           # 100.08 * 0.85 = 85.068 -> ROUND_HALF_EVEN -> 85.07
    assert (await ex.snapshot()).stops == {6: router.stop_trigger}
    assert get_positions_local(conn, "r")[6].state is State.OPEN
    assert get_order(conn, "r-6-1").status == "filled"
    assert "stop_placed" in kinds(conn)


async def test_reject_and_resize_from_exposure():
    conn, ex, strat, router, pf = make()
    await ex.submit(OrderRequest(client_order_id="pre", instrument_id=7, side="buy", quantity=Decimal("6"),
                                 reduce_only=False, ts=T0)); ex.drain_events()   # 600 notional in the crypto cluster
    await pf.on_bar({6: [bar(0)]}, "run")
    assert router.state is State.FLAT and get_order(conn, "r-6-1") is None
    assert list_decisions(conn, "r", 6)[0].verdicts["vet_exposure"].startswith("reject:")
    conn2, ex2, _, router2, pf2 = make()
    await ex2.submit(OrderRequest(client_order_id="pre", instrument_id=7, side="buy", quantity=Decimal("5.5"),
                                  reduce_only=False, ts=T0)); ex2.drain_events()  # ~550 notional -> room ~50 -> qty ~0.5
    await pf2.on_bar({6: [bar(0)]}, "run")
    assert Decimal("0.49") < get_order(conn2, "r-6-1").quantity <= Decimal("0.5")


async def test_exit_on_target_zero_and_flip():
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    strat.t = Decimal(0)
    await pf.on_bar({6: [bar(0), bar(1)]}, "run")
    assert router.state is State.EXIT_PENDING and get_order(conn, "r-6-2").reduce_only is True
    await pump(pf, ex)
    assert router.state is State.FLAT and router.size == 0 and strat.flattened == 1
    assert (await ex.snapshot()).stops == {}
    strat.t = Decimal(-1)
    await pf.on_bar({6: [bar(0), bar(1), bar(2)]}, "run"); await pump(pf, ex)
    assert router.size == -1
    strat.t = Decimal(1)                                        # flip: exit this bar only
    await pf.on_bar({6: [bar(i) for i in range(4)]}, "run"); await pump(pf, ex)
    assert router.state is State.FLAT
    await pf.on_bar({6: [bar(i) for i in range(5)]}, "run"); await pump(pf, ex)
    assert router.size == 1


async def test_timeout_adopts_landed_order_exactly_one_fill():
    conn, ex, strat, router, pf = make()
    ex.fail_next = "timeout"
    await pf.on_bar({6: [bar(0)]}, "run")
    o = get_order(conn, "r-6-1")
    assert o.status == "accepted" and "adopted" in o.reason
    await pump(pf, ex)
    assert router.state is State.OPEN and router.size == 1 and (await ex.snapshot()).position(6).size == 1
    assert "ack_lost" in kinds(conn)


async def test_dropped_then_retry_succeeds_with_same_id():
    conn, ex, strat, router, pf = make()
    ex.fail_queue = ["drop"]
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    assert router.state is State.OPEN and get_order(conn, "r-6-1").status == "filled"
    assert len([a for a in list_alerts(conn, "r") if a[2] == "retry"]) == 1


async def test_dropped_twice_halts():
    conn, ex, strat, router, pf = make()
    ex.fail_queue = ["drop", "drop"]
    await pf.on_bar({6: [bar(0)]}, "run")
    assert router.state is State.HALTED and (await ex.snapshot()).position(6) is None
    assert [a for a in list_alerts(conn, "r") if a[1] == "CRITICAL"]
    await pf.on_bar({6: [bar(0), bar(1)]}, "run")            # halted: no new orders
    assert get_order(conn, "r-6-2") is None
    router.clear_halt()
    assert router.state is State.FLAT


async def test_rejected_ack_reverts_state():
    conn, ex, strat, router, pf = make()
    ex.fail_next = "reject"
    await pf.on_bar({6: [bar(0)]}, "run")
    assert router.state is State.FLAT and get_order(conn, "r-6-1").status == "rejected"


async def test_fast_loop_liq_distance_exit():
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    ex.update_mark(6, Decimal(90))     # liq ~68.74 -> distance 23.6% < 25%
    await pf.on_fast({6: Decimal(90)})
    assert router.state is State.EXIT_PENDING and get_order(conn, "r-6-2").reason == "liq_distance"
    assert "margin_ratio" in kinds(conn)


async def test_fast_loop_funding_cost_exit():
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    for _ in range(3):
        ex.apply_funding(6, Decimal("0.007"))   # long pays 0.7 each -> 2.1 >= 2% of 100
    await pf.on_fast({6: Decimal(100)})
    assert get_order(conn, "r-6-2").reason == "funding_cost"


async def test_kill_switch_pause_and_shutdown():
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "pause")
    assert router.state is State.FLAT and list_decisions(conn, "r", 6)[0].note == "kill:pause"
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    await pf.on_bar({6: [bar(0), bar(1)]}, "shutdown"); await pump(pf, ex)
    assert router.state is State.HALTED and router.size == 0
    assert get_order(conn, "r-6-2").reason == "kill_shutdown" and "kill_switch" in kinds(conn)


async def test_external_stop_fill_flattens_and_alerts():
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    ex.update_mark(6, Decimal(80))
    ex.check_triggers()
    await pump(pf, ex)
    assert router.state is State.FLAT and strat.flattened == 1 and "stop_fired" in kinds(conn)


async def test_decision_trail_reconstructable_from_sqlite_only():
    conn, ex, strat, router, pf = make(clock=Ticker())
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    strat.t = Decimal(0)
    await pf.on_bar({6: [bar(0), bar(1)]}, "run"); await pump(pf, ex)
    trail = reconstruct(conn, "r", 6)
    assert [e.kind for e in trail][:3] == ["decision", "order", "alert"]      # decision -> fill -> stop_placed
    assert any("r-6-2" in e.summary and "filled" in e.summary for e in trail)
