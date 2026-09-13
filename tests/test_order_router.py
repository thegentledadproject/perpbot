import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.backtest.bars import Bar
from polyperps.execution.order_router import InstrumentRouter, Portfolio, apply_guards
from polyperps.execution.sim_executor import SimExecutor
from dataclasses import replace as dc_replace

from polyperps.execution.types import AccountSnapshot, FillUpdate, Intent, OrderRequest, State
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


class RacyExecutor(SimExecutor):
    """Simulates a live executor whose event pump beats the REST ack: the FIRST call to
    snapshot() (the one _submit_with_recovery makes after a timeout) drains the sim's queued
    events and dispatches them straight to `router.handle_event` before returning the snapshot -
    so by the time _landed() runs, the router has already applied the fill on its own."""
    router = None  # set after the router is constructed
    _raced = False

    async def snapshot(self):
        if not self._raced and self.router is not None:
            self._raced = True
            for ev in self.drain_events():
                await self.router.handle_event(ev)
        return await super().snapshot()


class YieldingExecutor(SimExecutor):
    """A sim whose venue calls actually suspend (like a live executor's network hops), so two
    Portfolio coroutines can genuinely interleave in the places a real run interleaves."""

    async def place_stop(self, instrument_id, trigger_price):
        await asyncio.sleep(0)                       # in flight: the venue hasn't recorded it yet
        return await super().place_stop(instrument_id, trigger_price)

    async def cancel_stop(self, instrument_id):
        await super().cancel_stop(instrument_id)
        await asyncio.sleep(0)

    async def snapshot(self):
        snap = await super().snapshot()
        await asyncio.sleep(0)
        return snap


def make(target=1, equity="1000", clock=lambda: T0, executor_cls=SimExecutor):
    conn = connect(":memory:")
    ex = executor_cls("r", equity=Decimal(equity), taker_fee_rate=FEE, clock=clock)
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
    assert get_order(conn, "r-6-1").status == "lost"
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
    for f in ex.check_triggers():       # C2: stop-fire fills are returned, not queued
        await pf.dispatch(f)
    assert router.state is State.FLAT and strat.flattened == 1 and "stop_fired" in kinds(conn)


async def test_decision_trail_reconstructable_from_sqlite_only():
    conn, ex, strat, router, pf = make(clock=Ticker())
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    strat.t = Decimal(0)
    await pf.on_bar({6: [bar(0), bar(1)]}, "run"); await pump(pf, ex)
    trail = reconstruct(conn, "r", 6)
    assert [e.kind for e in trail][:3] == ["decision", "order", "alert"]      # decision -> fill -> stop_placed
    assert any("r-6-2" in e.summary and "filled" in e.summary for e in trail)


# --- review fix round 1 -------------------------------------------------------------


async def test_timeout_race_snapshot_adopts_without_double_apply():
    """A live executor's event pump can apply this order's own fill (via handle_event) before
    the timed-out submit's snapshot() returns. _landed must still compare against the size the
    router had *before* the send (not whatever the race already mutated it to), and the
    subsequent 'accepted' write must not downgrade the 'filled' row the race already wrote."""
    conn = connect(":memory:")
    ex = RacyExecutor("r", equity=Decimal(1000), taker_fee_rate=FEE, clock=lambda: T0)
    ex.update_mark(6, Decimal(100)); ex.update_mark(7, Decimal(100))
    alerter = Alerter("r", [SqliteSink(conn)])
    strat = Strat(1)
    router = InstrumentRouter(run_id="r", instrument_id=6, category="crypto", strategy=strat, executor=ex, conn=conn,
                              alerter=alerter, categories=CATS, clock=lambda: T0)
    ex.router = router
    ex.fail_next = "timeout"
    snap0 = AccountSnapshot(equity=Decimal(1000), positions=(), open_orders=(), stops={}, in_liquidation=False, ts=T0)
    await router.on_bar([bar(0)], snap0, "run")
    assert router.state is State.OPEN and router.size == 1
    assert (await ex.snapshot()).position(6).size == 1        # exactly one fill, not doubled by a spurious retry
    assert get_order(conn, "r-6-1").status == "filled"         # not downgraded back to "accepted"
    assert "ack_lost" in kinds(conn)


async def test_unexpected_fill_while_flat_recovers_to_open():
    conn, ex, strat, router, pf = make()
    assert router.state is State.FLAT
    fill = FillUpdate(client_order_id="foreign-1", instrument_id=6, side="buy", quantity=Decimal(1),
                      price=Decimal("100.08"), fee=Decimal("0.04"), ts=T0)
    await router.handle_event(fill)
    assert router.state is State.OPEN and router.size == 1
    assert "unexpected_fill" in kinds(conn) and "stop_placed" in kinds(conn)


async def test_load_local_restores_seq_counters_across_restart():
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    assert router.state is State.OPEN and get_order(conn, "r-6-1") is not None
    row = get_positions_local(conn, "r")[6]

    alerter2 = Alerter("r", [SqliteSink(conn)])
    strat2 = Strat(0)
    router2 = InstrumentRouter(run_id="r", instrument_id=6, category="crypto", strategy=strat2, executor=ex, conn=conn,
                               alerter=alerter2, categories=CATS, clock=lambda: T0)
    router2.load_local(row)
    assert router2.state is State.OPEN and router2.size == 1
    assert router2.seq == 1 and router2._dseq == 1

    pf2 = Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter2, routers={6: router2})
    await pf2.on_bar({6: [bar(0), bar(1)]}, "run")     # target 0 -> exit decision; must not collide on seq
    assert router2.state is State.EXIT_PENDING
    assert get_order(conn, "r-6-2") is not None


# --- final review: C2 stop-fire vs reconcile race ------------------------------------------


async def test_stop_fire_with_concurrent_reconcile_does_not_halt():
    """The fast loop's contract (mirrored by fast_step below): stop-fire fills come back from
    check_triggers() and are dispatched right there, before on_fast and before any other task
    can observe the account. A reconcile scheduled alongside must see either the pre-fire state
    or the finished FLAT row - never local OPEN vs remote 0."""
    conn, ex, strat, router, pf = make(executor_cls=YieldingExecutor)
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    assert router.state is State.OPEN
    ex.update_mark(6, Decimal(80))
    mismatches = []

    async def fast_step():                           # == scripts/run_paper.py fast_loop body
        for f in ex.check_triggers():
            await pf.dispatch(f)
        await pf.on_fast({6: Decimal(80)})

    async def reconcile():
        mismatches.extend(await pf.reconcile_now())

    await asyncio.gather(fast_step(), reconcile())
    assert mismatches == []
    assert "halted" not in kinds(conn)
    assert router.state is State.FLAT and "stop_fired" in kinds(conn)


async def test_portfolio_lock_serialises_fill_handling_against_reconcile():
    """Without the Portfolio lock a reconcile can run inside handle_event's await on
    place_stop: local row already OPEN, venue not yet holding the stop -> a phantom
    missing_stop and a duplicate stop placement. With the lock, reconcile waits its turn."""
    conn, ex, strat, router, pf = make(executor_cls=YieldingExecutor)
    await pf.on_bar({6: [bar(0)]}, "run")
    assert router.state is State.ENTRY_PENDING
    fill = [e for e in ex.drain_events() if isinstance(e, FillUpdate)][0]
    mismatches = []

    async def reconcile():
        mismatches.extend(await pf.reconcile_now())

    await asyncio.gather(pf.dispatch(fill), reconcile())
    assert mismatches == []
    assert "stop_missing" not in kinds(conn)
    assert kinds(conn).count("stop_placed") == 1
    assert router.state is State.OPEN and (await ex.snapshot()).stops == {6: router.stop_trigger}


# --- final review: C3 warmup gate ------------------------------------------------------------


def test_ack_timeout_default_pinned():
    conn, ex, strat, router, pf = make()
    assert router.ack_timeout_s == 10.0


async def test_warmup_gate_skips_until_history_is_long_enough():
    conn, ex, strat, router, pf = make()
    strat.warmup = 3
    calls = []
    strat.target = lambda history: calls.append(len(history)) or Decimal(1)
    await pf.on_bar({6: [bar(0)]}, "run")
    await pf.on_bar({6: [bar(0), bar(1)]}, "run")
    assert [d.note for d in list_decisions(conn, "r", 6)] == ["skip:warmup", "skip:warmup"]
    assert calls == [] and get_order(conn, "r-6-1") is None and router.state is State.FLAT
    await pf.on_bar({6: [bar(0), bar(1), bar(2)]}, "run")
    assert calls == [3] and router.state is State.ENTRY_PENDING


# --- final review I5/I6/I7: alert transitions, LIQUIDATED, pnl alert ------------------------


async def test_margin_alert_only_on_level_transitions():
    """At 3x the entry liq distance (~0.313) is already under margin_warn (0.35), so a
    per-tick alert would fire every 20 s for the life of every position."""
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    await pf.on_fast({6: Decimal(100)})
    await pf.on_fast({6: Decimal(100)})
    margin = [a for a in list_alerts(conn, "r") if a[2] == "margin_ratio"]
    assert [a[1] for a in margin] == ["WARN"]
    ex.update_mark(6, Decimal(93))          # (93 - 68.72) / 93 = 0.261: CRITICAL, but >= 0.25 so no flatten
    await pf.on_fast({6: Decimal(93)})
    await pf.on_fast({6: Decimal(93)})
    margin = [a for a in list_alerts(conn, "r") if a[2] == "margin_ratio"]
    assert [a[1] for a in margin] == ["WARN", "CRITICAL"] and router.state is State.OPEN
    ex.update_mark(6, Decimal(100))          # back to WARN: one more
    await pf.on_fast({6: Decimal(100)})
    assert [a[1] for a in list_alerts(conn, "r") if a[2] == "margin_ratio"] == ["WARN", "CRITICAL", "WARN"]


class LiquidatingExecutor(SimExecutor):
    async def snapshot(self):
        return dc_replace(await super().snapshot(), in_liquidation=True)


async def test_in_liquidation_moves_router_to_liquidated_until_cleared():
    conn, ex, strat, router, pf = make(executor_cls=LiquidatingExecutor)
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    assert router.state is State.OPEN
    await pf.on_fast({6: Decimal(100)})
    assert router.state is State.LIQUIDATED
    liq = [a for a in list_alerts(conn, "r") if a[2] == "liquidation"]
    assert len(liq) == 1 and liq[0][1] == "CRITICAL" and liq[0][4] == {"size": "1.00000000", "mark": "100"}
    assert get_positions_local(conn, "r")[6].state is State.LIQUIDATED
    await pf.on_fast({6: Decimal(100)})                                  # no re-alert, no orders
    await pf.on_bar({6: [bar(0), bar(1)]}, "run")
    assert list_decisions(conn, "r", 6)[-1].note == "skip:LIQUIDATED"
    assert len([a for a in list_alerts(conn, "r") if a[2] == "liquidation"]) == 1
    assert get_order(conn, "r-6-2") is None
    router.clear_halt()                                                   # operator decision, like HALTED
    assert router.state is State.OPEN


async def test_pnl_alert_on_drawdown_level_transitions_only():
    conn = connect(":memory:")
    ex = SimExecutor("r", equity=Decimal(1000), taker_fee_rate=FEE, clock=lambda: T0)
    ex.update_mark(6, Decimal(100))
    alerter = Alerter("r", [SqliteSink(conn)])
    pf = Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter, routers={}, start_equity=ex.start_equity)
    await ex.submit(OrderRequest(client_order_id="x", instrument_id=6, side="buy", quantity=Decimal(10),
                                 reduce_only=False, ts=T0)); ex.drain_events()
    await pf.on_fast({6: Decimal(100)})
    assert kinds(conn) == []
    ex.update_mark(6, Decimal(94))            # 999.6 - 60.8 = 938.8 -> -6.1 %
    await pf.on_fast({6: Decimal(94)})
    await pf.on_fast({6: Decimal(94)})
    pnl = [a for a in list_alerts(conn, "r") if a[2] == "pnl_drawdown"]
    assert [a[1] for a in pnl] == ["WARN"]
    ex.update_mark(6, Decimal(90))            # -10.1 %
    await pf.on_fast({6: Decimal(90)})
    await pf.on_fast({6: Decimal(90)})
    assert [a[1] for a in list_alerts(conn, "r") if a[2] == "pnl_drawdown"] == ["WARN", "CRITICAL"]


async def test_portfolio_without_start_equity_emits_no_pnl_alert():
    conn, ex, strat, router, pf = make()
    ex._cash = Decimal(1)                     # any drawdown you like: nothing to compare against
    await pf.on_fast({6: Decimal(100)})
    assert "pnl_drawdown" not in kinds(conn)
