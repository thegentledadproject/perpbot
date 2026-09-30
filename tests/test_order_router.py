import asyncio
from types import SimpleNamespace

import pytest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.backtest.bars import Bar
from polyperps.execution.executor import ExecutorTimeout, GateClosed, ShadowRefused
from polyperps.execution.live_executor import ShadowExecutor
from polyperps.execution.order_router import InstrumentRouter, Portfolio, apply_guards
from polyperps.execution.sim_executor import SimExecutor
from dataclasses import replace as dc_replace

from polyperps.execution.types import AccountSnapshot, FillUpdate, Intent, OrderAck, OrderRequest, OrderUpdate, State
from polyperps.exchange.types import SourceType
from polyperps.monitor.alerts import Alerter, SqliteSink
from polyperps.monitor.decision_trail import reconstruct
from polyperps.risk.kill_switch import evaluate
from polyperps.risk.liquidation_guard import Allow, Reject, Resize, stop_price
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
    assert router.state is State.OPEN and router.size == 1 and router.entry == Decimal(100)
    assert router.stop_trigger == Decimal("85.00")           # 100 * 0.85
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
                                  reduce_only=False, ts=T0)); ex2.drain_events()  # 550 notional; its cost leaves equity 998.13 -> room 48.88 -> qty 0.48878
    await pf2.on_bar({6: [bar(0)]}, "run")
    assert Decimal("0.48") < get_order(conn2, "r-6-1").quantity <= Decimal("0.5")


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
    ex.update_mark(6, Decimal(90))     # liq 68.67 -> distance 23.7% < 25%
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

    async def fast_step():                           # == scripts/run_trader.py fast_loop body
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
    ex.update_mark(6, Decimal(93))          # (93 - 68.67) / 93 = 0.262: CRITICAL, but >= 0.25 so no flatten
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
    ex.update_mark(6, Decimal(94))            # 1000 - 5.65 cost - 60 = 934.35 -> -6.6 %
    await pf.on_fast({6: Decimal(94)})
    await pf.on_fast({6: Decimal(94)})
    pnl = [a for a in list_alerts(conn, "r") if a[2] == "pnl_drawdown"]
    assert [a[1] for a in pnl] == ["WARN"]
    ex.update_mark(6, Decimal(90))            # 894.35 -> -10.6 %
    await pf.on_fast({6: Decimal(90)})
    await pf.on_fast({6: Decimal(90)})
    assert [a[1] for a in list_alerts(conn, "r") if a[2] == "pnl_drawdown"] == ["WARN", "CRITICAL"]


async def test_portfolio_without_start_equity_emits_no_pnl_alert():
    conn, ex, strat, router, pf = make()
    ex._cash = Decimal(1)                     # any drawdown you like: nothing to compare against
    await pf.on_fast({6: Decimal(100)})
    assert "pnl_drawdown" not in kinds(conn)


async def test_loss_limit_shutdown_flattens_every_router_and_halts():
    conn = connect(":memory:")
    ex = SimExecutor("r", equity=Decimal(1000), taker_fee_rate=FEE, clock=lambda: T0)
    ex.update_mark(6, Decimal(100)); ex.update_mark(7, Decimal(100))
    alerter = Alerter("r", [SqliteSink(conn)])
    r6 = InstrumentRouter(run_id="r", instrument_id=6, category="crypto", strategy=Strat(1), executor=ex, conn=conn,
                          alerter=alerter, categories=CATS, clock=lambda: T0)
    r7 = InstrumentRouter(run_id="r", instrument_id=7, category="crypto", strategy=Strat(-1), executor=ex, conn=conn,
                          alerter=alerter, categories=CATS, clock=lambda: T0)
    pf = Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter, routers={6: r6, 7: r7},
                   start_equity=Decimal(1000))
    await pf.on_bar({6: [bar(0)], 7: [dc_replace(bar(0), instrument_id=7)]}, "run"); await pump(pf, ex)
    assert r6.size > 0 and r7.size < 0
    ex._cash -= Decimal(150)                                                  # a -15 % day
    kill = evaluate(live_sharpe=None, backtest_sharpe=None, mode="paper",
                    equity=(await ex.snapshot()).equity, start_equity=pf.start_equity)
    assert kill == "shutdown"
    await pf.on_bar({6: [bar(0), bar(1)]}, kill); await pump(pf, ex)          # only instrument 6's bar closed
    assert r6.state is State.HALTED and r7.state is State.HALTED
    assert r6.size == 0 and r7.size == 0 and get_order(conn, "r-7-2").reason == "kill_shutdown"
    await pf.on_bar({7: [dc_replace(bar(1), instrument_id=7)]}, kill)        # still breached: no second alert
    loss = [a for a in list_alerts(conn, "r") if a[2] == "loss_limit"]
    assert len(loss) == 1 and loss[0][1] == "CRITICAL"


async def test_position_filled_after_shutdown_is_flattened_on_next_shutdown_bar():
    conn = connect(":memory:")
    ex = SimExecutor("r", equity=Decimal(1000), taker_fee_rate=FEE, clock=lambda: T0)
    ex.update_mark(6, Decimal(100))
    alerter = Alerter("r", [SqliteSink(conn)])
    r6 = InstrumentRouter(run_id="r", instrument_id=6, category="crypto", strategy=Strat(1), executor=ex, conn=conn,
                          alerter=alerter, categories=CATS, clock=lambda: T0)
    pf = Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter, routers={6: r6}, start_equity=Decimal(1000))
    await pf.on_bar({6: [bar(0)]}, "run")                                     # entry sent, fill not yet pumped
    assert r6.state is State.ENTRY_PENDING
    await pf.on_bar({6: [bar(1)]}, "shutdown")                                # halts with the entry in flight
    assert r6.state is State.HALTED
    await pump(pf, ex)                                                        # the entry fills after the halt
    assert r6.size > 0 and r6.state is State.HALTED
    assert r6.stop_trigger is not None and (await ex.snapshot()).stops                # stop-guarded (spec 4.4)
    assert "unexpected_fill" not in kinds(conn) and "late_fill" in kinds(conn)
    await pf.on_bar({6: [bar(2)]}, "shutdown"); await pump(pf, ex)
    assert r6.size == 0 and r6.state is State.HALTED


def _one_router(start=True):
    conn = connect(":memory:")
    ex = SimExecutor("r", equity=Decimal(1000), taker_fee_rate=FEE, clock=lambda: T0)
    ex.update_mark(6, Decimal(100))
    alerter = Alerter("r", [SqliteSink(conn)])
    r6 = InstrumentRouter(run_id="r", instrument_id=6, category="crypto", strategy=Strat(1), executor=ex, conn=conn,
                          alerter=alerter, categories=CATS, clock=lambda: T0)
    return conn, ex, r6, Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter, routers={6: r6},
                                   start_equity=Decimal(1000))


def _cancelled(cid):
    return OrderUpdate(client_order_id=cid, status="cancelled", filled_quantity=Decimal(0), ts=T0)


async def test_cancelled_entry_in_flight_does_not_leave_halted():
    conn, ex, r6, pf = _one_router()
    await pf.on_bar({6: [bar(0)]}, "run")
    cid = r6._pending_cid
    await pf.on_bar({6: [bar(1)]}, "shutdown")
    ex.drain_events()                                                         # the entry never fills
    await pf.dispatch(_cancelled(cid))
    assert r6.state is State.HALTED


async def test_cancelled_kill_exit_in_flight_does_not_leave_halted():
    conn, ex, r6, pf = _one_router()
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    await pf.on_bar({6: [bar(1)]}, "shutdown")
    cid = r6._pending_cid
    ex.drain_events()                                                         # the kill exit never fills
    await pf.dispatch(_cancelled(cid))
    assert r6.state is State.HALTED and r6.size > 0


async def test_shutdown_flattens_an_exchange_position_the_router_never_saw():
    conn, ex, r6, pf = _one_router()
    await ex.submit(OrderRequest(client_order_id="x-1", instrument_id=6, side="buy", quantity=Decimal(2),
                                 reduce_only=False, ts=T0))
    ex.drain_events()                                                         # router saw nothing
    assert r6.size == 0 and (await ex.snapshot()).position(6).size == Decimal(2)
    await pf.on_bar({6: [bar(1)]}, "shutdown"); await pump(pf, ex)
    row = get_order(conn, "r-6-1")
    assert row.reason == "kill_shutdown" and row.reduce_only and row.side == "sell" and row.quantity == Decimal(2)
    assert r6.state is State.HALTED


class AckOnlyExecutor(SimExecutor):
    """The venue acks, but no fill ever happens."""
    async def submit(self, order):
        return OrderAck(client_order_id=order.client_order_id, exchange_order_id="x-1", status="accepted",
                        reason="", ts=self._clock())


class RaisingExecutor(SimExecutor):
    exc: Exception = RuntimeError("boom")

    async def submit(self, order):
        raise self.exc


class FillThenRaise(SimExecutor):
    async def submit(self, order):
        await super().submit(order)                  # the order reached the venue and filled...
        raise RuntimeError("connection reset")       # ...but the call errored on the way back


async def test_pending_timeout_adopts_a_lost_fill_and_places_the_stop():
    now = [T0]
    conn, ex, strat, router, pf = make(clock=lambda: now[0])
    await pf.on_bar({6: [bar(0)]}, "run")
    ex.drain_events()                                    # the fill never reaches us
    assert router.state is State.ENTRY_PENDING
    now[0] = T0 + timedelta(seconds=30)
    await pf.on_fast({6: Decimal(100)})
    assert router.state is State.ENTRY_PENDING           # not yet: strictly longer than 30 s
    now[0] = T0 + timedelta(seconds=31)
    await pf.on_fast({6: Decimal(100)})
    assert router.state is State.OPEN and router.size == 1
    entry = (await ex.snapshot()).position(6).entry_price
    assert (await ex.snapshot()).stops == {6: stop_price(side="long", entry=entry)}
    assert get_order(conn, "r-6-1").status == "timeout_adopted" and "pending_timeout" in kinds(conn)


async def test_pending_timeout_with_nothing_on_the_exchange_goes_flat():
    now = [T0]
    conn, ex, strat, router, pf = make(clock=lambda: now[0], executor_cls=AckOnlyExecutor)
    await pf.on_bar({6: [bar(0)]}, "run")
    assert router.state is State.ENTRY_PENDING
    now[0] = T0 + timedelta(seconds=31)
    await pf.on_fast({6: Decimal(100)})
    assert router.state is State.FLAT and router.size == 0
    assert get_order(conn, "r-6-1").status == "timeout_adopted"


async def test_late_fill_after_pending_timeout_is_not_counted_twice():
    """Review focus 1: the fill was delayed, not lost."""
    now = [T0]
    conn, ex, strat, router, pf = make(clock=lambda: now[0])
    await pf.on_bar({6: [bar(0)]}, "run")
    held = ex.drain_events()
    now[0] = T0 + timedelta(seconds=31)
    await pf.on_fast({6: Decimal(100)})
    assert router.state is State.OPEN and router.size == 1
    for ev in held:
        await pf.dispatch(ev)
    assert router.size == 1 and router.state is State.OPEN and "late_fill" in kinds(conn)
    assert get_order(conn, "r-6-1").status == "timeout_adopted"


async def test_late_fill_while_halted_places_a_stop_and_stays_halted():
    conn, ex, strat, router, pf = make()
    ex.fail_queue = ["drop", "drop"]
    await pf.on_bar({6: [bar(0)]}, "run")
    assert router.state is State.HALTED and get_order(conn, "r-6-1").status == "lost"
    await ex.submit(OrderRequest(client_order_id="r-6-1", instrument_id=6, side="buy", quantity=Decimal(1),
                                 reduce_only=False, ts=T0))  # the "lost" order lands after all
    await pump(pf, ex)
    assert router.state is State.HALTED and router.size == 1
    entry = (await ex.snapshot()).position(6).entry_price
    assert (await ex.snapshot()).stops == {6: stop_price(side="long", entry=entry)}


async def test_foreign_fill_while_halted_moves_the_size_and_places_a_stop():
    conn, ex, strat, router, pf = make()
    await router.halt("operator test")
    await router.handle_event(FillUpdate(client_order_id="foreign-1", instrument_id=6, side="buy",
                                         quantity=Decimal(1), price=Decimal(100), fee=Decimal(0), ts=T0))
    assert router.state is State.HALTED and router.size == 1
    assert (await ex.snapshot()).stops == {6: stop_price(side="long", entry=Decimal(100))}


@pytest.mark.parametrize("exc,status", [
    (GateClosed("instrument 6: POLYMARKET_LIVE_TRADING is not exactly 'true'"), "error"),
    (RuntimeError("connection reset"), "error"),
    (ShadowRefused("shadow"), "shadow_refused"),
])
async def test_submit_error_marks_the_order_alerts_and_halts(exc, status):
    conn, ex, strat, router, pf = make(executor_cls=RaisingExecutor)
    ex.exc = exc
    await pf.on_bar({6: [bar(0)]}, "run")
    assert router.state is State.HALTED and router.size == 0
    assert get_order(conn, "r-6-1").status == status
    crit = [a[2] for a in list_alerts(conn, "r") if a[1] == "CRITICAL"]
    assert "submit_error" in crit and "halted" in crit


async def test_fill_after_submit_error_is_not_counted_twice():
    """Review focus 1: the order landed before the call errored; its fill event arrives after we
    already adopted the exchange size."""
    conn, ex, strat, router, pf = make(executor_cls=FillThenRaise)
    await pf.on_bar({6: [bar(0)]}, "run")
    assert router.state is State.HALTED and router.size == 1                 # adopted from the exchange
    assert (await ex.snapshot()).stops == {6: stop_price(side="long", entry=router.entry)}
    await pump(pf, ex)                                                       # the fill event arrives late
    assert router.size == 1 and router.state is State.HALTED and "late_fill" in kinds(conn)
    assert get_order(conn, "r-6-1").status == "error"


class FlatAccount:
    """The real account as a perps session sees it: flat, no orders."""
    async def fetch_portfolio(self):
        return SimpleNamespace(positions=(), margin=SimpleNamespace(total_account_value=Decimal(1000)),
                               in_liquidation=False)

    async def fetch_open_orders(self):
        return ()


async def test_shadow_records_the_would_be_order_and_halts():
    conn = connect(":memory:")
    ex = ShadowExecutor(FlatAccount(), clock=lambda: T0)
    alerter = Alerter("r", [SqliteSink(conn)])
    router = InstrumentRouter(run_id="r", instrument_id=6, category="crypto", strategy=Strat(1), executor=ex,
                              conn=conn, alerter=alerter, categories=CATS, clock=lambda: T0)
    pf = Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter, routers={6: router})
    await pf.on_bar({6: [bar(0)]}, "run")
    o = get_order(conn, "r-6-1")
    assert o.status == "shadow_refused" and o.side == "buy" and router.state is State.HALTED
    assert list_decisions(conn, "r", 6)[0].client_order_id == "r-6-1"


class PartialThenTimeout(SimExecutor):
    async def submit(self, order):
        await super().submit(dc_replace(order, quantity=order.quantity / 2))    # half fills on the venue...
        raise ExecutorTimeout("ack lost after a partial fill")                  # ...and the ack is lost


async def test_partial_fill_then_timeout_is_landed_not_retried():
    conn, ex, strat, router, pf = make(executor_cls=PartialThenTimeout)
    await pf.on_bar({6: [bar(0)]}, "run")
    await pump(pf, ex)
    assert (await ex.snapshot()).position(6).size == Decimal("0.5")        # one partial, no second order
    assert "retry" not in kinds(conn) and "ack_lost" in kinds(conn)
    assert get_order(conn, "r-6-1").filled_quantity == Decimal("0.5")


async def test_order_filled_quantity_accumulates_across_fills():
    conn, ex, strat, router, pf = make(executor_cls=AckOnlyExecutor)
    await pf.on_bar({6: [bar(0)]}, "run")                                  # r-6-1: quantity 1, no fill yet
    await router.handle_event(FillUpdate(client_order_id="r-6-1", instrument_id=6, side="buy",
                                         quantity=Decimal("0.4"), price=Decimal(100), fee=Decimal(0), ts=T0))
    o = get_order(conn, "r-6-1")
    assert o.status == "partial" and o.filled_quantity == Decimal("0.4") and router.state is State.OPEN
    await router.handle_event(FillUpdate(client_order_id="r-6-1", instrument_id=6, side="buy",
                                         quantity=Decimal("0.6"), price=Decimal(101), fee=Decimal(0), ts=T0))
    o = get_order(conn, "r-6-1")
    assert o.status == "filled" and o.filled_quantity == Decimal("1.0") and o.avg_price == Decimal("100.6")
    assert router.size == 1 and "unexpected_fill" not in kinds(conn)


async def test_fill_without_client_id_reaches_its_instruments_router():
    """Regression pin: Portfolio already routed fills by instrument, so this passed before Task 7.
    What was missing is LiveReader keeping client-id-less fills (see test_live_executor)."""
    conn = connect(":memory:")
    ex = SimExecutor("r", equity=Decimal(1000), taker_fee_rate=FEE, clock=lambda: T0)
    ex.update_mark(6, Decimal(100)); ex.update_mark(7, Decimal(100))
    alerter = Alerter("r", [SqliteSink(conn)])
    r6 = InstrumentRouter(run_id="r", instrument_id=6, category="crypto", strategy=Strat(1), executor=ex, conn=conn,
                          alerter=alerter, categories=CATS, clock=lambda: T0)
    r7 = InstrumentRouter(run_id="r", instrument_id=7, category="crypto", strategy=Strat(1), executor=ex, conn=conn,
                          alerter=alerter, categories=CATS, clock=lambda: T0)
    pf = Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter, routers={6: r6, 7: r7})
    await pf.on_bar({6: [bar(0)], 7: [dc_replace(bar(0), instrument_id=7)]}, "run"); await pump(pf, ex)
    await pf.dispatch(FillUpdate(client_order_id="venue-999", instrument_id=7, side="sell", quantity=r7.size,
                                 price=Decimal(85), fee=Decimal(0), ts=T0))            # the venue stop fired
    assert r7.state is State.FLAT and r6.state is State.OPEN
    stop = [a for a in list_alerts(conn, "r") if a[2] == "stop_fired"]
    assert len(stop) == 1 and stop[0][3] == 7


class RestingAfterTimeout(AckOnlyExecutor):
    """The venue still lists the order we gave up on as resting."""
    cancelled = ()

    async def snapshot(self):
        return dc_replace(await super().snapshot(), open_orders=("r-6-1",))

    async def cancel(self, client_order_id):
        self.cancelled = (*self.cancelled, client_order_id)


async def test_reconcile_cancels_a_resting_order_we_gave_up_on():
    """Task 6 review (i): timeout_adopted is terminal, so a venue order with that id is cancelled."""
    now = [T0]
    conn, ex, strat, router, pf = make(clock=lambda: now[0], executor_cls=RestingAfterTimeout)
    await pf.on_bar({6: [bar(0)]}, "run")
    now[0] = T0 + timedelta(seconds=31)
    await pf.on_fast({6: Decimal(100)})
    assert get_order(conn, "r-6-1").status == "timeout_adopted"
    mismatches = await pf.reconcile_now()
    assert [m.kind for m in mismatches] == ["unknown_order"] and ex.cancelled == ("r-6-1",)


class StopRefused(SimExecutor):
    async def place_stop(self, instrument_id, trigger_price):
        raise ShadowRefused("shadow: stop refused")


async def test_adopt_survives_a_refused_stop_and_keeps_the_size():
    """Task 6 review (ii): a refused stop placement is recorded; the adopted size still persists."""
    now = [T0]
    conn, ex, strat, router, pf = make(clock=lambda: now[0], executor_cls=StopRefused)
    await pf.on_bar({6: [bar(0)]}, "run")
    ex.drain_events()                                    # the fill never reaches us
    now[0] = T0 + timedelta(seconds=31)
    await pf.on_fast({6: Decimal(100)})
    assert router.state is State.OPEN and router.size == 1 and "shadow_refused" in kinds(conn)
    assert get_positions_local(conn, "r")[6].size == 1


class RecoverStrat(Strat):
    def __init__(self, target=1):
        super().__init__(target); self.recovered = []
    def on_recover(self, sign):
        self.recovered.append(sign)


async def test_late_fill_reread_to_zero_cancels_the_stop_and_flattens_the_strategy():
    """Task 6 review (iii): the late-fill re-read ends like check_pending when the venue is flat."""
    now = [T0]
    conn, ex, strat, router, pf = make(clock=lambda: now[0])
    await pf.on_bar({6: [bar(0)]}, "run")
    held = ex.drain_events()
    now[0] = T0 + timedelta(seconds=31)
    await pf.on_fast({6: Decimal(100)})
    assert router.size == 1 and 6 in (await ex.snapshot()).stops
    await ex.submit(OrderRequest(client_order_id="x-1", instrument_id=6, side="sell", quantity=Decimal(1),
                                 reduce_only=True, ts=T0))
    ex.drain_events()                                    # closed on the venue; the router never hears of it
    for ev in held:
        await pf.dispatch(ev)
    assert router.state is State.FLAT and router.size == 0 and strat.flattened == 1
    assert (await ex.snapshot()).stops == {}


async def test_late_fill_reread_to_a_position_tells_the_strategy_its_side():
    """Task 6 review (iii): the order we timed out on lands after all; the strategy learns it is long."""
    now = [T0]
    conn, ex, _, router, pf = make(clock=lambda: now[0], executor_cls=AckOnlyExecutor)
    strat = router.strategy = RecoverStrat(1)
    await pf.on_bar({6: [bar(0)]}, "run")
    now[0] = T0 + timedelta(seconds=31)
    await pf.on_fast({6: Decimal(100)})
    assert router.state is State.FLAT
    await SimExecutor.submit(ex, OrderRequest(client_order_id="r-6-1", instrument_id=6, side="buy",
                                              quantity=Decimal(1), reduce_only=False, ts=T0))
    await pump(pf, ex)
    assert router.state is State.OPEN and router.size == 1 and strat.recovered == [1]


async def test_second_router_counts_the_first_routers_in_flight_entry():
    """Review focus 5: both routers decide in ONE on_bar call; r6's entry is sent but unfilled."""
    conn = connect(":memory:")
    ex = AckOnlyExecutor("r", equity=Decimal(200), taker_fee_rate=FEE, clock=lambda: T0)
    ex.update_mark(6, Decimal(100)); ex.update_mark(7, Decimal(100))
    alerter = Alerter("r", [SqliteSink(conn)])
    r6 = InstrumentRouter(run_id="r", instrument_id=6, category="crypto", strategy=Strat(1), executor=ex, conn=conn,
                          alerter=alerter, categories=CATS, clock=lambda: T0)
    r7 = InstrumentRouter(run_id="r", instrument_id=7, category="crypto", strategy=Strat(1), executor=ex, conn=conn,
                          alerter=alerter, categories=CATS, clock=lambda: T0)
    pf = Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter, routers={6: r6, 7: r7})
    await pf.on_bar({6: [bar(0)], 7: [dc_replace(bar(0), instrument_id=7)]}, "run")
    assert r6.state is State.ENTRY_PENDING and get_order(conn, "r-6-1").quantity == 1
    # equity 200: cluster net cap 0.6 x 200 = 120; r6's 100 in flight leaves 20 for r7
    assert get_order(conn, "r-7-1").quantity == Decimal("0.20000000")


async def test_funding_drift_alerts_when_charged_funding_is_3x_the_bar_rate():
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)                 # long 1 @ 100
    hist = [bar(0)]
    for i, charged in ((1, "0.0001"), (2, "0.0001"), (3, "0.0003")):
        ex.apply_funding(6, Decimal(charged))                                  # what the venue charged
        hist = hist + [bar(i, funding="0.0001")]                               # what the bar predicted
        await pf.on_bar({6: hist}, "run")
        if i == 2:
            assert "funding_drift" not in kinds(conn)                          # 1st sighting is a baseline; then 1x
    drift = [a for a in list_alerts(conn, "r") if a[2] == "funding_drift"]
    assert len(drift) == 1 and drift[0][1] == "WARN" and drift[0][3] == 6


async def test_incomplete_bar_exits_to_flat_and_never_enters():
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    gap = dc_replace(bar(1), complete=False)
    await pf.on_bar({6: [bar(0), gap]}, "run")
    assert get_order(conn, "r-6-2").reason == "data_gap"
    await pump(pf, ex)
    assert router.state is State.FLAT
    await pf.on_bar({6: [bar(0), gap, dc_replace(bar(2), complete=False)]}, "run")
    assert router.state is State.FLAT and list_decisions(conn, "r", 6)[-1].note == "skip:data_gap"
    await pf.on_bar({6: [bar(0), gap, bar(2)]}, "run")                   # the next complete bar enters again
    assert router.state is State.ENTRY_PENDING
