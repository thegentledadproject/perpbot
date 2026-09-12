from dataclasses import replace as dc_replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from polyperps.execution.order_router import InstrumentRouter
from polyperps.execution.sim_executor import SimExecutor
from polyperps.execution.state_recovery import RecoveryHalt, recover
from polyperps.execution.types import FillUpdate, OrderRequest, OrderRow, PositionLocalRow, State
from polyperps.monitor.alerts import Alerter, SqliteSink
from polyperps.storage.db import connect, get_order, list_alerts, list_recovery, upsert_order, upsert_position_local

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


class Strat:
    name = "t"; params = {}
    def target(self, h): return Decimal(0)
    def on_flatten(self): pass


def setup(equity="1000"):
    conn = connect(":memory:")
    ex = SimExecutor("r", equity=Decimal(equity), taker_fee_rate=Decimal("0.0004"), clock=lambda: T0)
    ex.update_mark(6, Decimal(100)); ex.update_mark(8, Decimal(100))
    alerter = Alerter("r", [SqliteSink(conn)])
    router = InstrumentRouter(run_id="r", instrument_id=6, category="crypto", strategy=Strat(), executor=ex, conn=conn,
                              alerter=alerter, categories={6: "crypto"}, clock=lambda: T0)
    return conn, ex, alerter, router


def order_row(cid, status):
    return OrderRow(client_order_id=cid, run_id="r", instrument_id=6, side="buy", quantity=Decimal(1), reduce_only=False,
                    status=status, exchange_order_id=None, filled_quantity=Decimal(0), avg_price=None,
                    submitted_at=T0, updated_at=T0, reason="strategy")


async def test_crash_between_decision_and_submit_is_abandoned_and_flat():
    conn, ex, alerter, router = setup()
    upsert_order(conn, order_row("r-6-1", "submitting"))
    rep = await recover(conn=conn, run_id="r", executor=ex, routers={6: router}, alerter=alerter, clock=lambda: T0)
    assert rep.abandoned == ["r-6-1"] and router.state is State.FLAT and rep.states == {6: "FLAT"}
    assert get_order(conn, "r-6-1").status == "abandoned"
    assert list_recovery(conn, "r")[0][1]["abandoned"] == ["r-6-1"]


async def test_crash_after_fill_rebuilds_open_and_replaces_stop():
    conn, ex, alerter, router = setup()
    await ex.submit(OrderRequest(client_order_id="r-6-1", instrument_id=6, side="buy", quantity=Decimal(1),
                                 reduce_only=False, ts=T0)); ex.drain_events()
    upsert_order(conn, order_row("r-6-1", "accepted"))
    upsert_position_local(conn, PositionLocalRow(run_id="r", instrument_id=6, state=State.ENTRY_PENDING, size=Decimal(0),
                                                 entry_price=None, stop_trigger=None, stop_order_id=None,
                                                 cumulative_funding=Decimal(0), updated_at=T0))
    rep = await recover(conn=conn, run_id="r", executor=ex, routers={6: router}, alerter=alerter, clock=lambda: T0)
    assert router.state is State.OPEN and router.size == 1 and router.entry == Decimal("100.08")
    assert rep.stops_replaced == [6] and (await ex.snapshot()).stops == {6: Decimal("85.07")}
    assert rep.abandoned == ["r-6-1"]   # not resting on the venue; its fill is already in the position


async def test_unknown_remote_position_is_critical():
    conn, ex, alerter, router = setup()
    await ex.submit(OrderRequest(client_order_id="x", instrument_id=8, side="sell", quantity=Decimal(2),
                                 reduce_only=False, ts=T0)); ex.drain_events()
    rep = await recover(conn=conn, run_id="r", executor=ex, routers={6: router}, alerter=alerter, clock=lambda: T0)
    assert rep.unknown_positions == [8]
    assert any(a[1] == "CRITICAL" and a[2] == "unknown_position" for a in list_alerts(conn, "r"))


async def test_halted_is_preserved():
    conn, ex, alerter, router = setup()
    upsert_position_local(conn, PositionLocalRow(run_id="r", instrument_id=6, state=State.HALTED, size=Decimal(0),
                                                 entry_price=None, stop_trigger=None, stop_order_id=None,
                                                 cumulative_funding=Decimal(0), updated_at=T0))
    await recover(conn=conn, run_id="r", executor=ex, routers={6: router}, alerter=alerter, clock=lambda: T0)
    assert router.state is State.HALTED


async def test_snapshot_failure_halts_recovery():
    conn, ex, alerter, router = setup()

    async def boom():
        raise RuntimeError("venue down")

    ex.snapshot = boom  # type: ignore[assignment]
    with pytest.raises(RecoveryHalt):
        await recover(conn=conn, run_id="r", executor=ex, routers={6: router}, alerter=alerter, clock=lambda: T0)


async def test_failure_after_snapshot_halts_and_records_one_finding():
    conn, ex, alerter, router = setup()
    await ex.submit(OrderRequest(client_order_id="r-6-1", instrument_id=6, side="buy", quantity=Decimal(1),
                                 reduce_only=False, ts=T0)); ex.drain_events()
    upsert_order(conn, order_row("r-6-1", "accepted"))
    upsert_position_local(conn, PositionLocalRow(run_id="r", instrument_id=6, state=State.ENTRY_PENDING, size=Decimal(0),
                                                 entry_price=None, stop_trigger=None, stop_order_id=None,
                                                 cumulative_funding=Decimal(0), updated_at=T0))

    async def boom(instrument_id, trigger_price):
        raise RuntimeError("place_stop unavailable")

    ex.place_stop = boom  # type: ignore[assignment]
    with pytest.raises(RecoveryHalt):
        await recover(conn=conn, run_id="r", executor=ex, routers={6: router}, alerter=alerter, clock=lambda: T0)

    recs = list_recovery(conn, "r")
    assert len(recs) == 1
    assert recs[0][1]["failed"] is not None and "RuntimeError" in recs[0][1]["failed"]
    critical = [a for a in list_alerts(conn, "r") if a[1] == "CRITICAL" and a[2] == "recovery_failed"]
    assert len(critical) == 1


async def test_adopted_resting_order_keeps_pending_bookkeeping():
    conn, ex, alerter, router = setup()
    upsert_order(conn, order_row("r-6-1", "accepted"))
    upsert_position_local(conn, PositionLocalRow(run_id="r", instrument_id=6, state=State.ENTRY_PENDING, size=Decimal(0),
                                                 entry_price=None, stop_trigger=None, stop_order_id=None,
                                                 cumulative_funding=Decimal(0), updated_at=T0))

    real_snapshot = ex.snapshot

    async def snapshot_with_resting_order():
        snap = await real_snapshot()
        return dc_replace(snap, open_orders=snap.open_orders + ("r-6-1",))

    ex.snapshot = snapshot_with_resting_order  # type: ignore[assignment]

    rep = await recover(conn=conn, run_id="r", executor=ex, routers={6: router}, alerter=alerter, clock=lambda: T0)
    assert rep.adopted == ["r-6-1"]
    assert router.state is State.ENTRY_PENDING and router._pending_cid == "r-6-1"

    await router.handle_event(FillUpdate(client_order_id="r-6-1", instrument_id=6, side="buy", quantity=Decimal(1),
                                         price=Decimal("100.08"), fee=Decimal("0.04"), ts=T0))
    assert router.state is State.OPEN
    assert not any(a[2] == "unexpected_fill" for a in list_alerts(conn, "r"))
