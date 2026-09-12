import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from polyperps.execution.executor import ExecutorTimeout
from polyperps.execution.sim_executor import MAINTENANCE_RATE, SimExecutor
from polyperps.execution.types import FillUpdate, OrderRequest, OrderUpdate

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)
FEE = Decimal("0.0004")


def make(persist=None, **kw):
    ex = SimExecutor("run1", equity=Decimal(1000), taker_fee_rate=FEE, spread_bps=Decimal(10),
                     impact_bps=Decimal(5), clock=lambda: T0, persist=persist, **kw)
    ex.update_mark(6, Decimal(100))
    return ex


def req(cid="run1-6-1", side="buy", qty="1", reduce_only=False):
    return OrderRequest(client_order_id=cid, instrument_id=6, side=side, quantity=Decimal(qty),
                        reduce_only=reduce_only, ts=T0)


async def test_buy_fills_at_mark_plus_costs_and_emits_events():
    ex = make()
    ack = await ex.submit(req())
    assert ack.status == "accepted" and ack.exchange_order_id == "sim-1"
    ev = ex.drain_events()
    assert [type(e) for e in ev] == [OrderUpdate, FillUpdate]
    fill = ev[1]
    # mark 100 * (1 + (5 + 5)/10000) = 100.10 ; fee = 1 * 100.10 * 0.0004 = 0.04004
    assert fill.price == Decimal("100.10") and fill.fee == Decimal("0.04004")
    snap = await ex.snapshot()
    p = snap.position(6)
    assert p.size == 1 and p.entry_price == Decimal("100.10")
    assert p.liquidation_price == (Decimal("100.10") * (1 - Decimal(1) / 3 + MAINTENANCE_RATE)).quantize(Decimal("0.01"))
    assert snap.equity == Decimal(1000) - fill.fee + (Decimal(100) - Decimal("100.10"))  # marked at mark 100


async def test_sell_short_and_funding_sign():
    ex = make()
    await ex.submit(req(side="sell"))
    ex.drain_events()
    p = (await ex.snapshot()).position(6)
    assert p.size == -1 and p.entry_price == Decimal("99.90")
    ex.apply_funding(6, Decimal("0.001"))   # positive funding: shorts RECEIVE
    p2 = (await ex.snapshot()).position(6)
    assert p2.cumulative_funding == Decimal("0.1")   # 1 * 100 * 0.001, received
    ex2 = make()
    await ex2.submit(req())
    ex2.drain_events()
    ex2.apply_funding(6, Decimal("0.001"))
    assert (await ex2.snapshot()).position(6).cumulative_funding == Decimal("-0.1")  # long pays


async def test_stop_fires_from_check_triggers_without_router():
    ex = make()
    await ex.submit(req())
    ex.drain_events()
    await ex.place_stop(6, Decimal(85))
    assert (await ex.snapshot()).stops == {6: Decimal(85)}
    ex.update_mark(6, Decimal(90))
    assert ex.check_triggers() == []
    ex.update_mark(6, Decimal(84))
    fills = ex.check_triggers()
    assert len(fills) == 1 and fills[0].side == "sell" and fills[0].quantity == 1
    snap = await ex.snapshot()
    assert snap.position(6) is None and snap.stops == {}


async def test_timeout_applies_fill_but_raises():
    ex = make()
    ex.fail_next = "timeout"
    with pytest.raises(ExecutorTimeout):
        await ex.submit(req())
    assert ex.fail_next is None
    assert (await ex.snapshot()).position(6).size == 1
    assert any(isinstance(e, FillUpdate) for e in ex.drain_events())


async def test_reject_applies_nothing():
    ex = make()
    ex.fail_next = "reject"
    ack = await ex.submit(req())
    assert ack.status == "rejected" and (await ex.snapshot()).position(6) is None


async def test_persist_and_restore_round_trip():
    saved = []
    ex = make(persist=saved.append)
    await ex.submit(req())
    ex.drain_events()
    await ex.place_stop(6, Decimal(85))
    assert saved  # persisted after each mutation
    restored = SimExecutor.from_json("run1", saved[-1], taker_fee_rate=FEE, clock=lambda: T0)
    restored.update_mark(6, Decimal(100))
    s1, s2 = await ex.snapshot(), await restored.snapshot()
    assert s1.positions == s2.positions and s1.stops == s2.stops and s1.equity == s2.equity


async def test_heartbeat_counts_and_reduce_only_cannot_open():
    ex = make()
    await ex.heartbeat()
    assert ex.heartbeat_count == 1
    ack = await ex.submit(req(reduce_only=True))
    assert ack.status == "rejected" and "reduce_only" in ack.reason
