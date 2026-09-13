from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from polyperps.execution.executor import GateClosed
from polyperps.execution.live_executor import LiveExecutor
from polyperps.execution.types import FillUpdate, OrderRequest, OrderUpdate, ReconcileNow
from polyperps.gates import ExecutionMode, GateDecision

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


class FakeSession:
    def __init__(self):
        self.calls = []
        self._events = []

    async def place_order(self, **kw):
        self.calls.append(("place_order", kw))
        return SimpleNamespace(order=SimpleNamespace(id=777))

    async def cancel_order(self, **kw):
        self.calls.append(("cancel_order", kw))

    async def place_position_tp_sl(self, **kw):
        self.calls.append(("tp_sl", kw))
        return SimpleNamespace(stop_loss=SimpleNamespace(order_id=888))

    async def arm_auto_cancel(self, **kw):
        self.calls.append(("arm", kw))

    async def fetch_portfolio(self):
        return SimpleNamespace(
            positions=(SimpleNamespace(instrument_id=6, size=Decimal("0.5"), entry_price=Decimal(100), leverage=3,
                                       position_value=Decimal(50), liquidation_price=Decimal(70),
                                       unrealized_pnl=Decimal(1), cumulative_funding=Decimal("-0.2")),),
            margin=SimpleNamespace(total_account_value=Decimal(1000)), withdrawable=Decimal(900),
            in_liquidation=False, timestamp=T0)

    async def fetch_open_orders(self):
        return (SimpleNamespace(client_order_id="r-6-3", id=1, tp_sl=None, instrument_id=6),
                SimpleNamespace(client_order_id=None, id=888, instrument_id=6,
                                tp_sl=SimpleNamespace(kind="sl", trigger_price=Decimal(85))))

    def __aiter__(self):
        async def gen():
            for e in self._events:
                yield e
        return gen()

    async def close(self):
        self.calls.append(("close", {}))


OPEN = lambda iid: GateDecision(True, "all gates passed")


def test_constructor_refuses_by_default_gate(monkeypatch):
    # The default gate reads POLYMARKET_LIVE_TRADING from the real os.environ
    # (see polyperps.gates.live_orders_allowed); set it explicitly so this
    # test exercises the third lock (SIGNAL_VALIDATED, hard-wired False in
    # this repo) deterministically, regardless of the host's ambient
    # environment. Same pattern as
    # tests/test_gates.py::test_defaults_read_module_flag_and_are_blocked.
    monkeypatch.setenv("POLYMARKET_LIVE_TRADING", "true")
    with pytest.raises(GateClosed, match="SIGNAL_VALIDATED|manual_review"):
        LiveExecutor(FakeSession(), instrument_ids=[6], modes={6: ExecutionMode.AUTO})
    with pytest.raises(GateClosed):
        LiveExecutor(FakeSession(), instrument_ids=[6], modes={}, gate=lambda i: GateDecision(False, "closed"))


async def test_submit_maps_payload_and_ack():
    s = FakeSession()
    ex = LiveExecutor(s, instrument_ids=[6], modes={}, gate=OPEN, clock=lambda: T0)
    ack = await ex.submit(OrderRequest(client_order_id="r-6-1", instrument_id=6, side="buy", quantity=Decimal("0.5"),
                                       reduce_only=False, ts=T0))
    # The installed SDK's OrderSide is Literal["BUY", "SELL"] (uppercase; see
    # .venv/Lib/site-packages/polymarket/models/types.py:6), while our own
    # OrderRequest.side is "buy"/"sell" -- submit() maps it before the call.
    assert s.calls[0] == ("place_order", {"instrument_id": 6, "side": "BUY", "quantity": Decimal("0.5"),
                                          "time_in_force": "ioc", "reduce_only": False, "client_order_id": "r-6-1"})
    assert ack.status == "accepted" and ack.exchange_order_id == "777"


async def test_stop_heartbeat_cancel():
    s = FakeSession()
    ex = LiveExecutor(s, instrument_ids=[6], modes={}, gate=OPEN, clock=lambda: T0)
    st = await ex.place_stop(6, Decimal(85))
    assert s.calls[-1][0] == "tp_sl" and s.calls[-1][1]["instrument_id"] == 6
    assert s.calls[-1][1]["stop_loss"].trigger_price == Decimal(85) and st.exchange_order_id == "888"
    await ex.cancel_stop(6)
    assert s.calls[-1] == ("cancel_order", {"order_id": 888})
    await ex.heartbeat()
    assert s.calls[-1] == ("arm", {"cancel_at": T0 + timedelta(seconds=60)})
    await ex.cancel("r-6-1")
    assert s.calls[-1] == ("cancel_order", {"client_order_id": "r-6-1"})


async def test_snapshot_maps_portfolio_and_stops():
    ex = LiveExecutor(FakeSession(), instrument_ids=[6], modes={}, gate=OPEN, clock=lambda: T0)
    snap = await ex.snapshot()
    assert snap.equity == Decimal(1000) and snap.position(6).liquidation_price == Decimal(70)
    assert snap.position(6).notional == Decimal(50) and snap.open_orders == ("r-6-3",)
    assert snap.stops == {6: Decimal(85)}


async def test_events_mapping():
    s = FakeSession()
    from polymarket.models.perps.events import PerpsResyncEvent
    s._events = [
        SimpleNamespace(type="order", payload=SimpleNamespace(client_order_id="r-6-1", status="filled", filled_quantity=Decimal(1)), timestamp=T0),
        SimpleNamespace(type="fill", payload=[SimpleNamespace(client_order_id="r-6-1", instrument_id=6, side="buy",
                                                             quantity=Decimal(1), price=Decimal(100), fee=Decimal("0.04"))], timestamp=T0),
        SimpleNamespace(type="balance", payload=None, timestamp=T0),
        PerpsResyncEvent.model_construct(),
    ]
    ex = LiveExecutor(s, instrument_ids=[6], modes={}, gate=OPEN, clock=lambda: T0)
    out = [e async for e in ex.events()]
    assert [type(e) for e in out] == [OrderUpdate, FillUpdate, ReconcileNow]
    assert out[1].instrument_id == 6 and out[1].fee == Decimal("0.04")
