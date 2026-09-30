"""scripts/live_smoke.py places REAL orders when a human runs it. Here it only ever meets a fake
session; the real open_session is replaced in every test."""

import asyncio
import importlib.util
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

T0 = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


def load():
    spec = importlib.util.spec_from_file_location("live_smoke", "scripts/live_smoke.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_refuses_without_the_typed_confirmation():
    mod = load()
    opened = []

    async def open_session(label):
        opened.append(label)
        raise AssertionError("must not open a session")

    for answers in (["7", mod.CONFIRM], ["6", "i accept real orders"], ["6", ""], ["", mod.CONFIRM]):
        it = iter(answers)
        code = mod.main(["--instrument", "6", "--quantity", "0.001"], ask=lambda _p: next(it),
                        say=lambda _m: None, open_session=open_session)
        assert code == 2
    assert opened == []


class FakeSmokeSession:
    """The IOC fills at 100; after the operator presses Enter the exchange stop fills (no client id)."""
    def __init__(self):
        self.calls = []
        self.size = Decimal(0)
        self.stopped = asyncio.Event()

    async def place_order(self, **kw):
        self.calls.append(("place_order", kw))
        self.size = Decimal(kw["quantity"])
        return SimpleNamespace(order=SimpleNamespace(id=1, status="filled"))

    async def place_position_tp_sl(self, **kw):
        self.calls.append(("tp_sl", kw))
        return SimpleNamespace(stop_loss=SimpleNamespace(order_id=2))

    def stop_fills(self):
        self.size = Decimal(0)
        self.stopped.set()

    async def fetch_portfolio(self):
        positions = () if self.size == 0 else (SimpleNamespace(
            instrument_id=6, size=self.size, entry_price=Decimal(100), leverage=3, position_value=self.size * 100,
            liquidation_price=None, unrealized_pnl=Decimal(0), cumulative_funding=Decimal("-0.01")),)
        return SimpleNamespace(positions=positions, margin=SimpleNamespace(total_account_value=Decimal(1000)),
                               in_liquidation=False)

    async def fetch_open_orders(self):
        return ()

    def __aiter__(self):
        async def gen():
            await self.stopped.wait()
            yield SimpleNamespace(type="fill", timestamp=T0, payload=[SimpleNamespace(
                client_order_id=None, order_id=2, instrument_id=6, side="short", quantity=Decimal("0.001"),
                price=Decimal(99), fee=Decimal(0))])
            await asyncio.Event().wait()
        return gen()

    async def close(self):
        self.calls.append(("close", {}))


def test_smoke_with_a_fake_session_places_order_and_stop_and_sees_the_venue_fill():
    mod = load()
    session = FakeSmokeSession()
    answers = iter(["6", mod.CONFIRM])

    def ask(prompt):
        if prompt.startswith("kill the bot now"):
            session.stop_fills()                  # the operator waited for the exchange stop
            return ""
        return next(answers)

    sdk = SimpleNamespace(closed=False)

    async def sdk_close():
        sdk.closed = True

    sdk.close = sdk_close

    async def open_session(label):
        return sdk, session

    said = []
    code = mod.main(["--instrument", "6", "--quantity", "0.001"], ask=ask, say=said.append,
                    open_session=open_session, poll_s=0)
    assert code == 0
    (order,) = [kw for name, kw in session.calls if name == "place_order"]
    assert order["side"] == "BUY" and order["time_in_force"] == "ioc" and order["reduce_only"] is False
    assert order["quantity"] == Decimal("0.001")
    (stop,) = [kw for name, kw in session.calls if name == "tp_sl"]
    assert stop["instrument_id"] == 6 and stop["stop_loss"].trigger_price == Decimal("99.00")
    assert ("close", {}) in session.calls and sdk.closed
    assert any("stop_filled_by_exchange': True" in s for s in said)
