from datetime import datetime, timezone
from decimal import Decimal

import pytest

from polyperps.execution.types import (
    AccountSnapshot, DecisionRow, FillUpdate, Intent, OrderAck, OrderRequest, PositionView, State,
)

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


def pos(iid=6, size="1", entry="100", liq="70"):
    return PositionView(instrument_id=iid, size=Decimal(size), entry_price=Decimal(entry),
                        notional=abs(Decimal(size)) * Decimal("100"), leverage=3,
                        liquidation_price=Decimal(liq) if liq else None,
                        unrealised_pnl=Decimal(0), cumulative_funding=Decimal(0))


def test_state_values():
    assert [s.value for s in State] == ["FLAT", "ENTRY_PENDING", "OPEN", "EXIT_PENDING", "LIQUIDATED", "HALTED"]


def test_snapshot_position_lookup():
    snap = AccountSnapshot(equity=Decimal(1000), positions=(pos(6), pos(7, size="-2")), open_orders=(),
                           stops={6: Decimal(85)}, in_liquidation=False, ts=T0)
    assert snap.position(6).size == 1 and snap.position(7).size == -2 and snap.position(8) is None


def test_datetimes_must_be_utc():
    with pytest.raises(ValueError):
        OrderRequest(client_order_id="r-6-1", instrument_id=6, side="buy", quantity=Decimal(1),
                     reduce_only=False, ts=datetime(2026, 9, 12, 12, 0))


def test_frozen_kw_only():
    ack = OrderAck(client_order_id="r-6-1", exchange_order_id="x1", status="accepted", reason="", ts=T0)
    with pytest.raises(AttributeError):
        ack.status = "rejected"  # type: ignore[misc]
    with pytest.raises(TypeError):
        FillUpdate("r-6-1")  # positional


def test_intent_defaults():
    i = Intent(instrument_id=6, side="sell", quantity=Decimal("0.5"), notional=Decimal(50))
    assert i.reduce_only is False and i.reason == "strategy"


def test_row_datetimes_must_be_utc():
    with pytest.raises(ValueError):
        DecisionRow(run_id="run1", instrument_id=6, seq=1, ts=datetime(2026, 9, 12, 12, 0),
                    state_before=State.FLAT, target=None)
