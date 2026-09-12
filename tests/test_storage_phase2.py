import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.execution.types import DecisionRow, Intent, OrderRow, PositionLocalRow, State
from polyperps.storage.db import (
    connect, get_order, get_positions_local, insert_alert, insert_decision, insert_recovery,
    list_alerts, list_decisions, list_orders, list_recovery, load_sim_account, save_sim_account,
    upsert_order, upsert_position_local,
)

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)
RUN = "run1"


def test_decisions_round_trip_ordered_by_seq():
    conn = connect(":memory:")
    intent = Intent(instrument_id=6, side="buy", quantity=Decimal("0.01"), notional=Decimal(100))
    for seq in (2, 1):
        insert_decision(conn, DecisionRow(run_id=RUN, instrument_id=6, seq=seq, ts=T0 + timedelta(seconds=seq),
                                          state_before=State.FLAT, target=Decimal(1),
                                          verdicts={"vet_entry": "allow", "vet_exposure": "allow"},
                                          intent=intent if seq == 1 else None,
                                          client_order_id=f"{RUN}-6-{seq}" if seq == 1 else None))
    rows = list_decisions(conn, RUN, 6)
    assert [r.seq for r in rows] == [1, 2]
    assert rows[0].intent == intent and rows[0].verdicts["vet_entry"] == "allow"
    assert rows[1].intent is None and rows[1].target == Decimal(1)


def test_orders_upsert_and_query():
    conn = connect(":memory:")
    row = OrderRow(client_order_id=f"{RUN}-6-1", run_id=RUN, instrument_id=6, side="buy", quantity=Decimal("0.01"),
                   reduce_only=False, status="submitting", exchange_order_id=None, filled_quantity=Decimal(0),
                   avg_price=None, submitted_at=T0, updated_at=T0, reason="strategy")
    upsert_order(conn, row)
    upsert_order(conn, replace(row, status="filled", filled_quantity=Decimal("0.01"),
                               avg_price=Decimal("100.05"), exchange_order_id="sim-1"))
    got = get_order(conn, f"{RUN}-6-1")
    assert got.status == "filled" and got.avg_price == Decimal("100.05") and got.exchange_order_id == "sim-1"
    assert [o.client_order_id for o in list_orders(conn, RUN, status="filled")] == [f"{RUN}-6-1"]
    assert list_orders(conn, RUN, status="open") == []
    assert get_order(conn, "nope") is None


def test_positions_local_upsert():
    conn = connect(":memory:")
    upsert_position_local(conn, PositionLocalRow(run_id=RUN, instrument_id=6, state=State.OPEN, size=Decimal("0.01"),
                                                 entry_price=Decimal(100), stop_trigger=Decimal(85), stop_order_id="s1",
                                                 cumulative_funding=Decimal("-0.1"), updated_at=T0))
    upsert_position_local(conn, PositionLocalRow(run_id=RUN, instrument_id=6, state=State.FLAT, size=Decimal(0),
                                                 entry_price=None, stop_trigger=None, stop_order_id=None,
                                                 cumulative_funding=Decimal(0), updated_at=T0 + timedelta(hours=1)))
    got = get_positions_local(conn, RUN)
    assert got[6].state is State.FLAT and got[6].entry_price is None


def test_sim_account_alerts_recovery():
    conn = connect(":memory:")
    assert load_sim_account(conn, RUN) is None
    save_sim_account(conn, RUN, json.dumps({"cash": "1000"}))
    save_sim_account(conn, RUN, json.dumps({"cash": "990"}))
    assert json.loads(load_sim_account(conn, RUN))["cash"] == "990"
    insert_alert(conn, run_id=RUN, level="WARN", kind="margin_ratio", instrument_id=6, detail_json='{"d": 0.3}', ts=T0)
    (a,) = list_alerts(conn, RUN)
    assert a[1:4] == ("WARN", "margin_ratio", 6) and a[4] == {"d": 0.3}
    insert_recovery(conn, run_id=RUN, ts=T0, findings_json='{"adopted": []}')
    assert list_recovery(conn, RUN)[0][1] == {"adopted": []}
