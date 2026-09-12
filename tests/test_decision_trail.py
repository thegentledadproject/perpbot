from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.execution.types import DecisionRow, Intent, OrderRow, State
from polyperps.monitor.decision_trail import reconstruct
from polyperps.storage.db import connect, insert_alert, insert_decision, insert_recovery, upsert_order

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


def test_reconstruct_orders_events_chronologically_from_sqlite_only():
    conn = connect(":memory:")
    insert_recovery(conn, run_id="r", ts=T0 - timedelta(minutes=1), findings_json='{"adopted": []}')
    intent = Intent(instrument_id=6, side="buy", quantity=Decimal(1), notional=Decimal(100))
    insert_decision(conn, DecisionRow(run_id="r", instrument_id=6, seq=1, ts=T0, state_before=State.FLAT,
                                      target=Decimal(1), verdicts={"vet_entry": "allow"}, intent=intent,
                                      client_order_id="r-6-1"))
    upsert_order(conn, OrderRow(client_order_id="r-6-1", run_id="r", instrument_id=6, side="buy", quantity=Decimal(1),
                                reduce_only=False, status="filled", exchange_order_id="sim-1", filled_quantity=Decimal(1),
                                avg_price=Decimal("100.05"), submitted_at=T0 + timedelta(seconds=1),
                                updated_at=T0 + timedelta(seconds=2), reason="strategy"))
    insert_alert(conn, run_id="r", level="INFO", kind="stop_placed", instrument_id=6, detail_json='{"trigger": "85"}',
                 ts=T0 + timedelta(seconds=3))
    insert_alert(conn, run_id="r", level="WARN", kind="unrelated", instrument_id=7, detail_json='{}', ts=T0)
    trail = reconstruct(conn, "r", 6)
    assert [e.kind for e in trail] == ["recovery", "decision", "order", "alert"]
    assert "r-6-1" in trail[1].summary and "filled" in trail[2].summary and "stop_placed" in trail[3].summary
