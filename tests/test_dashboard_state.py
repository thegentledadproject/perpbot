"""Numbers the dashboard shows, pinned against hand-computed values."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from polyperps.dashboard import state as st
from polyperps.exchange.types import Instrument
from polyperps.execution.sim_executor import SimExecutor
from polyperps.execution.types import DecisionRow, OrderRow, PositionLocalRow, State
from polyperps.storage.db import (
    connect,
    get_positions_local,
    insert_alert,
    insert_decision,
    insert_recovery,
    save_sim_account,
    upsert_order,
    upsert_position_local,
)

RUN = "paper-test"
T0 = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)

BLOB = {
    "cash": "10000",
    "start_equity": "10000",
    "n": 2,
    "positions": {
        "6": {"size": "0.001", "entry": "100000", "funding": "0.3"},
        "7": {"size": "-0.1", "entry": "4000", "funding": "0.5"},
    },
    "stops": {"6": "85000", "7": "4600"},
    "marks": {"6": "101000", "7": "4040"},
}


def instrument(iid: int, symbol: str, category: str = "crypto") -> Instrument:
    return Instrument(
        instrument_id=iid, symbol=symbol, category=category, funding_interval="1h",
        max_leverage=20, price_decimals=2, quantity_decimals=4,
        min_notional=Decimal("1"), isolated_only=True,
    )


INSTRUMENTS = {6: instrument(6, "BTC"), 7: instrument(7, "ETH")}


def seeded_conn():
    conn = connect(":memory:")
    save_sim_account(conn, RUN, json.dumps(BLOB))
    upsert_position_local(conn, PositionLocalRow(
        run_id=RUN, instrument_id=6, state=State.OPEN, size=Decimal("0.001"),
        entry_price=Decimal("100000"), stop_trigger=Decimal("85000"), stop_order_id=None,
        cumulative_funding=Decimal("0.3"), updated_at=T0))
    upsert_position_local(conn, PositionLocalRow(
        run_id=RUN, instrument_id=7, state=State.OPEN, size=Decimal("-0.1"),
        entry_price=Decimal("4000"), stop_trigger=Decimal("4600"), stop_order_id=None,
        cumulative_funding=Decimal("0.5"), updated_at=T0))
    upsert_order(conn, OrderRow(
        client_order_id="6-open", run_id=RUN, instrument_id=6, side="BUY",
        quantity=Decimal("0.001"), reduce_only=False, status="filled", exchange_order_id=None,
        filled_quantity=Decimal("0.001"), avg_price=Decimal("100000"),
        submitted_at=T0 - timedelta(hours=2), updated_at=T0 - timedelta(hours=2), reason="strategy"))
    upsert_order(conn, OrderRow(
        client_order_id="7-open", run_id=RUN, instrument_id=7, side="SELL",
        quantity=Decimal("0.1"), reduce_only=False, status="filled", exchange_order_id=None,
        filled_quantity=Decimal("0.1"), avg_price=Decimal("4000"),
        submitted_at=T0 - timedelta(hours=1), updated_at=T0 - timedelta(hours=1), reason="strategy"))
    return conn


def test_account_numbers():
    conn = seeded_conn()
    account, positions = st.account_and_positions(conn, run_id=RUN, instruments=INSTRUMENTS)
    # equity = cash + 0.001*(101000-100000) + (-0.1)*(4040-4000) = 10000 + 1 - 4
    assert account["equity"] == "9997"
    assert account["start_equity"] == "10000"
    assert account["pnl_since_start"] == "-3"
    assert account["unrealized"] == "-3"
    # gross = 0.001*101000 + 0.1*4040 = 101 + 404 = 505
    assert account["gross_exposure"] == pytest.approx(505 / 9997)
    assert account["gross_limit"] == 1.0
    # both crypto: net = +101 - 404 = -303
    assert account["cluster_net"] == pytest.approx(303 / 9997)
    assert account["cluster_limit"] == 0.6
    assert account["leverage"] == 3
    assert account["kill_switch"] == "unarmed"
    assert len(positions) == 2


def test_position_numbers_long():
    conn = seeded_conn()
    _, positions = st.account_and_positions(conn, run_id=RUN, instruments=INSTRUMENTS)
    p = {x["instrument_id"]: x for x in positions}[6]
    assert p["name"] == "BTC"
    assert p["side"] == "LONG"
    assert p["state"] == "OPEN"
    assert p["size"] == "0.001"
    assert p["entry_price"] == "100000"
    assert p["mark"] == "101000"
    assert p["pnl"] == "1"
    # liq = 100000 * (1 - 1/3 + 0.02) = 68666.67
    assert p["liq_price"] == "68666.67"
    assert p["liq_distance"] == pytest.approx((101000 - 68666.67) / 101000)
    assert p["adverse_move"] == 0.0            # moved in our favour
    assert p["funding_paid"] == pytest.approx(0.3 / 100)
    assert p["stop_trigger"] == "85000"
    assert p["opened_at"] == (T0 - timedelta(hours=2)).isoformat()


def test_position_numbers_short():
    conn = seeded_conn()
    _, positions = st.account_and_positions(conn, run_id=RUN, instruments=INSTRUMENTS)
    p = {x["instrument_id"]: x for x in positions}[7]
    assert p["side"] == "SHORT"
    assert p["pnl"] == "-4"
    # liq = 4000 * (1 + 1/3 - 0.02) = 5253.33
    assert p["liq_price"] == "5253.33"
    assert p["liq_distance"] == pytest.approx((5253.33 - 4040) / 4040)
    assert p["adverse_move"] == pytest.approx(0.01)   # short, price rose 1 %
    assert p["funding_paid"] == pytest.approx(0.5 / 400)


async def test_liq_price_matches_sim_executor():
    ex = SimExecutor.from_json(RUN, json.dumps(BLOB), taker_fee_rate=Decimal("0"))
    snap = await ex.snapshot()
    by_id = {v.instrument_id: v for v in snap.positions}
    assert st.liq_price(Decimal("0.001"), Decimal("100000")) == by_id[6].liquidation_price
    assert st.liq_price(Decimal("-0.1"), Decimal("4000")) == by_id[7].liquidation_price
    assert Decimal(st.account_and_positions(seeded_conn(), run_id=RUN, instruments=INSTRUMENTS)[0]["equity"]) == snap.equity


def test_unknown_instruments_fall_back():
    conn = seeded_conn()
    account, positions = st.account_and_positions(conn, run_id=RUN, instruments=None)
    assert account["cluster_net"] is None
    assert {p["name"] for p in positions} == {"inst 6", "inst 7"}


def test_no_sim_account_yet():
    conn = connect(":memory:")
    assert st.account_and_positions(conn, run_id=RUN, instruments=INSTRUMENTS) == (None, [])


def test_guards_ok_when_quiet():
    conn = seeded_conn()
    account, positions = st.account_and_positions(conn, run_id=RUN, instruments=INSTRUMENTS)
    g = st.guards(conn, run_id=RUN, account=account, positions=positions)
    assert g == {
        "margin": "ok", "liquidation": "ok", "exposure": "ok",
        "halted": [], "reconciliation": None,
    }


def test_guards_margin_follows_latest_alert():
    conn = seeded_conn()
    insert_alert(conn, run_id=RUN, level="WARN", kind="margin_ratio", instrument_id=6,
                 detail_json="{}", ts=T0 - timedelta(minutes=5))
    insert_alert(conn, run_id=RUN, level="INFO", kind="margin_ratio", instrument_id=6,
                 detail_json="{}", ts=T0 - timedelta(minutes=1))
    insert_alert(conn, run_id=RUN, level="CRITICAL", kind="pnl_drawdown", instrument_id=None,
                 detail_json="{}", ts=T0)
    account, positions = st.account_and_positions(conn, run_id=RUN, instruments=INSTRUMENTS)
    g = st.guards(conn, run_id=RUN, account=account, positions=positions)
    assert g["margin"] == "INFO"          # latest margin_ratio row, not the CRITICAL pnl one


def test_guards_halted_and_reconciliation():
    conn = seeded_conn()
    upsert_position_local(conn, PositionLocalRow(
        run_id=RUN, instrument_id=7, state=State.HALTED, size=Decimal("0"),
        entry_price=None, stop_trigger=None, stop_order_id=None,
        cumulative_funding=Decimal("0"), updated_at=T0))
    insert_recovery(conn, run_id=RUN, ts=T0, findings_json=json.dumps({"stop_missing": [6]}))
    account, positions = st.account_and_positions(conn, run_id=RUN, instruments=INSTRUMENTS)
    g = st.guards(conn, run_id=RUN, account=account, positions=positions)
    assert g["halted"] == [7]
    assert g["reconciliation"] == {"findings": 1, "at": T0.isoformat()}


def test_guards_breach_flags():
    conn = seeded_conn()
    account, positions = st.account_and_positions(conn, run_id=RUN, instruments=INSTRUMENTS)
    positions[0]["liq_distance"] = 0.20
    account["gross_exposure"] = 1.2
    g = st.guards(conn, run_id=RUN, account=account, positions=positions)
    assert g["liquidation"] == "breach"
    assert g["exposure"] == "breach"


def test_recent_decisions_newest_first_and_capped():
    conn = connect(":memory:")
    for seq in range(60):
        insert_decision(conn, DecisionRow(
            run_id=RUN, instrument_id=6, seq=seq, ts=T0 + timedelta(minutes=seq),
            state_before=State.FLAT, target=None, verdicts={"vet_entry": "allow"},
            intent=None, client_order_id=None, note="skip:warmup" if seq < 48 else "hold"))
    insert_decision(conn, DecisionRow(
        run_id=RUN, instrument_id=7, seq=0, ts=T0 + timedelta(minutes=100),
        state_before=State.OPEN, target=Decimal("1"), verdicts={}, intent=None,
        client_order_id="7-x", note="flip"))
    rows = st.recent_decisions(conn, run_id=RUN, instrument_ids=(6, 7))
    assert len(rows) == 50
    assert rows[0] == {
        "ts": (T0 + timedelta(minutes=100)).isoformat(), "instrument_id": 7, "note": "flip",
        "state_before": "OPEN", "target": "1", "verdicts": {}, "client_order_id": "7-x",
    }
    assert rows[1]["note"] == "hold" and rows[1]["instrument_id"] == 6


def test_recent_alerts_newest_first():
    conn = connect(":memory:")
    insert_alert(conn, run_id=RUN, level="WARN", kind="margin_ratio", instrument_id=6,
                 detail_json=json.dumps({"ratio": 0.33}), ts=T0)
    insert_alert(conn, run_id=RUN, level="INFO", kind="stop_placed", instrument_id=6,
                 detail_json="{}", ts=T0 + timedelta(minutes=1))
    rows = st.recent_alerts(conn, run_id=RUN)
    assert [r["kind"] for r in rows] == ["stop_placed", "margin_ratio"]
    assert rows[1] == {"ts": T0.isoformat(), "level": "WARN", "kind": "margin_ratio",
                       "instrument_id": 6, "detail": {"ratio": 0.33}}


def test_is_clean():
    conn = seeded_conn()
    assert st.is_clean(conn, run_id=RUN) is True
    insert_alert(conn, run_id=RUN, level="CRITICAL", kind="kill_switch", instrument_id=None,
                 detail_json="{}", ts=T0)
    assert st.is_clean(conn, run_id=RUN) is False


def test_is_clean_false_when_halted():
    conn = seeded_conn()
    upsert_position_local(conn, PositionLocalRow(
        run_id=RUN, instrument_id=6, state=State.LIQUIDATED, size=Decimal("0"),
        entry_price=None, stop_trigger=None, stop_order_id=None,
        cumulative_funding=Decimal("0"), updated_at=T0))
    assert st.is_clean(conn, run_id=RUN) is False
