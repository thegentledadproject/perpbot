import json
import logging
from datetime import datetime, timezone
from decimal import Decimal

import httpx

from polyperps.monitor.alerts import (
    ALERT_THRESHOLDS, Alert, Alerter, AlertThresholds, LogSink, SqliteSink, TelegramSink,
    funding_drift_alert, margin_alert, pnl_alert,
)
from polyperps.storage.db import connect, list_alerts

T0 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc)


def test_thresholds_pinned():
    assert ALERT_THRESHOLDS == AlertThresholds(margin_warn=Decimal("0.35"), margin_critical=Decimal("0.28"),
                                               pnl_warn=Decimal("-0.05"), pnl_critical=Decimal("-0.10"),
                                               funding_drift_x=Decimal("3"))


def test_margin_pnl_funding_helpers():
    assert margin_alert(Decimal("0.40"), 6, T0) is None
    assert margin_alert(Decimal("0.30"), 6, T0).level == "WARN"
    assert margin_alert(Decimal("0.20"), 6, T0).level == "CRITICAL"
    assert pnl_alert(Decimal(960), Decimal(1000), T0) is None
    assert pnl_alert(Decimal(940), Decimal(1000), T0).level == "WARN"
    assert pnl_alert(Decimal(890), Decimal(1000), T0).level == "CRITICAL"
    assert funding_drift_alert(Decimal("0.0002"), Decimal("0.0001"), 6, T0) is None
    a = funding_drift_alert(Decimal("0.0004"), Decimal("0.0001"), 6, T0)
    assert a is not None and a.kind == "funding_drift" and a.level == "WARN"


def test_alerter_fans_out_and_sqlite_sink_writes(caplog):
    conn = connect(":memory:")
    alerter = Alerter("run1", [LogSink(), SqliteSink(conn)])
    with caplog.at_level(logging.INFO, logger="polyperps.alerts"):
        alerter.emit(Alert(level="WARN", kind="margin_ratio", instrument_id=6, detail={"distance": "0.3"}, ts=T0))
    (row,) = list_alerts(conn, "run1")
    assert row[1:4] == ("WARN", "margin_ratio", 6) and row[4] == {"distance": "0.3"}
    assert any('"kind": "margin_ratio"' in r.message for r in caplog.records)


def test_telegram_sink_posts_only_critical_and_never_leaks_token(caplog):
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((str(request.url), json.loads(request.content)))
        return httpx.Response(200, json={"ok": True})

    sink = TelegramSink(token="SECRET123", chat_id="42", transport=httpx.MockTransport(handler))
    sink.emit("run1", Alert(level="WARN", kind="x", instrument_id=None, detail={}, ts=T0))
    assert seen == []
    sink.emit("run1", Alert(level="CRITICAL", kind="reconcile_mismatch", instrument_id=6, detail={"k": 1}, ts=T0))
    assert seen[0][0] == "https://api.telegram.org/botSECRET123/sendMessage"
    assert seen[0][1]["chat_id"] == "42" and "reconcile_mismatch" in seen[0][1]["text"]

    def failing(request):
        raise httpx.ConnectError("boom")

    bad = TelegramSink(token="SECRET123", chat_id="42", transport=httpx.MockTransport(failing))
    with caplog.at_level(logging.WARNING, logger="polyperps.alerts"):
        bad.emit("run1", Alert(level="CRITICAL", kind="x", instrument_id=None, detail={}, ts=T0))  # must not raise
    assert all("SECRET123" not in r.message for r in caplog.records)


def test_alerter_swallows_sink_errors():
    class Boom:
        def emit(self, run_id, alert):
            raise RuntimeError("sink down")

    Alerter("run1", [Boom()]).emit(Alert(level="INFO", kind="x", instrument_id=None, detail={}, ts=T0))
