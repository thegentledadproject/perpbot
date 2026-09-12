"""Spec 2.5 alerts. Three sinks behind Alerter.emit(); nothing here may raise
into the router. Telegram is CRITICAL-only; the token never reaches a log line."""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal, Protocol

import httpx

from polyperps.storage import db

Level = Literal["INFO", "WARN", "CRITICAL"]
_log = logging.getLogger("polyperps.alerts")


@dataclass(frozen=True, slots=True, kw_only=True)
class Alert:
    level: Level
    kind: str
    instrument_id: int | None
    detail: dict
    ts: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class AlertThresholds:
    margin_warn: Decimal
    margin_critical: Decimal
    pnl_warn: Decimal
    pnl_critical: Decimal
    funding_drift_x: Decimal


ALERT_THRESHOLDS = AlertThresholds(
    margin_warn=Decimal("0.35"),
    margin_critical=Decimal("0.28"),
    pnl_warn=Decimal("-0.05"),
    pnl_critical=Decimal("-0.10"),
    funding_drift_x=Decimal("3"),
)


class Sink(Protocol):
    def emit(self, run_id: str, alert: Alert) -> None: ...


def _payload(run_id: str, a: Alert) -> dict:
    return {"run_id": run_id, "ts": a.ts.isoformat(), "level": a.level, "kind": a.kind,
            "instrument_id": a.instrument_id, "detail": a.detail}


class LogSink:
    def __init__(self, logger: logging.Logger = _log) -> None:
        self._log = logger

    def emit(self, run_id: str, alert: Alert) -> None:
        line = json.dumps(_payload(run_id, alert), default=str, sort_keys=True)
        level = {"INFO": logging.INFO, "WARN": logging.WARNING, "CRITICAL": logging.CRITICAL}[alert.level]
        self._log.log(level, line)


class SqliteSink:
    def __init__(self, conn) -> None:
        self._conn = conn

    def emit(self, run_id: str, alert: Alert) -> None:
        db.insert_alert(self._conn, run_id=run_id, level=alert.level, kind=alert.kind,
                        instrument_id=alert.instrument_id, detail_json=json.dumps(alert.detail, default=str), ts=alert.ts)


class TelegramSink:
    """CRITICAL only. Failures are logged (without the token) and swallowed."""

    def __init__(self, *, token: str, chat_id: str, transport: httpx.BaseTransport | None = None,
                 timeout_s: float = 5.0) -> None:
        self._url = f"https://api.telegram.org/bot{token}/sendMessage"
        self._chat_id = chat_id
        self._http = httpx.Client(timeout=timeout_s, transport=transport)

    def emit(self, run_id: str, alert: Alert) -> None:
        if alert.level != "CRITICAL":
            return
        text = (f"[polyperps {run_id}] CRITICAL {alert.kind} instrument={alert.instrument_id} "
                f"{json.dumps(alert.detail, default=str)}")
        try:
            resp = self._http.post(self._url, json={"chat_id": self._chat_id, "text": text})
            resp.raise_for_status()
        except Exception as exc:
            _log.warning("telegram sink failed: %s", type(exc).__name__)


class Alerter:
    def __init__(self, run_id: str, sinks: Sequence[Sink]) -> None:
        self._run_id = run_id
        self._sinks = list(sinks)

    def emit(self, alert: Alert) -> None:
        for sink in self._sinks:
            try:
                sink.emit(self._run_id, alert)
            except Exception as exc:
                _log.warning("alert sink %s failed: %s", type(sink).__name__, type(exc).__name__)


# --- pure threshold helpers ---------------------------------------------------


def margin_alert(liq_distance: Decimal, instrument_id: int, ts: datetime,
                 t: AlertThresholds = ALERT_THRESHOLDS) -> Alert | None:
    if liq_distance < t.margin_critical:
        level: Level = "CRITICAL"
    elif liq_distance < t.margin_warn:
        level = "WARN"
    else:
        return None
    return Alert(level=level, kind="margin_ratio", instrument_id=instrument_id,
                 detail={"liq_distance": str(liq_distance)}, ts=ts)


def pnl_alert(equity: Decimal, start_equity: Decimal, ts: datetime, t: AlertThresholds = ALERT_THRESHOLDS) -> Alert | None:
    if start_equity <= 0:
        return None
    dd = (equity - start_equity) / start_equity
    if dd <= t.pnl_critical:
        level: Level = "CRITICAL"
    elif dd <= t.pnl_warn:
        level = "WARN"
    else:
        return None
    return Alert(level=level, kind="pnl_drawdown", instrument_id=None, detail={"drawdown": str(dd)}, ts=ts)


def funding_drift_alert(realised_rate: Decimal, expected_rate: Decimal, instrument_id: int, ts: datetime,
                        t: AlertThresholds = ALERT_THRESHOLDS) -> Alert | None:
    if expected_rate == 0:
        return None
    ratio = abs(realised_rate) / abs(expected_rate)
    if ratio < t.funding_drift_x:
        return None
    return Alert(level="WARN", kind="funding_drift", instrument_id=instrument_id,
                 detail={"realised": str(realised_rate), "expected": str(expected_rate), "ratio": str(ratio)}, ts=ts)
