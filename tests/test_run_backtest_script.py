"""Exercises scripts/run_backtest.py end to end against a synthetic local DB.

No network, no real data: a temp SQLite DB is seeded with just enough NATIVE
candles/funding for h1 to trade, then the script's main() is invoked in-process
via monkeypatched argv/env, matching how it would actually be launched.
"""

import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from polyperps.exchange.types import Candle, FeeSchedule, FundingObservation, SourceType
from polyperps.signal.validation_log import read_records
from polyperps.storage.db import connect, insert_candle, insert_fee, insert_funding

UTC = timezone.utc
HOUR = timedelta(hours=1)
NATIVE = SourceType.POLYMARKET_REST

_SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "run_backtest.py"


def _load_script():
    spec = importlib.util.spec_from_file_location("run_backtest", _SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _seed_db(db_path: Path, *, n_bars: int, with_fee: bool, last_open: datetime | None = None) -> None:
    conn = connect(db_path)
    try:
        if with_fee:
            insert_fee(conn, FeeSchedule(category="equity", taker_fee_rate=Decimal("0.0004"),
                                         maker_fee_rate=Decimal("0.0002"),
                                         fetched_at=datetime.now(UTC)))
        if n_bars:
            # Default anchor: real wall-clock "now" with a buffer, matching a run without --end.
            if last_open is None:
                last_open = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - 3 * HOUR
            first_open = last_open - (n_bars - 1) * HOUR
            for i in range(n_bars):
                open_ts = first_open + i * HOUR
                insert_candle(conn, Candle(instrument_id=6, interval="1h", open_ts=open_ts,
                                           open=Decimal("100"), high=Decimal("100"), low=Decimal("100"),
                                           close=Decimal("100"), volume=Decimal("1"), trades=1,
                                           received_ts=open_ts, source_type=NATIVE))
                # 6-bar funding spike well inside the train slice so h1 actually trades.
                rate = Decimal("0.01") if 150 <= i < 156 else Decimal("0.0001")
                insert_funding(conn, FundingObservation(instrument_id=6, funding_rate=rate,
                                                        exchange_ts=open_ts + HOUR, received_ts=open_ts + HOUR,
                                                        source_type=NATIVE))
    finally:
        conn.close()


def test_run_backtest_h1_native_appends_one_record(tmp_path, monkeypatch):
    db_path = tmp_path / "t.sqlite3"
    log_path = tmp_path / "log.jsonl"
    _seed_db(db_path, n_bars=400, with_fee=True)

    monkeypatch.setenv("POLYPERPS_DB_PATH", str(db_path))
    monkeypatch.setenv("POLYPERPS_INSTRUMENT_IDS", "6")
    monkeypatch.setattr(sys, "argv", [
        "run_backtest.py",
        "--hypothesis", "h1",
        "--instrument", "6",
        "--source", "native",
        "--fee-category", "equity",
        "--log-path", str(log_path),
    ])

    module = _load_script()
    module.main()

    records = read_records(path=log_path)
    assert len(records) == 1
    record = records[0]
    assert record["hypothesis"] == "h1"
    assert record["fee_category_used"] == "equity"
    assert record["passed"] is False  # 400 native hours is far below the 60-day/1000-period bar

    holdout = record["holdout"]
    assert "sharpe" in holdout
    assert "fills_at_hourly_open" in holdout
    # no book snapshots seeded -> every bar of the holdout input (warm-up tail + holdout) is constant
    assert holdout["bars_constant_spread"] == holdout["n"] + 1 + record["params_chosen"]["lookback"]
    assert holdout["fills_at_hourly_open"] == holdout["fills"]  # no 1m candles seeded
    assert holdout["ci_lo"] is not None and holdout["ci_hi"] is not None  # >=48 holdout returns


def test_run_backtest_no_fee_row_exits(tmp_path, monkeypatch):
    db_path = tmp_path / "t.sqlite3"
    log_path = tmp_path / "log.jsonl"
    _seed_db(db_path, n_bars=0, with_fee=False)

    monkeypatch.setenv("POLYPERPS_DB_PATH", str(db_path))
    monkeypatch.setenv("POLYPERPS_INSTRUMENT_IDS", "6")
    monkeypatch.setattr(sys, "argv", [
        "run_backtest.py",
        "--hypothesis", "h1",
        "--instrument", "6",
        "--source", "native",
        "--fee-category", "equity",
        "--log-path", str(log_path),
    ])

    module = _load_script()
    with pytest.raises(SystemExit) as exc_info:
        module.main()
    assert "no fee row" in str(exc_info.value)


def _run(monkeypatch, db_path, log_path, *extra):
    monkeypatch.setenv("POLYPERPS_DB_PATH", str(db_path))
    monkeypatch.setenv("POLYPERPS_INSTRUMENT_IDS", "6")
    monkeypatch.setattr(sys, "argv", [
        "run_backtest.py", "--hypothesis", "h1", "--instrument", "6", "--source", "native",
        "--fee-category", "equity", "--log-path", str(log_path), *extra,
    ])
    _load_script().main()


def test_same_end_reproduces_holdout_and_dataset_and_trims_trailing_incomplete(tmp_path, monkeypatch):
    db_path = tmp_path / "t.sqlite3"
    log_path = tmp_path / "log.jsonl"
    last_open = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    _seed_db(db_path, n_bars=400, with_fee=True, last_open=last_open)
    end = last_open + 5 * HOUR  # four empty hours after the last candle

    _run(monkeypatch, db_path, log_path, "--end", end.isoformat())
    _run(monkeypatch, db_path, log_path, "--end", "2026-08-01T17:00:00Z")  # same instant, other spelling

    a, b = read_records(path=log_path)
    assert a["run_id"] != b["run_id"]
    assert a["end"] == b["end"] == end.isoformat()
    assert a["holdout"] == b["holdout"]
    assert a["dataset"] == b["dataset"]
    # trailing incomplete hours are trimmed: the tested span ends at the close of the last candle
    assert a["dataset"]["tested_end"] == (last_open + HOUR).isoformat()
    assert a["dataset"]["end"] == last_open.isoformat()
    assert a["dataset"]["bars"] == a["dataset"]["complete_bars"] == 400
    assert a["dataset"]["tested_days"] == "16.67"  # 400 h / 24, quantised to 0.01
    assert a["dataset"]["holdout_bars"] == 120
