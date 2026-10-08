"""Routes, read-only DB access, and error mapping of the dashboard server."""
from __future__ import annotations

import http.client
import json
import sqlite3
import threading
import urllib.error
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from polyperps.dashboard.server import DashboardServer
from polyperps.storage.db import connect

T0 = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    p = tmp_path / "polyperps.sqlite3"
    connect(p).close()          # creates the schema
    return p


@pytest.fixture
def server(db_path: Path):
    srv = DashboardServer(
        bind=("127.0.0.1", 0), db_path=db_path, run_id="paper-test", instrument_ids=(6, 7),
        hypothesis="h1", instruments_provider=lambda: None, host="testbox", clock=lambda: T0,
    )
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield srv
    srv.shutdown()
    srv.server_close()


def get(server: DashboardServer, path: str):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{server.port}{path}", timeout=5) as r:
            return r.status, r.headers, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.headers, e.read()


def test_index_is_html_that_polls_the_api(server):
    status, headers, body = get(server, "/")
    assert status == 200
    assert headers["Content-Type"].startswith("text/html")
    assert b"/api/state" in body


def test_state_is_json_with_top_level_keys(server):
    status, headers, body = get(server, "/api/state")
    assert status == 200
    assert headers["Content-Type"].startswith("application/json")
    assert headers["Cache-Control"] == "no-store"
    s = json.loads(body)
    assert set(s) == {"generated_at", "run", "account", "positions", "guards", "feed",
                      "decisions", "alerts", "locks", "road", "runs"}
    assert s["runs"] == [{"run_id": "paper-test", "hypothesis": "h1"}]
    assert s["run"]["host"] == "testbox" and s["generated_at"] == T0.isoformat()
    assert "Python" not in headers["Server"]


def test_unknown_path_is_404_json(server):
    status, _, body = get(server, "/nope")
    assert status == 404 and json.loads(body) == {"error": "not_found"}


def test_post_is_405(server):
    req = urllib.request.Request(f"http://127.0.0.1:{server.port}/api/state", data=b"x", method="POST")
    with pytest.raises(urllib.error.HTTPError) as ei:
        urllib.request.urlopen(req, timeout=5)
    assert ei.value.code == 405


def test_post_with_non_numeric_content_length_is_405(server):
    # urllib recomputes Content-Length from the body, so a malformed header needs
    # http.client directly to reproduce the int(...) crash this guards against.
    conn = http.client.HTTPConnection("127.0.0.1", server.port, timeout=5)
    try:
        conn.putrequest("POST", "/api/state", skip_host=True)
        conn.putheader("Content-Length", "abc")
        conn.endheaders()
        resp = conn.getresponse()
        assert resp.status == 405
        resp.read()
    finally:
        conn.close()


def test_missing_db_is_503(server, db_path: Path):
    db_path.unlink()
    status, _, body = get(server, "/api/state")
    assert status == 503 and json.loads(body) == {"error": "db_unavailable"}


def test_unserializable_state_is_500_and_recovers(server, monkeypatch):
    # A stray Decimal (or NaN/enum) escaping json.dumps must not leak a raw traceback to
    # the client -- it should be caught and mapped to the same 500 as any other state failure,
    # and the failure must not wedge the server for the next request.
    monkeypatch.setattr(server, "state", lambda: {"x": Decimal("1")})
    status, _, body = get(server, "/api/state")
    assert status == 500 and json.loads(body) == {"error": "internal"}

    monkeypatch.undo()
    status, _, body = get(server, "/api/state")
    assert status == 200
    assert set(json.loads(body)) == {"generated_at", "run", "account", "positions", "guards",
                                      "feed", "decisions", "alerts", "locks", "road", "runs"}


def test_connection_is_read_only(db_path: Path):
    conn = DashboardServer.open_readonly(db_path)
    with pytest.raises(sqlite3.OperationalError):
        conn.execute("INSERT INTO recovery VALUES ('x', 'y', '{}')")
    conn.close()


def test_index_has_the_panels(server):
    _, _, body = get(server, "/")
    text = body.decode()
    for marker in ("id=\"positions\"", "id=\"trail\"", "id=\"guards\"", "id=\"feed\"",
                   "id=\"locks\"", "id=\"road\"", "id=\"stale\"", "setInterval"):
        assert marker in text


def _snapshot(db_path: Path, run_id: str) -> None:
    from polyperps.execution.types import AccountSnapshot
    from polyperps.storage.db import save_account_snapshot
    conn = connect(db_path)
    save_account_snapshot(conn, run_id, AccountSnapshot(equity=Decimal("1000"), positions=(), open_orders=(),
                                                        stops={}, in_liquidation=False, ts=T0),
                          start_equity=Decimal("1000"), executor="sim")
    conn.close()


def test_sibling_runs_are_listed_and_selectable(server, db_path: Path):
    for run_id in ("paper-test", "paper-test-h5", "paper-test-h3", "paper-other-h3", "paper-testing"):
        _snapshot(db_path, run_id)
    s = json.loads(get(server, "/api/state")[2])
    assert [r["run_id"] for r in s["runs"]] == ["paper-test", "paper-test-h3", "paper-test-h5"]
    assert s["run"]["run_id"] == "paper-test" and s["run"]["hypothesis"] == "h1"
    status, _, body = get(server, "/api/state?run=paper-test-h3")
    assert status == 200
    s = json.loads(body)
    assert s["run"]["run_id"] == "paper-test-h3" and s["run"]["hypothesis"] == "h3"


@pytest.mark.parametrize("run", ["paper-other-h3", "nope", "paper-testing"])
def test_unknown_run_is_404(server, db_path: Path, run: str):
    _snapshot(db_path, run)
    status, _, body = get(server, f"/api/state?run={run}")
    assert status == 404 and json.loads(body) == {"error": "unknown_run"}
