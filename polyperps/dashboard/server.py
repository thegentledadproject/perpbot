"""Stdlib HTTP server for the read-only dashboard.

Two routes: "/" (the page) and "/api/state" (JSON). Each state request opens
its own read-only SQLite connection, so this process can never write the
trading DB and never shares a connection between threads.
"""
from __future__ import annotations

import json
import logging
import socket
import sqlite3
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from pathlib import Path
from typing import Callable, Mapping, Sequence

from polyperps.dashboard.state import InstrumentInfo, build_state

log = logging.getLogger(__name__)


def _load_index() -> bytes:
    return (resources.files("polyperps.dashboard") / "static" / "index.html").read_bytes()


class DashboardServer:
    def __init__(
        self, *, bind: tuple[str, int], db_path: Path, run_id: str,
        instrument_ids: Sequence[int], hypothesis: str,
        instruments_provider: Callable[[], Mapping[int, InstrumentInfo] | None],
        host: str | None = None, clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._db_path = Path(db_path)
        self._run_id = run_id
        self._instrument_ids = tuple(instrument_ids)
        self._hypothesis = hypothesis
        self._instruments = instruments_provider
        self._host = host or socket.gethostname()
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._index = _load_index()
        self._httpd = ThreadingHTTPServer(bind, self._handler_class())
        self._httpd.daemon_threads = True

    @property
    def port(self) -> int:
        return self._httpd.server_address[1]

    def serve_forever(self) -> None:
        self._httpd.serve_forever()

    def shutdown(self) -> None:
        self._httpd.shutdown()

    def server_close(self) -> None:
        self._httpd.server_close()

    @staticmethod
    def open_readonly(db_path: Path) -> sqlite3.Connection:
        uri = "file:" + Path(db_path).resolve().as_posix() + "?mode=ro"
        return sqlite3.connect(uri, uri=True)

    def state(self) -> dict:
        conn = self.open_readonly(self._db_path)
        try:
            return build_state(
                conn, run_id=self._run_id, instrument_ids=self._instrument_ids,
                instruments=self._instruments(), hypothesis=self._hypothesis,
                host=self._host, now=self._clock(),
            )
        finally:
            conn.close()

    def _handler_class(self):
        server = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "polyperps-dashboard"

            def log_message(self, fmt, *args):   # route stdlib access log to logging
                log.debug("%s " + fmt, self.address_string(), *args)

            def _send(self, status: HTTPStatus, body: bytes, ctype: str, *, head: bool = False) -> None:
                self.send_response(status)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                if ctype.startswith("application/json"):
                    self.send_header("Cache-Control", "no-store")
                self.end_headers()
                if not head:
                    self.wfile.write(body)

            def _json(self, status: HTTPStatus, payload: dict, *, head: bool = False) -> None:
                self._send(status, json.dumps(payload).encode(), "application/json; charset=utf-8", head=head)

            def _route(self, *, head: bool) -> None:
                path = self.path.split("?", 1)[0]
                if path == "/":
                    self._send(HTTPStatus.OK, server._index, "text/html; charset=utf-8", head=head)
                elif path == "/api/state":
                    try:
                        payload = server.state()
                    except sqlite3.OperationalError as e:
                        log.warning("dashboard: db unavailable: %s", e)
                        self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "db_unavailable"}, head=head)
                        return
                    except Exception:
                        log.exception("dashboard: state failed")
                        self._json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "internal"}, head=head)
                        return
                    self._json(HTTPStatus.OK, payload, head=head)
                else:
                    self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"}, head=head)

            def do_GET(self) -> None:
                self._route(head=False)

            def do_HEAD(self) -> None:
                self._route(head=True)

            def _reject(self) -> None:
                # Drain any request body the client is still sending before responding.
                # Closing the socket first can race a client write and trigger a Windows
                # WinError 10053 (connection aborted) instead of a clean read of the 405.
                length = int(self.headers.get("Content-Length", 0) or 0)
                if length:
                    self.rfile.read(length)
                self._json(HTTPStatus.METHOD_NOT_ALLOWED, {"error": "method_not_allowed"})

            do_POST = do_PUT = do_DELETE = do_PATCH = _reject

        return Handler
