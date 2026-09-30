"""Manual live smoke test (Phase 2b Part A spec §7). USER-RUN ONLY, in Phase 3. PLACES REAL ORDERS.

    POLYPERPS_ALLOW_ENV_SECRETS=1 POLYMARKET_PRIVATE_KEY=0x... \
        .venv/Scripts/python scripts/live_smoke.py --instrument 6 --quantity <venue minimum>

What it proves is exchange behaviour, not our code: it calls the SDK perps session directly for
its two writes (a minimum-size IOC market order, then the position stop 1 % away) and reads
positions and fills through LiveReader. It never goes through LiveExecutor (whose gate stays
closed until a signal validates) or the router. It refuses to start unless you type the
instrument id and I ACCEPT REAL ORDERS. Never run by a unit, a timer or a test.

Report: whether the stop fill came from the exchange (a fill with no client id, reported as
venue-<order id>), and the last cumulative funding seen with its sign - hold across a funding
settlement (the top of the hour) before the stop fills if you want the sign check.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import sys
import time
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from polyperps.execution.live_executor import LiveReader, open_session
from polyperps.execution.types import FillUpdate

CONFIRM = "I ACCEPT REAL ORDERS"
STOP_DISTANCE = Decimal("0.01")
POLL_S = 5.0
STOP_WAIT_S = 600.0   # how long to wait for the exchange stop to fill before telling the operator to close by hand


def confirmed(instrument_id: int, ask: Callable[[str], str]) -> bool:
    try:
        if ask(f"Type the instrument id ({instrument_id}) to trade for real: ").strip() != str(instrument_id):
            return False
        return ask(f"Type exactly '{CONFIRM}': ").strip() == CONFIRM
    except EOFError:
        return False


def _no_stop_warning(instrument_id: int) -> str:
    return (f"A REAL POSITION MAY BE OPEN on instrument {instrument_id} with NO stop. "
            "Close it now in the Polymarket app or with a reduce-only order.")


async def smoke(session: Any, *, instrument_id: int, quantity: Decimal, side: str,
                ask: Callable[[str], str], say: Callable[[str], None], poll_s: float = POLL_S) -> dict:
    from polymarket.models.perps.requests import PerpsPositionTpSlTrigger

    reader = LiveReader(session)
    fills: list[FillUpdate] = []

    async def collect() -> None:
        async for ev in reader.events():
            if isinstance(ev, FillUpdate) and ev.instrument_id == instrument_id:
                fills.append(ev)

    collector = asyncio.ensure_future(collect())
    try:
        try:
            cid = f"smoke-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
            placed = await session.place_order(instrument_id=instrument_id, side=side.upper(), quantity=quantity,
                                               time_in_force="ioc", reduce_only=False, client_order_id=cid)
        except BaseException:
            say("the entry order MAY HAVE FILLED on instrument " + str(instrument_id) +
                ". Check the Polymarket app and close any position by hand.")
            raise
        try:
            say(f"order {cid}: status={getattr(getattr(placed, 'order', None), 'status', None)}")
            pos = (await reader.snapshot()).position(instrument_id)
            if pos is None or pos.size == 0:
                say("no position after the order; nothing to protect. Stopping.")
                return {"opened": False}
            factor = 1 - STOP_DISTANCE if pos.size > 0 else 1 + STOP_DISTANCE
            trigger = (pos.entry_price * factor).quantize(Decimal("0.01"))
            await session.place_position_tp_sl(instrument_id=instrument_id,
                                               stop_loss=PerpsPositionTpSlTrigger(trigger_price=trigger))
            say(f"position {pos.size} @ {pos.entry_price}; exchange stop at {trigger}")
            ask("kill the bot now: stop every polyperps service so nothing but the exchange-side stop can "
                "close this position, then press Enter to start watching for the stop fill")
            funding = pos.cumulative_funding
            deadline = time.monotonic() + STOP_WAIT_S
            while True:
                p = (await reader.snapshot()).position(instrument_id)
                if p is None or p.size == 0:
                    break
                funding = p.cumulative_funding
                say(f"still open: size={p.size} cumulative_funding={p.cumulative_funding}")
                if time.monotonic() >= deadline:
                    say(f"the stop did not fill within {STOP_WAIT_S:g}s. " + _no_stop_warning(instrument_id))
                    return {"opened": True, "stop_filled_by_exchange": False, "timed_out": True}
                await asyncio.sleep(poll_s)
            for _ in range(3):   # the stop's fill can reach the event stream just after the position reads flat
                if any(f.client_order_id.startswith("venue-") for f in fills):
                    break
                await asyncio.sleep(poll_s)
            venue = [f for f in fills if f.client_order_id.startswith("venue-")]
            sign = "negative (paid, as PositionView expects)" if funding < 0 else (
                "positive (received)" if funding > 0 else "zero (no settlement while open)")
            report = {"opened": True, "stop_trigger": str(trigger), "stop_filled_by_exchange": bool(venue),
                      "exit_fills": [(f.side, str(f.quantity), str(f.price)) for f in venue],
                      "cumulative_funding": str(funding), "funding_sign": sign}
            say(f"report: {report}")
            return report
        except BaseException:
            say(_no_stop_warning(instrument_id))
            raise
    finally:
        collector.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await collector


async def _run(args, ask, say, open_session_fn, poll_s: float) -> int:
    sdk, session = await open_session_fn("polyperps-smoke")
    try:
        report = await smoke(session, instrument_id=args.instrument, quantity=args.quantity, side=args.side,
                             ask=ask, say=say, poll_s=poll_s)
    finally:
        with contextlib.suppress(Exception):
            await session.close()
        with contextlib.suppress(Exception):
            await sdk.close()
    return 0 if report.get("stop_filled_by_exchange") else 1


def _positive(text: str) -> Decimal:
    value = Decimal(text)
    if not value.is_finite() or value <= 0:
        raise argparse.ArgumentTypeError("must be a finite number > 0")
    return value


def main(argv: list[str] | None = None, *, ask: Callable[[str], str] = input, say: Callable[[str], None] = print,
         open_session: Callable = open_session, poll_s: float = POLL_S) -> int:
    ap = argparse.ArgumentParser(description="Manual live smoke test. PLACES REAL ORDERS.")
    ap.add_argument("--instrument", type=int, required=True)
    ap.add_argument("--quantity", type=_positive, required=True, help="the venue's minimum order size")
    ap.add_argument("--side", choices=["buy", "sell"], default="buy")
    args = ap.parse_args(argv)
    if not confirmed(args.instrument, ask):
        say("refused: confirmation not typed exactly; nothing was sent")
        return 2
    return asyncio.run(_run(args, ask, say, open_session, poll_s))


if __name__ == "__main__":
    sys.exit(main())
