"""Phase 2a paper run: the exact router/guards/reconciliation path, SimExecutor last mile.

    POLYPERPS_INSTRUMENT_IDS=6,7 .venv/Scripts/python scripts/run_paper.py --executor sim --hypothesis h1

`--executor live` is refused in Phase 2a (exit 2) before anything is constructed.
Needs a network path where Polymarket resolves (public WS only; no credentials).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import sys
from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from polyperps.backtest.bars import build_bars, floor_hour
from polyperps.config import load_settings
from polyperps.data_ingest.market_feed import MarketFeed
from polyperps.exchange.client import PolymarketPerpsClient
from polyperps.exchange.types import SourceType
from polyperps.execution.live_bars import LiveBarBuilder
from polyperps.execution.order_router import InstrumentRouter, Portfolio
from polyperps.execution.sim_executor import SimExecutor
from polyperps.execution.state_recovery import recover
from polyperps.gates import ExecutionMode, live_orders_allowed
from polyperps.monitor.alerts import Alerter, LogSink, SqliteSink, TelegramSink
from polyperps.risk.kill_switch import evaluate as kill_evaluate
from polyperps.security.key_management import SecretUnavailable, load_secret
from polyperps.signal.sufficiency import BAR
from polyperps.signal.validation_log import read_records
from polyperps.storage import db
from polyperps.strategies import GRIDS, build_strategy

log = logging.getLogger("polyperps.paper")
HEARTBEAT_S = 20
RECONCILE_S = 60
INITIAL_BACKOFF_S = 1.0
MAX_BACKOFF_S = 60.0
_HOUR = timedelta(hours=1)
_SEED_WINDOW_FACTOR = 2   # scan 2x the wanted hours so gaps in stored candles still yield N complete bars


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--executor", choices=["sim", "live"], required=True)
    ap.add_argument("--hypothesis", choices=sorted(GRIDS), required=True)
    ap.add_argument("--params-from", default=None, help="run_id in validation_log.jsonl")
    ap.add_argument("--grid-index", type=int, default=0)
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--equity", default="1000")
    ap.add_argument("--fee-category", default="equity")
    ap.add_argument("--clear-halt", type=int, default=None)
    return ap


def _params(args) -> dict:
    if args.params_from:
        for r in read_records():
            if r["run_id"] == args.params_from:
                out = {}
                for k, v in r["params_chosen"].items():
                    out[k] = Decimal(v) if isinstance(v, str) and "." in v else int(v)
                return out
        raise SystemExit(f"run_id {args.params_from} not found in validation log")
    return GRIDS[args.hypothesis][args.grid_index]


def _alerter(run_id: str, conn) -> Alerter:
    sinks = [LogSink(), SqliteSink(conn)]
    try:
        sinks.append(TelegramSink(token=load_secret("TELEGRAM_BOT_TOKEN"), chat_id=load_secret("TELEGRAM_CHAT_ID")))
    except SecretUnavailable:
        log.info("telegram sink not configured")
    return Alerter(run_id, sinks)


def seed_history(conn, builder: LiveBarBuilder, wanted: Mapping[int, int], *, now: datetime,
                 source_type: SourceType = SourceType.POLYMARKET_REST) -> dict[int, int]:
    """C3: pay the strategy's warm-up from stored 1h candles (Phase 1 build_bars) instead of
    waiting `lookback` live hours. Per instrument, the last `wanted[iid]` COMPLETE bars strictly
    before the current hour are appended to the builder; spread is normalised to the constant
    proxy the live builder stamps. Zero bars is fine - the router's warmup gate covers it."""
    end = floor_hour(now)
    seeded: dict[int, int] = {}
    for iid, n in wanted.items():
        bars = []
        if n > 0:
            raw = build_bars(conn, iid, source_type, start=end - n * _SEED_WINDOW_FACTOR * _HOUR, end=end)
            bars = [replace(b, spread_bps=BAR.proxy_spread_bps, spread_source="constant")
                    for b in raw if b.complete][-n:]
            builder.seed(iid, bars)
        seeded[iid] = len(bars)
        log.info("seeded %d bars for instrument %d", len(bars), iid)
    return seeded


async def run_once(args, settings) -> None:
    if args.hypothesis == "h2":
        raise SystemExit("h2 needs a live proxy feed; not wired in Phase 2a")
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = db.connect(settings.db_path)
    run_id = args.run_id   # minted once in main(); a supervised restart must reopen the same paper account
    alerter = _alerter(run_id, conn)
    fee = db.latest_fee(conn, args.fee_category)
    if fee is None:
        raise SystemExit(f"no fee row for {args.fee_category!r}; run scripts/store_fees.py")
    saved = db.load_sim_account(conn, run_id)

    def persist(text: str) -> None:
        db.save_sim_account(conn, run_id, text)

    executor = (SimExecutor.from_json(run_id, saved, taker_fee_rate=fee.taker_fee_rate, persist=persist) if saved
                else SimExecutor(run_id, equity=Decimal(args.equity), taker_fee_rate=fee.taker_fee_rate, persist=persist))

    client = PolymarketPerpsClient.create_public(rate_per_sec=settings.rest_rate_per_sec, burst=settings.rest_burst)
    instruments = {i.instrument_id: i for i in await client.fetch_instruments()}
    unknown = [i for i in settings.instrument_ids if i not in instruments]
    if unknown:
        raise SystemExit(f"unknown instrument ids {unknown}")
    categories = {i: instruments[i].category for i in settings.instrument_ids}
    params = _params(args)
    routers = {
        iid: InstrumentRouter(run_id=run_id, instrument_id=iid, category=categories[iid],
                              strategy=build_strategy(args.hypothesis, params),
                              executor=executor, conn=conn, alerter=alerter, categories=categories)
        for iid in settings.instrument_ids
    }
    pf = Portfolio(run_id=run_id, executor=executor, conn=conn, alerter=alerter, routers=routers)
    rep = await recover(conn=conn, run_id=run_id, executor=executor, routers=routers, alerter=alerter)
    log.info("recovery: %s", rep.to_dict())
    if args.clear_halt is not None and args.clear_halt in routers:
        routers[args.clear_halt].clear_halt()
        log.warning("cleared HALT on %s by operator request", args.clear_halt)

    builder = LiveBarBuilder()
    warm = max(int(getattr(routers[i].strategy, "warmup", 0) or 0) for i in settings.instrument_ids) if routers else 0
    seed_history(conn, builder, {i: max(warm, int(params.get("lookback", 0))) for i in settings.instrument_ids},
                 now=datetime.now(timezone.utc))
    marks: dict[int, Decimal] = {}
    closed_bars: asyncio.Queue = asyncio.Queue()
    stop = asyncio.Event()

    def on_accept(tick):
        db.insert_tick(conn, tick)
        marks[tick.instrument_id] = tick.mark_price
        executor.update_mark(tick.instrument_id, tick.mark_price)
        bar = builder.on_tick(tick)
        if bar is not None:
            executor.apply_funding(bar.instrument_id, bar.funding_rate)
            closed_bars.put_nowait(bar)

    ticks = client.stream_ticks(settings.instrument_ids)
    feed = MarketFeed(ticks=ticks, bounds=settings.bounds, on_accept=on_accept)

    async def bar_loop():
        while not stop.is_set():
            bar = await closed_bars.get()
            kill = kill_evaluate(live_sharpe=None, backtest_sharpe=None, mode="paper")
            await pf.on_bar({bar.instrument_id: builder.history(bar.instrument_id)}, kill)

    async def fast_loop():
        while not stop.is_set():
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), HEARTBEAT_S)
            if stop.is_set():
                break
            await executor.heartbeat()
            # check_triggers() mutates the sim account synchronously and RETURNS the stop-fire
            # fills (never queues them). Dispatch them right here, in this task, before on_fast
            # and before reconcile_loop can take the Portfolio lock - otherwise a reconcile could
            # see local OPEN vs remote 0 and halt on a stop that simply hasn't been delivered.
            for fill in executor.check_triggers():
                await pf.dispatch(fill)
            await pf.on_fast(dict(marks))

    async def reconcile_loop():
        while not stop.is_set():
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), RECONCILE_S)
            if stop.is_set():
                break
            await pf.reconcile_now()

    tasks = [asyncio.create_task(t()) for t in (bar_loop, fast_loop, reconcile_loop, pf.run_event_pump)]
    try:
        await feed.run()
    finally:
        stop.set()
        # Deliberately not flushing builder.close_all() here: the currently-open hour is
        # partial, and close_all() stamps whatever it has as complete=True. Dropping it
        # is correct - it picks back up on the next tick after restart.
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        with contextlib.suppress(Exception):
            await ticks.aclose()
        await client.close()
        conn.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = build_parser().parse_args()
    settings = load_settings()
    if args.executor == "live":
        reasons = [live_orders_allowed(i, modes={i: ExecutionMode.AUTO for i in settings.instrument_ids}).reason
                   for i in settings.instrument_ids]
        print(f"--executor live is not available in Phase 2a. Gate says: {reasons}", file=sys.stderr)
        raise SystemExit(2)
    args.run_id = args.run_id or f"paper-{datetime.now(timezone.utc):%Y%m%dT%H%M%S%f}"
    log.info("run_id %s", args.run_id)
    asyncio.run(_supervise(args, settings))


async def _supervise(args, settings) -> None:
    backoff = INITIAL_BACKOFF_S
    while True:
        started = asyncio.get_running_loop().time()
        try:
            await run_once(args, settings)
            log.warning("feed ended; restarting")
        except (KeyboardInterrupt, asyncio.CancelledError, SystemExit):
            raise
        except Exception:
            log.exception("paper run crashed; restarting in %.0fs", backoff)
        ran = asyncio.get_running_loop().time() - started
        backoff = INITIAL_BACKOFF_S if ran > 300 else min(backoff * 2, MAX_BACKOFF_S)
        await asyncio.sleep(backoff)


def _raise_keyboard_interrupt(signum, frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)
    try:
        main()
    except KeyboardInterrupt:
        pass
