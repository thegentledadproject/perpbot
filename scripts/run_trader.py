"""One trader for sim, shadow and live (Phase 2b Part A spec §3): the exact router, guards,
reconciliation and recovery path, with the executor chosen by --executor.

    POLYPERPS_INSTRUMENT_IDS=6,7 .venv/Scripts/python scripts/run_trader.py --executor sim --hypothesis h1

sim     SimExecutor, a paper account kept in the DB. Public WS only; no credentials.
shadow  the real account, read-only: decisions run, every would-be order is recorded as
        shadow_refused and its instrument halts. Needs POLYMARKET_PRIVATE_KEY on the box.
        Refuses to start (RecoveryHalt) while the account holds a position without a stop or
        an open order this run did not place - recovery would have to write to fix those.
live    LiveExecutor. Exits 2 unless all three locks are open (they are not in Part A).

shadow and live need an explicit --run-id and refuse (exit 2) a run_id another executor wrote.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import sys
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from polyperps.backtest.bars import build_bars, floor_hour
from polyperps.config import load_settings
from polyperps.data_ingest.market_feed import MarketFeed
from polyperps.exchange.client import PolymarketPerpsClient
from polyperps.exchange.types import SourceType
from polyperps.execution.executor import Executor, GateClosed
from polyperps.execution.live_bars import LiveBarBuilder
from polyperps.execution.live_executor import LiveExecutor, ShadowExecutor, open_session
from polyperps.execution.order_router import InstrumentRouter, Portfolio
from polyperps.execution.sim_executor import SimExecutor
from polyperps.execution.state_recovery import recover
from polyperps.gates import ExecutionMode, live_orders_allowed
from polyperps.monitor.alerts import Alerter, default_sinks
from polyperps.risk.kill_switch import evaluate as kill_evaluate
from polyperps.signal.sufficiency import BAR
from polyperps.signal.validation_log import read_records
from polyperps.storage import db
from polyperps.strategies import GRIDS, build_strategy

log = logging.getLogger("polyperps.trader")
HEARTBEAT_S = 20
RECONCILE_S = 60
MODES: dict[int, ExecutionMode] = {}   # no per-instrument AUTO store yet: every instrument is MANUAL_REVIEW
_HOUR = timedelta(hours=1)
_SEED_WINDOW_FACTOR = 2   # scan 2x the wanted hours so gaps in stored candles still yield N complete bars


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument("--executor", choices=["sim", "shadow", "live"], required=True)
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
    return Alerter(run_id, default_sinks(conn))


def refuse_foreign_run_id(conn, run_id: str, name: str) -> None:
    """Exit 2 if run_id's rows belong to a different executor than `name`."""
    prev = db.load_account_snapshot(conn, run_id)
    owner = prev[2] if prev is not None else ("sim" if db.load_sim_account(conn, run_id) else None)
    if owner is not None and owner != name:
        # Its rows, baseline and halts belong to another account; adopting them would be wrong.
        print(f"--executor {name} refused: run_id {run_id!r} already belongs to the {owner} executor; "
              f"pick a new --run-id", file=sys.stderr)
        raise SystemExit(2)


async def build_executor(mode: str, *, run_id: str, conn, fee_rate: Decimal, equity: Decimal,
                         instrument_ids: Sequence[int], session: Any = None) -> Executor:
    """sim: the paper account from the DB (or a fresh one). shadow/live: the real account through
    `session`; live raises GateClosed unless all three locks are open for every instrument."""
    refuse_foreign_run_id(conn, run_id, mode)
    if mode == "sim":
        saved = db.load_sim_account(conn, run_id)

        def persist(text: str) -> None:
            db.save_sim_account(conn, run_id, text)

        if saved:
            return SimExecutor.from_json(run_id, saved, taker_fee_rate=fee_rate, persist=persist)
        return SimExecutor(run_id, equity=equity, taker_fee_rate=fee_rate, persist=persist)
    ex = (ShadowExecutor(session) if mode == "shadow"
          else LiveExecutor(session, instrument_ids=instrument_ids, modes=MODES))
    prev = db.load_account_snapshot(conn, run_id)
    # The loss-limit baseline must survive restarts, or a restart after -9 % would reset it.
    if prev is not None:
        ex.start_equity = prev[1]
    else:
        snap = await ex.snapshot()
        ex.start_equity = snap.equity
        # Saved now, not at the first fast-loop tick, so a crash inside HEARTBEAT_S can't re-baseline.
        db.save_account_snapshot(conn, run_id, snap, start_equity=ex.start_equity, executor=ex.name)
    return ex


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


def _recover_strategies(routers: Mapping[int, InstrumentRouter]) -> None:
    """I4: recover() rebuilt each router's position from the exchange; a Phase 1 strategy also
    keeps its own _position, which would otherwise restart at 0 and flatten the book on the
    next bar. Tell it which side it is on (optional hook: skipped when the strategy lacks it)."""
    for router in routers.values():
        if router.size == 0:
            continue
        hook = getattr(router.strategy, "on_recover", None)
        if callable(hook):
            hook(1 if router.size > 0 else -1)


async def run_until_first_exits(*coros) -> None:
    """Run every coroutine; the first to finish, by returning or raising, ends the rest. Its
    exception propagates, so a dead background loop takes the process down (systemd restarts
    it and counts it) instead of the bot running on without that loop."""
    tasks = [asyncio.ensure_future(c) for c in coros]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in done:
            t.result()
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def run_once(args, settings) -> None:
    if args.hypothesis in ("h2", "h4"):
        raise SystemExit(f"{args.hypothesis} needs a live proxy feed; not wired")
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = db.connect(settings.db_path)
    run_id = args.run_id   # minted once in main(); systemd passes --run-id so every restart reopens the same account
    alerter = _alerter(run_id, conn)
    fee = db.latest_fee(conn, args.fee_category)
    if fee is None:
        conn.close()
        raise SystemExit(f"no fee row for {args.fee_category!r}; run scripts/store_fees.py")
    kill_mode = "live" if args.executor == "live" else "paper"   # shadow must reach submit to exercise it

    stop = asyncio.Event()
    client = PolymarketPerpsClient.create_public(rate_per_sec=settings.rest_rate_per_sec, burst=settings.rest_burst)
    sdk = session = ticks = None
    try:
        if args.executor != "sim":
            refuse_foreign_run_id(conn, run_id, args.executor)   # before the wallet key is loaded
            sdk, session = await open_session(f"polyperps-{args.executor}")
        try:
            executor = await build_executor(args.executor, run_id=run_id, conn=conn, fee_rate=fee.taker_fee_rate,
                                            equity=Decimal(args.equity), instrument_ids=settings.instrument_ids,
                                            session=session)
        except GateClosed as exc:
            print(f"--executor live refused: {exc}", file=sys.stderr)
            raise SystemExit(2) from exc

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
        pf = Portfolio(run_id=run_id, executor=executor, conn=conn, alerter=alerter, routers=routers,
                       start_equity=executor.start_equity)
        rep = await recover(conn=conn, run_id=run_id, executor=executor, routers=routers, alerter=alerter)
        log.info("recovery: %s", rep.to_dict())
        _recover_strategies(routers)
        if args.clear_halt is not None and args.clear_halt in routers:
            routers[args.clear_halt].clear_halt()
            log.warning("cleared HALT on %s by operator request", args.clear_halt)

        builder = LiveBarBuilder()
        warm = max(int(getattr(routers[i].strategy, "warmup", 0) or 0) for i in settings.instrument_ids) if routers else 0
        seed_history(conn, builder, {i: max(warm, int(params.get("lookback", 0))) for i in settings.instrument_ids},
                     now=datetime.now(timezone.utc))
        marks: dict[int, Decimal] = {}
        closed_bars: asyncio.Queue = asyncio.Queue()

        def on_accept(tick):
            marks[tick.instrument_id] = tick.mark_price
            executor.on_tick(tick)
            bar = builder.on_tick(tick)
            if bar is not None:
                executor.on_bar(bar)
                closed_bars.put_nowait(bar)

        ticks = client.stream_ticks(settings.instrument_ids)
        feed = MarketFeed(ticks=ticks, bounds=settings.bounds, on_accept=on_accept)

        async def bar_loop():
            while not stop.is_set():
                bar = await closed_bars.get()
                snap = await executor.snapshot()
                kill = kill_evaluate(live_sharpe=None, backtest_sharpe=None, mode=kill_mode,
                                     equity=snap.equity, start_equity=executor.start_equity)
                await pf.on_bar({bar.instrument_id: builder.history(bar.instrument_id)}, kill)

        async def fast_loop():
            while not stop.is_set():
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), HEARTBEAT_S)
                if stop.is_set():
                    break
                await executor.heartbeat()
                # poll_fills() (sim: check_triggers) mutates the sim account synchronously and RETURNS
                # the stop-fire fills. Dispatch them right here, before on_fast and before the
                # reconcile loop can take the Portfolio lock - otherwise a reconcile could see local
                # OPEN vs remote 0 and halt on a stop that simply hasn't been delivered.
                for fill in executor.poll_fills():
                    await pf.dispatch(fill)
                snap = await pf.on_fast(dict(marks))
                db.save_account_snapshot(conn, run_id, snap, start_equity=executor.start_equity,
                                         executor=executor.name)

        async def reconcile_loop():
            while not stop.is_set():
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(stop.wait(), RECONCILE_S)
                if stop.is_set():
                    break
                await pf.reconcile_now()

        await run_until_first_exits(feed.run(), bar_loop(), fast_loop(), reconcile_loop(), pf.run_event_pump())
        log.warning("trader run ended; exiting so systemd restarts it")
    finally:
        stop.set()
        # Deliberately not flushing builder.close_all() here: the currently-open hour is
        # partial, and close_all() stamps whatever it has as complete=True. Dropping it
        # is correct - it picks back up on the next tick after restart.
        if ticks is not None:
            with contextlib.suppress(Exception):
                await ticks.aclose()
        if session is not None:
            with contextlib.suppress(Exception):
                await session.close()
        if sdk is not None:
            with contextlib.suppress(Exception):
                await sdk.close()
        await client.close()
        conn.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = build_parser().parse_args()
    settings = load_settings()
    if args.executor != "sim" and not args.run_id:
        # A minted id would make every systemd restart a fresh account: new baseline, nothing to recover.
        print(f"--executor {args.executor} needs an explicit --run-id", file=sys.stderr)
        raise SystemExit(2)
    if args.executor == "live":
        # Checked before the wallet key is loaded; LiveExecutor's constructor checks again.
        closed = [d.reason for i in settings.instrument_ids
                  if not (d := live_orders_allowed(i, modes=MODES)).allowed]
        if closed:
            print(f"--executor live refused: {closed}", file=sys.stderr)
            raise SystemExit(2)
    args.run_id = args.run_id or f"{args.executor}-{datetime.now(timezone.utc):%Y%m%dT%H%M%S%f}"
    log.info("run_id %s", args.run_id)
    asyncio.run(run_once(args, settings))


def _raise_keyboard_interrupt(signum, frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)
    try:
        main()
    except KeyboardInterrupt:
        pass
