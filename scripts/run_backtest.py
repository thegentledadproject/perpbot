"""Spec 1.2: run one hypothesis on one instrument/source, log the result.

    POLYPERPS_INSTRUMENT_IDS=6 .venv/Scripts/python scripts/run_backtest.py \
        --hypothesis h1 --instrument 6 --source hyperliquid [--fee-category crypto] [--seed 42]

Grid points are evaluated on the chronological train slice; the best by train
Sharpe runs ONCE on the holdout; the holdout gets a block-bootstrap CI. One JSON
record is appended to polyperps/signal/validation_log.jsonl whether the run
screened, passed, or failed. Refuses to run without a stored fee row.
h2 needs BOTH native and hyperliquid data loaded (it trades the native leg).

Controller ruling (Task 10): the live fee schedule publishes only one category,
`equity` (taker 0.0004), while instruments 6/7 are `crypto`. --fee-category
defaults to "crypto" and is recorded verbatim in the run log as
`fee_category_used` -- there is no silent substitution to `equity`.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from polyperps.backtest.bars import Bar, build_bars, load_minute_closes
from polyperps.backtest.harness import run_backtest
from polyperps.backtest.stats import (
    block_bootstrap_ci, chronological_split, hit_rate, max_drawdown, sharpe, turnover,
)
from polyperps.config import load_settings
from polyperps.exchange.types import SourceType
from polyperps.signal.sufficiency import BAR, check_dataset
from polyperps.signal.validation_log import LOG_PATH, append_record, evaluate_run, make_run_id
from polyperps.storage import db
from polyperps.strategies import GRIDS, build_strategy

_SOURCES = {"native": SourceType.POLYMARKET_REST, "hyperliquid": SourceType.PROXY_HYPERLIQUID}
_EPOCH = datetime(2000, 1, 1, tzinfo=timezone.utc)


def _stats(res, *, bootstrap: bool, seed: int) -> dict:
    out = {
        "sharpe": sharpe(res.returns),
        "max_dd": str(max_drawdown([e for _, e in res.equity])),
        "hit_rate": hit_rate(res.trade_pnls),
        "turnover": str(turnover(res.fill_notionals, notional=BAR.notional_usd)),
        "n": len(res.returns),
        "fills": res.fills,
        "fills_unavailable": res.fills_unavailable,
        "fills_at_hourly_open": res.fills_at_hourly_open,
        "bars_constant_spread": res.bars_constant_spread,
        "final_equity": str(res.equity[-1][1]) if res.equity else "0",
    }
    if bootstrap:
        try:
            lo, hi = block_bootstrap_ci(res.returns, block_len=BAR.block_len, resamples=BAR.resamples,
                                        ci=float(BAR.bootstrap_ci), seed=seed)
            out["ci_lo"], out["ci_hi"] = lo, hi
        except ValueError as exc:
            out["ci_lo"], out["ci_hi"] = None, None
            out["ci_note"] = str(exc)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hypothesis", choices=sorted(GRIDS), required=True)
    ap.add_argument("--instrument", type=int, required=True)
    ap.add_argument("--source", choices=sorted(_SOURCES), required=True)
    ap.add_argument("--fee-category", default="crypto")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--log-path", type=Path, default=LOG_PATH)
    args = ap.parse_args()

    settings = load_settings()
    conn = db.connect(settings.db_path)
    now = datetime.now(timezone.utc)
    source = _SOURCES[args.source]
    try:
        fee = db.latest_fee(conn, args.fee_category)
        if fee is None:
            raise SystemExit(
                f"no fee row for category {args.fee_category!r}; run scripts/store_fees.py, "
                "or pass --fee-category equity to use the published schedule explicitly "
                "(recorded in the run log)"
            )

        bars = build_bars(conn, args.instrument, source, start=_EPOCH, end=now)
        if not bars:
            raise SystemExit(f"no bars for {args.instrument}/{source.value}")
        first = _first_complete(bars)
        bars = [b for b in bars if b.open_ts >= first]
        if len(bars) < 4 * BAR.block_len:
            raise SystemExit(f"only {len(bars)} bars for {args.instrument}/{source.value}; nothing to test")
        minute_closes = load_minute_closes(conn, args.instrument, source, start=bars[0].open_ts, end=now)

        proxy_closes = None
        if args.hypothesis == "h2":
            if source is not SourceType.POLYMARKET_REST:
                raise SystemExit("h2 trades the native leg: use --source native")
            proxy_bars = build_bars(conn, args.instrument, SourceType.PROXY_HYPERLIQUID,
                                    start=bars[0].open_ts, end=now)
            proxy_closes = {b.open_ts: b.close for b in proxy_bars if b.complete and b.close is not None}

        train, holdout = chronological_split(bars, holdout_fraction=BAR.holdout_fraction)

        trials = []
        for params in GRIDS[args.hypothesis]:
            strat = build_strategy(args.hypothesis, params, proxy_close_by_hour=proxy_closes)
            res = run_backtest(train, strat, minute_closes=minute_closes, taker_fee_rate=fee.taker_fee_rate,
                               warmup=strat.warmup)
            trials.append((params, _stats(res, bootstrap=False, seed=args.seed)))
        best_params, best_train = max(trials, key=lambda t: t[1]["sharpe"])

        strat = build_strategy(args.hypothesis, best_params, proxy_close_by_hour=proxy_closes)
        # holdout run is seeded with the tail of train so warm-up does not eat the holdout
        hold_input = train[-strat.warmup:] + holdout if strat.warmup else holdout
        hres = run_backtest(hold_input, strat, minute_closes=minute_closes, taker_fee_rate=fee.taker_fee_rate,
                            warmup=strat.warmup)
        hstats = _stats(hres, bootstrap=True, seed=args.seed)

        suff = check_dataset(conn, args.instrument, source, now=now)
        screened, passed = evaluate_run(source_type=source, sufficiency=suff,
                                        holdout_sharpe=hstats["sharpe"], ci_lo=hstats["ci_lo"], ci_hi=hstats["ci_hi"])
        record = {
            "run_id": make_run_id(now, args.hypothesis, args.instrument, source),
            "ts": now.isoformat(),
            "hypothesis": args.hypothesis,
            "instrument_id": args.instrument,
            "source_type": source.value,
            "params_chosen": best_params,
            "grid_tried": [p for p, _ in trials],
            "dataset": {"start": bars[0].open_ts.isoformat(), "end": bars[-1].open_ts.isoformat(),
                        "bars": len(bars), "complete_bars": sum(b.complete for b in bars),
                        "funding_periods": suff.funding_periods, "days": str(suff.days)},
            "fee_used": str(fee.taker_fee_rate), "fee_category_used": args.fee_category,
            "fee_fetched_at": fee.fetched_at.isoformat(),
            "latency_s": BAR.latency_s, "impact_bps": str(BAR.impact_bps), "seed": args.seed,
            "train": best_train, "holdout": hstats,
            "sufficiency": {"met": suff.met, "shortfall": suff.shortfall},
            "screened": screened, "passed": passed,
        }
        append_record(record, path=args.log_path)
        ci_lo, ci_hi = hstats["ci_lo"], hstats["ci_hi"]
        ci_str = f"({ci_lo:.5f},{ci_hi:.5f})" if ci_lo is not None and ci_hi is not None else "(n/a)"
        print(f"{record['run_id']}: params={best_params} holdout_sharpe={hstats['sharpe']:.2f} "
              f"ci={ci_str} screened={screened} passed={passed}")
        if suff.shortfall:
            print(f"  sufficiency shortfall: {suff.shortfall}")
    finally:
        conn.close()


def _first_complete(bars: list[Bar]) -> datetime:
    for b in bars:
        if b.complete:
            return b.open_ts
    return bars[-1].open_ts


if __name__ == "__main__":
    main()
