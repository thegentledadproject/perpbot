"""Part A §6.5: one fixed bar sequence through the backtest harness and through Portfolio +
SimExecutor gives identical trades (time, side, quantity) and the same P&L to the cent."""

from datetime import datetime, timedelta, timezone
from decimal import ROUND_DOWN, Decimal

from polyperps.backtest.bars import Bar
from polyperps.backtest.harness import run_backtest
from polyperps.exchange.types import SourceType
from polyperps.execution.order_router import InstrumentRouter, Portfolio
from polyperps.execution.sim_executor import SimExecutor
from polyperps.execution.types import FillUpdate
from polyperps.monitor.alerts import Alerter, SqliteSink
from polyperps.storage.db import connect

T0 = datetime(2026, 9, 20, 0, 0, tzinfo=timezone.utc)
H = timedelta(hours=1)
FEE = Decimal("0.0004")
Q = Decimal("0.00000001")

# open, high, low, close, funding, complete, target decided at this bar's close
ROWS = [
    ("100", "100", "100", "100", "0", True, 1),        # 0  enter long
    ("100", "102", "100", "102", "0", True, 1),        # 1  hold
    ("102", "104", "102", "104", "0", True, -1),       # 2  flip: exit only
    ("104", "104", "104", "104", "0", True, -1),       # 3  re-enter short
    ("104", "121", "104", "118", "0", True, 1),        # 4  short stop 119.60 hit intrabar; enter long at close
    ("118", "118", "118", "118", "0.012", True, 1),    # 5  long pays 1.2
    ("118", "118", "118", "118", "0.012", True, 0),    # 6  paid 2.4 >= 2 % of notional: funding exit
    ("118", "118", "118", "118", "0", True, 1),        # 7  enter long
    ("118", "120", "118", "120", "0", False, 1),       # 8  incomplete bar: exit data_gap, no entry
    ("120", "120", "120", "120", "0", True, 0),        # 9  stay flat
    ("120", "120", "120", "120", "0", True, 0),        # 10 last bar: only a fill price
]


class Script:
    name = "script"
    params = {}

    def target(self, history):
        return Decimal(ROWS[len(history) - 1][6])

    def on_flatten(self):
        pass


def bars():
    return [Bar(instrument_id=6, source_type=SourceType.POLYMARKET_WS, open_ts=T0 + i * H, open=Decimal(o),
                high=Decimal(h), low=Decimal(lo), close=Decimal(c), index_close=None, funding_rate=Decimal(f),
                spread_bps=Decimal(5), spread_source="constant", complete=complete)
            for i, (o, h, lo, c, f, complete, _) in enumerate(ROWS)]


async def run_router(bs):
    """The runner's order of events, one bar at a time: intrabar extreme (stop check), then at the
    close: mark, funding hook, fast loop (guards), bar decision."""
    now = [T0]
    conn = connect(":memory:")
    ex = SimExecutor("p", equity=Decimal(1000), taker_fee_rate=FEE, spread_bps=Decimal(5), impact_bps=Decimal(5),
                     clock=lambda: now[0])
    alerter = Alerter("p", [SqliteSink(conn)])
    router = InstrumentRouter(run_id="p", instrument_id=6, category="crypto", strategy=Script(), executor=ex,
                              conn=conn, alerter=alerter, categories={6: "crypto"}, clock=lambda: now[0])
    pf = Portfolio(run_id="p", executor=ex, conn=conn, alerter=alerter, routers={6: router})
    fills: list[FillUpdate] = []

    async def deliver(evs):
        for ev in evs:
            if isinstance(ev, FillUpdate):
                fills.append(ev)
            await pf.dispatch(ev)

    for t in range(len(bs) - 1):
        bar, nxt = bs[t], bs[t + 1]
        if router.size != 0:                          # intrabar: the extreme on the stop's side
            now[0] = bar.open_ts
            ex.update_mark(6, bar.low if router.size > 0 else bar.high)
            await deliver(ex.poll_fills())
        now[0] = nxt.open_ts                           # the bar closes on the next hour's first tick
        ex.update_mark(6, bar.close)
        ex.on_bar(bar)
        await pf.on_fast({6: bar.close})
        await deliver(ex.drain_events())
        await pf.on_bar({6: bs[: t + 1]}, "run")
        await deliver(ex.drain_events())
    return [(f.ts, f.side, f.quantity) for f in fills], (await ex.snapshot()).equity - Decimal(1000)


def q(price):
    return (Decimal(100) / Decimal(price)).quantize(Q, rounding=ROUND_DOWN)


async def test_backtest_and_router_trade_identically():
    bs = bars()
    res = run_backtest(bs, Script(), minute_closes={}, taker_fee_rate=FEE, warmup=0)
    r_trades, r_pnl = await run_router(bs)
    assert res.trades == [
        (T0 + 1 * H, "buy", q(100)),     # enter long
        (T0 + 3 * H, "sell", q(100)),    # flip: exit...
        (T0 + 4 * H, "sell", q(104)),    # ...re-enter short next bar
        (T0 + 4 * H, "buy", q(104)),     # stop at 119.60, intrabar
        (T0 + 5 * H, "buy", q(118)),     # enter long
        (T0 + 7 * H, "sell", q(118)),    # funding-cost exit
        (T0 + 8 * H, "buy", q(118)),     # enter long
        (T0 + 9 * H, "sell", q(118)),    # data_gap exit
    ]
    assert r_trades == res.trades
    assert abs(r_pnl - res.equity[-1][1]) < Decimal("0.01")
