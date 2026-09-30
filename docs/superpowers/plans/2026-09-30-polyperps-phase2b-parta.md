# Phase 2b Part A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the live order path safe and make backtest, paper and live follow one set of rules, so the code that goes live is the code the paper soak has run.

**Architecture:** One runner (`scripts/run_trader.py --executor sim|shadow|live`) drives every mode through the `Executor` protocol, which gains three hooks (`on_tick`, `on_bar`, `poll_fills`); the live read side becomes `LiveReader`, shared by the gated `LiveExecutor` and a write-refusing `ShadowExecutor`. The router enforces "a venue position always has a venue stop", times out pending orders, adopts the exchange size on submit errors, retries correctly, counts in-flight orders in exposure and obeys a hard loss limit. The backtest harness learns the router's rules (flip = exit then re-enter, guards, intrabar stop, gap exit, one cost model) and is pinned to the router by a golden parity test.

**Tech Stack:** Python 3.11+ stdlib (`asyncio`, `sqlite3`, `decimal`, `json`, `argparse`), `polymarket-client==0.10.0` (already pinned), pytest + pytest-asyncio (`asyncio_mode = "auto"`).

**Spec:** `docs/superpowers/specs/2026-09-30-polyperps-phase2b-parta-design.md`

## Global Constraints

- Router trading rules (entry, exit, flip-as-exit-then-reenter, guards, limits) are unchanged.
- `live` stays behind all three locks (`gates.live_orders_allowed`); nothing in this spec opens one.
- No deploy file ever contains `--executor live` or `POLYMARKET_LIVE_TRADING` (existing test).
- `PAPER_RUN_ID` stays `paper-soak-1`.
- Limits are fixed in code and never self-adjust (roadmap hard boundary).
- No new dependencies. SDK stays `polymarket-client==0.10.0`.
- All new tests use fakes; no network, no real orders.
- Run tests with `.venv/Scripts/python -m pytest` (Windows venv; use the Bash tool with POSIX syntax). Baseline is 381 passed.
- Commit messages end with a blank line, then `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- All deploy files are LF only (no `\r`).

## Review Focus

1. A fill that an adoption already counted arrives late (submit error after the order landed; pending timeout then the delayed fill) and must not double the position. Pinned in Task 6: `test_fill_after_submit_error_is_not_counted_twice`, `test_late_fill_after_pending_timeout_is_not_counted_twice`.
2. A restart after a drawdown must keep the original loss-limit baseline, not take the post-loss equity as the new start. Pinned in Task 4: `test_build_executor_keeps_the_loss_baseline_across_restarts`.
3. The venue answers a same-id retry with `duplicate_order`, and that status also arrives on the event stream; it must count as landed and must not revert a pending router. Pinned in Task 7: `test_duplicate_order_counts_as_landed_not_rejected`.
4. The runner calls `Portfolio.on_bar` with ONE instrument's bar; a loss-limit shutdown must still flatten and halt the routers whose bar did not close. Pinned in Task 3: `test_loss_limit_shutdown_flattens_every_router_and_halts`.
5. Two routers entering in the same `Portfolio.on_bar` call: the second must count the first's just-sent, not-yet-filled entry. Pinned in Task 8: `test_second_router_counts_the_first_routers_in_flight_entry`.

## Decisions

1. SimExecutor fills at the mark (a stop at its trigger) and books the whole cost, from `costs.fill_cost(notional=LIMITS.notional_usd)`, as `FillUpdate.fee`; entry and stop prices lose the old 7.5 bps slippage (Task 10).
2. The harness values a closing leg at its current value (`|delta| x notional x price / entry`) for cost and funding, the amount the venue charges on, so harness and sim feed identical numbers into `fill_cost` (Task 10).
3. Harness gap rule: a bar with `complete=False` exits to flat at its close and never enters (the router's rule); the old look-ahead flatten stays only for a next bar with no price at all (a stored-data hole; live never delivers such a bar) (Task 10).
4. The harness calls `strategy.on_flatten()` after every trade that ends flat, because the router's fill handler does; so after a flip the strategy re-enters only if it still wants the other side (Task 10).
5. Harness exits taken at a bar close (gap, guard) are stamped with the close time (`nxt.open_ts`), the intrabar stop with `bar.open_ts`; `BacktestResult.trades` records `(ts, side, quantity)` (Task 10).
6. Shadow: all four writes raise `ShadowRefused`. Submit → order row `shadow_refused` + CRITICAL `submit_error` + halt; reconciliation catches `ShadowRefused` per mismatch (WARN `shadow_refused`) and keeps running; recovery does not catch it, so shadow refuses to start (RecoveryHalt) while the account holds a position without a stop or an order this run did not place (Tasks 1, 6).
7. Kill-switch mode: sim and shadow evaluate as `"paper"` (shadow must reach `submit` to exercise the order path), live as `"live"` (Task 4).
8. Shadow/live `start_equity` = the `start_equity` of the run's `account_snapshots` row if one exists, else the first snapshot's equity; sim keeps its persisted JSON baseline (Task 4).
9. `kill_switch.evaluate` takes `equity` / `start_equity` as optional keywords (None = loss limit not evaluated) so existing callers stay valid; the runner always passes both and a runner test pins it (Tasks 3, 4).
10. The shutdown alert is emitted once by the Portfolio, kind `loss_limit` when the loss limit is at or below −10 %, else `divergence` (Task 3).
11. Late fills for an order we already gave up on (row status `lost`, `error`, `shadow_refused`, or pending-timeout `adopted`, and not the router's current pending id) re-read the exchange (re-adopt) instead of adding the fill; order updates never rewrite those rows. This reconciles spec §4.4 (apply late fills) with §4.5 (adopt the exchange size) without double counting (Task 6).
12. Recovery treats LIQUIDATED like HALTED (state kept; today a restart silently turns LIQUIDATED into OPEN) and adopts the exchange size and stop into both (Task 5).
13. The stop invariant also covers venue positions on instruments with no router (placed with `LIMITS`), in reconciliation and recovery (Task 5).
14. `OrderUpdate` sets the order row's status only; `filled_quantity` / `avg_price` are owned by fills and accumulate; a fill never downgrades a terminal status (Task 7).
15. `duplicate_order` maps to `None` in `_SDK_ORDER_STATUS` (events skip it) and to an accepted ack with reason `duplicate_order` in `submit` (Task 7).
16. Fills without a client order id get `client_order_id = f"venue-{order_id}"` (Task 7).
17. In-flight exposure counts the non-reduce-only intent of every other router in `ENTRY_PENDING`, recomputed per router inside the same `on_bar` call; an order adopted by recovery (`adopt_pending`) carries no intent and is not counted (`ponytail:` comment) (Task 8).
18. Funding drift runs in every mode; in sim the charged funding equals the bar rate exactly, so it stays quiet (Task 9).
19. `run_trader.MODES = {}`: there is no per-instrument AUTO store, so `--executor live` always exits 2 in Part A; `main()` checks the gate before the wallet key is loaded, and the `LiveExecutor` constructor stays the real lock (Task 4).
20. `live_smoke.py` takes `--quantity` from the operator (the venue's minimum) instead of computing it (Task 12).
21. Screens re-run: exactly the two existing Hyperliquid screens, h1 and h3 on instrument 6 with `--fee-category equity` (Task 11).
22. The dashboard's `run.executor` reads the executor name from `account_snapshots`, `"sim"` until the first row exists (Task 2).
23. A later partial fill of our own order while the router is not FLAT raises no `unexpected_fill` alert (Task 7).

---

### Task 1: Executor hooks, LiveReader, ShadowExecutor

**Files:**
- Modify: `polyperps/execution/executor.py:1-32` (add `ShadowRefused`, hooks, `start_equity`)
- Modify: `polyperps/execution/live_executor.py:1-215` (split into `LiveReader` / `LiveExecutor` / `ShadowExecutor`, add `open_session`)
- Modify: `polyperps/execution/sim_executor.py:70-81` (add `on_tick`, `on_bar`, `poll_fills`)
- Modify: `polyperps/execution/order_router.py:19` (import) and `:421-454` (`Portfolio._reconcile` catches `ShadowRefused`)
- Test: `tests/test_live_executor.py`, `tests/test_sim_executor.py`, `tests/test_reconciliation.py`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `polyperps.execution.executor.ShadowRefused(RuntimeError)`
  - `Executor` protocol: `start_equity: Decimal | None`, `on_tick(tick: Tick) -> None`, `on_bar(bar: Bar) -> None`, `poll_fills() -> list[FillUpdate]`
  - `SimExecutor.on_tick(tick)` = `update_mark`; `SimExecutor.on_bar(bar)` = `apply_funding` (skips `funding_rate is None`); `SimExecutor.poll_fills()` = `check_triggers()`
  - `polyperps.execution.live_executor.LiveReader(session, *, clock=_utcnow)` with `name = "live-reader"`, `start_equity = None`, no-op hooks, `snapshot()`, `events()`, `close()`
  - `LiveExecutor(LiveReader)` (constructor signature unchanged), `ShadowExecutor(LiveReader)` with `name = "shadow"`
  - `async def open_session(label: str) -> tuple[Any, Any]` returning `(sdk_client, perps_session)`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_live_executor.py` and change its imports to:

```python
from polyperps.execution.executor import GateClosed, ShadowRefused
from polyperps.execution.live_executor import LiveExecutor, LiveReader, ShadowExecutor
```

```python
def _req(cid="r-6-1"):
    return OrderRequest(client_order_id=cid, instrument_id=6, side="buy", quantity=Decimal(1), reduce_only=False, ts=T0)


async def test_shadow_refuses_every_write_and_sends_nothing():
    s = FakeSession()
    ex = ShadowExecutor(s, clock=lambda: T0)
    with pytest.raises(ShadowRefused):
        await ex.submit(_req())
    with pytest.raises(ShadowRefused):
        await ex.cancel("r-6-1")
    with pytest.raises(ShadowRefused):
        await ex.place_stop(6, Decimal(85))
    with pytest.raises(ShadowRefused):
        await ex.cancel_stop(6)
    await ex.heartbeat()                       # nothing to keep alive: shadow never has orders
    assert s.calls == []


async def test_shadow_reads_the_real_account_without_a_gate():
    ex = ShadowExecutor(FakeSession(), clock=lambda: T0)   # no gate argument: construction never checks locks
    snap = await ex.snapshot()
    assert snap.position(6).size == Decimal("0.5") and snap.stops == {6: Decimal(85)}
    assert ex.name == "shadow" and ex.start_equity is None and ex.poll_fills() == []
    assert ex.on_tick(None) is None and ex.on_bar(None) is None


def test_live_executor_is_a_gated_live_reader():
    assert issubclass(LiveExecutor, LiveReader) and issubclass(ShadowExecutor, LiveReader)
    with pytest.raises(GateClosed):
        LiveExecutor(FakeSession(), instrument_ids=[6], modes={}, gate=lambda iid: GateDecision(False, "closed"))
```

Append to `tests/test_sim_executor.py` and add imports `from polyperps.backtest.bars import Bar` and `from polyperps.exchange.types import SourceType, Tick`:

```python
def _tick(mark):
    return Tick(instrument_id=6, mark_price=Decimal(mark), index_price=Decimal(mark), last_price=Decimal(mark),
                funding_rate=Decimal("0.001"), next_funding=T0, exchange_ts=T0, received_ts=T0,
                source_type=SourceType.POLYMARKET_WS)


def _bar(rate):
    c = Decimal(100)
    return Bar(instrument_id=6, source_type=SourceType.POLYMARKET_WS, open_ts=T0, open=c, high=c, low=c, close=c,
               index_close=None, funding_rate=None if rate is None else Decimal(rate), spread_bps=Decimal(5),
               spread_source="constant", complete=True)


async def test_executor_hooks_drive_marks_funding_and_stops():
    ex = make()
    await ex.submit(req())
    ex.drain_events()
    ex.on_tick(_tick("100"))
    ex.on_bar(_bar("0.001"))               # long pays 1 * 100 * 0.001
    ex.on_bar(_bar(None))                  # a bar without a rate pays nothing
    assert (await ex.snapshot()).position(6).cumulative_funding == Decimal("-0.1")
    await ex.place_stop(6, Decimal(85))
    ex.on_tick(_tick("84"))
    fills = ex.poll_fills()
    assert len(fills) == 1 and fills[0].side == "sell"
```

Append to `tests/test_reconciliation.py` and add imports `from types import SimpleNamespace` and `from polyperps.execution.live_executor import ShadowExecutor`:

```python
class AlienOrderAccount:
    """A real account (as the perps session sees it) with one order this run never placed."""
    async def fetch_portfolio(self):
        return SimpleNamespace(positions=(), margin=SimpleNamespace(total_account_value=Decimal(1000)),
                               in_liquidation=False)

    async def fetch_open_orders(self):
        return (SimpleNamespace(client_order_id="alien-1", id=5, tp_sl=None, instrument_id=6),)


async def test_reconcile_in_shadow_records_refused_writes_and_keeps_running():
    conn = connect(":memory:")
    ex = ShadowExecutor(AlienOrderAccount(), clock=lambda: T0)
    alerter = Alerter("r", [SqliteSink(conn)])
    pf = Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter, routers={})
    ms = await pf.reconcile_now()             # must not raise: the reconcile loop keeps running
    assert [m.kind for m in ms] == ["unknown_order"]
    kinds = [a[2] for a in list_alerts(conn, "r")]
    assert "shadow_refused" in kinds and "unknown_order" not in kinds
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_live_executor.py tests/test_sim_executor.py tests/test_reconciliation.py -v`
Expected: FAIL. `ImportError: cannot import name 'ShadowRefused'` (collection error in test_live_executor.py and test_reconciliation.py); `AttributeError: 'SimExecutor' object has no attribute 'on_tick'`.

- [ ] **Step 3: Implement**

Replace `polyperps/execution/executor.py` with:

```python
"""The only surface the router talks to (spec section 4.2; Part A §3.2 hooks)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal
from typing import TYPE_CHECKING, Protocol

from polyperps.execution.types import (
    AccountSnapshot, FillUpdate, OrderAck, OrderRequest, OrderUpdate, ReconcileNow, StopAck,
)

if TYPE_CHECKING:
    from polyperps.backtest.bars import Bar
    from polyperps.exchange.types import Tick


class ExecutorTimeout(RuntimeError):
    """The executor did not acknowledge in time; the order MAY have landed."""


class GateClosed(RuntimeError):
    """A live executor was requested while a live-order gate is closed."""


class ShadowRefused(RuntimeError):
    """Shadow mode: a write reached the executor and was refused; nothing was sent."""


class Executor(Protocol):
    name: str
    start_equity: Decimal | None   # loss-limit / pnl baseline

    async def submit(self, order: OrderRequest) -> OrderAck: ...
    async def cancel(self, client_order_id: str) -> None: ...
    async def place_stop(self, instrument_id: int, trigger_price: Decimal) -> StopAck: ...
    async def cancel_stop(self, instrument_id: int) -> None: ...
    async def heartbeat(self) -> None: ...
    async def snapshot(self) -> AccountSnapshot: ...
    def events(self) -> AsyncIterator[OrderUpdate | FillUpdate | ReconcileNow]: ...
    async def close(self) -> None: ...
    # Runner hooks. Sim: marks, funding and stop triggers happen here. Live/shadow: the venue owns them.
    def on_tick(self, tick: Tick) -> None: ...
    def on_bar(self, bar: Bar) -> None: ...
    def poll_fills(self) -> list[FillUpdate]: ...
```

Replace `polyperps/execution/live_executor.py` with (the SDK verification block is kept verbatim):

```python
"""Live venue access over polymarket-client's PerpsSession (spec section 4.4; Part A §3.3).

The ONLY execution module that imports the SDK. Three classes:

  LiveReader      the read side (snapshot, events, close). No gate: it cannot place anything.
  LiveExecutor    LiveReader + the write methods. Its constructor calls the three-lock gate for
                  every instrument and raises GateClosed unless all are open;
                  scripts/run_trader.py exits 2 while any lock is closed.
  ShadowExecutor  LiveReader whose writes raise ShadowRefused: the strategy runs against the
                  real account and every would-be order is recorded, nothing is sent.

SDK verification (2026-09-12, polymarket-client==0.10.0 installed in .venv;
verified by reading source, never by calling the network):

  PerpsOrderPlacement.order.id                 verified unchanged
      .venv/Lib/site-packages/polymarket/models/perps/results.py:28-34
      (PerpsOrderPlacement.order: PerpsOrder), orders.py:59 (PerpsOrder.id).
  PerpsPlacedTpSlOrders.stop_loss.order_id     verified unchanged
      .venv/Lib/site-packages/polymarket/models/perps/results.py:10-24.
  PerpsTpSlOrderFields.kind / .trigger_price   verified unchanged
      .venv/Lib/site-packages/polymarket/models/perps/orders.py:40-48
      (kind: Literal["tp", "sl"]; trigger_price: Decimal).
  PerpsFill.side                               DIFFERS from the brief.
      .venv/Lib/site-packages/polymarket/models/perps/types.py:26 defines
      PerpsSide = Literal["long", "short"], not "buy"/"sell". events() below
      maps long->buy, short->sell to satisfy FillUpdate.side (decision:
      FillUpdate.side is "buy"/"sell"), with an identity fallback for
      already-lowercase buy/sell values (as used by this module's tests).
  place_order(side=...)                        DIFFERS from the brief.
      .venv/Lib/site-packages/polymarket/models/types.py:6 defines
      OrderSide = Literal["BUY", "SELL"], uppercase. Our own OrderRequest.side
      is "buy"/"sell" (polyperps/execution/types.py). submit() below maps
      "buy"->"BUY", "sell"->"SELL" before calling session.place_order so the
      payload matches what the installed SDK actually accepts.
  PerpsOrderStatus (order acks/updates)        review finding, fixed.
      .venv/Lib/site-packages/polymarket/models/perps/types.py:42-67 defines a
      much larger PerpsOrderStatus than our own OrderStatus (spec section
      4.1): e.g. an IOC order that finds no liquidity comes back with status
      "ioc_no_fill", not a plain rejection. _SDK_ORDER_STATUS below maps every
      SDK status onto our OrderStatus (rejection-shaped statuses ->
      "rejected") or to None for TP/SL trigger-lifecycle statuses that never
      apply to a plain order ack/update. submit() now inspects
      placement.order.status through this table instead of always acking
      "accepted"; events() applies the same table to order-update payloads,
      skipping TP/SL lifecycle statuses and logging+skipping anything
      genuinely unrecognised.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from polymarket.errors import RequestRejectedError
from polymarket.models.perps.events import PerpsResyncEvent
from polymarket.models.perps.requests import PerpsPositionTpSlTrigger

from polyperps.execution.executor import GateClosed, ShadowRefused
from polyperps.execution.types import (
    AccountSnapshot, FillUpdate, OrderAck, OrderRequest, OrderStatus, OrderUpdate, PositionView, ReconcileNow,
    StopAck,
)
from polyperps.gates import ExecutionMode, GateDecision, live_orders_allowed

if TYPE_CHECKING:
    from polyperps.backtest.bars import Bar
    from polyperps.exchange.types import Tick

_log = logging.getLogger(__name__)

_FILL_SIDE_MAP = {"long": "buy", "short": "sell", "buy": "buy", "sell": "sell"}
_ORDER_SIDE_MAP = {"buy": "BUY", "sell": "SELL"}

# Maps the SDK's PerpsOrderStatus (polymarket/models/perps/types.py:42-67) onto
# our own OrderStatus (polyperps/execution/types.py). Statuses that mean "the
# order did not/will not rest or fill" collapse to "rejected"; the TP/SL
# trigger-lifecycle statuses map to None since they never apply to a plain
# order ack/update and are skipped by callers below.
_SDK_ORDER_STATUS: dict[str, OrderStatus | None] = {
    "accepted": "accepted",
    "open": "open",
    "partial": "partial",
    "filled": "filled",
    "cancelled": "cancelled",
    "auto_cancelled": "auto_cancelled",
    "post_only_rejected": "rejected",
    "fok_unfilled": "rejected",
    "ioc_no_fill": "rejected",
    "ioc_expired": "rejected",
    "stp_cancelled": "rejected",
    "zero_quantity": "rejected",
    "duplicate_order": "rejected",
    "order_not_found": "rejected",
    "reduce_only_invalid": "rejected",
    "reduce_only_expired": "rejected",
    "order_expired": "rejected",
    "expired": "rejected",
    "untriggered": None,
    "armed": None,
    "triggered": None,
    "parent_cancelled": None,
    "position_closed": None,
    "position_flipped": None,
    "reduce_only_invalid_at_trigger": None,
}
_UNMAPPED = object()  # sentinel: status string not in _SDK_ORDER_STATUS at all


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def open_session(label: str) -> tuple[Any, Any]:
    """(sdk client, perps session) for the wallet in POLYMARKET_PRIVATE_KEY; the caller closes
    both. Signs with the real wallet, so no test ever calls it."""
    from polymarket import AsyncSecureClient

    from polyperps.security.key_management import load_secret

    client = await AsyncSecureClient.create(private_key=load_secret("POLYMARKET_PRIVATE_KEY"))
    return client, await client.open_perps_session(label=label)


class LiveReader:
    """Read side of the live account. No gate: nothing here can place, cancel or arm anything."""

    name = "live-reader"

    def __init__(self, session: Any, *, clock: Callable[[], datetime] = _utcnow) -> None:
        self._s = session
        self._clock = clock
        self._stop_ids: dict[int, int] = {}
        self.start_equity: Decimal | None = None   # loss-limit baseline; set by the runner

    # --- runner hooks: the venue owns marks, funding and stop triggers -------
    def on_tick(self, tick: Tick) -> None:
        return None

    def on_bar(self, bar: Bar) -> None:
        return None

    def poll_fills(self) -> list[FillUpdate]:
        return []

    async def snapshot(self) -> AccountSnapshot:
        pf = await self._s.fetch_portfolio()
        orders = await self._s.fetch_open_orders()
        positions = tuple(
            PositionView(instrument_id=int(p.instrument_id), size=p.size, entry_price=p.entry_price,
                         notional=abs(p.position_value), leverage=int(p.leverage),
                         liquidation_price=p.liquidation_price, unrealised_pnl=p.unrealized_pnl,
                         cumulative_funding=p.cumulative_funding)
            for p in pf.positions if p.size != 0)
        open_ids = tuple(o.client_order_id for o in orders if o.client_order_id and o.tp_sl is None)
        stops: dict[int, Decimal] = {}
        for o in orders:
            tp_sl = getattr(o, "tp_sl", None)
            if tp_sl is not None and getattr(tp_sl, "kind", None) == "sl":
                stops[int(o.instrument_id)] = tp_sl.trigger_price
                self._stop_ids[int(o.instrument_id)] = int(o.id)
        return AccountSnapshot(equity=pf.margin.total_account_value, positions=positions, open_orders=open_ids,
                               stops=stops, in_liquidation=bool(pf.in_liquidation), ts=self._clock())

    async def events(self) -> AsyncIterator[OrderUpdate | FillUpdate | ReconcileNow]:
        async for ev in self._s:
            if isinstance(ev, PerpsResyncEvent):
                yield ReconcileNow("resync")
                continue
            kind = getattr(ev, "type", None)
            if kind == "order":
                p = ev.payload
                mapped = _SDK_ORDER_STATUS.get(p.status, _UNMAPPED)
                if mapped is _UNMAPPED:
                    _log.warning("live: unmapped order status %s", p.status)
                elif mapped is not None and p.client_order_id:
                    yield OrderUpdate(client_order_id=p.client_order_id, status=mapped,
                                      filled_quantity=p.filled_quantity, ts=ev.timestamp)
            elif kind == "fill":
                for f in ev.payload:
                    if f.client_order_id:
                        yield FillUpdate(client_order_id=f.client_order_id, instrument_id=int(f.instrument_id),
                                         side=_FILL_SIDE_MAP.get(f.side, f.side), quantity=f.quantity,
                                         price=f.price, fee=f.fee, ts=ev.timestamp)

    async def close(self) -> None:
        await self._s.close()


class LiveExecutor(LiveReader):
    name = "live"

    def __init__(
        self,
        session: Any,
        *,
        instrument_ids: Sequence[int],
        modes: Mapping[int, ExecutionMode],
        gate: Callable[[int], GateDecision] | None = None,
        clock: Callable[[], datetime] = _utcnow,
        dead_man_s: int = 60,
    ) -> None:
        if not instrument_ids:
            raise GateClosed("no instruments to gate")
        check = gate if gate is not None else (lambda iid: live_orders_allowed(iid, modes=modes))
        for iid in instrument_ids:
            d = check(iid)
            if not d.allowed:
                raise GateClosed(f"instrument {iid}: {d.reason}")
        super().__init__(session, clock=clock)
        self._dead_man = timedelta(seconds=dead_man_s)

    async def submit(self, order: OrderRequest) -> OrderAck:
        try:
            placement = await self._s.place_order(
                instrument_id=order.instrument_id, side=_ORDER_SIDE_MAP[order.side], quantity=order.quantity,
                time_in_force="ioc", reduce_only=order.reduce_only, client_order_id=order.client_order_id)
        except RequestRejectedError as exc:
            return OrderAck(client_order_id=order.client_order_id, exchange_order_id=None, status="rejected",
                            reason=str(exc), ts=self._clock())
        placed = getattr(placement, "order", None)
        xid = getattr(placed, "id", None)
        exchange_order_id = str(xid) if xid is not None else None
        raw_status = getattr(placed, "status", None)
        mapped = _SDK_ORDER_STATUS.get(raw_status, _UNMAPPED)
        if mapped is _UNMAPPED:
            return OrderAck(client_order_id=order.client_order_id, exchange_order_id=exchange_order_id,
                            status="rejected", reason=f"unknown status {raw_status}", ts=self._clock())
        if mapped in ("rejected", "cancelled", "auto_cancelled"):
            return OrderAck(client_order_id=order.client_order_id, exchange_order_id=exchange_order_id,
                            status="rejected", reason=str(raw_status), ts=self._clock())
        return OrderAck(client_order_id=order.client_order_id, exchange_order_id=exchange_order_id,
                        status="accepted", reason="", ts=self._clock())

    async def cancel(self, client_order_id: str) -> None:
        await self._s.cancel_order(client_order_id=client_order_id)

    async def place_stop(self, instrument_id: int, trigger_price: Decimal) -> StopAck:
        placed = await self._s.place_position_tp_sl(
            instrument_id=instrument_id, stop_loss=PerpsPositionTpSlTrigger(trigger_price=trigger_price))
        oid = getattr(getattr(placed, "stop_loss", None), "order_id", None)
        if oid is not None:
            self._stop_ids[instrument_id] = int(oid)
        return StopAck(instrument_id=instrument_id, trigger_price=trigger_price,
                       exchange_order_id=str(oid) if oid is not None else None, ts=self._clock())

    async def cancel_stop(self, instrument_id: int) -> None:
        oid = self._stop_ids.pop(instrument_id, None)
        if oid is not None:
            await self._s.cancel_order(order_id=oid)

    async def heartbeat(self) -> None:
        await self._s.arm_auto_cancel(cancel_at=self._clock() + self._dead_man)


class ShadowExecutor(LiveReader):
    """Part A §3.3: the real account, read-only. Every write raises ShadowRefused (the router
    records the order as shadow_refused and halts the instrument); heartbeat is a no-op because
    shadow never has an order to protect."""

    name = "shadow"

    async def submit(self, order: OrderRequest) -> OrderAck:
        raise ShadowRefused(f"shadow: submit {order.client_order_id} refused")

    async def cancel(self, client_order_id: str) -> None:
        raise ShadowRefused(f"shadow: cancel {client_order_id} refused")

    async def place_stop(self, instrument_id: int, trigger_price: Decimal) -> StopAck:
        raise ShadowRefused(f"shadow: stop for {instrument_id} at {trigger_price} refused")

    async def cancel_stop(self, instrument_id: int) -> None:
        raise ShadowRefused(f"shadow: cancel stop for {instrument_id} refused")

    async def heartbeat(self) -> None:
        return None
```

In `polyperps/execution/sim_executor.py`, add at the top (after the existing imports):

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from polyperps.backtest.bars import Bar
    from polyperps.exchange.types import Tick
```

and add these three methods right after `apply_funding`:

```python
    # --- runner hooks (Part A §3.2) ----------------------------------------
    def on_tick(self, tick: Tick) -> None:
        self.update_mark(tick.instrument_id, tick.mark_price)

    def on_bar(self, bar: Bar) -> None:
        if bar.funding_rate is not None:
            self.apply_funding(bar.instrument_id, bar.funding_rate)

    def poll_fills(self) -> list[FillUpdate]:
        return self.check_triggers()
```

In `polyperps/execution/order_router.py`, change the import line to
`from polyperps.execution.executor import Executor, ExecutorTimeout, ShadowRefused`
and replace `Portfolio._reconcile` with:

```python
    async def _reconcile(self) -> list[Mismatch]:
        snapshot = await self.executor.snapshot()
        local = db.get_positions_local(self.conn, self.run_id)
        # "known_orders" is what diff() treats as ours-and-possibly-still-resting on the venue.
        # Terminal rows (filled/cancelled/auto_cancelled/rejected/lost) are done as far as we're
        # concerned; if the venue still lists one as open that's a genuine unknown_order mismatch
        # worth raising, not something to mask by including every order id we've ever sent.
        known = {o.client_order_id for o in db.list_orders(self.conn, self.run_id)
                 if o.status not in _TERMINAL_ORDER_STATUSES}
        mismatches = diff(local=local, remote=snapshot, run_id=self.run_id, known_orders=known)
        for m in mismatches:
            router = self.routers.get(m.instrument_id) if m.instrument_id is not None else None
            detail = {"local": m.local, "remote": m.remote}
            now = _utcnow()
            try:
                if m.kind == "size":
                    if router is not None:
                        await router.halt(f"reconcile: size mismatch local={m.local} remote={m.remote}")
                    else:
                        self.alerter.emit(Alert(level="CRITICAL", kind="unknown_position",
                                                instrument_id=m.instrument_id, detail=detail, ts=now))
                elif m.kind == "unknown_order":
                    await self.executor.cancel(m.remote)
                    self.alerter.emit(Alert(level="WARN", kind="unknown_order", instrument_id=None, detail=detail,
                                            ts=now))
                elif m.kind == "missing_stop":
                    if router is not None:
                        await router.replace_stop()
                    self.alerter.emit(Alert(level="WARN", kind="stop_missing", instrument_id=m.instrument_id,
                                            detail=detail, ts=now))
                elif m.kind == "stop_without_position":
                    await self.executor.cancel_stop(m.instrument_id)
                    self.alerter.emit(Alert(level="INFO", kind="stop_orphan_cancelled", instrument_id=m.instrument_id,
                                            detail=detail, ts=now))
                else:
                    self.alerter.emit(Alert(level="WARN", kind="stop_drift", instrument_id=m.instrument_id,
                                            detail=detail, ts=now))
            except ShadowRefused:
                # Shadow mode: the response would have written to the real account. Record it and
                # keep reconciling (Part A §3.3) instead of taking the reconcile loop down.
                self.alerter.emit(Alert(level="WARN", kind="shadow_refused", instrument_id=m.instrument_id,
                                        detail=detail, ts=now))
        return mismatches
```

- [ ] **Step 4: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/test_live_executor.py tests/test_sim_executor.py tests/test_reconciliation.py -v`
Expected: PASS.

- [ ] **Step 5: Run the full suite**

Run: `.venv/Scripts/python -m pytest -q`
Expected: all pass, 0 failed.

- [ ] **Step 6: Commit**

```bash
git add polyperps/execution/executor.py polyperps/execution/live_executor.py polyperps/execution/sim_executor.py polyperps/execution/order_router.py tests/test_live_executor.py tests/test_sim_executor.py tests/test_reconciliation.py
git commit -m "$(cat <<'EOF'
feat(execution): executor hooks, LiveReader split and ShadowExecutor

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: `account_snapshots` table; the dashboard reads it

**Files:**
- Modify: `polyperps/storage/db.py:23` (import), `:104` (schema), after `:472` (new functions)
- Modify: `polyperps/dashboard/state.py:70-149` (`account_and_positions`), `:278-284` (`run_info`)
- Test: `tests/test_storage_phase2.py`, `tests/test_dashboard_state.py`

**Interfaces:**
- Consumes: `AccountSnapshot`, `PositionView` (existing).
- Produces:
  - `db.save_account_snapshot(conn, run_id: str, snap: AccountSnapshot, *, start_equity: Decimal, executor: str) -> None` (one row per run_id, latest wins)
  - `db.load_account_snapshot(conn, run_id: str) -> tuple[AccountSnapshot, Decimal, str] | None` → `(snapshot, start_equity, executor_name)`
  - `dashboard.state.account_and_positions` reads the snapshot row; `run_info(...)["executor"]` is the row's executor name, `"sim"` when none.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_storage_phase2.py` (add `AccountSnapshot, PositionView` to its `polyperps.execution.types` import and `load_account_snapshot, save_account_snapshot` to its `polyperps.storage.db` import; `T0`, `RUN`, `connect`, `Decimal` already exist there):

```python
def test_account_snapshot_round_trip_keeps_latest_per_run():
    conn = connect(":memory:")
    assert load_account_snapshot(conn, RUN) is None
    pos = PositionView(instrument_id=6, size=Decimal("-0.5"), entry_price=Decimal(100), notional=Decimal(51),
                       leverage=3, liquidation_price=None, unrealised_pnl=Decimal("-1"),
                       cumulative_funding=Decimal("0.02"))
    first = AccountSnapshot(equity=Decimal(1000), positions=(), open_orders=(), stops={}, in_liquidation=False, ts=T0)
    later = AccountSnapshot(equity=Decimal("999"), positions=(pos,), open_orders=("r-6-3",), stops={6: Decimal(115)},
                            in_liquidation=True, ts=T0)
    save_account_snapshot(conn, RUN, first, start_equity=Decimal(1000), executor="shadow")
    save_account_snapshot(conn, RUN, later, start_equity=Decimal(1000), executor="shadow")
    snap, start, executor = load_account_snapshot(conn, RUN)
    assert snap == later and start == Decimal(1000) and executor == "shadow"
    assert conn.execute("SELECT COUNT(*) FROM account_snapshots").fetchone()[0] == 1
```

In `tests/test_dashboard_state.py`:
- change the imports: `from polyperps.execution.types import AccountSnapshot, DecisionRow, OrderRow, PositionLocalRow, PositionView, State`; in the `polyperps.storage.db` import replace `save_sim_account,` with `save_account_snapshot,`.
- add after `BLOB`:

```python
# The same account as BLOB, as the runner writes it each fast loop (SimExecutor.from_json(BLOB).snapshot()).
SNAP = AccountSnapshot(
    equity=Decimal("9997"),
    positions=(
        PositionView(instrument_id=6, size=Decimal("0.001"), entry_price=Decimal("100000"), notional=Decimal("101"),
                     leverage=3, liquidation_price=Decimal("68666.67"), unrealised_pnl=Decimal("1"),
                     cumulative_funding=Decimal("-0.3")),
        PositionView(instrument_id=7, size=Decimal("-0.1"), entry_price=Decimal("4000"), notional=Decimal("404"),
                     leverage=3, liquidation_price=Decimal("5253.33"), unrealised_pnl=Decimal("-4"),
                     cumulative_funding=Decimal("-0.5")),
    ),
    open_orders=(), stops={6: Decimal("85000"), 7: Decimal("4600")}, in_liquidation=False, ts=T0)
```

- in `seeded_conn()` replace `save_sim_account(conn, RUN, json.dumps(BLOB))` with
  `save_account_snapshot(conn, RUN, SNAP, start_equity=Decimal("10000"), executor="sim")`.
- replace `test_liq_price_matches_sim_executor` and `test_no_sim_account_yet` with:

```python
async def test_seeded_snapshot_is_what_the_sim_reports():
    ex = SimExecutor.from_json(RUN, json.dumps(BLOB), taker_fee_rate=Decimal("0"))
    snap = await ex.snapshot()
    assert snap.positions == SNAP.positions and snap.equity == SNAP.equity and snap.stops == SNAP.stops
    by_id = {v.instrument_id: v for v in snap.positions}
    assert st.liq_price(Decimal("0.001"), Decimal("100000")) == by_id[6].liquidation_price
    assert st.liq_price(Decimal("-0.1"), Decimal("4000")) == by_id[7].liquidation_price


def test_no_account_snapshot_yet():
    conn = connect(":memory:")
    assert st.account_and_positions(conn, run_id=RUN, instruments=INSTRUMENTS) == (None, [])


def test_venue_liquidation_price_is_shown_and_executor_is_reported():
    conn = connect(":memory:")
    live_pos = PositionView(
        instrument_id=6, size=Decimal("0.001"), entry_price=Decimal("100000"), notional=Decimal("101"), leverage=3,
        liquidation_price=Decimal("70000"), unrealised_pnl=Decimal("1"), cumulative_funding=Decimal("0"))
    save_account_snapshot(conn, RUN, AccountSnapshot(equity=Decimal("500"), positions=(live_pos,), open_orders=(),
                                                     stops={}, in_liquidation=False, ts=T0),
                          start_equity=Decimal("500"), executor="shadow")
    account, positions = st.account_and_positions(conn, run_id=RUN, instruments=INSTRUMENTS)
    assert positions[0]["liq_price"] == "70000" and positions[0]["stop_trigger"] is None
    assert account["equity"] == "500"
    s = st.build_state(conn, run_id=RUN, instrument_ids=(6,), instruments=INSTRUMENTS, hypothesis="h1",
                       host="box", now=T0, env={}, signal_validated=False)
    assert s["run"]["executor"] == "shadow"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_storage_phase2.py tests/test_dashboard_state.py -v`
Expected: FAIL, `ImportError: cannot import name 'load_account_snapshot'` / `'save_account_snapshot'`.

- [ ] **Step 3: Implement**

In `polyperps/storage/db.py`:
- change the execution import to
  `from polyperps.execution.types import AccountSnapshot, DecisionRow, Intent, OrderRow, PositionLocalRow, PositionView, State`
- in `_SCHEMA`, after the `sim_account` line, add:

```sql
CREATE TABLE IF NOT EXISTS account_snapshots (
    run_id TEXT PRIMARY KEY, ts TEXT NOT NULL, executor TEXT NOT NULL, start_equity TEXT NOT NULL, json TEXT NOT NULL
);
```

- after `load_sim_account`, add:

```python
def save_account_snapshot(conn: sqlite3.Connection, run_id: str, snap: AccountSnapshot, *,
                          start_equity: Decimal, executor: str) -> None:
    """Part A §3.4: the runner's latest fast-loop snapshot, one row per run (sim, shadow and live alike)."""
    blob = json.dumps({
        "equity": str(snap.equity), "in_liquidation": snap.in_liquidation, "open_orders": list(snap.open_orders),
        "stops": {str(i): str(t) for i, t in snap.stops.items()},
        "positions": [{"instrument_id": p.instrument_id, "size": str(p.size), "entry_price": str(p.entry_price),
                       "notional": str(p.notional), "leverage": p.leverage,
                       "liquidation_price": str(p.liquidation_price) if p.liquidation_price is not None else None,
                       "unrealised_pnl": str(p.unrealised_pnl), "cumulative_funding": str(p.cumulative_funding)}
                      for p in snap.positions],
    })
    conn.execute("INSERT INTO account_snapshots VALUES (?,?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET "
                 "ts=excluded.ts, executor=excluded.executor, start_equity=excluded.start_equity, json=excluded.json",
                 (run_id, _ts(snap.ts), executor, str(start_equity), blob))
    conn.commit()


def load_account_snapshot(conn: sqlite3.Connection, run_id: str) -> tuple[AccountSnapshot, Decimal, str] | None:
    r = conn.execute("SELECT ts, executor, start_equity, json FROM account_snapshots WHERE run_id=?",
                     (run_id,)).fetchone()
    if r is None:
        return None
    d = json.loads(r[3])
    positions = tuple(
        PositionView(instrument_id=p["instrument_id"], size=Decimal(p["size"]), entry_price=Decimal(p["entry_price"]),
                     notional=Decimal(p["notional"]), leverage=p["leverage"],
                     liquidation_price=_dec(p["liquidation_price"]), unrealised_pnl=Decimal(p["unrealised_pnl"]),
                     cumulative_funding=Decimal(p["cumulative_funding"]))
        for p in d["positions"])
    snap = AccountSnapshot(equity=Decimal(d["equity"]), positions=positions, open_orders=tuple(d["open_orders"]),
                           stops={int(i): Decimal(t) for i, t in d["stops"].items()},
                           in_liquidation=d["in_liquidation"], ts=_parse_ts(r[0]))
    return snap, Decimal(r[2]), r[1]
```

In `polyperps/dashboard/state.py`, replace `account_and_positions` with:

```python
def account_and_positions(
    conn, *, run_id: str, instruments: Mapping[int, InstrumentInfo] | None,
) -> tuple[dict | None, list[dict]]:
    loaded = db.load_account_snapshot(conn, run_id)
    if loaded is None:
        return None, []
    snap, start_equity, _executor = loaded
    local = db.get_positions_local(conn, run_id)
    fills = db.list_orders(conn, run_id, status="filled")

    positions: list[dict] = []
    unreal = _ZERO
    gross = _ZERO
    net_by_cluster: dict[str, Decimal] = {}
    all_known = instruments is not None
    for p in snap.positions:
        iid, size, entry, funding = p.instrument_id, p.size, p.entry_price, p.cumulative_funding
        if size == 0:
            continue
        mark = p.notional / abs(size)
        pnl = p.unrealised_pnl
        unreal += pnl
        notional = p.notional
        gross += notional
        # the venue's own number in shadow/live; the sim's formula when a snapshot carries none
        liq = p.liquidation_price if p.liquidation_price is not None else liq_price(size, entry)
        liq_distance = abs(mark - liq) / mark if mark else _ZERO
        move = (mark - entry) / entry if entry else _ZERO
        adverse = max(_ZERO, -move if size > 0 else move)
        base = abs(size) * entry
        # cumulative_funding is negative when funding was PAID (execution/types.py
        # PositionView docstring), so negate it here: paid funding must show as a positive cost.
        funding_paid = -funding / base if base else _ZERO
        inst = instruments.get(iid) if instruments else None
        if inst is None:
            all_known = False
        else:
            cl = cluster_of(inst.category)
            net_by_cluster[cl] = net_by_cluster.get(cl, _ZERO) + (notional if size > 0 else -notional)
        row = local.get(iid)
        positions.append({
            "instrument_id": iid,
            "name": _name(iid, instruments),
            "state": str(row.state) if row is not None else str(State.OPEN),
            "side": "LONG" if size > 0 else "SHORT",
            "size": _s(size),
            "entry_price": _s(entry),
            "mark": _s(mark),
            "pnl": _s(pnl),
            "liq_price": _s(liq),
            "liq_distance": _f(liq_distance),
            "adverse_move": _f(adverse),
            "funding_paid": _f(funding_paid),
            "stop_trigger": _s(snap.stops[iid]) if iid in snap.stops else None,
            "opened_at": _opened_at(fills, iid),
        })

    equity = snap.equity
    cluster_net: float | None = None
    if all_known:
        worst = max((abs(v) for v in net_by_cluster.values()), default=_ZERO)
        cluster_net = _f(worst / equity) if equity else 0.0
    account = {
        "equity": _s(equity),
        "start_equity": _s(start_equity),
        "pnl_since_start": _s(equity - start_equity),
        "unrealized": _s(unreal),
        "gross_exposure": _f(gross / equity) if equity else 0.0,
        "gross_limit": _f(EXPOSURE.gross),
        "cluster_net": cluster_net,
        "cluster_limit": _f(EXPOSURE.cluster_net),
        "leverage": LIMITS.max_leverage,
        "kill_switch": "unarmed",   # divergence thresholds are None (risk/kill_switch.py)
    }
    return account, positions
```

Delete `import json` from the top of `state.py` (its only use was the `json.loads` removed above).

Replace `run_info` with:

```python
def run_info(conn, *, run_id: str, hypothesis: str, host: str, now: datetime) -> dict:
    started = _started_at(conn, run_id)
    uptime = int((now - started).total_seconds()) if started else 0
    loaded = db.load_account_snapshot(conn, run_id)
    return {
        "run_id": run_id, "executor": loaded[2] if loaded is not None else "sim", "hypothesis": hypothesis,
        "host": host, "started_at": _iso(started) if started else None, "uptime_s": max(0, uptime),
    }
```

- [ ] **Step 4: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/test_storage_phase2.py tests/test_dashboard_state.py tests/test_dashboard_server.py -v`
Expected: PASS.

- [ ] **Step 5: Run the full suite**

Run: `.venv/Scripts/python -m pytest -q`
Expected: all pass, 0 failed.

- [ ] **Step 6: Commit**

```bash
git add polyperps/storage/db.py polyperps/dashboard/state.py tests/test_storage_phase2.py tests/test_dashboard_state.py
git commit -m "$(cat <<'EOF'
feat(dashboard): read the account from an account_snapshots table, not the sim JSON

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Hard loss limit

**Files:**
- Modify: `polyperps/risk/kill_switch.py:1-50`
- Modify: `polyperps/execution/order_router.py:26` (import), `:130-135` (router shutdown branch), after `:337` (new `shutdown`), `:371-377` (`Portfolio.on_bar`)
- Test: `tests/test_kill_switch.py`, `tests/test_order_router.py`

**Interfaces:**
- Consumes: `ALERT_THRESHOLDS.pnl_warn` (−0.05), `.pnl_critical` (−0.10) from `polyperps/monitor/alerts.py`.
- Produces:
  - `kill_switch.loss_limit(equity: Decimal | None, start_equity: Decimal | None) -> Action`
  - `kill_switch.evaluate(*, live_sharpe, backtest_sharpe, mode, equity: Decimal | None = None, start_equity: Decimal | None = None, thresholds=THRESHOLDS) -> Action` (strictest of divergence and loss limit)
  - `InstrumentRouter.shutdown(mark: Decimal) -> None` (flatten if OPEN, CRITICAL `kill_switch`, HALTED; no-op when HALTED/LIQUIDATED)
  - `Portfolio.on_bar(histories, kill)`: on `"shutdown"` calls `Portfolio._shutdown(snapshot, histories)` over EVERY router and emits one CRITICAL `loss_limit` (or `divergence`).

Decision: `equity`/`start_equity` are optional keywords (Decision 9). Decision: the alert kind is chosen by recomputing `loss_limit` in the Portfolio (Decision 10).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_kill_switch.py` (change the import to `from polyperps.risk.kill_switch import THRESHOLDS, KillThresholds, divergence, evaluate, loss_limit`):

```python
def test_loss_limit_pauses_at_minus_5_and_shuts_down_at_minus_10():
    s = Decimal(1000)
    assert loss_limit(Decimal("951"), s) == "run"
    assert loss_limit(Decimal("950"), s) == "pause"
    assert loss_limit(Decimal("901"), s) == "pause"
    assert loss_limit(Decimal("900"), s) == "shutdown"
    assert loss_limit(Decimal("900"), None) == "run" and loss_limit(Decimal("900"), Decimal(0)) == "run"
    assert loss_limit(None, s) == "run"


def test_evaluate_takes_the_strictest_of_divergence_and_loss():
    t = KillThresholds(pause=Decimal("0.5"), shutdown=Decimal("1.0"))
    flat = {"equity": Decimal(1000), "start_equity": Decimal(1000)}
    assert evaluate(live_sharpe=-0.1, backtest_sharpe=1.2, mode="live", thresholds=t, **flat) == "shutdown"
    assert evaluate(live_sharpe=1.0, backtest_sharpe=1.2, mode="live", thresholds=t,
                    equity=Decimal(940), start_equity=Decimal(1000)) == "pause"
    assert evaluate(live_sharpe=None, backtest_sharpe=None, mode="paper",
                    equity=Decimal(890), start_equity=Decimal(1000)) == "shutdown"
    assert evaluate(live_sharpe=None, backtest_sharpe=None, mode="live", **flat) == "pause"   # None thresholds
    assert evaluate(live_sharpe=None, backtest_sharpe=None, mode="paper", **flat) == "run"
```

Append to `tests/test_order_router.py` (add `from polyperps.risk.kill_switch import evaluate`):

```python
async def test_loss_limit_shutdown_flattens_every_router_and_halts():
    conn = connect(":memory:")
    ex = SimExecutor("r", equity=Decimal(1000), taker_fee_rate=FEE, clock=lambda: T0)
    ex.update_mark(6, Decimal(100)); ex.update_mark(7, Decimal(100))
    alerter = Alerter("r", [SqliteSink(conn)])
    r6 = InstrumentRouter(run_id="r", instrument_id=6, category="crypto", strategy=Strat(1), executor=ex, conn=conn,
                          alerter=alerter, categories=CATS, clock=lambda: T0)
    r7 = InstrumentRouter(run_id="r", instrument_id=7, category="crypto", strategy=Strat(-1), executor=ex, conn=conn,
                          alerter=alerter, categories=CATS, clock=lambda: T0)
    pf = Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter, routers={6: r6, 7: r7},
                   start_equity=Decimal(1000))
    await pf.on_bar({6: [bar(0)], 7: [dc_replace(bar(0), instrument_id=7)]}, "run"); await pump(pf, ex)
    assert r6.size > 0 and r7.size < 0
    ex._cash -= Decimal(150)                                                  # a -15 % day
    kill = evaluate(live_sharpe=None, backtest_sharpe=None, mode="paper",
                    equity=(await ex.snapshot()).equity, start_equity=pf.start_equity)
    assert kill == "shutdown"
    await pf.on_bar({6: [bar(0), bar(1)]}, kill); await pump(pf, ex)          # only instrument 6's bar closed
    assert r6.state is State.HALTED and r7.state is State.HALTED
    assert r6.size == 0 and r7.size == 0 and get_order(conn, "r-7-2").reason == "kill_shutdown"
    await pf.on_bar({7: [dc_replace(bar(1), instrument_id=7)]}, kill)        # still breached: no second alert
    loss = [a for a in list_alerts(conn, "r") if a[2] == "loss_limit"]
    assert len(loss) == 1 and loss[0][1] == "CRITICAL"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_kill_switch.py tests/test_order_router.py -v`
Expected: FAIL, `ImportError: cannot import name 'loss_limit'`; `TypeError: evaluate() got an unexpected keyword argument 'equity'`.

- [ ] **Step 3: Implement**

Replace `polyperps/risk/kill_switch.py` with:

```python
"""Spec 2.6 mechanism plus the Part A §5 hard loss limit.

Divergence: THRESHOLDS are None until Phase 2b Part B sets them from a passing native record.
With None, a live run evaluates to "pause" (cannot start) and a paper run to "run". An undefined
divergence (missing sharpe input, or backtest_sharpe == 0) falls back to the same mode default.

Loss limit (fixed in code, never self-adjusting): equity at or below -5 % of start equity pauses
new entries; at or below -10 % shuts down (flatten everything, halt every router). Clearing it is
a human decision (--clear-halt). The stricter of the two checks wins."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from polyperps.monitor.alerts import ALERT_THRESHOLDS

Mode = Literal["paper", "live"]
Action = Literal["run", "pause", "shutdown"]
_RANK: dict[str, int] = {"run": 0, "pause": 1, "shutdown": 2}


@dataclass(frozen=True, slots=True, kw_only=True)
class KillThresholds:
    pause: Decimal | None
    shutdown: Decimal | None


THRESHOLDS = KillThresholds(pause=None, shutdown=None)


def divergence(live: float | None, backtest: float | None) -> Decimal | None:
    if live is None or backtest is None or backtest == 0.0:
        return None
    return Decimal(str(abs(live - backtest) / abs(backtest)))


def _divergence_action(live_sharpe: float | None, backtest_sharpe: float | None, mode: Mode,
                       thresholds: KillThresholds) -> Action:
    if thresholds.pause is None or thresholds.shutdown is None:
        return "pause" if mode == "live" else "run"
    d = divergence(live_sharpe, backtest_sharpe)
    if d is None:
        return "pause" if mode == "live" else "run"
    if d >= thresholds.shutdown:
        return "shutdown"
    if d >= thresholds.pause:
        return "pause"
    return "run"


def loss_limit(equity: Decimal | None, start_equity: Decimal | None) -> Action:
    if equity is None or start_equity is None or start_equity <= 0:
        return "run"
    drawdown = equity / start_equity - 1
    if drawdown <= ALERT_THRESHOLDS.pnl_critical:
        return "shutdown"
    if drawdown <= ALERT_THRESHOLDS.pnl_warn:
        return "pause"
    return "run"


def evaluate(
    *,
    live_sharpe: float | None,
    backtest_sharpe: float | None,
    mode: Mode,
    equity: Decimal | None = None,
    start_equity: Decimal | None = None,
    thresholds: KillThresholds = THRESHOLDS,
) -> Action:
    return max(_divergence_action(live_sharpe, backtest_sharpe, mode, thresholds),
               loss_limit(equity, start_equity), key=_RANK.__getitem__)
```

In `polyperps/execution/order_router.py`:
- change `from polyperps.risk.kill_switch import Action` to `from polyperps.risk.kill_switch import Action, loss_limit`.
- in `InstrumentRouter.on_bar`, replace

```python
        if kill == "shutdown":
            if self.state is State.OPEN:
                await self._exit(mark, "kill_shutdown", target=None)
            self._alert("CRITICAL", "kill_switch", action="shutdown")
            self._set_state(State.HALTED)
            return
```

with

```python
        if kill == "shutdown":
            await self.shutdown(mark)
            return
```

- add after `halt()`:

```python
    async def shutdown(self, mark: Decimal) -> None:
        """Kill switch / loss limit (Part A §5): flatten if open, then HALTED until --clear-halt."""
        if self.state in (State.HALTED, State.LIQUIDATED):
            return
        if self.state is State.OPEN:
            await self._exit(mark, "kill_shutdown", target=None)
        self._alert("CRITICAL", "kill_switch", action="shutdown")
        self._set_state(State.HALTED)
```

- replace `Portfolio.on_bar` with:

```python
    async def on_bar(self, histories: Mapping[int, Sequence[Bar]], kill: Action) -> None:
        async with self._lock:
            snapshot = await self.executor.snapshot()
            if kill == "shutdown":
                await self._shutdown(snapshot, histories)
                return
            for iid, history in histories.items():
                router = self.routers.get(iid)
                if router is not None and history:
                    await router.on_bar(history, snapshot, kill)

    async def _shutdown(self, snapshot: AccountSnapshot, histories: Mapping[int, Sequence[Bar]]) -> None:
        """Flatten every open position and halt EVERY router, not only the ones whose bar just
        closed (the runner passes one instrument per call). A persisting breach alerts once."""
        live = [r for r in self.routers.values() if r.state not in (State.HALTED, State.LIQUIDATED)]
        if not live:
            return
        cause = "loss_limit" if loss_limit(snapshot.equity, self.start_equity) == "shutdown" else "divergence"
        self.alerter.emit(Alert(level="CRITICAL", kind=cause, instrument_id=None,
                                detail={"equity": str(snapshot.equity), "start_equity": str(self.start_equity)},
                                ts=snapshot.ts))
        for router in live:
            hist = histories.get(router.instrument_id)
            pos = snapshot.position(router.instrument_id)
            if hist and hist[-1].close is not None:
                mark = hist[-1].close
            elif pos is not None and pos.size != 0:
                mark = pos.notional / abs(pos.size)
            else:
                mark = router.entry or Decimal(0)
            await router.shutdown(mark)
```

- [ ] **Step 4: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/test_kill_switch.py tests/test_order_router.py -v`
Expected: PASS (including the existing `test_kill_switch_pause_and_shutdown`).

- [ ] **Step 5: Run the full suite**

Run: `.venv/Scripts/python -m pytest -q`
Expected: all pass, 0 failed.

- [ ] **Step 6: Commit**

```bash
git add polyperps/risk/kill_switch.py polyperps/execution/order_router.py tests/test_kill_switch.py tests/test_order_router.py
git commit -m "$(cat <<'EOF'
feat(risk): hard loss limit - pause at -5 %, flatten and halt every router at -10 %

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: `scripts/run_trader.py` replaces `run_paper.py`

**Files:**
- Rename + rewrite: `scripts/run_paper.py` → `scripts/run_trader.py`
- Rename + rewrite: `tests/test_run_paper_script.py` → `tests/test_run_trader_script.py`
- Modify: `polyperps/execution/order_router.py:379-394` (`Portfolio.on_fast` returns the snapshot; drop the stale funding_drift comment later in Task 9), `:138` (comment)
- Modify: `polyperps/execution/sim_executor.py:157` (docstring), `polyperps/execution/live_bars.py:74` (docstring)
- Modify: `deploy/polyperps-paper.service` (Description, ExecStart), `deploy/env.example:16` (comment), `README.md:94-115`
- Test: `tests/test_run_trader_script.py`, `tests/test_deploy_files.py`

**Interfaces:**
- Consumes: `LiveReader`/`ShadowExecutor`/`LiveExecutor`/`open_session` (Task 1); `db.save_account_snapshot` / `db.load_account_snapshot` (Task 2); `kill_switch.evaluate(..., equity=, start_equity=)` (Task 3).
- Produces:
  - `scripts/run_trader.py`: `build_parser()` (`--executor {sim,shadow,live}`), `async build_executor(mode, *, run_id, conn, fee_rate, equity, instrument_ids, session=None) -> Executor`, `seed_history(...)`, `_recover_strategies(...)`, `run_until_first_exits(*coros)`, `run_once(args, settings)`, `main()`; module constants `HEARTBEAT_S = 20`, `RECONCILE_S = 60`, `MODES: dict[int, ExecutionMode] = {}`; module-level names `open_session`, `PolymarketPerpsClient`, `_alerter` (tests monkeypatch them).
  - `Portfolio.on_fast(marks) -> AccountSnapshot` (the snapshot it evaluated).

Decision: sim and shadow evaluate the kill switch as `"paper"`, live as `"live"` (Decision 7). Decision: shadow/live baseline from the last `account_snapshots` row, else the first snapshot (Decision 8). Decision: `MODES = {}`; `main()` pre-checks the gate before the wallet key loads (Decision 19).

- [ ] **Step 1: Move the files and write the failing tests**

```bash
git mv scripts/run_paper.py scripts/run_trader.py
git mv tests/test_run_paper_script.py tests/test_run_trader_script.py
```

Replace `tests/test_run_trader_script.py` with:

```python
import asyncio
import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from polyperps.exchange.types import Candle, FeeSchedule, FundingObservation, Instrument, SourceType, Tick
from polyperps.execution.executor import GateClosed
from polyperps.execution.live_bars import LiveBarBuilder
from polyperps.execution.live_executor import ShadowExecutor
from polyperps.execution.order_router import Portfolio
from polyperps.execution.sim_executor import SimExecutor
from polyperps.execution.types import AccountSnapshot, State
from polyperps.monitor.alerts import Alerter, SqliteSink
from polyperps.storage.db import (
    connect, get_positions_local, insert_candle, insert_fee, insert_funding, list_alerts, list_decisions,
    load_account_snapshot, save_account_snapshot,
)

T0 = datetime(2026, 9, 12, 0, 0, tzinfo=timezone.utc)
H = timedelta(hours=1)
NATIVE = SourceType.POLYMARKET_REST


def load():
    spec = importlib.util.spec_from_file_location("run_trader", "scripts/run_trader.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class FakeAccount:
    """The authenticated perps session, read side only: a flat account, no orders, a quiet event stream."""
    def __init__(self, equity="1000"):
        self.equity = Decimal(equity)
        self.closed = False

    async def fetch_portfolio(self):
        return SimpleNamespace(positions=(), margin=SimpleNamespace(total_account_value=self.equity),
                               in_liquidation=False)

    async def fetch_open_orders(self):
        return ()

    def __aiter__(self):
        async def gen():
            await asyncio.Event().wait()
            yield None  # never reached
        return gen()

    async def close(self):
        self.closed = True


class EndedStream(FakeAccount):
    def __aiter__(self):
        async def gen():
            return
            yield None  # makes this an async generator
        return gen()


class BrokenStream(FakeAccount):
    def __aiter__(self):
        async def gen():
            raise ConnectionError("socket closed")
            yield None
        return gen()


def test_live_exits_2_before_touching_the_wallet_while_any_lock_is_closed(monkeypatch, tmp_path):
    monkeypatch.setenv("POLYPERPS_INSTRUMENT_IDS", "6")
    monkeypatch.setenv("POLYPERPS_DB_PATH", str(tmp_path / "t.sqlite3"))
    monkeypatch.setattr(sys, "argv", ["run_trader.py", "--executor", "live", "--hypothesis", "h1"])
    mod = load()

    async def no_session(label):
        raise AssertionError("the wallet key must not be loaded while a lock is closed")

    monkeypatch.setattr(mod, "open_session", no_session)
    with pytest.raises(SystemExit) as e:
        mod.main()
    assert e.value.code == 2


def test_parser_defaults_and_modes():
    mod = load()
    args = mod.build_parser().parse_args(["--executor", "sim", "--hypothesis", "h1"])
    assert args.equity == "1000" and args.fee_category == "equity" and args.grid_index == 0
    for mode in ("sim", "shadow", "live"):
        assert mod.build_parser().parse_args(["--executor", mode, "--hypothesis", "h1"]).executor == mode
    assert mod.MODES == {}


def test_constants_pinned():
    mod = load()
    assert mod.HEARTBEAT_S == 20 and mod.RECONCILE_S == 60


async def test_build_executor_builds_each_mode_with_fakes():
    mod = load()
    conn = connect(":memory:")
    kw = dict(run_id="t", conn=conn, fee_rate=Decimal("0.0004"), equity=Decimal(1000), instrument_ids=[6])
    sim = await mod.build_executor("sim", **kw)
    assert isinstance(sim, SimExecutor) and sim.start_equity == Decimal(1000)
    shadow = await mod.build_executor("shadow", session=FakeAccount("1234"), **kw)
    assert isinstance(shadow, ShadowExecutor) and shadow.start_equity == Decimal(1234)
    with pytest.raises(GateClosed):                      # the constructor is still the real lock
        await mod.build_executor("live", session=FakeAccount(), **kw)


async def test_build_executor_keeps_the_loss_baseline_across_restarts():
    """Review focus 2: a restart after a -9 % day must not make the post-loss equity the new start."""
    mod = load()
    conn = connect(":memory:")
    save_account_snapshot(conn, "t", AccountSnapshot(equity=Decimal(910), positions=(), open_orders=(), stops={},
                                                     in_liquidation=False, ts=T0),
                          start_equity=Decimal(1000), executor="shadow")
    shadow = await mod.build_executor("shadow", run_id="t", conn=conn, fee_rate=Decimal("0.0004"),
                                      equity=Decimal(1000), instrument_ids=[6], session=FakeAccount("910"))
    assert shadow.start_equity == Decimal(1000)


def test_main_runs_once_and_lets_a_crash_exit(monkeypatch, tmp_path):
    """systemd (Restart=always) is the supervisor: a crash must leave the process so systemd
    counts it, not loop inside it. run_id is still minted once in main()."""
    monkeypatch.setenv("POLYPERPS_INSTRUMENT_IDS", "6")
    monkeypatch.setenv("POLYPERPS_DB_PATH", str(tmp_path / "t.sqlite3"))
    monkeypatch.setattr(sys, "argv", ["run_trader.py", "--executor", "sim", "--hypothesis", "h1"])
    mod = load()
    seen = []

    async def fake_run_once(args, settings):
        seen.append(args.run_id)
        raise RuntimeError("database is locked")

    monkeypatch.setattr(mod, "run_once", fake_run_once)
    with pytest.raises(RuntimeError, match="locked"):
        mod.main()
    assert len(seen) == 1 and seen[0].startswith("sim-")


def test_run_until_first_exits_propagates_a_dead_loop_and_cancels_the_rest():
    mod = load()
    cancelled = []

    async def forever():
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    async def dies():
        raise RuntimeError("database is locked")

    with pytest.raises(RuntimeError, match="locked"):
        asyncio.run(mod.run_until_first_exits(forever(), dies()))
    assert cancelled == [True]


def test_run_until_first_exits_returns_when_one_finishes_cleanly():
    mod = load()

    async def forever():
        await asyncio.sleep(3600)

    async def ends():
        return None

    asyncio.run(asyncio.wait_for(mod.run_until_first_exits(forever(), ends()), 5))


def test_event_stream_end_or_error_ends_the_run():
    """§4.10: when the live event stream ends or raises, the pump returns (or raises) and the
    whole run ends, so systemd restarts it and recovery runs."""
    mod = load()

    async def forever():
        await asyncio.sleep(3600)

    def pump(session):
        pf = Portfolio(run_id="t", executor=ShadowExecutor(session), conn=connect(":memory:"),
                       alerter=Alerter("t", []), routers={})
        return pf.run_event_pump()

    asyncio.run(asyncio.wait_for(mod.run_until_first_exits(forever(), pump(EndedStream())), 5))
    with pytest.raises(ConnectionError):
        asyncio.run(asyncio.wait_for(mod.run_until_first_exits(forever(), pump(BrokenStream())), 5))


# --- run_once end to end with fakes ------------------------------------------------------------

T1 = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


def _tick(minutes, mark="100"):
    ts = T1 + timedelta(minutes=minutes)
    return Tick(instrument_id=6, mark_price=Decimal(mark), index_price=Decimal(mark), last_price=Decimal(mark),
                funding_rate=Decimal("0.0000125"), next_funding=ts, exchange_ts=ts, received_ts=ts,
                source_type=SourceType.POLYMARKET_WS, sequence=minutes)


class FakePublicClient:
    """Public market data: instrument 6 and three ticks that close one hourly bar, then the stream ends."""
    async def fetch_instruments(self):
        return (Instrument(instrument_id=6, symbol="BTC", category="crypto", funding_interval="1h", max_leverage=20,
                           price_decimals=2, quantity_decimals=4, min_notional=Decimal("1"), isolated_only=True),)

    async def stream_ticks(self, ids):
        for m in (1, 30, 61):
            yield _tick(m)
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.2)     # let the bar and fast loops see the closed bar before the run ends

    async def close(self):
        pass


def _setup(monkeypatch, tmp_path, executor):
    monkeypatch.setenv("POLYPERPS_INSTRUMENT_IDS", "6")
    db_path = tmp_path / "t.sqlite3"
    monkeypatch.setenv("POLYPERPS_DB_PATH", str(db_path))
    conn = connect(db_path)
    insert_fee(conn, FeeSchedule(category="equity", taker_fee_rate=Decimal("0.0004"), maker_fee_rate=Decimal(0),
                                 fetched_at=T1))
    conn.close()
    mod = load()
    monkeypatch.setattr(mod, "PolymarketPerpsClient", SimpleNamespace(create_public=lambda **kw: FakePublicClient()))
    monkeypatch.setattr(mod, "_alerter", lambda run_id, conn: Alerter(run_id, [SqliteSink(conn)]))
    monkeypatch.setattr(mod, "HEARTBEAT_S", 0.01)
    monkeypatch.setattr(mod, "RECONCILE_S", 0.01)
    args = mod.build_parser().parse_args(["--executor", executor, "--hypothesis", "h1", "--run-id", "t"])
    return mod, args, db_path


def test_run_once_sim_wires_hooks_bars_and_account_snapshots(monkeypatch, tmp_path):
    mod, args, db_path = _setup(monkeypatch, tmp_path, "sim")
    asyncio.run(mod.run_once(args, mod.load_settings()))
    conn = connect(db_path)
    snap, start_equity, executor = load_account_snapshot(conn, "t")
    assert executor == "sim" and start_equity == Decimal(1000) and snap.equity == Decimal(1000)
    assert [d.note for d in list_decisions(conn, "t", 6)] == ["skip:warmup"]   # the closed bar reached the router


def test_run_once_shadow_reads_the_account_and_trips_the_loss_limit(monkeypatch, tmp_path):
    mod, args, db_path = _setup(monkeypatch, tmp_path, "shadow")
    conn = connect(db_path)
    save_account_snapshot(conn, "t", AccountSnapshot(equity=Decimal(1000), positions=(), open_orders=(), stops={},
                                                     in_liquidation=False, ts=T1),
                          start_equity=Decimal(1000), executor="shadow")
    conn.close()
    session = FakeAccount("890")                           # -11 % against the persisted baseline
    sdk = SimpleNamespace(closed=False)

    async def sdk_close():
        sdk.closed = True

    sdk.close = sdk_close

    async def fake_open_session(label):
        return sdk, session

    monkeypatch.setattr(mod, "open_session", fake_open_session)
    asyncio.run(mod.run_once(args, mod.load_settings()))
    conn = connect(db_path)
    snap, start_equity, executor = load_account_snapshot(conn, "t")
    assert executor == "shadow" and start_equity == Decimal(1000) and snap.equity == Decimal(890)
    assert ("CRITICAL", "loss_limit") in [(a[1], a[2]) for a in list_alerts(conn, "t")]
    assert get_positions_local(conn, "t")[6].state is State.HALTED
    assert session.closed and sdk.closed


def _candle(ts, close="100"):
    return Candle(instrument_id=6, interval="1h", open_ts=ts, open=Decimal("99"), high=Decimal("101"),
                  low=Decimal("98"), close=Decimal(close), volume=Decimal("1"), trades=1, received_ts=ts,
                  source_type=NATIVE)


def _funding(ts, rate="0.0001"):
    return FundingObservation(instrument_id=6, funding_rate=Decimal(rate), exchange_ts=ts, received_ts=ts,
                              source_type=NATIVE)


def test_seed_history_loads_closed_complete_bars_from_stored_candles():
    """C3: the strategy's warm-up is paid from stored 1h candles instead of waiting `lookback`
    hours of live ticks. Only complete (candle + funding) bars strictly before the current hour
    count; spread is the constant proxy the live builder also stamps."""
    mod = load()
    conn = connect(":memory:")
    now = T0 + 6 * H + timedelta(minutes=20)              # hour 6 is open: must not be seeded
    for h in range(7):
        insert_candle(conn, _candle(T0 + h * H, close=str(100 + h)))
        if h != 2:                                         # hour 2 has no settlement row -> incomplete
            insert_funding(conn, _funding(T0 + (h + 1) * H))
    builder = LiveBarBuilder()
    counts = mod.seed_history(conn, builder, {6: 4, 7: 4}, now=now, source_type=NATIVE)
    hist = builder.history(6)
    assert counts == {6: 4, 7: 0} and len(hist) == 4
    assert [b.open_ts for b in hist] == [T0 + h * H for h in (1, 3, 4, 5)]
    assert all(b.complete and b.spread_source == "constant" for b in hist)
    assert hist[-1].close == Decimal(105) and hist[-1].funding_rate == Decimal("0.0001")
    assert builder.history(7) == []


def test_seed_history_zero_bars_is_fine():
    mod = load()
    conn = connect(":memory:")
    builder = LiveBarBuilder()
    assert mod.seed_history(conn, builder, {6: 48}, now=T0, source_type=NATIVE) == {6: 0}
    assert builder.history(6) == []


def test_recover_strategies_tells_open_routers_their_side():
    """I4: after recover(), a router carrying a position tells its strategy which side it is
    on, so the strategy's internal _position matches the book instead of restarting at 0."""
    mod = load()

    class Recording:
        def __init__(self): self.calls = []
        def on_recover(self, sign): self.calls.append(sign)

    class NoHook:
        pass

    class R:
        def __init__(self, size, strategy): self.size, self.strategy = Decimal(size), strategy

    long_s, short_s, flat_s, plain = Recording(), Recording(), Recording(), NoHook()
    routers = {6: R("1", long_s), 7: R("-2", short_s), 8: R("0", flat_s), 9: R("1", plain)}
    mod._recover_strategies(routers)
    assert long_s.calls == [1] and short_s.calls == [-1] and flat_s.calls == []
```

Append to `tests/test_deploy_files.py`:

```python
def test_paper_unit_runs_the_trader_in_sim_mode():
    text = _read("polyperps-paper.service")
    assert "scripts/run_trader.py --executor sim " in text
    assert "--run-id ${PAPER_RUN_ID}" in text
    assert not (REPO_ROOT / "scripts" / "run_paper.py").exists()
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_run_trader_script.py tests/test_deploy_files.py -v`
Expected: FAIL. `build_executor` missing (AttributeError), `invalid choice: 'shadow'`, `MODES` missing, `startswith("sim-")` false, the paper unit still names `run_paper.py` (ExecStart script check and the new test).

- [ ] **Step 3: Implement**

In `polyperps/execution/order_router.py`, change `Portfolio.on_fast` to return the snapshot it used:

```python
    async def on_fast(self, marks: Mapping[int, Decimal]) -> AccountSnapshot:
        async with self._lock:
            snapshot = await self.executor.snapshot()
            for iid, mark in marks.items():
                router = self.routers.get(iid)
                if router is not None:
                    await router.on_fast(mark, snapshot)
            if self.start_equity is not None:
                # I7: once per tick, transitions only (None -> WARN -> CRITICAL and back), like I5.
                a = pnl_alert(snapshot.equity, self.start_equity, snapshot.ts)
                level = a.level if a is not None else None
                if a is not None and level != self._last_pnl_level:
                    self.alerter.emit(a)
                self._last_pnl_level = level
            # funding_drift_alert would go here (realised vs expected funding per instrument);
            # deferred to Phase 2b - the sim's funding is the predicted rate, so it can never drift.
            return snapshot
```

and change the comment on line 138 from `run_paper seeds history` to `run_trader seeds history`.

In `polyperps/execution/sim_executor.py` `check_triggers` docstring, change `(run_paper's fast loop)` to `(run_trader's fast loop, via poll_fills)`. In `polyperps/execution/live_bars.py` `close_all` docstring, change `run_paper.py` to `run_trader.py`.

Replace `scripts/run_trader.py` with:

```python
"""One trader for sim, shadow and live (Phase 2b Part A spec §3): the exact router, guards,
reconciliation and recovery path, with the executor chosen by --executor.

    POLYPERPS_INSTRUMENT_IDS=6,7 .venv/Scripts/python scripts/run_trader.py --executor sim --hypothesis h1

sim     SimExecutor, a paper account kept in the DB. Public WS only; no credentials.
shadow  the real account, read-only: decisions run, every would-be order is recorded as
        shadow_refused and its instrument halts. Needs POLYMARKET_PRIVATE_KEY on the box.
        Refuses to start (RecoveryHalt) while the account holds a position without a stop or
        an open order this run did not place - recovery would have to write to fix those.
live    LiveExecutor. Exits 2 unless all three locks are open (they are not in Part A).
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


async def build_executor(mode: str, *, run_id: str, conn, fee_rate: Decimal, equity: Decimal,
                         instrument_ids: Sequence[int], session: Any = None) -> Executor:
    """sim: the paper account from the DB (or a fresh one). shadow/live: the real account through
    `session`; live raises GateClosed unless all three locks are open for every instrument."""
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
    ex.start_equity = prev[1] if prev is not None else (await ex.snapshot()).equity
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
    if args.hypothesis == "h2":
        raise SystemExit("h2 needs a live proxy feed; not wired")
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
```

In `deploy/polyperps-paper.service`, change the Description line to
`Description=polyperps paper execution (run_trader.py, SimExecutor only)`
and the ExecStart line to
`ExecStart=/opt/polyperps/.venv/bin/python scripts/run_trader.py --executor sim --hypothesis ${PAPER_HYPOTHESIS} --run-id ${PAPER_RUN_ID}`
(keep LF line endings: edit with the Edit tool, never a Windows editor).

In `deploy/env.example`, change `# SQLite path shared by run_feed.py and run_paper.py.` to `# SQLite path shared by run_feed.py and run_trader.py.`

In `README.md`, replace the whole `## Phase 2a — paper execution (no live orders)` section body (from `Spec: ...phase2a-design.md` through `...set in Phase 2b from a passing native record).`) with:

```markdown
Specs: `docs/superpowers/specs/2026-09-12-polyperps-phase2a-design.md`,
`docs/superpowers/specs/2026-09-30-polyperps-phase2b-parta-design.md`. One runner,
`scripts/run_trader.py`, drives the router, guards, reconciliation, recovery and alerts in
every mode; only the last mile changes. Pre-registered limits: 3x leverage, 25 %
liquidation-distance floor, 15 % exchange-side stop, 2 % funding-cost exit, gross exposure
1.0x / cluster net 0.6x equity, loss limit -5 % pause / -10 % flatten and halt,
kill-switch divergence thresholds `None`.

| Step | Command |
|------|---------|
| paper run (sim) | `POLYPERPS_INSTRUMENT_IDS=6,7 .venv/Scripts/python scripts/run_trader.py --executor sim --hypothesis h1 --run-id soak-2026-09-13` |
| shadow run (real account, read-only; needs the wallet key) | same with `--executor shadow --run-id shadow-1` |
| clear a halted (or liquidated) instrument (human decision) | add `--clear-halt 6` to the run command |
| Telegram CRITICAL alerts (optional) | put TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID files in /etc/credstore/ (root:root 0600) |

`--run-id` is required for a soak: it names the account rows recovery reads, so a restart
(manual or systemd) reopens the same book. Without it a fresh id is minted per process
start. Strategy warm-up is seeded from stored 1h candles at start (`seeded N bars for
instrument I` in the log); until the history is long enough the router writes
`skip:warmup` decisions and sends nothing.

`--executor live` exits 2 while any of the three locks is closed. `LiveExecutor` cannot be
constructed unless all three are open, and re-checks them on every order.
```

- [ ] **Step 4: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/test_run_trader_script.py tests/test_deploy_files.py tests/test_order_router.py -v`
Expected: PASS.

- [ ] **Step 5: Run the full suite**

Run: `.venv/Scripts/python -m pytest -q`
Expected: all pass, 0 failed. Also run `grep -rn "run_paper" --include=*.py --include=*.service --include=*.sh --include=*.example polyperps scripts tests deploy` and expect no output.

- [ ] **Step 6: Commit**

```bash
git add scripts/run_trader.py tests/test_run_trader_script.py tests/test_deploy_files.py polyperps/execution/order_router.py polyperps/execution/sim_executor.py polyperps/execution/live_bars.py deploy/polyperps-paper.service deploy/env.example README.md
git commit -m "$(cat <<'EOF'
feat(runner): run_trader.py drives sim, shadow and live; replaces run_paper.py

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Stop invariant in reconciliation and recovery

**Files:**
- Modify: `polyperps/execution/reconciliation.py:1-65`
- Modify: `polyperps/execution/order_router.py:21-24` (import `PositionView`), new module function after `apply_guards` (`:41-50`), `Portfolio._reconcile` (`:421-454`)
- Modify: `polyperps/execution/state_recovery.py:1-138`
- Test: `tests/test_reconciliation.py`, `tests/test_state_recovery.py`

**Interfaces:**
- Consumes: `stop_price(side=, entry=, limits=)` (existing), `ShadowRefused` catch in `_reconcile` (Task 1).
- Produces:
  - `order_router.place_exchange_stop(executor: Executor, pos: PositionView, limits: RiskLimits = LIMITS) -> Decimal` (places the stop at the stop distance from the EXCHANGE entry; returns the trigger)
  - `reconciliation.diff` raises `missing_stop` for every instrument with remote size ≠ 0 and no remote stop, whatever the local state (including no row)
  - `recover()` applies the invariant to every instrument (HALTED and LIQUIDATED rows keep their state but adopt the exchange size/stop; unknown instruments get a stop)

Decision: recovery preserves LIQUIDATED as well as HALTED (Decision 12). Decision: positions on instruments with no router also get a stop (Decision 13).

- [ ] **Step 1: Write the failing tests**

In `tests/test_reconciliation.py` add imports `import pytest`, `from polyperps.execution.types import OrderRequest`, `from polyperps.risk.liquidation_guard import stop_price`.

Replace the second half of `test_diff_stop_drift_and_missing_stop_and_unknown_position` (the line `assert [m.kind for m in ms] == ["missing_stop", "size"]` and the next one) with:

```python
    assert [(m.kind, m.instrument_id) for m in ms] == [("missing_stop", 6), ("missing_stop", 8), ("size", 8)]
    assert ms[2].local == "0"
```

Replace `test_diff_skips_size_and_stop_checks_for_pending_local_rows` with:

```python
def test_diff_skips_size_checks_for_pending_rows_but_never_the_stop():
    # An order is in flight (ENTRY_PENDING): the persisted size is still pre-fill (0) while the
    # remote already reflects the fill. The size is the fill handler's business - but a venue
    # position without a venue stop is a mismatch whatever the local state (Part A §4).
    ms = diff(local={6: local(6, State.ENTRY_PENDING, "0")}, remote=remote([pv(6, "1")]),
              run_id="r", known_orders=set())
    assert [m.kind for m in ms] == ["missing_stop"]
    assert diff(local={6: local(6, State.ENTRY_PENDING, "0")}, remote=remote([pv(6, "1")], stops={6: Decimal(85)}),
                run_id="r", known_orders=set()) == []
```

Append:

```python
STATES = [State.OPEN, State.HALTED, State.LIQUIDATED, State.ENTRY_PENDING, State.EXIT_PENDING, State.FLAT]


@pytest.mark.parametrize("state", STATES + [None])
def test_missing_stop_is_raised_for_every_local_state(state):
    loc = {} if state is None else {6: local(6, state, "1")}
    ms = diff(local=loc, remote=remote([pv(6, "1")]), run_id="r", known_orders=set())
    assert [m.kind for m in ms if m.instrument_id == 6].count("missing_stop") == 1


@pytest.mark.parametrize("state", STATES)
async def test_reconcile_restores_the_stop_from_the_exchange_entry_for_every_state(state):
    conn, ex, router, pf = await make_open()
    await ex.cancel_stop(6)                                   # the venue lost our stop
    router.state = state
    router._persist()
    ms = await pf.reconcile_now()
    assert "missing_stop" in [m.kind for m in ms]
    entry = (await ex.snapshot()).position(6).entry_price
    assert (await ex.snapshot()).stops == {6: stop_price(side="long", entry=entry)}


async def test_reconcile_restores_the_stop_for_a_position_with_no_router():
    conn, ex, router, pf = await make_open()
    ex.update_mark(7, Decimal(50))
    await ex.submit(OrderRequest(client_order_id="elsewhere", instrument_id=7, side="sell", quantity=Decimal(2),
                                 reduce_only=False, ts=T0))
    ex.drain_events()
    await pf.reconcile_now()
    entry7 = (await ex.snapshot()).position(7).entry_price
    assert (await ex.snapshot()).stops[7] == stop_price(side="short", entry=entry7)
```

In `tests/test_state_recovery.py` add `from polyperps.risk.liquidation_guard import stop_price`, add `assert rep.stops_replaced == [8]` at the end of `test_unknown_remote_position_is_critical`, and append:

```python
@pytest.mark.parametrize("state", [None, State.FLAT, State.ENTRY_PENDING, State.OPEN, State.EXIT_PENDING,
                                   State.HALTED, State.LIQUIDATED])
async def test_recovery_guards_every_venue_position_with_a_stop(state):
    conn, ex, alerter, router = setup()
    await ex.submit(OrderRequest(client_order_id="r-6-1", instrument_id=6, side="buy", quantity=Decimal(1),
                                 reduce_only=False, ts=T0))
    ex.drain_events()
    if state is not None:
        upsert_position_local(conn, PositionLocalRow(run_id="r", instrument_id=6, state=state, size=Decimal(0),
                                                     entry_price=None, stop_trigger=None, stop_order_id=None,
                                                     cumulative_funding=Decimal(0), updated_at=T0))
    rep = await recover(conn=conn, run_id="r", executor=ex, routers={6: router}, alerter=alerter, clock=lambda: T0)
    entry = (await ex.snapshot()).position(6).entry_price
    assert (await ex.snapshot()).stops == {6: stop_price(side="long", entry=entry)} and rep.stops_replaced == [6]
    assert router.size == 1 and router.stop_trigger == stop_price(side="long", entry=entry)
    frozen = state in (State.HALTED, State.LIQUIDATED)
    assert router.state is (state if frozen else State.OPEN)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_reconciliation.py tests/test_state_recovery.py -v`
Expected: FAIL. `missing_stop` absent for non-OPEN states; HALTED/LIQUIDATED recovery places no stop (and LIQUIDATED becomes OPEN); `stops_replaced == [8]` fails.

- [ ] **Step 3: Implement**

Replace `polyperps/execution/reconciliation.py` with:

```python
"""Spec 2.4: local rows vs the executor's view. Pure diff; responses live in Portfolio.reconcile_now().

Amendment vs spec section 7.1: `liq_price_drift` is replaced by `stop_drift` (we do not
store a local liquidation price; we do store our stop trigger).

Invariant (Phase 2b Part A §4): a venue position always has a venue stop. `missing_stop` is
raised for every instrument whose remote size is non-zero and that has no remote stop, whatever
the local row says (OPEN, HALTED, LIQUIDATED, a pending state, FLAT, or no row at all).

Controller ruling: a local row in ENTRY_PENDING/EXIT_PENDING has an order in flight, so its
persisted size is transiently stale by design - the fill handler owns that row's size once the
fill lands. diff() therefore skips the size and stop_drift comparisons for such rows (never the
missing_stop check); unknown_order detection does not consult local row state."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from polyperps.execution.types import AccountSnapshot, PositionLocalRow, State

Kind = Literal["size", "unknown_order", "missing_stop", "stop_without_position", "stop_drift"]


@dataclass(frozen=True, slots=True, kw_only=True)
class Mismatch:
    kind: Kind
    instrument_id: int | None
    local: str
    remote: str


def diff(
    *,
    local: Mapping[int, PositionLocalRow],
    remote: AccountSnapshot,
    run_id: str,
    known_orders: set[str],
    stop_drift_tolerance: Decimal = Decimal("0.05"),
) -> list[Mismatch]:
    out: list[Mismatch] = []
    remote_pos = {p.instrument_id: p for p in remote.positions}
    for iid in sorted(set(local) | set(remote_pos)):
        lrow = local.get(iid)
        rsize = remote_pos[iid].size if iid in remote_pos else Decimal(0)
        if rsize != 0 and iid not in remote.stops:
            lstop = lrow.stop_trigger if lrow is not None else None
            out.append(Mismatch(kind="missing_stop", instrument_id=iid, local=str(lstop), remote="none"))
        if lrow is not None and lrow.state in (State.ENTRY_PENDING, State.EXIT_PENDING):
            continue  # order in flight; fill handler owns this row's size
        lsize = lrow.size if lrow is not None else Decimal(0)
        if lsize != rsize:
            out.append(Mismatch(kind="size", instrument_id=iid, local=str(lsize), remote=str(rsize)))
            continue
        if rsize != 0 and lrow is not None and lrow.state is State.OPEN:
            rstop = remote.stops.get(iid)
            lstop = lrow.stop_trigger
            if rstop is not None and lstop is not None and lstop != 0 and abs(rstop - lstop) / lstop > stop_drift_tolerance:
                out.append(Mismatch(kind="stop_drift", instrument_id=iid, local=str(lstop), remote=str(rstop)))
    for iid, trig in remote.stops.items():
        if iid not in remote_pos or remote_pos[iid].size == 0:
            out.append(Mismatch(kind="stop_without_position", instrument_id=iid, local="none", remote=str(trig)))
    prefix = f"{run_id}-"
    for oid in remote.open_orders:
        if not oid.startswith(prefix) or oid not in known_orders:
            out.append(Mismatch(kind="unknown_order", instrument_id=None, local="none", remote=oid))
    return out
```

In `polyperps/execution/order_router.py`:
- add `PositionView` to the `polyperps.execution.types` import.
- add after `apply_guards`:

```python
async def place_exchange_stop(executor: Executor, pos: PositionView, limits: RiskLimits = LIMITS) -> Decimal:
    """Invariant (Part A §4): a venue position always has a venue stop. Placed at the router's stop
    distance from the EXCHANGE entry price, so it holds whatever our local row says."""
    trigger = stop_price(side="long" if pos.size > 0 else "short", entry=pos.entry_price, limits=limits)
    await executor.place_stop(pos.instrument_id, trigger)
    return trigger
```

- in `Portfolio._reconcile`, replace the `missing_stop` branch

```python
                elif m.kind == "missing_stop":
                    if router is not None:
                        await router.replace_stop()
                    self.alerter.emit(Alert(level="WARN", kind="stop_missing", instrument_id=m.instrument_id,
                                            detail=detail, ts=now))
```

with

```python
                elif m.kind == "missing_stop":
                    pos = snapshot.position(m.instrument_id)
                    trigger = await place_exchange_stop(self.executor, pos,
                                                        router.limits if router is not None else LIMITS)
                    if router is not None:
                        router.stop_trigger = trigger
                        router._persist()
                    self.alerter.emit(Alert(level="WARN", kind="stop_missing", instrument_id=m.instrument_id,
                                            detail={**detail, "trigger": str(trigger)}, ts=now))
```

Replace `polyperps/execution/state_recovery.py` with:

```python
"""Spec 2.4b: rebuild router state from the executor (exchange = truth) before any strategy runs.

Part A §4.2: every instrument is read, HALTED and LIQUIDATED rows included. Those two keep their
state (a human clears them) but adopt the exchange size, and every venue position - ours, frozen
or unknown - ends recovery with a venue stop."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal

from polyperps.execution.executor import Executor
from polyperps.execution.order_router import InstrumentRouter, place_exchange_stop
from polyperps.execution.types import State
from polyperps.monitor.alerts import Alert, Alerter
from polyperps.storage import db

_FROZEN = (State.HALTED, State.LIQUIDATED)


class RecoveryHalt(RuntimeError):
    pass


@dataclass
class RecoveryReport:
    adopted: list[str] = field(default_factory=list)
    abandoned: list[str] = field(default_factory=list)
    cancelled: list[str] = field(default_factory=list)
    stops_replaced: list[int] = field(default_factory=list)
    unknown_positions: list[int] = field(default_factory=list)
    adopted_untracked: list[int] = field(default_factory=list)   # I8: venue position, no/FLAT local row
    states: dict[int, str] = field(default_factory=dict)
    failed: str | None = None

    def to_dict(self) -> dict:
        return {"adopted": self.adopted, "abandoned": self.abandoned, "cancelled": self.cancelled,
                "stops_replaced": self.stops_replaced, "unknown_positions": self.unknown_positions,
                "adopted_untracked": self.adopted_untracked, "states": self.states, "failed": self.failed}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def recover(
    *,
    conn,
    run_id: str,
    executor: Executor,
    routers: Mapping[int, InstrumentRouter],
    alerter: Alerter,
    clock: Callable[[], datetime] = _utcnow,
) -> RecoveryReport:
    try:
        snap = await executor.snapshot()
    except Exception as exc:
        raise RecoveryHalt(f"executor snapshot unavailable: {type(exc).__name__}") from exc

    rep = RecoveryReport()
    current_iid: int | None = None
    try:
        local = db.get_positions_local(conn, run_id)

        for iid, router in routers.items():
            current_iid = iid
            row = local.get(iid)
            if row is not None:
                router.load_local(row)
            frozen = row is not None and row.state in _FROZEN
            pos = snap.position(iid)
            if pos is not None and pos.size != 0:
                if not frozen and (row is None or row.state is State.FLAT):
                    # I8: we never recorded opening this. The exchange is truth, so adopt it under
                    # a fresh stop - but an operator must know the book moved without us.
                    rep.adopted_untracked.append(iid)
                    alerter.emit(Alert(level="WARN", kind="adopted_untracked", instrument_id=iid,
                                       detail={"instrument_id": str(iid), "size": str(pos.size),
                                               "entry": str(pos.entry_price)}, ts=clock()))
                router.size, router.entry, router.cumulative_funding = pos.size, pos.entry_price, pos.cumulative_funding
                if not frozen:
                    router.state = State.OPEN
                if iid in snap.stops:
                    router.stop_trigger = snap.stops[iid]
                else:
                    router.stop_trigger = await place_exchange_stop(executor, pos, router.limits)
                    rep.stops_replaced.append(iid)
            else:
                router.size, router.entry, router.stop_trigger = Decimal(0), None, None
                if not frozen:
                    router.state = State.FLAT
            router._persist()
            rep.states[iid] = router.state.value
        current_iid = None

        for pos in snap.positions:
            if pos.size != 0 and pos.instrument_id not in routers:
                current_iid = pos.instrument_id
                rep.unknown_positions.append(pos.instrument_id)
                alerter.emit(Alert(level="CRITICAL", kind="unknown_position", instrument_id=pos.instrument_id,
                                   detail={"size": str(pos.size)}, ts=clock()))
                if pos.instrument_id not in snap.stops:
                    await place_exchange_stop(executor, pos)
                    rep.stops_replaced.append(pos.instrument_id)
        current_iid = None

        prefix = f"{run_id}-"
        for o in db.list_orders(conn, run_id):
            if o.status not in ("submitting", "accepted"):
                continue
            current_iid = o.instrument_id
            resting = o.client_order_id in snap.open_orders
            if resting:
                router = routers.get(o.instrument_id)
                pending_row = local.get(o.instrument_id)
                if router is not None and pending_row is not None and pending_row.state in (
                        State.ENTRY_PENDING, State.EXIT_PENDING):
                    router.adopt_pending(o.client_order_id, pending_row.state)
                status = "adopted"
            else:
                status = "abandoned"
            db.upsert_order(conn, replace(o, status=status, updated_at=clock()))
            (rep.adopted if status == "adopted" else rep.abandoned).append(o.client_order_id)
        current_iid = None
        for oid in snap.open_orders:
            if not oid.startswith(prefix):
                await executor.cancel(oid)
                rep.cancelled.append(oid)

        # adopt_pending() (in the order loop above) can move a router past the state the
        # per-router loop recorded (e.g. FLAT -> ENTRY_PENDING for a still-resting order), so
        # re-derive from the routers themselves rather than trust the earlier snapshot.
        rep.states = {iid: r.state.value for iid, r in routers.items()}
    except Exception as exc:
        rep.failed = f"{type(exc).__name__}: {exc}"
        db.insert_recovery(conn, run_id=run_id, ts=clock(), findings_json=json.dumps(rep.to_dict()))
        alerter.emit(Alert(level="CRITICAL", kind="recovery_failed", instrument_id=current_iid,
                           detail={"error": type(exc).__name__}, ts=clock()))
        raise RecoveryHalt(f"recovery failed: {type(exc).__name__}: {exc}") from exc

    db.insert_recovery(conn, run_id=run_id, ts=clock(), findings_json=json.dumps(rep.to_dict()))
    alerter.emit(Alert(level="INFO", kind="recovery", instrument_id=None,
                       detail={"states": rep.states, "abandoned": len(rep.abandoned)}, ts=clock()))
    return rep
```

- [ ] **Step 4: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/test_reconciliation.py tests/test_state_recovery.py tests/test_order_router.py -v`
Expected: PASS.

- [ ] **Step 5: Run the full suite**

Run: `.venv/Scripts/python -m pytest -q`
Expected: all pass, 0 failed.

- [ ] **Step 6: Commit**

```bash
git add polyperps/execution/reconciliation.py polyperps/execution/order_router.py polyperps/execution/state_recovery.py tests/test_reconciliation.py tests/test_state_recovery.py
git commit -m "$(cat <<'EOF'
fix(execution): every venue position gets a venue stop, whatever the local state

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Pending timeout, late fills, submit errors, per-order gate

**Files:**
- Modify: `polyperps/execution/order_router.py` (`_TERMINAL_ORDER_STATUSES` `:34`, new constants, `InstrumentRouter.__init__` `:69-82`, `_send` `:200-234`, `handle_event` `:266-307`, `adopt_pending` `:343-350`, new `check_pending` / `_adopt`, `Portfolio.on_fast`)
- Modify: `polyperps/execution/live_executor.py` (`LiveExecutor.__init__`, `submit`)
- Test: `tests/test_order_router.py`, `tests/test_live_executor.py`

**Interfaces:**
- Consumes: `place_exchange_stop(executor, pos, limits)` (Task 5), `ShadowRefused` (Task 1), `Portfolio.on_fast -> AccountSnapshot` (Task 4).
- Produces:
  - `order_router.PENDING_TIMEOUT_S = 30`
  - `order_router._GAVE_UP_STATUSES = frozenset({"lost", "error", "shadow_refused", "adopted"})`
  - `InstrumentRouter.check_pending(snapshot: AccountSnapshot) -> None` (called by `Portfolio.on_fast` for every router before the per-mark checks)
  - `InstrumentRouter._adopt(snapshot: AccountSnapshot) -> None` (exchange size → local size; stop ensured; pending cleared)
  - `InstrumentRouter._pending_since: datetime | None`
  - Order row statuses `error`, `shadow_refused`, `adopted` (pending timeout); alerts `pending_timeout` (WARN), `submit_error` (CRITICAL), `late_fill` (WARN)
  - `LiveExecutor.submit` raises `GateClosed` when the instrument's gate is closed at submit time.

Decision: a late fill for an order we gave up on re-reads the exchange instead of adding the fill, and order updates never rewrite those rows (Decision 11).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_order_router.py` (add imports: `import pytest`, `from types import SimpleNamespace`, `from polyperps.execution.executor import GateClosed, ShadowRefused`, `from polyperps.execution.live_executor import ShadowExecutor`, `from polyperps.execution.types import OrderAck`, `from polyperps.risk.liquidation_guard import stop_price`):

```python
class AckOnlyExecutor(SimExecutor):
    """The venue acks, but no fill ever happens."""
    async def submit(self, order):
        return OrderAck(client_order_id=order.client_order_id, exchange_order_id="x-1", status="accepted",
                        reason="", ts=self._clock())


class RaisingExecutor(SimExecutor):
    exc: Exception = RuntimeError("boom")

    async def submit(self, order):
        raise self.exc


class FillThenRaise(SimExecutor):
    async def submit(self, order):
        await super().submit(order)                  # the order reached the venue and filled...
        raise RuntimeError("connection reset")       # ...but the call errored on the way back


async def test_pending_timeout_adopts_a_lost_fill_and_places_the_stop():
    now = [T0]
    conn, ex, strat, router, pf = make(clock=lambda: now[0])
    await pf.on_bar({6: [bar(0)]}, "run")
    ex.drain_events()                                    # the fill never reaches us
    assert router.state is State.ENTRY_PENDING
    now[0] = T0 + timedelta(seconds=30)
    await pf.on_fast({6: Decimal(100)})
    assert router.state is State.ENTRY_PENDING           # not yet: strictly longer than 30 s
    now[0] = T0 + timedelta(seconds=31)
    await pf.on_fast({6: Decimal(100)})
    assert router.state is State.OPEN and router.size == 1
    entry = (await ex.snapshot()).position(6).entry_price
    assert (await ex.snapshot()).stops == {6: stop_price(side="long", entry=entry)}
    assert get_order(conn, "r-6-1").status == "adopted" and "pending_timeout" in kinds(conn)


async def test_pending_timeout_with_nothing_on_the_exchange_goes_flat():
    now = [T0]
    conn, ex, strat, router, pf = make(clock=lambda: now[0], executor_cls=AckOnlyExecutor)
    await pf.on_bar({6: [bar(0)]}, "run")
    assert router.state is State.ENTRY_PENDING
    now[0] = T0 + timedelta(seconds=31)
    await pf.on_fast({6: Decimal(100)})
    assert router.state is State.FLAT and router.size == 0
    assert get_order(conn, "r-6-1").status == "adopted"


async def test_late_fill_after_pending_timeout_is_not_counted_twice():
    """Review focus 1: the fill was delayed, not lost."""
    now = [T0]
    conn, ex, strat, router, pf = make(clock=lambda: now[0])
    await pf.on_bar({6: [bar(0)]}, "run")
    held = ex.drain_events()
    now[0] = T0 + timedelta(seconds=31)
    await pf.on_fast({6: Decimal(100)})
    assert router.state is State.OPEN and router.size == 1
    for ev in held:
        await pf.dispatch(ev)
    assert router.size == 1 and router.state is State.OPEN and "late_fill" in kinds(conn)
    assert get_order(conn, "r-6-1").status == "adopted"


async def test_late_fill_while_halted_places_a_stop_and_stays_halted():
    conn, ex, strat, router, pf = make()
    ex.fail_queue = ["drop", "drop"]
    await pf.on_bar({6: [bar(0)]}, "run")
    assert router.state is State.HALTED and get_order(conn, "r-6-1").status == "lost"
    await ex.submit(OrderRequest(client_order_id="r-6-1", instrument_id=6, side="buy", quantity=Decimal(1),
                                 reduce_only=False, ts=T0))  # the "lost" order lands after all
    await pump(pf, ex)
    assert router.state is State.HALTED and router.size == 1
    entry = (await ex.snapshot()).position(6).entry_price
    assert (await ex.snapshot()).stops == {6: stop_price(side="long", entry=entry)}


async def test_foreign_fill_while_halted_moves_the_size_and_places_a_stop():
    conn, ex, strat, router, pf = make()
    await router.halt("operator test")
    await router.handle_event(FillUpdate(client_order_id="foreign-1", instrument_id=6, side="buy",
                                         quantity=Decimal(1), price=Decimal(100), fee=Decimal(0), ts=T0))
    assert router.state is State.HALTED and router.size == 1
    assert (await ex.snapshot()).stops == {6: stop_price(side="long", entry=Decimal(100))}


@pytest.mark.parametrize("exc,status", [
    (GateClosed("instrument 6: POLYMARKET_LIVE_TRADING is not exactly 'true'"), "error"),
    (RuntimeError("connection reset"), "error"),
    (ShadowRefused("shadow"), "shadow_refused"),
])
async def test_submit_error_marks_the_order_alerts_and_halts(exc, status):
    conn, ex, strat, router, pf = make(executor_cls=RaisingExecutor)
    ex.exc = exc
    await pf.on_bar({6: [bar(0)]}, "run")
    assert router.state is State.HALTED and router.size == 0
    assert get_order(conn, "r-6-1").status == status
    crit = [a[2] for a in list_alerts(conn, "r") if a[1] == "CRITICAL"]
    assert "submit_error" in crit and "halted" in crit


async def test_fill_after_submit_error_is_not_counted_twice():
    """Review focus 1: the order landed before the call errored; its fill event arrives after we
    already adopted the exchange size."""
    conn, ex, strat, router, pf = make(executor_cls=FillThenRaise)
    await pf.on_bar({6: [bar(0)]}, "run")
    assert router.state is State.HALTED and router.size == 1                 # adopted from the exchange
    assert (await ex.snapshot()).stops == {6: stop_price(side="long", entry=router.entry)}
    await pump(pf, ex)                                                       # the fill event arrives late
    assert router.size == 1 and router.state is State.HALTED and "late_fill" in kinds(conn)
    assert get_order(conn, "r-6-1").status == "error"


class FlatAccount:
    """The real account as a perps session sees it: flat, no orders."""
    async def fetch_portfolio(self):
        return SimpleNamespace(positions=(), margin=SimpleNamespace(total_account_value=Decimal(1000)),
                               in_liquidation=False)

    async def fetch_open_orders(self):
        return ()


async def test_shadow_records_the_would_be_order_and_halts():
    conn = connect(":memory:")
    ex = ShadowExecutor(FlatAccount(), clock=lambda: T0)
    alerter = Alerter("r", [SqliteSink(conn)])
    router = InstrumentRouter(run_id="r", instrument_id=6, category="crypto", strategy=Strat(1), executor=ex,
                              conn=conn, alerter=alerter, categories=CATS, clock=lambda: T0)
    pf = Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter, routers={6: router})
    await pf.on_bar({6: [bar(0)]}, "run")
    o = get_order(conn, "r-6-1")
    assert o.status == "shadow_refused" and o.side == "buy" and router.state is State.HALTED
    assert list_decisions(conn, "r", 6)[0].client_order_id == "r-6-1"
```

Append to `tests/test_live_executor.py`:

```python
async def test_submit_rechecks_the_gate_on_every_order():
    s = FakeSession()
    lock = {"open": True}
    ex = LiveExecutor(s, instrument_ids=[6], modes={}, gate=lambda iid: GateDecision(lock["open"], "env flipped"),
                      clock=lambda: T0)
    lock["open"] = False
    with pytest.raises(GateClosed):
        await ex.submit(_req())
    assert not any(c[0] == "place_order" for c in s.calls)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_order_router.py tests/test_live_executor.py -v`
Expected: FAIL. The pending router never times out (`State.ENTRY_PENDING` after 31 s); submit errors propagate out of `pf.on_bar` (`RuntimeError: connection reset`, `GateClosed`, `ShadowRefused`); the HALTED late fill places no stop; `submit` places the order with the gate closed.

- [ ] **Step 3: Implement**

In `polyperps/execution/order_router.py`:
- replace line 34 and add constants:

```python
_TERMINAL_ORDER_STATUSES = frozenset({"filled", "cancelled", "auto_cancelled", "rejected", "lost", "error",
                                      "shadow_refused"})
# Orders we stopped waiting for and replaced with the exchange's size. A later fill for one of these
# re-reads the exchange instead of adding to a size that may already include it (Part A §4.4/§4.5).
_GAVE_UP_STATUSES = frozenset({"lost", "error", "shadow_refused", "adopted"})
PENDING_TIMEOUT_S = 30
```

- change the import to `from polyperps.execution.executor import Executor, ExecutorTimeout, ShadowRefused` (already there from Task 1) and ensure `OrderRow` is imported from `polyperps.execution.types` (it is).
- in `InstrumentRouter.__init__`, after `self._pending_cid: str | None = None` add
  `self._pending_since: datetime | None = None`
- replace `_send` with:

```python
    async def _send(self, intent: Intent, *, target: Decimal | None, verdicts: dict[str, str]) -> None:
        self.seq += 1   # order-send counter: only real sends consume a client_order_id slot
        cid = f"{self.run_id}-{self.instrument_id}-{self.seq}"
        self._record(target=target, verdicts=verdicts, intent=intent, cid=cid, note=intent.reason)
        now = self.clock()
        db.upsert_order(self.conn, OrderRow(client_order_id=cid, run_id=self.run_id, instrument_id=self.instrument_id,
                                            side=intent.side, quantity=intent.quantity, reduce_only=intent.reduce_only,
                                            status="submitting", exchange_order_id=None, filled_quantity=Decimal(0),
                                            avg_price=None, submitted_at=now, updated_at=now, reason=intent.reason))
        prior = self.state
        self._pending_cid = cid
        self._pending_since = now
        self._set_state(State.EXIT_PENDING if intent.reduce_only else State.ENTRY_PENDING)
        # Captured before the await: with a live executor, the event pump can apply this same
        # order's WS fill through handle_event() (updating self.size) while we're still waiting
        # on the REST ack, so anything computed from self.size after the await would be racy.
        size_before = self.size
        req = OrderRequest(client_order_id=cid, instrument_id=self.instrument_id, side=intent.side,
                           quantity=intent.quantity, reduce_only=intent.reduce_only, ts=now)
        try:
            ack = await self._submit_with_recovery(req, size_before)
        except Exception as exc:
            # Part A §4.5: any submit error other than a timeout (GateClosed, ShadowRefused, a
            # transport error). The order may or may not be on the venue: take the venue's size as
            # ours, guard it, and stop trading this instrument until a human looks.
            self._update_order(cid, status="shadow_refused" if isinstance(exc, ShadowRefused) else "error",
                               reason=f"{type(exc).__name__}: {exc}")
            self._alert("CRITICAL", "submit_error", client_order_id=cid, error=type(exc).__name__)
            await self._adopt(await self.executor.snapshot())
            await self.halt(f"submit error: {type(exc).__name__}")
            return
        if ack is None:
            self._update_order(cid, status="lost", reason="no ack after timeout and retry")
            await self.halt("order lost after timeout and retry")
            return
        if ack.status == "rejected":
            self._update_order(cid, status="rejected", reason=ack.reason)
            self._alert("WARN", "order_rejected", client_order_id=cid, reason=ack.reason)
            self._pending_cid = None
            self._set_state(prior)
            return
        # skip_if_terminal: if the event pump already raced this fill through handle_event()
        # (see size_before above), the row is already "filled" - don't downgrade it back to
        # "accepted".
        self._update_order(cid, status="accepted", exchange_order_id=ack.exchange_order_id,
                           reason=ack.reason or intent.reason, skip_if_terminal=True)
```

- add after `_update_order`:

```python
    async def check_pending(self, snapshot: AccountSnapshot) -> None:
        """Part A §4.3: a router must not sit in a pending state forever waiting for a fill that
        never arrives. After PENDING_TIMEOUT_S, adopt the exchange's size."""
        if self.state not in (State.ENTRY_PENDING, State.EXIT_PENDING) or self._pending_since is None:
            return
        if (self.clock() - self._pending_since).total_seconds() <= PENDING_TIMEOUT_S:
            return
        cid = self._pending_cid
        self._alert("WARN", "pending_timeout", client_order_id=cid, state=self.state.value)
        if cid is not None:
            self._update_order(cid, status="adopted", reason="pending timeout: adopted the exchange size")
        await self._adopt(snapshot)
        self._set_state(State.OPEN if self.size != 0 else State.FLAT)
        if self.size == 0:
            hook = getattr(self.strategy, "on_flatten", None)
            if callable(hook):
                hook()

    async def _adopt(self, snapshot: AccountSnapshot) -> None:
        """Take the exchange's size as ours (exchange = truth) and make sure a venue stop guards it.
        Leaves the state to the caller."""
        self._pending_cid = None
        self._pending_since = None
        pos = snapshot.position(self.instrument_id)
        if pos is None or pos.size == 0:
            self.size, self.entry, self.stop_trigger = Decimal(0), None, None
            self.cumulative_funding = Decimal(0)
        else:
            self.size, self.entry, self.cumulative_funding = pos.size, pos.entry_price, pos.cumulative_funding
            if self.instrument_id in snapshot.stops:
                self.stop_trigger = snapshot.stops[self.instrument_id]
            else:
                self.stop_trigger = await place_exchange_stop(self.executor, pos, self.limits)
        self._persist()
```

- replace `handle_event` with:

```python
    async def handle_event(self, ev: OrderUpdate | FillUpdate) -> None:
        if isinstance(ev, OrderUpdate):
            if ev.client_order_id == self._pending_cid and ev.status in ("cancelled", "auto_cancelled", "rejected"):
                self._update_order(ev.client_order_id, status=ev.status)
                self._alert("WARN", "order_" + ev.status, client_order_id=ev.client_order_id)
                self._pending_cid = None
                self._set_state(State.OPEN if self.size != 0 else State.FLAT)
            else:
                row = db.get_order(self.conn, ev.client_order_id)
                if row is not None and row.status not in _GAVE_UP_STATUSES:
                    self._update_order(ev.client_order_id, status=ev.status, filled_quantity=ev.filled_quantity)
            return
        if ev.instrument_id != self.instrument_id:
            return
        row = db.get_order(self.conn, ev.client_order_id)
        if row is not None and row.status in _GAVE_UP_STATUSES and ev.client_order_id != self._pending_cid:
            # We already replaced this order with the exchange's size (or gave up on it). The
            # exchange is truth: re-read it instead of adding a fill the adoption may include.
            self._alert("WARN", "late_fill", client_order_id=ev.client_order_id, state=self.state.value)
            await self._adopt(await self.executor.snapshot())
            if self.state is not State.HALTED:
                self._set_state(State.OPEN if self.size != 0 else State.FLAT)
            return
        size_before = self.size
        ours = row is not None
        self._apply_fill(ev)
        if ours:
            self._update_order(ev.client_order_id, status="filled", filled_quantity=ev.quantity, avg_price=ev.price)
        if self.state is State.ENTRY_PENDING and ev.client_order_id == self._pending_cid and self.size != 0:
            self._pending_cid = None
            self._set_state(State.OPEN)
            await self.replace_stop()
        elif self.size == 0:
            was_pending = self.state is State.EXIT_PENDING and ev.client_order_id == self._pending_cid
            self._pending_cid = None
            self.stop_trigger = None
            await self.executor.cancel_stop(self.instrument_id)
            if self.state is not State.HALTED:      # a shutdown already forced HALTED before this fill landed
                self._set_state(State.FLAT)
            hook = getattr(self.strategy, "on_flatten", None)
            if callable(hook):
                hook()
            if not was_pending and not ours:
                self._alert("WARN", "stop_fired", price=ev.price, quantity=ev.quantity)
        elif self.state is State.HALTED:
            # Part A §4.4: a fill that lands while HALTED still moves the position. Guard it; stay HALTED.
            self._alert("WARN", "late_fill", client_order_id=ev.client_order_id, state=self.state.value,
                        size_after=self.size)
            await self.replace_stop()
        else:
            # Neither "our pending entry landed" nor "flattened" - a fill we weren't tracking
            # (e.g. one that arrives after clear_halt(), or for an id we never sent). Surface it
            # rather than silently leaving positions_local out of step with the size we just applied.
            self._alert("WARN", "unexpected_fill", client_order_id=ev.client_order_id, state=self.state.value,
                        size_before=size_before, size_after=self.size)
            if self.state is State.FLAT:
                self._set_state(State.OPEN)
                await self.replace_stop()
```

- in `adopt_pending`, after `self._pending_cid = client_order_id` add `self._pending_since = self.clock()`.
- replace `Portfolio.on_fast` with:

```python
    async def on_fast(self, marks: Mapping[int, Decimal]) -> AccountSnapshot:
        async with self._lock:
            snapshot = await self.executor.snapshot()
            for router in self.routers.values():
                await router.check_pending(snapshot)
            for iid, mark in marks.items():
                router = self.routers.get(iid)
                if router is not None:
                    await router.on_fast(mark, snapshot)
            if self.start_equity is not None:
                # I7: once per tick, transitions only (None -> WARN -> CRITICAL and back), like I5.
                a = pnl_alert(snapshot.equity, self.start_equity, snapshot.ts)
                level = a.level if a is not None else None
                if a is not None and level != self._last_pnl_level:
                    self.alerter.emit(a)
                self._last_pnl_level = level
            # funding_drift_alert would go here (realised vs expected funding per instrument);
            # deferred to Phase 2b - the sim's funding is the predicted rate, so it can never drift.
            return snapshot
```

In `polyperps/execution/live_executor.py`, in `LiveExecutor.__init__` add `self._gate = check` after `super().__init__(session, clock=clock)`, and make the first lines of `LiveExecutor.submit`:

```python
    async def submit(self, order: OrderRequest) -> OrderAck:
        # Part A §4.8: the locks are re-read on every order, not only at construction; a lock
        # that closed mid-run (env flipped, approval revoked) stops the next order here.
        d = self._gate(order.instrument_id)
        if not d.allowed:
            raise GateClosed(f"instrument {order.instrument_id}: {d.reason}")
        try:
            placement = await self._s.place_order(
```

(the rest of `submit` unchanged).

- [ ] **Step 4: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/test_order_router.py tests/test_live_executor.py tests/test_state_recovery.py -v`
Expected: PASS.

- [ ] **Step 5: Run the full suite**

Run: `.venv/Scripts/python -m pytest -q`
Expected: all pass, 0 failed.

- [ ] **Step 6: Commit**

```bash
git add polyperps/execution/order_router.py polyperps/execution/live_executor.py tests/test_order_router.py tests/test_live_executor.py
git commit -m "$(cat <<'EOF'
fix(router): pending timeout, late fills, submit errors and a per-order gate check

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Retry correctness and fills without a client id

**Files:**
- Modify: `polyperps/execution/order_router.py` (`_landed` `:249-255`, `handle_event`, new `_record_fill`)
- Modify: `polyperps/execution/live_executor.py` (`_SDK_ORDER_STATUS["duplicate_order"]`, `LiveExecutor.submit` return reason, `LiveReader.events` fill branch)
- Test: `tests/test_order_router.py`, `tests/test_live_executor.py`

**Interfaces:**
- Consumes: `_GAVE_UP_STATUSES`, `_adopt` (Task 6).
- Produces:
  - `_landed(req, snap, size_before) -> bool`: True when the order rests or the position moved in the order's direction by any amount.
  - `InstrumentRouter._record_fill(row: OrderRow, ev: FillUpdate) -> None`: accumulates `filled_quantity`, weights `avg_price`, status `filled`/`partial` unless already terminal.
  - `LiveExecutor.submit` acks `duplicate_order` as `accepted` with `reason="duplicate_order"`; `LiveReader.events` skips `duplicate_order` updates and yields fills without a client id as `client_order_id=f"venue-{order_id}"`.

Decision: order updates set status only (Decision 14). Decision: duplicate mapping (Decision 15). Decision: `venue-` prefix (Decision 16). Decision: no `unexpected_fill` for later partial fills of our own order (Decision 23).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_order_router.py` (`dc_replace` is already imported there; extend the Task 6 import to `from polyperps.execution.executor import ExecutorTimeout, GateClosed, ShadowRefused`):

```python
class PartialThenTimeout(SimExecutor):
    async def submit(self, order):
        await super().submit(dc_replace(order, quantity=order.quantity / 2))    # half fills on the venue...
        raise ExecutorTimeout("ack lost after a partial fill")                  # ...and the ack is lost


async def test_partial_fill_then_timeout_is_landed_not_retried():
    conn, ex, strat, router, pf = make(executor_cls=PartialThenTimeout)
    await pf.on_bar({6: [bar(0)]}, "run")
    await pump(pf, ex)
    assert (await ex.snapshot()).position(6).size == Decimal("0.5")        # one partial, no second order
    assert "retry" not in kinds(conn) and "ack_lost" in kinds(conn)
    assert get_order(conn, "r-6-1").filled_quantity == Decimal("0.5")


async def test_order_filled_quantity_accumulates_across_fills():
    conn, ex, strat, router, pf = make(executor_cls=AckOnlyExecutor)
    await pf.on_bar({6: [bar(0)]}, "run")                                  # r-6-1: quantity 1, no fill yet
    await router.handle_event(FillUpdate(client_order_id="r-6-1", instrument_id=6, side="buy",
                                         quantity=Decimal("0.4"), price=Decimal(100), fee=Decimal(0), ts=T0))
    o = get_order(conn, "r-6-1")
    assert o.status == "partial" and o.filled_quantity == Decimal("0.4") and router.state is State.OPEN
    await router.handle_event(FillUpdate(client_order_id="r-6-1", instrument_id=6, side="buy",
                                         quantity=Decimal("0.6"), price=Decimal(101), fee=Decimal(0), ts=T0))
    o = get_order(conn, "r-6-1")
    assert o.status == "filled" and o.filled_quantity == Decimal("1.0") and o.avg_price == Decimal("100.6")
    assert router.size == 1 and "unexpected_fill" not in kinds(conn)


async def test_fill_without_client_id_reaches_its_instruments_router():
    conn = connect(":memory:")
    ex = SimExecutor("r", equity=Decimal(1000), taker_fee_rate=FEE, clock=lambda: T0)
    ex.update_mark(6, Decimal(100)); ex.update_mark(7, Decimal(100))
    alerter = Alerter("r", [SqliteSink(conn)])
    r6 = InstrumentRouter(run_id="r", instrument_id=6, category="crypto", strategy=Strat(1), executor=ex, conn=conn,
                          alerter=alerter, categories=CATS, clock=lambda: T0)
    r7 = InstrumentRouter(run_id="r", instrument_id=7, category="crypto", strategy=Strat(1), executor=ex, conn=conn,
                          alerter=alerter, categories=CATS, clock=lambda: T0)
    pf = Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter, routers={6: r6, 7: r7})
    await pf.on_bar({6: [bar(0)], 7: [dc_replace(bar(0), instrument_id=7)]}, "run"); await pump(pf, ex)
    await pf.dispatch(FillUpdate(client_order_id="venue-999", instrument_id=7, side="sell", quantity=r7.size,
                                 price=Decimal(85), fee=Decimal(0), ts=T0))            # the venue stop fired
    assert r7.state is State.FLAT and r6.state is State.OPEN
    stop = [a for a in list_alerts(conn, "r") if a[2] == "stop_fired"]
    assert len(stop) == 1 and stop[0][3] == 7
```

Append to `tests/test_live_executor.py`:

```python
async def test_duplicate_order_counts_as_landed_not_rejected():
    """Review focus 3: a same-id retry answered with duplicate_order is the original order."""
    s = FakeSession(order_status="duplicate_order")
    ex = LiveExecutor(s, instrument_ids=[6], modes={}, gate=OPEN, clock=lambda: T0)
    ack = await ex.submit(_req())
    assert ack.status == "accepted" and ack.reason == "duplicate_order"
    s._events = [SimpleNamespace(type="order", timestamp=T0, payload=SimpleNamespace(
        client_order_id="r-6-1", status="duplicate_order", filled_quantity=Decimal(0)))]
    assert [e async for e in ex.events()] == []       # must not reach a pending router as "rejected"


async def test_fill_without_client_id_is_kept_and_routed_by_instrument():
    s = FakeSession()
    s._events = [SimpleNamespace(type="fill", timestamp=T0, payload=[SimpleNamespace(
        client_order_id=None, order_id=999, instrument_id=7, side="short", quantity=Decimal(1),
        price=Decimal(85), fee=Decimal(0))])]
    ex = LiveExecutor(s, instrument_ids=[6], modes={}, gate=OPEN, clock=lambda: T0)
    (fill,) = [e async for e in ex.events()]
    assert fill.client_order_id == "venue-999" and fill.instrument_id == 7 and fill.side == "sell"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_order_router.py tests/test_live_executor.py -v`
Expected: FAIL. The partial-fill test sees `retry` and size 1.5 (`_landed` wants the exact quantity); accumulation shows `filled_quantity == 0.6` and `unexpected_fill`; `duplicate_order` acks `rejected`; the client-id-less fill is dropped (`ValueError: not enough values to unpack`).

- [ ] **Step 3: Implement**

In `polyperps/execution/order_router.py`, replace `_landed` with:

```python
    def _landed(self, req: OrderRequest, snap: AccountSnapshot, size_before: Decimal) -> bool:
        """Part A §4.6: the order landed if it rests, or if the position moved in its direction by
        ANY amount (a partial fill counts). Retrying a partly filled order would double it."""
        if req.client_order_id in snap.open_orders:
            return True
        pos = snap.position(self.instrument_id)
        moved = (pos.size if pos is not None else Decimal(0)) - size_before
        return moved > 0 if req.side == "buy" else moved < 0
```

Add after `_update_order`:

```python
    def _record_fill(self, row: OrderRow, ev: FillUpdate) -> None:
        """Part A §4.6: an order's filled quantity accumulates across fills; a terminal status
        (e.g. an IOC already reported cancelled) is never downgraded."""
        filled = row.filled_quantity + ev.quantity
        if row.avg_price is None or row.filled_quantity == 0:
            avg = ev.price
        else:
            avg = (row.avg_price * row.filled_quantity + ev.price * ev.quantity) / filled
        if row.status in _TERMINAL_ORDER_STATUSES:
            status = row.status
        else:
            status = "filled" if filled >= row.quantity else "partial"
        self._update_order(row.client_order_id, status=status, filled_quantity=filled, avg_price=avg)
```

Replace `handle_event` with:

```python
    async def handle_event(self, ev: OrderUpdate | FillUpdate) -> None:
        if isinstance(ev, OrderUpdate):
            if ev.client_order_id == self._pending_cid and ev.status in ("cancelled", "auto_cancelled", "rejected"):
                self._update_order(ev.client_order_id, status=ev.status)
                self._alert("WARN", "order_" + ev.status, client_order_id=ev.client_order_id)
                self._pending_cid = None
                self._set_state(State.OPEN if self.size != 0 else State.FLAT)
            else:
                row = db.get_order(self.conn, ev.client_order_id)
                if row is not None and row.status not in _GAVE_UP_STATUSES:
                    # status only: filled_quantity belongs to the fills, which accumulate it
                    self._update_order(ev.client_order_id, status=ev.status)
            return
        if ev.instrument_id != self.instrument_id:
            return
        row = db.get_order(self.conn, ev.client_order_id)
        if row is not None and row.status in _GAVE_UP_STATUSES and ev.client_order_id != self._pending_cid:
            # We already replaced this order with the exchange's size (or gave up on it). The
            # exchange is truth: re-read it instead of adding a fill the adoption may include.
            self._alert("WARN", "late_fill", client_order_id=ev.client_order_id, state=self.state.value)
            self._update_order(ev.client_order_id, filled_quantity=row.filled_quantity + ev.quantity)
            await self._adopt(await self.executor.snapshot())
            if self.state is not State.HALTED:
                self._set_state(State.OPEN if self.size != 0 else State.FLAT)
            return
        size_before = self.size
        ours = row is not None
        self._apply_fill(ev)
        if ours:
            self._record_fill(row, ev)
        if self.state is State.ENTRY_PENDING and ev.client_order_id == self._pending_cid and self.size != 0:
            self._pending_cid = None
            self._set_state(State.OPEN)
            await self.replace_stop()
        elif self.size == 0:
            was_pending = self.state is State.EXIT_PENDING and ev.client_order_id == self._pending_cid
            self._pending_cid = None
            self.stop_trigger = None
            await self.executor.cancel_stop(self.instrument_id)
            if self.state is not State.HALTED:      # a shutdown already forced HALTED before this fill landed
                self._set_state(State.FLAT)
            hook = getattr(self.strategy, "on_flatten", None)
            if callable(hook):
                hook()
            if not was_pending and not ours:
                self._alert("WARN", "stop_fired", price=ev.price, quantity=ev.quantity)
        elif self.state is State.HALTED:
            # Part A §4.4: a fill that lands while HALTED still moves the position. Guard it; stay HALTED.
            self._alert("WARN", "late_fill", client_order_id=ev.client_order_id, state=self.state.value,
                        size_after=self.size)
            await self.replace_stop()
        elif ours and self.state is not State.FLAT:
            pass   # a later partial fill of our own order; the venue stop is position-level
        else:
            # Neither "our pending entry landed" nor "flattened" - a fill we weren't tracking
            # (e.g. one that arrives after clear_halt(), or for an id we never sent). Surface it
            # rather than silently leaving positions_local out of step with the size we just applied.
            self._alert("WARN", "unexpected_fill", client_order_id=ev.client_order_id, state=self.state.value,
                        size_before=size_before, size_after=self.size)
            if self.state is State.FLAT:
                self._set_state(State.OPEN)
                await self.replace_stop()
```

In `polyperps/execution/live_executor.py`:
- change the table entry to `"duplicate_order": None,   # a same-id retry: the original order stands (Part A §4.6)`
- in `LiveExecutor.submit`, replace the final return with:

```python
        return OrderAck(client_order_id=order.client_order_id, exchange_order_id=exchange_order_id,
                        status="accepted", reason="duplicate_order" if raw_status == "duplicate_order" else "",
                        ts=self._clock())
```

- in `LiveReader.events`, replace the fill branch with:

```python
            elif kind == "fill":
                for f in ev.payload:
                    # Part A §4.7: stop and liquidation fills carry no client id; keep them and
                    # let the Portfolio route them by instrument.
                    cid = f.client_order_id or f"venue-{f.order_id}"
                    yield FillUpdate(client_order_id=cid, instrument_id=int(f.instrument_id),
                                     side=_FILL_SIDE_MAP.get(f.side, f.side), quantity=f.quantity,
                                     price=f.price, fee=f.fee, ts=ev.timestamp)
```

- [ ] **Step 4: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/test_order_router.py tests/test_live_executor.py tests/test_state_recovery.py tests/test_decision_trail.py -v`
Expected: PASS.

- [ ] **Step 5: Run the full suite**

Run: `.venv/Scripts/python -m pytest -q`
Expected: all pass, 0 failed.

- [ ] **Step 6: Commit**

```bash
git add polyperps/execution/order_router.py polyperps/execution/live_executor.py tests/test_order_router.py tests/test_live_executor.py
git commit -m "$(cat <<'EOF'
fix(router): partial fills count as landed, duplicate_order is landed, fills accumulate, venue fills are kept

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: Exposure counts in-flight orders

**Files:**
- Modify: `polyperps/risk/liquidation_guard.py:63-82` (`vet_entry`)
- Modify: `polyperps/risk/portfolio_exposure.py:29-63` (`vet_exposure`)
- Modify: `polyperps/execution/order_router.py` (`InstrumentRouter.__init__`, `on_bar`, `_send`, `Portfolio.on_bar`)
- Test: `tests/test_portfolio_exposure.py`, `tests/test_liquidation_guard.py`, `tests/test_order_router.py`

**Interfaces:**
- Consumes: `Portfolio.on_bar` with shutdown path (Task 3), `_send` (Task 6).
- Produces:
  - `vet_entry(intent, *, mark, snapshot, limits=LIMITS, pending: Sequence[Intent] = ()) -> Verdict`
  - `vet_exposure(intent, *, positions, equity, categories, limits=EXPOSURE, pending: Sequence[Intent] = ()) -> Verdict`
  - `InstrumentRouter.pending_intent: Intent | None` (set in `_send`)
  - `InstrumentRouter.on_bar(history, snapshot, kill, pending: Sequence[Intent] = ())`

Decision: pending = other routers' non-reduce-only intents in `ENTRY_PENDING`, recomputed per router (Decision 17).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_portfolio_exposure.py`:

```python
def test_pending_entry_counts_like_a_position():
    # BTC long 500 is still in flight (no position yet): an ETH long 500 is resized by the cluster cap
    v = vet_exposure(intent(7, "500"), positions=[], equity=Decimal(1000), categories=CATS,
                     pending=[intent(6, "500")])
    assert v == Resize(quantity=Decimal("1.00000000"))


def test_pending_exit_is_not_counted():
    exit_ = Intent(instrument_id=6, side="sell", quantity=Decimal(5), notional=Decimal(500), reduce_only=True)
    assert vet_exposure(intent(7, "100"), positions=[], equity=Decimal(1000), categories=CATS,
                        pending=[exit_]) == Allow()
```

Append to `tests/test_liquidation_guard.py`:

```python
def test_pending_entry_counts_toward_the_leverage_cap():
    # 3x of 100 equity = 300; 250 already in flight leaves 50 -> qty 0.5
    v = vet_entry(intent("100"), mark=Decimal(100), snapshot=snap(equity="100"), pending=[intent("250")])
    assert v == Resize(quantity=Decimal("0.50000000"))
```

Append to `tests/test_order_router.py`:

```python
async def test_second_router_counts_the_first_routers_in_flight_entry():
    """Review focus 5: both routers decide in ONE on_bar call; r6's entry is sent but unfilled."""
    conn = connect(":memory:")
    ex = AckOnlyExecutor("r", equity=Decimal(200), taker_fee_rate=FEE, clock=lambda: T0)
    ex.update_mark(6, Decimal(100)); ex.update_mark(7, Decimal(100))
    alerter = Alerter("r", [SqliteSink(conn)])
    r6 = InstrumentRouter(run_id="r", instrument_id=6, category="crypto", strategy=Strat(1), executor=ex, conn=conn,
                          alerter=alerter, categories=CATS, clock=lambda: T0)
    r7 = InstrumentRouter(run_id="r", instrument_id=7, category="crypto", strategy=Strat(1), executor=ex, conn=conn,
                          alerter=alerter, categories=CATS, clock=lambda: T0)
    pf = Portfolio(run_id="r", executor=ex, conn=conn, alerter=alerter, routers={6: r6, 7: r7})
    await pf.on_bar({6: [bar(0)], 7: [dc_replace(bar(0), instrument_id=7)]}, "run")
    assert r6.state is State.ENTRY_PENDING and get_order(conn, "r-6-1").quantity == 1
    # equity 200: cluster net cap 0.6 x 200 = 120; r6's 100 in flight leaves 20 for r7
    assert get_order(conn, "r-7-1").quantity == Decimal("0.20000000")
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_portfolio_exposure.py tests/test_liquidation_guard.py tests/test_order_router.py -v`
Expected: FAIL, `TypeError: vet_exposure() got an unexpected keyword argument 'pending'` (and the same for `vet_entry`); the router test sees r-7-1 quantity 1.

- [ ] **Step 3: Implement**

In `polyperps/risk/liquidation_guard.py`, add `from collections.abc import Sequence` and replace `vet_entry` with:

```python
def vet_entry(intent: Intent, *, mark: Decimal, snapshot: AccountSnapshot, limits: RiskLimits = LIMITS,
              pending: Sequence[Intent] = ()) -> Verdict:
    """`pending`: other instruments' entry orders sent but not yet filled (Part A §4.9); they
    count like positions so two quick entries cannot share the same headroom."""
    if snapshot.equity <= 0:
        return Reject(reason="equity <= 0")
    existing = (sum((p.notional for p in snapshot.positions), Decimal(0))
                + sum((i.notional for i in pending if not i.reduce_only), Decimal(0)))
    allowed = intent.notional

    cap_notional = limits.max_leverage * snapshot.equity - existing
    if cap_notional <= 0:
        return Reject(reason=f"leverage cap {limits.max_leverage}x already used")
    allowed = min(allowed, cap_notional)

    max_lev_for_floor = Decimal(1) / (limits.min_liq_distance + limits.maintenance_rate)
    floor_notional = max_lev_for_floor * snapshot.equity - existing
    if floor_notional <= 0:
        return Reject(reason="liquidation-distance floor leaves no room")
    allowed = min(allowed, floor_notional)

    if allowed >= intent.notional:
        return Allow()
    return Resize(quantity=(allowed / mark).quantize(_Q, rounding=ROUND_DOWN))
```

In `polyperps/risk/portfolio_exposure.py`, replace `vet_exposure` with:

```python
def vet_exposure(
    intent: Intent,
    *,
    positions: Sequence[PositionView],
    equity: Decimal,
    categories: Mapping[int, str],
    limits: ExposureLimits = EXPOSURE,
    pending: Sequence[Intent] = (),
) -> Verdict:
    """`pending`: other instruments' entry orders sent but not yet filled (Part A §4.9)."""
    if equity <= 0:
        return Reject(reason="equity <= 0")

    sign = Decimal(1) if intent.side == "buy" else Decimal(-1)
    my_cluster = cluster_of(categories.get(intent.instrument_id, "other"))
    entries = [i for i in pending if not i.reduce_only]
    gross_existing = (sum((p.notional for p in positions), Decimal(0))
                      + sum((i.notional for i in entries), Decimal(0)))
    net_existing = (
        sum(((Decimal(1) if p.size > 0 else Decimal(-1)) * p.notional
             for p in positions if cluster_of(categories.get(p.instrument_id, "other")) == my_cluster),
            Decimal(0))
        + sum(((Decimal(1) if i.side == "buy" else Decimal(-1)) * i.notional
               for i in entries if cluster_of(categories.get(i.instrument_id, "other")) == my_cluster),
              Decimal(0))
    )

    allowed = intent.notional
    gross_room = limits.gross * equity - gross_existing
    if gross_room <= 0:
        return Reject(reason="gross exposure cap already used")
    allowed = min(allowed, gross_room)

    cluster_room = limits.cluster_net * equity - sign * net_existing
    if cluster_room <= 0:
        return Reject(reason=f"cluster net cap already used in {my_cluster}")
    allowed = min(allowed, cluster_room)

    if allowed >= intent.notional:
        return Allow()
    mark = intent.notional / intent.quantity
    return Resize(quantity=(allowed / mark).quantize(_Q, rounding=ROUND_DOWN))
```

In `polyperps/execution/order_router.py`:
- in `InstrumentRouter.__init__`, after `self._pending_since: datetime | None = None` add
  `self.pending_intent: Intent | None = None   # the order in flight; counted by other routers' exposure checks`
- in `_send`, after `self._pending_since = now` add `self.pending_intent = intent`
- change the `on_bar` signature to

```python
    async def on_bar(self, history: Sequence[Bar], snapshot: AccountSnapshot, kill: Action,
                     pending: Sequence[Intent] = ()) -> None:
```

  and the two guard calls inside it to

```python
            final, labels = apply_guards(intent, [
                ("vet_entry", vet_entry(intent, mark=mark, snapshot=snapshot, limits=self.limits, pending=pending)),
                ("vet_exposure", vet_exposure(intent, positions=snapshot.positions, equity=snapshot.equity,
                                              categories=self.categories, limits=self.exposure, pending=pending)),
            ])
```

- replace `Portfolio.on_bar` with:

```python
    async def on_bar(self, histories: Mapping[int, Sequence[Bar]], kill: Action) -> None:
        async with self._lock:
            snapshot = await self.executor.snapshot()
            if kill == "shutdown":
                await self._shutdown(snapshot, histories)
                return
            for iid, history in histories.items():
                router = self.routers.get(iid)
                if router is not None and history:
                    # Recomputed per router: an entry sent earlier in THIS loop is in flight too.
                    # ponytail: an order adopted by recovery (adopt_pending) has no intent and is not
                    # counted; rebuild one from its order row if recovered in-flight entries matter.
                    pending = [r.pending_intent for r in self.routers.values()
                               if r is not router and r.state is State.ENTRY_PENDING and r.pending_intent is not None]
                    await router.on_bar(history, snapshot, kill, pending=pending)
```

- [ ] **Step 4: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/test_portfolio_exposure.py tests/test_liquidation_guard.py tests/test_order_router.py -v`
Expected: PASS.

- [ ] **Step 5: Run the full suite**

Run: `.venv/Scripts/python -m pytest -q`
Expected: all pass, 0 failed.

- [ ] **Step 6: Commit**

```bash
git add polyperps/risk/liquidation_guard.py polyperps/risk/portfolio_exposure.py polyperps/execution/order_router.py tests/test_portfolio_exposure.py tests/test_liquidation_guard.py tests/test_order_router.py
git commit -m "$(cat <<'EOF'
fix(risk): leverage and exposure caps count entry orders still in flight

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: Funding drift wiring

**Files:**
- Modify: `polyperps/execution/order_router.py` (import `funding_drift_alert`, `Portfolio.__init__`, `Portfolio.on_bar`, new `Portfolio._check_funding`, drop the deferred comment in `Portfolio.on_fast`)
- Test: `tests/test_order_router.py`

**Interfaces:**
- Consumes: `funding_drift_alert(realised_rate, expected_rate, instrument_id, ts)` (existing, `polyperps/monitor/alerts.py:154`), `Portfolio.on_bar` (Task 8).
- Produces: `Portfolio._funding_seen: dict[int, Decimal]`, `Portfolio._check_funding(histories, snapshot) -> None`, run on every non-shutdown `on_bar`.

Decision: runs in every mode (Decision 18).

- [ ] **Step 1: Write the failing test**

Append to `tests/test_order_router.py`:

```python
async def test_funding_drift_alerts_when_charged_funding_is_3x_the_bar_rate():
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)                 # long 1 @ 100
    hist = [bar(0)]
    for i, charged in ((1, "0.0001"), (2, "0.0001"), (3, "0.0003")):
        ex.apply_funding(6, Decimal(charged))                                  # what the venue charged
        hist = hist + [bar(i, funding="0.0001")]                               # what the bar predicted
        await pf.on_bar({6: hist}, "run")
        if i == 2:
            assert "funding_drift" not in kinds(conn)                          # 1st sighting is a baseline; then 1x
    drift = [a for a in list_alerts(conn, "r") if a[2] == "funding_drift"]
    assert len(drift) == 1 and drift[0][1] == "WARN" and drift[0][3] == 6
```

- [ ] **Step 2: Run it to verify it fails**

Run: `.venv/Scripts/python -m pytest tests/test_order_router.py::test_funding_drift_alerts_when_charged_funding_is_3x_the_bar_rate -v`
Expected: FAIL, `assert 0 == 1` (no `funding_drift` alert is ever emitted).

- [ ] **Step 3: Implement**

In `polyperps/execution/order_router.py`:
- change the alerts import to `from polyperps.monitor.alerts import Alert, Alerter, funding_drift_alert, margin_alert, pnl_alert`
- in `Portfolio.__init__`, after `self._last_pnl_level: str | None = None` add
  `self._funding_seen: dict[int, Decimal] = {}   # last cumulative funding per instrument, for §4.11`
- in `Portfolio.on_fast`, delete the two comment lines starting `# funding_drift_alert would go here`.
- replace `Portfolio.on_bar` with:

```python
    async def on_bar(self, histories: Mapping[int, Sequence[Bar]], kill: Action) -> None:
        async with self._lock:
            snapshot = await self.executor.snapshot()
            if kill == "shutdown":
                await self._shutdown(snapshot, histories)
                return
            self._check_funding(histories, snapshot)
            for iid, history in histories.items():
                router = self.routers.get(iid)
                if router is not None and history:
                    # Recomputed per router: an entry sent earlier in THIS loop is in flight too.
                    # ponytail: an order adopted by recovery (adopt_pending) has no intent and is not
                    # counted; rebuild one from its order row if recovered in-flight entries matter.
                    pending = [r.pending_intent for r in self.routers.values()
                               if r is not router and r.state is State.ENTRY_PENDING and r.pending_intent is not None]
                    await router.on_bar(history, snapshot, kill, pending=pending)

    def _check_funding(self, histories: Mapping[int, Sequence[Bar]], snapshot: AccountSnapshot) -> None:
        """Part A §4.11: the funding the venue actually charged since the last bar (delta of the
        position's cumulative funding) vs bar.funding_rate x notional. Sign convention: negative
        cumulative funding = paid (PositionView); the live smoke script confirms the SDK's."""
        for iid, history in histories.items():
            pos = snapshot.position(iid)
            if pos is None or pos.size == 0 or not history or history[-1].funding_rate is None:
                self._funding_seen.pop(iid, None)
                continue
            prev = self._funding_seen.get(iid)
            self._funding_seen[iid] = pos.cumulative_funding
            if prev is None or pos.notional == 0:
                continue   # first sighting of this position: a baseline, nothing to compare
            realised_rate = (prev - pos.cumulative_funding) / pos.notional                # paid is positive
            expected_rate = history[-1].funding_rate * (1 if pos.size > 0 else -1)       # longs pay positive rates
            a = funding_drift_alert(realised_rate, expected_rate, iid, snapshot.ts)
            if a is not None:
                self.alerter.emit(a)
```

- [ ] **Step 4: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/test_order_router.py -v`
Expected: PASS.

- [ ] **Step 5: Run the full suite**

Run: `.venv/Scripts/python -m pytest -q`
Expected: all pass, 0 failed.

- [ ] **Step 6: Commit**

```bash
git add polyperps/execution/order_router.py tests/test_order_router.py
git commit -m "$(cat <<'EOF'
feat(monitor): wire funding_drift_alert - charged funding vs the bar rate, each closed bar

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 10: Backtest parity (one cost model, router rules in the harness, gap marking, golden test)

**Files:**
- Modify: `polyperps/risk/liquidation_guard.py` (new `liquidation_price` after `stop_price`)
- Modify: `polyperps/execution/sim_executor.py:1-68` (docstring, constructor), `:111-120` (`_fill`), `:153-178` (`check_triggers`), `:183-200` (`snapshot`)
- Modify: `polyperps/dashboard/state.py:50-57` (`liq_price` delegates)
- Modify: `polyperps/backtest/harness.py:1-189` (rewrite)
- Modify: `polyperps/execution/live_bars.py:12-58` (gap marking)
- Modify: `polyperps/execution/order_router.py` (`InstrumentRouter.on_bar`: data_gap)
- Create: `tests/test_parity.py`
- Test: `tests/test_sim_executor.py`, `tests/test_harness.py`, `tests/test_live_bars.py`, `tests/test_order_router.py`, `tests/test_state_recovery.py`

**Interfaces:**
- Consumes: `costs.fill_cost(*, notional_delta, notional, spread_bps, taker_fee_rate, impact_bps)`, `check_open`, `funding_exit_due`, `stop_price` (existing); `InstrumentRouter.on_bar(..., pending=())` (Task 8); `SimExecutor.on_bar`/`poll_fills` (Task 1).
- Produces:
  - `liquidation_guard.liquidation_price(size: Decimal, entry: Decimal, limits: RiskLimits = LIMITS) -> Decimal`
  - `SimExecutor(..., notional: Decimal = LIMITS.notional_usd)`: fills at the mark (stops at the trigger), `fee = fill_cost(...)`
  - `BacktestResult.trades: list[tuple[datetime, str, Decimal]]` (`ts`, `"buy"|"sell"`, quantity quantized to 1e-8 ROUND_DOWN); ledger kinds `"stop"`, `"guard_exit"`
  - `LiveBarBuilder`: the first bar after a missing hour has `complete=False`
  - Router: on `history[-1].complete is False` → exit OPEN with reason `data_gap`, else record `skip:data_gap`; never enters.

Decisions: 1, 2, 3, 4, 5 (see `## Decisions`).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_parity.py`:

```python
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
```

In `tests/test_harness.py`:
- in `test_gap_forces_flatten_and_blocks_reentry_until_complete`, change `assert (T0 + 2 * H, "gap_flatten") in kinds          # flattened at bar 2 close before the gap at bar 3` to
  `assert (T0 + 3 * H, "gap_flatten") in kinds          # flattened at bar 2's close (stamped with the close time), before the hole at bar 3`
- replace `test_flip_realises_trade_pnl` with:

```python
def test_flip_exits_then_reenters_next_bar():
    bars = [bar(0, "100"), bar(1, "100"), bar(2, "105"), bar(3, "105"), bar(4, "105")]

    class Flip:
        name = "flip"; params = {}
        def target(self, history):
            return Decimal(1) if len(history) < 3 else Decimal(-1)

    res = run_backtest(bars, Flip(), minute_closes=minutes(bars), taker_fee_rate=Decimal(0), warmup=0,
                       impact_bps=Decimal(0))
    assert res.trade_pnls == [Decimal("5")]  # long from 100, exited at 105
    # exit leg traded at its current value (105), then a fresh 100 short one bar later
    assert res.fill_notionals == [Decimal("100"), Decimal("105"), Decimal("100")]
    assert [side for _, side, _ in res.trades] == ["buy", "sell", "sell"]
    assert [ts for ts, _, _ in res.trades] == [T0 + 1 * H, T0 + 3 * H, T0 + 4 * H]
```

- append:

```python
def test_intrabar_stop_fires_at_the_stop_price():
    bars = [bar(0, "100"), bar(1, "100"), replace(bar(2, "90"), low=Decimal("80")), bar(3, "90")]
    res = run_backtest(bars, Const(1), minute_closes=minutes(bars), taker_fee_rate=Decimal(0), warmup=0,
                       impact_bps=Decimal(0))
    (stop,) = [r for r in res.ledger if r.kind == "stop"]
    assert stop.price == Decimal("85.00") and stop.ts == T0 + 2 * H     # 100 * (1 - 0.15)


def test_incomplete_but_priced_bar_exits_and_blocks_entry():
    bars = [bar(0), bar(1), replace(bar(2), complete=False), bar(3), bar(4)]
    res = run_backtest(bars, Const(1), minute_closes=minutes(bars), taker_fee_rate=Decimal(0), warmup=0)
    rows = [(r.ts, r.kind) for r in res.ledger if r.kind in ("fill", "gap_flatten")]
    assert rows == [(T0 + 1 * H, "fill"), (T0 + 3 * H, "gap_flatten"), (T0 + 4 * H, "fill")]


def test_funding_cost_guard_exits_at_the_close():
    bars = [bar(0), bar(1, funding="0.012"), bar(2, funding="0.012"), bar(3), bar(4)]
    res = run_backtest(bars, Const(1), minute_closes=minutes(bars), taker_fee_rate=Decimal(0), warmup=0,
                       impact_bps=Decimal(0))
    (guard,) = [r for r in res.ledger if r.kind == "guard_exit"]
    assert guard.ts == T0 + 3 * H and guard.position == 0     # 2.4 paid >= 2 % of 100 at bar 2's close
```

In `tests/test_live_bars.py` append:

```python
def test_first_bar_after_a_missing_hour_is_incomplete():
    b = LiveBarBuilder()
    b.on_tick(tick(1, "100"))
    first = b.on_tick(tick(121, "101"))            # hour 1 had no ticks at all
    assert first.open_ts == T0 and first.complete
    after_gap = b.on_tick(tick(181, "102"))
    assert after_gap.open_ts == T0 + timedelta(hours=2) and after_gap.complete is False
    assert b.on_tick(tick(241, "103")).complete     # the next full hour is complete again
```

In `tests/test_order_router.py` append:

```python
async def test_incomplete_bar_exits_to_flat_and_never_enters():
    conn, ex, strat, router, pf = make()
    await pf.on_bar({6: [bar(0)]}, "run"); await pump(pf, ex)
    gap = dc_replace(bar(1), complete=False)
    await pf.on_bar({6: [bar(0), gap]}, "run")
    assert get_order(conn, "r-6-2").reason == "data_gap"
    await pump(pf, ex)
    assert router.state is State.FLAT
    await pf.on_bar({6: [bar(0), gap, dc_replace(bar(2), complete=False)]}, "run")
    assert router.state is State.FLAT and list_decisions(conn, "r", 6)[-1].note == "skip:data_gap"
    await pf.on_bar({6: [bar(0), gap, bar(2)]}, "run")                   # the next complete bar enters again
    assert router.state is State.ENTRY_PENDING
```

Update the existing literals that encoded the old 7.5 bps fill slippage:
- `tests/test_sim_executor.py::test_buy_fills_at_mark_plus_costs_and_emits_events` body after `fill = ev[1]`:

```python
    # fills at the mark; cost = fill_cost(traded 100, notional 100, spread 10, fee 0.0004, impact 5)
    #      = 100 * (0.0004 + 10/20000 + 5/10000 * 1) = 0.14
    assert fill.price == Decimal(100) and fill.fee == Decimal("0.14")
    snap = await ex.snapshot()
    p = snap.position(6)
    assert p.size == 1 and p.entry_price == Decimal(100)
    assert p.liquidation_price == (Decimal(100) * (1 - Decimal(1) / 3 + MAINTENANCE_RATE)).quantize(Decimal("0.01"))
    assert snap.equity == Decimal(1000) - fill.fee
```

- `tests/test_sim_executor.py::test_sell_short_and_funding_sign`: `p.entry_price == Decimal("99.90")` → `p.entry_price == Decimal(100)`
- `tests/test_order_router.py::test_entry_then_open_with_stop_and_rows`: `router.entry == Decimal("100.08")` → `router.entry == Decimal(100)`; `router.stop_trigger == Decimal("85.07")           # 100.08 * 0.85 ...` → `router.stop_trigger == Decimal("85.00")`
- `tests/test_order_router.py::test_reject_and_resize_from_exposure`: `Decimal("0.49") < get_order(conn2, "r-6-1").quantity` → `Decimal("0.48") < get_order(conn2, "r-6-1").quantity` (the 5.5-lot pre-trade now pays impact scaled by its 5.5x turnover: equity 998.13, room 48.88)
- `tests/test_state_recovery.py::test_crash_after_fill_rebuilds_open_and_replaces_stop`: `Decimal("100.08")` → `Decimal(100)`; `Decimal("85.07")` → `Decimal("85.00")`
- `tests/test_state_recovery.py::test_adopting_untracked_position_warns_and_is_reported`: `"entry": "99.92"` → `"entry": "100"`

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_parity.py tests/test_harness.py tests/test_live_bars.py tests/test_sim_executor.py tests/test_order_router.py tests/test_state_recovery.py -v`
Expected: FAIL. `AttributeError: 'BacktestResult' object has no attribute 'trades'`; no `stop` / `guard_exit` ledger rows; the flip is a single fill; the live bar after a gap is `complete=True`; the router enters on an incomplete bar; sim prices still carry slippage (`100.08` / `100.10`).

- [ ] **Step 3: Implement**

In `polyperps/risk/liquidation_guard.py`, add after `stop_price`:

```python
def liquidation_price(size: Decimal, entry: Decimal, limits: RiskLimits = LIMITS) -> Decimal:
    """Isolated-margin liquidation price at limits.max_leverage (a documented assumption; live uses
    the exchange's own number). Shared by SimExecutor, the backtest guards and the dashboard."""
    lev = Decimal(limits.max_leverage)
    if size > 0:
        liq = entry * (1 - Decimal(1) / lev + limits.maintenance_rate)
    else:
        liq = entry * (1 + Decimal(1) / lev - limits.maintenance_rate)
    return liq.quantize(Decimal("0.01"))
```

In `polyperps/dashboard/state.py`, add `liquidation_price` to the `polyperps.risk.liquidation_guard` import and replace `liq_price` with:

```python
def liq_price(size: Decimal, entry: Decimal) -> Decimal:
    """The sim's formula, for snapshots that carry no venue liquidation price."""
    return liquidation_price(size, entry)
```

In `polyperps/execution/sim_executor.py`:
- replace the module docstring with

```python
"""Paper executor (spec section 4.3): in-memory account, fills at the live mark with the SAME cost
model as the backtest (costs.fill_cost, Part A §6.3), self-firing stops, JSON persistence so a
restart exercises recovery. Liquidation price uses liquidation_guard.liquidation_price (a
documented assumption; live uses the exchange's own number)."""
```

- replace everything from `from __future__ import annotations` down to (and including) the `_P = Decimal("0.01")` line with:

```python
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Literal

from polyperps.backtest.costs import fill_cost
from polyperps.execution.executor import ExecutorTimeout
from polyperps.execution.types import (
    AccountSnapshot, FillUpdate, OrderAck, OrderRequest, OrderUpdate, PositionView, StopAck,
)
from polyperps.risk.liquidation_guard import LIMITS, liquidation_price
from polyperps.signal.sufficiency import BAR

if TYPE_CHECKING:
    from polyperps.backtest.bars import Bar
    from polyperps.exchange.types import Tick

MAINTENANCE_RATE = LIMITS.maintenance_rate
```

  (`_BPS`, `_P` and `ROUND_HALF_EVEN` are gone; `MAINTENANCE_RATE` stays because tests import it).
- in `__init__`, add the keyword `notional: Decimal = LIMITS.notional_usd,` after `impact_bps`, and replace `self._slip = (spread_bps / 2 + impact_bps) / _BPS` with

```python
        self._spread, self._impact = spread_bps, impact_bps
        self._notional = notional   # fill_cost's turnover reference: the router's fixed order size
```

- replace `_fill` with:

```python
    def _fill(self, order: OrderRequest, now: datetime, quantity: Decimal | None = None,
              price: Decimal | None = None) -> FillUpdate:
        qty = order.quantity if quantity is None else quantity
        px = self._marks[order.instrument_id] if price is None else price
        fee = fill_cost(notional_delta=qty * px, notional=self._notional, spread_bps=self._spread,
                        taker_fee_rate=self._fee, impact_bps=self._impact)
        self._cash -= fee
        s = Decimal(1) if order.side == "buy" else Decimal(-1)
        self._apply_position(order.instrument_id, s * qty, px)
        return FillUpdate(client_order_id=order.client_order_id, instrument_id=order.instrument_id, side=order.side,
                          quantity=qty, price=px, fee=fee, ts=now)
```

- in `check_triggers`, replace the three lines

```python
                self._marks[iid] = trig  # stops fill at the trigger (plus slippage)
                fill = self._fill(req, self._clock())
                self._marks[iid] = mark
```

  with `fill = self._fill(req, self._clock(), price=trig)  # stops fill at the trigger`
- in `snapshot`, replace the `if p.size > 0: liq = ... else: liq = ...` block and the `liquidation_price=liq.quantize(_P)` argument with `liquidation_price=liquidation_price(p.size, p.entry)` (delete the local `liq` computation).

In `polyperps/execution/live_bars.py`:
- change the import to `from polyperps.backtest.bars import HOUR, Bar, floor_hour`
- replace `_Acc` and `on_tick`, and use the flag in `_close`:

```python
class _Acc:
    __slots__ = ("open_ts", "o", "h", "l", "c", "index", "funding", "source", "complete")

    def __init__(self, t: Tick, complete: bool = True) -> None:
        self.open_ts = floor_hour(t.exchange_ts)
        self.o = self.h = self.l = self.c = t.mark_price
        self.index, self.funding, self.source = t.index_price, t.funding_rate, t.source_type
        self.complete = complete

    def add(self, t: Tick) -> None:
        self.h, self.l, self.c = max(self.h, t.mark_price), min(self.l, t.mark_price), t.mark_price
        self.index, self.funding = t.index_price, t.funding_rate
```

```python
    def _close(self, iid: int) -> Bar:
        a = self._acc.pop(iid)
        bar = Bar(instrument_id=iid, source_type=a.source, open_ts=a.open_ts, open=a.o, high=a.h, low=a.l, close=a.c,
                  index_close=a.index, funding_rate=a.funding, spread_bps=self._spread, spread_source="constant",
                  complete=a.complete)
        h = self._hist[iid]
        h.append(bar)
        del h[:-self._max]
        return bar

    def on_tick(self, tick: Tick) -> Bar | None:
        iid = tick.instrument_id
        acc = self._acc.get(iid)
        if acc is None:
            self._acc[iid] = _Acc(tick)
            return None
        hour = floor_hour(tick.exchange_ts)
        if hour > acc.open_ts:
            closed = self._close(iid)
            # Part A §6.4: the first bar after a missing hour is incomplete; the router exits on it.
            self._acc[iid] = _Acc(tick, complete=hour == acc.open_ts + HOUR)
            return closed
        acc.add(tick)
        return None
```

In `polyperps/execution/order_router.py`, in `InstrumentRouter.on_bar`, insert right after the `kill == "shutdown"` block:

```python
        if not history[-1].complete:
            # Part A §6.4, the backtest's rule: a bar after missing data exits to flat and never enters.
            if self.state is State.OPEN:
                await self._exit(mark, "data_gap", target=None)
            else:
                self._record(target=None, verdicts={}, intent=None, cid=None, note="skip:data_gap")
            return
```

Replace `polyperps/backtest/harness.py` with:

```python
"""Event-driven hourly backtest (spec 5.2), following the live router's rules (Phase 2b Part A §6).

Point-in-time: the strategy receives bars[:t+1]. Fills happen at the 1-minute close latency_s
after the NEXT bar opens; no minute candle -> the hourly open (counted). At each bar, in order:
  1. the 15 % stop fires intrabar when the bar's high/low crosses it, at the stop price;
  2. funding is paid on the position's value at the bar close;
  3. a bar that is not complete exits to flat at its close and never enters (a next bar with no
     price at all - a hole in stored data - is flattened before, as it always was);
  4. the liquidation-distance and funding-cost exits (risk.liquidation_guard) run at the close;
  5. the strategy decides; a flip exits this bar and re-enters next bar only if the strategy
     still wants the other side.
Every trade that ends flat calls strategy.on_flatten(), as the router's fill handler does.
Costs are costs.fill_cost on the traded value - the same call SimExecutor makes. Fixed notional;
the guards use the router's 3x liquidation price.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import ROUND_DOWN, Decimal
from typing import Literal

from polyperps.backtest.bars import Bar, floor_minute
from polyperps.backtest.costs import fill_cost
from polyperps.backtest.strategy import Strategy, clamp_target
from polyperps.execution.types import PositionView
from polyperps.risk.liquidation_guard import LIMITS, check_open, funding_exit_due, liquidation_price, stop_price
from polyperps.signal.sufficiency import BAR

Kind = Literal["funding", "fill", "fill_unavailable", "gap_flatten", "mark", "stop", "guard_exit"]
_Q = Decimal("0.00000001")


@dataclass(frozen=True, slots=True, kw_only=True)
class LedgerRow:
    ts: datetime
    kind: Kind
    position: Decimal
    price: Decimal | None
    cash_delta: Decimal
    equity: Decimal
    note: str = ""


@dataclass(slots=True)
class BacktestResult:
    ledger: list[LedgerRow] = field(default_factory=list)
    equity: list[tuple[datetime, Decimal]] = field(default_factory=list)
    returns: list[Decimal] = field(default_factory=list)
    trade_pnls: list[Decimal] = field(default_factory=list)
    fill_notionals: list[Decimal] = field(default_factory=list)
    trades: list[tuple[datetime, str, Decimal]] = field(default_factory=list)  # (ts, side, quantity) per fill
    params: dict[str, object] = field(default_factory=dict)  # harness params + {"strategy_params": {...}}
    bars_total: int = 0
    bars_complete: int = 0
    bars_constant_spread: int = 0  # bars whose spread came from the constant fallback, not the book
    fills: int = 0
    fills_unavailable: int = 0
    fills_at_hourly_open: int = 0


def _sign(x: Decimal) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def _units(fraction: Decimal, notional: Decimal, price: Decimal) -> Decimal:
    """Contracts for a leg worth |fraction| x notional at `price`: the router's quantity rule."""
    return (abs(fraction) * notional / price).quantize(_Q, rounding=ROUND_DOWN)


class _Book:
    """Mutable position state for one run."""

    def __init__(self, notional: Decimal) -> None:
        self.notional = notional
        self.cash = Decimal(0)
        self.position = Decimal(0)
        self.entry = Decimal(0)
        self.funding = Decimal(0)   # cumulative since entry; negative = paid (PositionView convention)

    def unrealised(self, price: Decimal) -> Decimal:
        if self.position == 0:
            return Decimal(0)
        return self.position * self.notional * (price / self.entry - 1)

    def equity(self, price: Decimal | None) -> Decimal:
        return self.cash + (self.unrealised(price) if price is not None else Decimal(0))

    def view(self, instrument_id: int, price: Decimal) -> PositionView:
        """The book as the router's guards see a venue position."""
        size = self.position * self.notional / self.entry
        return PositionView(instrument_id=instrument_id, size=size, entry_price=self.entry,
                            notional=abs(size) * price, leverage=LIMITS.max_leverage,
                            liquidation_price=liquidation_price(size, self.entry),
                            unrealised_pnl=size * (price - self.entry), cumulative_funding=self.funding)


def run_backtest(
    bars: Sequence[Bar],
    strategy: Strategy,
    *,
    minute_closes: Mapping[datetime, Decimal],
    taker_fee_rate: Decimal,
    warmup: int,
    latency_s: int = BAR.latency_s,
    impact_bps: Decimal = BAR.impact_bps,
    notional: Decimal = BAR.notional_usd,
) -> BacktestResult:
    res = BacktestResult(
        params={"taker_fee_rate": str(taker_fee_rate), "latency_s": str(latency_s),
                "impact_bps": str(impact_bps), "notional": str(notional), "warmup": str(warmup),
                "strategy": strategy.name,
                "strategy_params": {k: str(v) for k, v in strategy.params.items()}},
        bars_total=len(bars),
        bars_complete=sum(1 for b in bars if b.complete),
        bars_constant_spread=sum(1 for b in bars if b.spread_source == "constant"),
    )
    book = _Book(notional)
    latency = timedelta(seconds=latency_s)
    last_equity = Decimal(0)  # equity starts at 0, so the first mark's return includes entry costs

    def log(ts: datetime, kind: Kind, price: Decimal | None, cash_delta: Decimal, note: str = "") -> None:
        res.ledger.append(LedgerRow(ts=ts, kind=kind, position=book.position, price=price,
                                    cash_delta=cash_delta, equity=book.equity(price), note=note))

    def trade_to(target: Decimal, price: Decimal, spread_bps: Decimal, ts: datetime, kind: Kind,
                 note: str = "") -> None:
        delta = target - book.position
        if delta == 0:
            return
        old_position, old_entry = book.position, book.entry
        # Flips never reach here: the decision step turns a flip into an exit (router rule).
        reducing = old_position != 0 and abs(target) < abs(old_position)
        if old_position != 0 and target == 0:
            realised = old_position * notional * (price / old_entry - 1)
            book.cash += realised
            res.trade_pnls.append(realised)
            new_entry = Decimal(0)
        elif reducing:
            # Same-direction reduction: realise only the closed portion; the retained leg keeps
            # its original cost basis.
            closed = abs(old_position) - abs(target)
            realised = closed * _sign(old_position) * notional * (price / old_entry - 1)
            book.cash += realised
            res.trade_pnls.append(realised)
            new_entry = old_entry
        else:
            # Opening from flat, or a same-direction increase: nothing realised; entry becomes
            # the size-weighted average of the retained and added notional.
            new_entry = (abs(old_position) * old_entry + abs(delta) * price) / abs(target)
        if reducing:
            # A closing leg trades at its current value - what the venue charges fees on.
            value = abs(delta) * notional * price / old_entry
            qty = _units(delta, notional, old_entry)
        else:
            value = abs(delta) * notional
            qty = _units(delta, notional, price)
        cost = fill_cost(notional_delta=value, notional=notional, spread_bps=spread_bps,
                         taker_fee_rate=taker_fee_rate, impact_bps=impact_bps)
        book.cash -= cost
        book.position = target
        book.entry = new_entry
        res.fill_notionals.append(value)
        res.trades.append((ts, "buy" if delta > 0 else "sell", qty))
        res.fills += 1
        log(ts, kind, price, -cost, note)
        if target == 0:
            book.funding = Decimal(0)
            hook = getattr(strategy, "on_flatten", None)   # simple test strategies may not have one
            if callable(hook):
                hook()

    def mark(ts: datetime, price: Decimal | None) -> None:
        nonlocal last_equity
        eq = book.equity(price)
        res.equity.append((ts, eq))
        res.returns.append((eq - last_equity) / notional)
        last_equity = eq
        log(ts, "mark", price, Decimal(0))

    for t in range(warmup, len(bars) - 1):
        bar, nxt = bars[t], bars[t + 1]

        # 1. Intrabar stop.
        # ponytail: stop and guards are bar-granular vs the router's 20 s loop; move to 1m bars once the 1m backfill exists
        if book.position != 0 and bar.high is not None and bar.low is not None:
            long = book.position > 0
            trigger = stop_price(side="long" if long else "short", entry=book.entry)
            if (bar.low <= trigger) if long else (bar.high >= trigger):
                trade_to(Decimal(0), trigger, bar.spread_bps, bar.open_ts, "stop")

        # 2. Funding on the position's value at the close.
        if book.position != 0 and bar.funding_rate is not None and bar.close is not None:
            paid = -book.position * notional * (bar.close / book.entry) * bar.funding_rate
            book.cash += paid
            book.funding += paid
            log(bar.open_ts, "funding", bar.close, paid)

        # 3. Gaps. Invariant: a position can only be non-zero here if the previous iteration saw
        # this bar priced (nxt.close not None), so bar.close is never None when book.position != 0.
        if not bar.complete or nxt.close is None:
            if book.position != 0 and bar.close is not None:
                trade_to(Decimal(0), bar.close, bar.spread_bps, nxt.open_ts, "gap_flatten")
            mark(nxt.open_ts, nxt.close)
            continue

        # 4. The router's protective exits, at the close.
        if book.position != 0:
            view = book.view(bar.instrument_id, bar.close)
            if check_open(view, mark=bar.close) == "flatten" or funding_exit_due(view):
                trade_to(Decimal(0), bar.close, bar.spread_bps, nxt.open_ts, "guard_exit")

        # 5. Decide. A flip exits this bar; re-entry is next bar's decision.
        target = clamp_target(strategy.target(bars[: t + 1]))
        if book.position != 0 and target != 0 and _sign(target) != _sign(book.position):
            target = Decimal(0)
        if target != book.position:
            fill_ts = nxt.open_ts + latency
            price = minute_closes.get(floor_minute(fill_ts))
            if price is not None:
                trade_to(target, price, bar.spread_bps, nxt.open_ts, "fill")
            elif nxt.open is not None:
                # Spec amendment (Task 5): proxy 1m candles exist for ~3.5 days only.
                # Fall back to the hourly open and COUNT it so records show the reliance.
                res.fills_at_hourly_open += 1
                trade_to(target, nxt.open, bar.spread_bps, nxt.open_ts, "fill",
                         note="fill_source=hourly_open")
            else:
                res.fills_unavailable += 1
                log(nxt.open_ts, "fill_unavailable", None, Decimal(0), f"no price at {fill_ts.isoformat()}")

        mark(nxt.open_ts, nxt.close)

    return res
```

- [ ] **Step 4: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/test_parity.py tests/test_harness.py tests/test_live_bars.py tests/test_sim_executor.py tests/test_order_router.py tests/test_state_recovery.py tests/test_strategies.py tests/test_run_backtest_script.py tests/test_dashboard_state.py -v`
Expected: PASS. If `test_parity` fails, compare `res.trades` with `r_trades` element by element before touching either side: the rules listed in the harness docstring are the contract, and the router's rules do not change.

- [ ] **Step 5: Run the full suite**

Run: `.venv/Scripts/python -m pytest -q`
Expected: all pass, 0 failed.

- [ ] **Step 6: Commit**

```bash
git add polyperps/risk/liquidation_guard.py polyperps/execution/sim_executor.py polyperps/dashboard/state.py polyperps/backtest/harness.py polyperps/execution/live_bars.py polyperps/execution/order_router.py tests/test_parity.py tests/test_harness.py tests/test_live_bars.py tests/test_sim_executor.py tests/test_order_router.py tests/test_state_recovery.py
git commit -m "$(cat <<'EOF'
feat(backtest): harness follows the router's rules; one cost model; gap bars; golden parity test

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 11: `harness_version` in every record; the gate accepts only the current one

**Files:**
- Modify: `polyperps/backtest/harness.py` (constant after the imports)
- Modify: `scripts/run_backtest.py:32` (import), `:138-159` (record)
- Modify: `polyperps/signal/base.py:1-51` (docstring, `_record_passes`)
- Test: `tests/test_signal_gate.py`, `tests/test_run_backtest_script.py`

**Interfaces:**
- Consumes: the Task 10 harness.
- Produces: `polyperps.backtest.harness.HARNESS_VERSION = 2`; every validation record carries `"harness_version": 2`; `signal.base._record_passes` rejects any record whose `harness_version` is not exactly the int `HARNESS_VERSION`.

Decision: re-run exactly the two existing Hyperliquid screens (Decision 21).

- [ ] **Step 1: Write the failing tests**

In `tests/test_signal_gate.py` add `from polyperps.backtest.harness import HARNESS_VERSION`, change `PASSING` to

```python
PASSING = {"run_id": "r1", "passed": True, "source_type": "polymarket_rest", "harness_version": HARNESS_VERSION,
           "sufficiency": {"met": True, "shortfall": {}}, "holdout": {"fills_at_hourly_open": 0}}
```

and append:

```python
def test_record_from_an_older_harness_is_rejected(tmp_path):
    cases = [{k: v for k, v in PASSING.items() if k != "harness_version"},   # Phase 1 records have none
             {**PASSING, "harness_version": HARNESS_VERSION - 1},
             {**PASSING, "harness_version": str(HARNESS_VERSION)}]
    for n, bad in enumerate(cases):
        d = tmp_path / f"case{n}"
        d.mkdir()
        log, val = _files(d, record=bad, validated=APPROVAL)
        assert load_validated(validated_path=val, log_path=log) is False


def test_harness_version_pinned():
    assert HARNESS_VERSION == 2
```

In `tests/test_run_backtest_script.py`, add `from polyperps.backtest.harness import HARNESS_VERSION` and, in `test_run_backtest_h1_native_appends_one_record`, after `assert record["hypothesis"] == "h1"` add `assert record["harness_version"] == HARNESS_VERSION`.

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_signal_gate.py tests/test_run_backtest_script.py -v`
Expected: FAIL, `ImportError: cannot import name 'HARNESS_VERSION'`.

- [ ] **Step 3: Implement**

In `polyperps/backtest/harness.py`, add after `_Q = Decimal("0.00000001")`:

```python
# Bump on any change to the harness's trading rules or cost model. Validation records carry it and
# the live gate (signal.base) accepts only records at the current version.
# 1 = Phase 1 harness (flip in one fill, no guards); 2 = Part A router parity.
HARNESS_VERSION = 2
```

In `scripts/run_backtest.py`, change the harness import to `from polyperps.backtest.harness import HARNESS_VERSION, run_backtest` and add `"harness_version": HARNESS_VERSION,` to `record` right after `"hypothesis": args.hypothesis,`.

In `polyperps/signal/base.py`:
- add `from polyperps.backtest.harness import HARNESS_VERSION` to the imports.
- in the module docstring, after the line `holdout.fills_at_hourly_open == 0` add a line
  `             and harness_version == the current HARNESS_VERSION (Part A §6.6)`
- replace `_record_passes` with:

```python
def _record_passes(record: dict, run_id: str) -> bool:
    """`passed` is necessary but not sufficient: the gate re-derives the pre-registered
    conditions from the record so a hand-edited or stale `passed` flag cannot open it.
    Any missing key is a False."""
    if record.get("run_id") != run_id or record.get("passed") is not True:
        return False
    version = record.get("harness_version")
    if type(version) is not int or version != HARNESS_VERSION:
        return False   # a result from older trading rules says nothing about the code that trades
    if record.get("source_type") not in _NATIVE_VALUES:
        return False
    sufficiency = record.get("sufficiency")
    if not isinstance(sufficiency, dict) or sufficiency.get("met") is not True:
        return False
    holdout = record.get("holdout")
    if not isinstance(holdout, dict):
        return False
    fallback_fills = holdout.get("fills_at_hourly_open")
    # exact int 0 only: JSON false/None/"0" must not read as zero fills
    return type(fallback_fills) is int and fallback_fills == 0
```

- [ ] **Step 4: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/test_signal_gate.py tests/test_run_backtest_script.py tests/test_validation_log.py tests/test_gates.py -v`
Expected: PASS.

- [ ] **Step 5: Run the full suite**

Run: `.venv/Scripts/python -m pytest -q`
Expected: all pass, 0 failed.

- [ ] **Step 6: Commit**

```bash
git add polyperps/backtest/harness.py scripts/run_backtest.py polyperps/signal/base.py tests/test_signal_gate.py tests/test_run_backtest_script.py
git commit -m "$(cat <<'EOF'
feat(signal): harness_version in every validation record; the gate accepts only the current version

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 7 (operator, not the implementer): re-run the Hyperliquid screens under harness 2**

This step needs the DB that holds the Hyperliquid candles and funding for instrument 6 (the box's `/var/lib/polyperps/polyperps.sqlite3`, or a local DB with the same backfill); Hyperliquid is reachable from home, Polymarket is not. The implementer stops here and hands these commands to the user.

Local (records go straight into the committed log):

```bash
POLYPERPS_INSTRUMENT_IDS=6 .venv/Scripts/python scripts/run_backtest.py --hypothesis h1 --instrument 6 --source hyperliquid --fee-category equity
POLYPERPS_INSTRUMENT_IDS=6 .venv/Scripts/python scripts/run_backtest.py --hypothesis h3 --instrument 6 --source hyperliquid --fee-category equity
```

On the box (PuTTY session `polyperps-ec2`), write to a scratch log and copy the two new lines into `polyperps/signal/validation_log.jsonl` locally:

```bash
cd /opt/polyperps && sudo -u polyperps env POLYPERPS_INSTRUMENT_IDS=6 POLYPERPS_DB_PATH=/var/lib/polyperps/polyperps.sqlite3 .venv/bin/python scripts/run_backtest.py --hypothesis h1 --instrument 6 --source hyperliquid --fee-category equity --log-path /var/lib/polyperps/screens-v2.jsonl
cd /opt/polyperps && sudo -u polyperps env POLYPERPS_INSTRUMENT_IDS=6 POLYPERPS_DB_PATH=/var/lib/polyperps/polyperps.sqlite3 .venv/bin/python scripts/run_backtest.py --hypothesis h3 --instrument 6 --source hyperliquid --fee-category equity --log-path /var/lib/polyperps/screens-v2.jsonl
```

Then commit the appended records:

```bash
git add polyperps/signal/validation_log.jsonl
git commit -m "$(cat <<'EOF'
chore(signal): re-run the h1/h3 Hyperliquid screens under harness_version 2

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 12: `scripts/live_smoke.py` (user-run only)

**Files:**
- Create: `scripts/live_smoke.py`
- Create: `tests/test_live_smoke.py`
- Modify: `tests/test_deploy_files.py` (append)

**Interfaces:**
- Consumes: `LiveReader(session)`, `open_session(label)` (Task 1); `venue-{order_id}` fills (Task 7).
- Produces: `scripts/live_smoke.py` with `CONFIRM = "I ACCEPT REAL ORDERS"`, `STOP_DISTANCE = Decimal("0.01")`, `POLL_S = 5.0`, `confirmed(instrument_id, ask) -> bool`, `async smoke(session, *, instrument_id, quantity, side, ask, say, poll_s) -> dict`, `main(argv=None, *, ask=input, say=print, open_session=open_session, poll_s=POLL_S) -> int` (2 = refused before any session, 0 = the exchange's stop closed the position, 1 = otherwise).

Decision: `--quantity` is supplied by the operator (Decision 20).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_live_smoke.py`:

```python
"""scripts/live_smoke.py places REAL orders when a human runs it. Here it only ever meets a fake
session; the real open_session is replaced in every test."""

import asyncio
import importlib.util
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

T0 = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


def load():
    spec = importlib.util.spec_from_file_location("live_smoke", "scripts/live_smoke.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_refuses_without_the_typed_confirmation():
    mod = load()
    opened = []

    async def open_session(label):
        opened.append(label)
        raise AssertionError("must not open a session")

    for answers in (["7", mod.CONFIRM], ["6", "i accept real orders"], ["6", ""], ["", mod.CONFIRM]):
        it = iter(answers)
        code = mod.main(["--instrument", "6", "--quantity", "0.001"], ask=lambda _p: next(it),
                        say=lambda _m: None, open_session=open_session)
        assert code == 2
    assert opened == []


class FakeSmokeSession:
    """The IOC fills at 100; after the operator presses Enter the exchange stop fills (no client id)."""
    def __init__(self):
        self.calls = []
        self.size = Decimal(0)
        self.stopped = asyncio.Event()

    async def place_order(self, **kw):
        self.calls.append(("place_order", kw))
        self.size = Decimal(kw["quantity"])
        return SimpleNamespace(order=SimpleNamespace(id=1, status="filled"))

    async def place_position_tp_sl(self, **kw):
        self.calls.append(("tp_sl", kw))
        return SimpleNamespace(stop_loss=SimpleNamespace(order_id=2))

    def stop_fills(self):
        self.size = Decimal(0)
        self.stopped.set()

    async def fetch_portfolio(self):
        positions = () if self.size == 0 else (SimpleNamespace(
            instrument_id=6, size=self.size, entry_price=Decimal(100), leverage=3, position_value=self.size * 100,
            liquidation_price=None, unrealized_pnl=Decimal(0), cumulative_funding=Decimal("-0.01")),)
        return SimpleNamespace(positions=positions, margin=SimpleNamespace(total_account_value=Decimal(1000)),
                               in_liquidation=False)

    async def fetch_open_orders(self):
        return ()

    def __aiter__(self):
        async def gen():
            await self.stopped.wait()
            yield SimpleNamespace(type="fill", timestamp=T0, payload=[SimpleNamespace(
                client_order_id=None, order_id=2, instrument_id=6, side="short", quantity=Decimal("0.001"),
                price=Decimal(99), fee=Decimal(0))])
            await asyncio.Event().wait()
        return gen()

    async def close(self):
        self.calls.append(("close", {}))


def test_smoke_with_a_fake_session_places_order_and_stop_and_sees_the_venue_fill():
    mod = load()
    session = FakeSmokeSession()
    answers = iter(["6", mod.CONFIRM])

    def ask(prompt):
        if prompt.startswith("kill the bot now"):
            session.stop_fills()                  # the operator waited for the exchange stop
            return ""
        return next(answers)

    sdk = SimpleNamespace(closed=False)

    async def sdk_close():
        sdk.closed = True

    sdk.close = sdk_close

    async def open_session(label):
        return sdk, session

    said = []
    code = mod.main(["--instrument", "6", "--quantity", "0.001"], ask=ask, say=said.append,
                    open_session=open_session, poll_s=0)
    assert code == 0
    (order,) = [kw for name, kw in session.calls if name == "place_order"]
    assert order["side"] == "BUY" and order["time_in_force"] == "ioc" and order["reduce_only"] is False
    assert order["quantity"] == Decimal("0.001")
    (stop,) = [kw for name, kw in session.calls if name == "tp_sl"]
    assert stop["instrument_id"] == 6 and stop["stop_loss"].trigger_price == Decimal("99.00")
    assert ("close", {}) in session.calls and sdk.closed
    assert any("stop_filled_by_exchange': True" in s for s in said)
```

Append to `tests/test_deploy_files.py`:

```python
@pytest.mark.parametrize("name", UNIT_FILES + SHELL_FILES + ["polyperps-health.service", "polyperps-prune.service"])
def test_no_deploy_file_runs_the_live_smoke_script(name):
    assert "live_smoke" not in _read(name)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/Scripts/python -m pytest tests/test_live_smoke.py tests/test_deploy_files.py -v`
Expected: FAIL, `FileNotFoundError` for `scripts/live_smoke.py` (the new deploy test passes already).

- [ ] **Step 3: Implement**

Create `scripts/live_smoke.py`:

```python
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
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from polyperps.execution.live_executor import LiveReader, open_session
from polyperps.execution.types import FillUpdate

CONFIRM = "I ACCEPT REAL ORDERS"
STOP_DISTANCE = Decimal("0.01")
POLL_S = 5.0


def confirmed(instrument_id: int, ask: Callable[[str], str]) -> bool:
    if ask(f"Type the instrument id ({instrument_id}) to trade for real: ").strip() != str(instrument_id):
        return False
    return ask(f"Type exactly '{CONFIRM}': ").strip() == CONFIRM


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
        cid = f"smoke-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
        placed = await session.place_order(instrument_id=instrument_id, side=side.upper(), quantity=quantity,
                                           time_in_force="ioc", reduce_only=False, client_order_id=cid)
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
        ask("kill the bot now, press Enter after the stop fills")
        funding = pos.cumulative_funding
        while True:
            p = (await reader.snapshot()).position(instrument_id)
            if p is None or p.size == 0:
                break
            funding = p.cumulative_funding
            say(f"still open: size={p.size} cumulative_funding={p.cumulative_funding}")
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


def main(argv: list[str] | None = None, *, ask: Callable[[str], str] = input, say: Callable[[str], None] = print,
         open_session: Callable = open_session, poll_s: float = POLL_S) -> int:
    ap = argparse.ArgumentParser(description="Manual live smoke test. PLACES REAL ORDERS.")
    ap.add_argument("--instrument", type=int, required=True)
    ap.add_argument("--quantity", type=Decimal, required=True, help="the venue's minimum order size")
    ap.add_argument("--side", choices=["buy", "sell"], default="buy")
    args = ap.parse_args(argv)
    if not confirmed(args.instrument, ask):
        say("refused: confirmation not typed exactly; nothing was sent")
        return 2
    return asyncio.run(_run(args, ask, say, open_session, poll_s))


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: Run the tests**

Run: `.venv/Scripts/python -m pytest tests/test_live_smoke.py tests/test_deploy_files.py -v`
Expected: PASS.

- [ ] **Step 5: Run the full suite**

Run: `.venv/Scripts/python -m pytest -q`
Expected: all pass, 0 failed.

- [ ] **Step 6: Commit**

```bash
git add scripts/live_smoke.py tests/test_live_smoke.py tests/test_deploy_files.py
git commit -m "$(cat <<'EOF'
feat(scripts): user-run live smoke script behind a typed confirmation, tested with a fake session

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
)"
```

---

## Spec coverage

| Spec section | Task |
|---|---|
| §3.1 `run_trader.py`, three modes, `live` exits 2, unit switched, `run_paper.py` deleted | 4 |
| §3.2 `on_tick` / `on_bar` / `poll_fills` on the protocol | 1 (wired in 4) |
| §3.3 `LiveReader`, `ShadowExecutor`, `ShadowRefused` handled like a submit error, reconcile/recovery on the real account | 1, 6 (submit), 5 (recovery) |
| §3.4 `account_snapshots` table, dashboard reads it | 2 (written by the runner in 4) |
| §4.1 `missing_stop` for every local state; stop from the exchange entry | 5 |
| §4.2 recovery reads every instrument incl. HALTED | 5 |
| §4.3 `PENDING_TIMEOUT_S = 30`, adopt, `adopted`, WARN `pending_timeout` | 6 |
| §4.4 late fill while HALTED | 6 |
| §4.5 submit errors → `error`, CRITICAL `submit_error`, adopt, halt | 6 |
| §4.6 `_landed` any-direction, `duplicate_order` = landed, filled quantity accumulates | 7 |
| §4.7 fills without a client id | 7 |
| §4.8 gate re-checked on every order | 6 |
| §4.9 exposure counts in-flight orders | 8 |
| §4.10 event stream end/raise ends the process | 4 (`test_event_stream_end_or_error_ends_the_run`) |
| §4.11 funding drift wired | 9 |
| §5 loss limit −5 % pause / −10 % flatten + halt, strictest wins, runner passes equity each bar | 3, 4 |
| §6.1 flip = exit then re-enter next bar | 10 |
| §6.2 guards + intrabar stop in the backtest, `ponytail:` comment | 10 |
| §6.3 SimExecutor uses `costs.fill_cost` | 10 |
| §6.4 `LiveBarBuilder` gap marking, router `data_gap` exit | 10 |
| §6.5 golden parity test | 10 |
| §6.6 `harness_version`, gate rejects old versions, screens re-run | 11 |
| §7 `live_smoke.py`, typed confirmation, fake session only | 12 |
| §8 tests: stop invariant × states (5), pending timeout (6), retry (7), submit error + GateClosed (6), fill without client id (7), in-flight exposure (8), loss limit (3), shadow (1, 6), parity + gap + old version (10, 11), runner three modes (4), dashboard (2), smoke refusal (12) | as listed |
