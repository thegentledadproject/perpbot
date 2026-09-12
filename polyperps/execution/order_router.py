"""Spec section 6: per-instrument state machine and the portfolio that drives it.

Rows before side effects: decisions + orders are written BEFORE executor.submit;
positions_local after every state change. Client order ids are
f"{run_id}-{instrument_id}-{seq}" and are reused on a retry after timeout.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from typing import Literal

from polyperps.backtest.bars import Bar
from polyperps.backtest.strategy import Strategy, clamp_target
from polyperps.execution.executor import Executor, ExecutorTimeout
from polyperps.execution.types import (
    AccountSnapshot, DecisionRow, FillUpdate, Intent, OrderAck, OrderRequest, OrderRow, OrderUpdate,
    PositionLocalRow, ReconcileNow, State,
)
from polyperps.monitor.alerts import Alert, Alerter, margin_alert
from polyperps.risk.kill_switch import Action
from polyperps.risk.liquidation_guard import (
    LIMITS, Reject, Resize, RiskLimits, Verdict, check_open, funding_exit_due, stop_price, verdict_label, vet_entry,
)
from polyperps.risk.portfolio_exposure import EXPOSURE, ExposureLimits, vet_exposure
from polyperps.storage import db

_Q = Decimal("0.00000001")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def apply_guards(intent: Intent, verdicts: Sequence[tuple[str, Verdict]]) -> tuple[Intent | None, dict[str, str]]:
    labels = {name: verdict_label(v) for name, v in verdicts}
    if any(isinstance(v, Reject) for _, v in verdicts):
        return None, labels
    qty = min([v.quantity for _, v in verdicts if isinstance(v, Resize)] + [intent.quantity])
    if qty == intent.quantity:
        return intent, labels
    notional = (intent.notional * qty / intent.quantity).quantize(Decimal("0.01"))
    return Intent(instrument_id=intent.instrument_id, side=intent.side, quantity=qty, notional=notional,
                  reduce_only=intent.reduce_only, reason=intent.reason), labels


class InstrumentRouter:
    def __init__(
        self,
        *,
        run_id: str,
        instrument_id: int,
        category: str,
        strategy: Strategy,
        executor: Executor,
        conn,
        alerter: Alerter,
        categories: Mapping[int, str],
        clock: Callable[[], datetime] = _utcnow,
        limits: RiskLimits = LIMITS,
        exposure: ExposureLimits = EXPOSURE,
        ack_timeout_s: float = 10.0,
    ) -> None:
        self.run_id, self.instrument_id, self.category = run_id, instrument_id, category
        self.strategy, self.executor, self.conn, self.alerter = strategy, executor, conn, alerter
        self.categories, self.clock, self.limits, self.exposure = categories, clock, limits, exposure
        self.ack_timeout_s = ack_timeout_s
        self.state = State.FLAT
        self.seq = 0
        self.size = Decimal(0)
        self.entry: Decimal | None = None
        self.stop_trigger: Decimal | None = None
        self.cumulative_funding = Decimal(0)
        self._pending_cid: str | None = None
        self._dseq = 0  # decision-row primary-key counter; distinct from self.seq (order count / cid numbering)

    # --- persistence helpers ------------------------------------------------
    def load_local(self, row: PositionLocalRow) -> None:
        self.state, self.size, self.entry = row.state, row.size, row.entry_price
        self.stop_trigger, self.cumulative_funding = row.stop_trigger, row.cumulative_funding

    def _persist(self) -> None:
        db.upsert_position_local(self.conn, PositionLocalRow(
            run_id=self.run_id, instrument_id=self.instrument_id, state=self.state, size=self.size,
            entry_price=self.entry, stop_trigger=self.stop_trigger, stop_order_id=None,
            cumulative_funding=self.cumulative_funding, updated_at=self.clock()))

    def _set_state(self, state: State) -> None:
        self.state = state
        self._persist()

    def _alert(self, level: Literal["INFO", "WARN", "CRITICAL"], kind: str, **detail) -> None:
        self.alerter.emit(Alert(level=level, kind=kind, instrument_id=self.instrument_id,
                                detail={k: str(v) for k, v in detail.items()}, ts=self.clock()))

    def _record(self, *, target: Decimal | None, verdicts: dict[str, str], intent: Intent | None,
                cid: str | None, note: str = "") -> None:
        self._dseq += 1
        db.insert_decision(self.conn, DecisionRow(run_id=self.run_id, instrument_id=self.instrument_id, seq=self._dseq,
                                                  ts=self.clock(), state_before=self.state, target=target,
                                                  verdicts=verdicts, intent=intent, client_order_id=cid, note=note))

    # --- bar cycle --------------------------------------------------------------
    async def on_bar(self, history: Sequence[Bar], snapshot: AccountSnapshot, kill: Action) -> None:
        if self.state in (State.HALTED, State.LIQUIDATED, State.ENTRY_PENDING, State.EXIT_PENDING):
            self._record(target=None, verdicts={}, intent=None, cid=None, note=f"skip:{self.state.value}")
            return
        mark = history[-1].close
        if mark is None:
            self._record(target=None, verdicts={}, intent=None, cid=None, note="skip:no_close")
            return
        if kill == "shutdown":
            if self.state is State.OPEN:
                await self._exit(mark, "kill_shutdown", target=None)
            self._alert("CRITICAL", "kill_switch", action="shutdown")
            self._set_state(State.HALTED)
            return
        target = clamp_target(self.strategy.target(history))
        if self.state is State.FLAT:
            if target == 0 or kill == "pause":
                self._record(target=target, verdicts={}, intent=None, cid=None,
                             note="kill:pause" if kill == "pause" else "flat:no_target")
                return
            side = "buy" if target > 0 else "sell"
            qty = (self.limits.notional_usd / mark).quantize(_Q, rounding=ROUND_DOWN)
            intent = Intent(instrument_id=self.instrument_id, side=side, quantity=qty, notional=self.limits.notional_usd)
            final, labels = apply_guards(intent, [
                ("vet_entry", vet_entry(intent, mark=mark, snapshot=snapshot, limits=self.limits)),
                ("vet_exposure", vet_exposure(intent, positions=snapshot.positions, equity=snapshot.equity,
                                              categories=self.categories, limits=self.exposure)),
            ])
            if final is None:
                self._record(target=target, verdicts=labels, intent=None, cid=None, note="rejected")
                return
            await self._send(final, target=target, verdicts=labels)
            return
        # OPEN
        flip = (self.size > 0 and target < 0) or (self.size < 0 and target > 0)
        if target == 0 or flip:
            await self._exit(mark, "strategy" if target == 0 else "flip", target=target)
        else:
            self._record(target=target, verdicts={}, intent=None, cid=None, note="hold")

    async def _exit(self, mark: Decimal, reason: str, *, target: Decimal | None) -> None:
        side = "sell" if self.size > 0 else "buy"
        intent = Intent(instrument_id=self.instrument_id, side=side, quantity=abs(self.size),
                        notional=(abs(self.size) * mark).quantize(Decimal("0.01")), reduce_only=True, reason=reason)
        await self._send(intent, target=target, verdicts={})

    # --- fast loop --------------------------------------------------------------
    async def on_fast(self, mark: Decimal, snapshot: AccountSnapshot) -> None:
        if self.state is not State.OPEN:
            return
        pos = snapshot.position(self.instrument_id)
        if pos is None:
            return  # vanished: reconciliation decides
        if pos.liquidation_price is not None:
            a = margin_alert(abs(pos.liquidation_price - mark) / mark, self.instrument_id, self.clock())
            if a is not None:
                self.alerter.emit(a)
        if check_open(pos, mark=mark, limits=self.limits) == "flatten":
            await self._exit(mark, "liq_distance", target=None)
        elif funding_exit_due(pos, limits=self.limits):
            await self._exit(mark, "funding_cost", target=None)

    # --- sending with idempotent retry ----------------------------------------
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
        self._set_state(State.EXIT_PENDING if intent.reduce_only else State.ENTRY_PENDING)
        req = OrderRequest(client_order_id=cid, instrument_id=self.instrument_id, side=intent.side,
                           quantity=intent.quantity, reduce_only=intent.reduce_only, ts=now)
        ack = await self._submit_with_recovery(req)
        if ack is None:
            await self.halt("order lost after timeout and retry")
            return
        if ack.status == "rejected":
            self._update_order(cid, status="rejected", reason=ack.reason)
            self._alert("WARN", "order_rejected", client_order_id=cid, reason=ack.reason)
            self._pending_cid = None
            self._set_state(prior)
            return
        self._update_order(cid, status="accepted", exchange_order_id=ack.exchange_order_id,
                           reason=ack.reason or intent.reason)

    async def _submit_with_recovery(self, req: OrderRequest) -> OrderAck | None:
        for attempt in (1, 2):
            try:
                return await asyncio.wait_for(self.executor.submit(req), self.ack_timeout_s)
            except (asyncio.TimeoutError, ExecutorTimeout):
                snap = await self.executor.snapshot()
                if self._landed(req, snap):
                    self._alert("WARN", "ack_lost", client_order_id=req.client_order_id, attempt=attempt)
                    return OrderAck(client_order_id=req.client_order_id, exchange_order_id=None, status="accepted",
                                    reason="adopted after timeout", ts=self.clock())
                if attempt == 1:
                    self._alert("WARN", "retry", client_order_id=req.client_order_id)
        return None

    def _landed(self, req: OrderRequest, snap: AccountSnapshot) -> bool:
        if req.client_order_id in snap.open_orders:
            return True
        delta = req.quantity if req.side == "buy" else -req.quantity
        pos = snap.position(self.instrument_id)
        actual = pos.size if pos is not None else Decimal(0)
        return actual == self.size + delta

    def _update_order(self, cid: str, **changes) -> None:
        row = db.get_order(self.conn, cid)
        if row is None:
            return
        db.upsert_order(self.conn, replace(row, **changes, updated_at=self.clock()))

    # --- events -------------------------------------------------------------------
    async def handle_event(self, ev: OrderUpdate | FillUpdate) -> None:
        if isinstance(ev, OrderUpdate):
            if ev.client_order_id == self._pending_cid and ev.status in ("cancelled", "auto_cancelled", "rejected"):
                self._update_order(ev.client_order_id, status=ev.status)
                self._alert("WARN", "order_" + ev.status, client_order_id=ev.client_order_id)
                self._pending_cid = None
                self._set_state(State.OPEN if self.size != 0 else State.FLAT)
            elif db.get_order(self.conn, ev.client_order_id) is not None:
                self._update_order(ev.client_order_id, status=ev.status, filled_quantity=ev.filled_quantity)
            return
        if ev.instrument_id != self.instrument_id:
            return
        ours = db.get_order(self.conn, ev.client_order_id) is not None
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

    def _apply_fill(self, ev: FillUpdate) -> None:
        delta = ev.quantity if ev.side == "buy" else -ev.quantity
        old, new = self.size, self.size + delta
        entry = self.entry or Decimal(0)
        if old == 0 or new == 0 or (old > 0) != (new > 0):
            self.entry = ev.price if new != 0 else None
        elif abs(new) > abs(old):
            self.entry = (abs(old) * entry + abs(delta) * ev.price) / abs(new)
        self.size = new
        if new == 0:
            self.cumulative_funding = Decimal(0)
        self._persist()

    async def replace_stop(self) -> None:
        if self.size == 0 or self.entry is None:
            return
        trigger = stop_price(side="long" if self.size > 0 else "short", entry=self.entry, limits=self.limits)
        await self.executor.place_stop(self.instrument_id, trigger)
        self.stop_trigger = trigger
        self._persist()
        self._alert("INFO", "stop_placed", trigger=trigger)

    async def halt(self, reason: str) -> None:
        self._alert("CRITICAL", "halted", reason=reason)
        self._pending_cid = None
        self._set_state(State.HALTED)

    def clear_halt(self) -> None:
        self._set_state(State.OPEN if self.size != 0 else State.FLAT)


class Portfolio:
    def __init__(self, *, run_id: str, executor: Executor, conn, alerter: Alerter,
                 routers: Mapping[int, InstrumentRouter],
                 reconcile: Callable[[], Awaitable[None]] | None = None) -> None:
        self.run_id, self.executor, self.conn, self.alerter = run_id, executor, conn, alerter
        self.routers = dict(routers)
        self.reconcile = reconcile

    async def on_bar(self, histories: Mapping[int, Sequence[Bar]], kill: Action) -> None:
        snapshot = await self.executor.snapshot()
        for iid, history in histories.items():
            router = self.routers.get(iid)
            if router is not None and history:
                await router.on_bar(history, snapshot, kill)

    async def on_fast(self, marks: Mapping[int, Decimal]) -> None:
        snapshot = await self.executor.snapshot()
        for iid, mark in marks.items():
            router = self.routers.get(iid)
            if router is not None:
                await router.on_fast(mark, snapshot)

    async def dispatch(self, ev: OrderUpdate | FillUpdate | ReconcileNow) -> None:
        if isinstance(ev, ReconcileNow):
            if self.reconcile is not None:
                await self.reconcile()
            return
        if isinstance(ev, FillUpdate):
            router = self.routers.get(ev.instrument_id)
            if router is not None:
                await router.handle_event(ev)
            return
        for router in self.routers.values():
            await router.handle_event(ev)

    async def run_event_pump(self) -> None:
        async for ev in self.executor.events():
            await self.dispatch(ev)
