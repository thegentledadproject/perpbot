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
