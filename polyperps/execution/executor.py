"""The only surface the router talks to (spec section 4.2)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Protocol

from polyperps.execution.types import AccountSnapshot, FillUpdate, OrderAck, OrderRequest, OrderUpdate, StopAck


class ExecutorTimeout(RuntimeError):
    """The executor did not acknowledge in time; the order MAY have landed."""


class GateClosed(RuntimeError):
    """A live executor was requested while a live-order gate is closed."""


class Executor(Protocol):
    name: str

    async def submit(self, order: OrderRequest) -> OrderAck: ...
    async def cancel(self, client_order_id: str) -> None: ...
    async def place_stop(self, instrument_id: int, trigger_price: Decimal) -> StopAck: ...
    async def heartbeat(self) -> None: ...
    async def snapshot(self) -> AccountSnapshot: ...
    def events(self) -> AsyncIterator[OrderUpdate | FillUpdate]: ...
    async def close(self) -> None: ...
