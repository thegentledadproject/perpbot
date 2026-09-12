"""Execution value types. Our own frozen dataclasses on both sides of the
Executor boundary; SDK models never cross it (spec section 4.1)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Literal

from polyperps.exchange.types import _require_aware


class State(StrEnum):
    FLAT = "FLAT"
    ENTRY_PENDING = "ENTRY_PENDING"
    OPEN = "OPEN"
    EXIT_PENDING = "EXIT_PENDING"
    LIQUIDATED = "LIQUIDATED"
    HALTED = "HALTED"


Side = Literal["buy", "sell"]
AckStatus = Literal["accepted", "rejected"]
OrderStatus = Literal["accepted", "open", "partial", "filled", "cancelled", "auto_cancelled", "rejected"]


@dataclass(frozen=True, slots=True, kw_only=True)
class Intent:
    instrument_id: int
    side: Side
    quantity: Decimal
    notional: Decimal
    reduce_only: bool = False
    reason: str = "strategy"


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderRequest:
    client_order_id: str
    instrument_id: int
    side: Side
    quantity: Decimal
    reduce_only: bool
    ts: datetime

    def __post_init__(self) -> None:
        _require_aware(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderAck:
    client_order_id: str
    exchange_order_id: str | None
    status: AckStatus
    reason: str
    ts: datetime

    def __post_init__(self) -> None:
        _require_aware(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class StopAck:
    instrument_id: int
    trigger_price: Decimal
    exchange_order_id: str | None
    ts: datetime

    def __post_init__(self) -> None:
        _require_aware(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderUpdate:
    client_order_id: str
    status: OrderStatus
    filled_quantity: Decimal
    ts: datetime

    def __post_init__(self) -> None:
        _require_aware(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class FillUpdate:
    client_order_id: str
    instrument_id: int
    side: Side
    quantity: Decimal
    price: Decimal
    fee: Decimal
    ts: datetime

    def __post_init__(self) -> None:
        _require_aware(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class PositionView:
    instrument_id: int
    size: Decimal            # signed: + long, - short
    entry_price: Decimal
    notional: Decimal        # |size| x mark
    leverage: int
    liquidation_price: Decimal | None
    unrealised_pnl: Decimal
    cumulative_funding: Decimal   # negative = paid


@dataclass(frozen=True, slots=True, kw_only=True)
class AccountSnapshot:
    equity: Decimal
    positions: tuple[PositionView, ...]
    open_orders: tuple[str, ...]      # client order ids resting on the venue
    stops: dict[int, Decimal]         # instrument_id -> trigger price
    in_liquidation: bool
    ts: datetime

    def __post_init__(self) -> None:
        _require_aware(self)

    def position(self, instrument_id: int) -> PositionView | None:
        for p in self.positions:
            if p.instrument_id == instrument_id:
                return p
        return None


# --- persisted rows ----------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class DecisionRow:
    run_id: str
    instrument_id: int
    seq: int
    ts: datetime
    state_before: State
    target: Decimal | None
    verdicts: dict[str, str] = field(default_factory=dict)
    intent: Intent | None = None
    client_order_id: str | None = None
    note: str = ""

    def __post_init__(self) -> None:
        _require_aware(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderRow:
    client_order_id: str
    run_id: str
    instrument_id: int
    side: Side
    quantity: Decimal
    reduce_only: bool
    status: str
    exchange_order_id: str | None
    filled_quantity: Decimal
    avg_price: Decimal | None
    submitted_at: datetime
    updated_at: datetime
    reason: str

    def __post_init__(self) -> None:
        _require_aware(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class PositionLocalRow:
    run_id: str
    instrument_id: int
    state: State
    size: Decimal
    entry_price: Decimal | None
    stop_trigger: Decimal | None
    stop_order_id: str | None
    cumulative_funding: Decimal
    updated_at: datetime

    def __post_init__(self) -> None:
        _require_aware(self)


@dataclass(frozen=True, slots=True)
class ReconcileNow:
    reason: str = ""
