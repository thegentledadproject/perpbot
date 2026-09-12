"""Spec 2.2: per-position leverage cap and liquidation-distance floor. Pure functions.

LIMITS is pre-registered (2026-09-12) and pinned by tests/test_liquidation_guard.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from typing import Literal

from polyperps.execution.types import AccountSnapshot, Intent, PositionView

_Q = Decimal("0.00000001")


@dataclass(frozen=True, slots=True, kw_only=True)
class RiskLimits:
    max_leverage: int
    min_liq_distance: Decimal
    stop_distance: Decimal
    notional_usd: Decimal
    max_funding_cost: Decimal
    maintenance_rate: Decimal


LIMITS = RiskLimits(
    max_leverage=3,
    min_liq_distance=Decimal("0.25"),
    stop_distance=Decimal("0.15"),
    notional_usd=Decimal("100"),
    max_funding_cost=Decimal("0.02"),
    maintenance_rate=Decimal("0.02"),
)


@dataclass(frozen=True, slots=True)
class Allow:
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class Resize:
    quantity: Decimal


@dataclass(frozen=True, slots=True, kw_only=True)
class Reject:
    reason: str


Verdict = Allow | Resize | Reject


def verdict_label(v: Verdict) -> str:
    if isinstance(v, Allow):
        return "allow"
    if isinstance(v, Resize):
        return f"resize:{v.quantity}"
    return f"reject:{v.reason}"


def vet_entry(intent: Intent, *, mark: Decimal, snapshot: AccountSnapshot, limits: RiskLimits = LIMITS) -> Verdict:
    if snapshot.equity <= 0:
        return Reject(reason="equity <= 0")
    existing = sum((p.notional for p in snapshot.positions), Decimal(0))
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


def check_open(position: PositionView, *, mark: Decimal, limits: RiskLimits = LIMITS) -> Literal["hold", "flatten"]:
    if position.liquidation_price is None or position.size == 0:
        return "hold"
    distance = abs(position.liquidation_price - mark) / mark
    return "flatten" if distance < limits.min_liq_distance else "hold"


def stop_price(*, side: Literal["long", "short"], entry: Decimal, limits: RiskLimits = LIMITS) -> Decimal:
    factor = (1 - limits.stop_distance) if side == "long" else (1 + limits.stop_distance)
    return (entry * factor).quantize(Decimal("0.01"))


def funding_exit_due(position: PositionView, *, limits: RiskLimits = LIMITS) -> bool:
    paid = -position.cumulative_funding
    return paid >= limits.max_funding_cost * position.notional
