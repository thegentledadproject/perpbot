"""Spec 2.2b: aggregate exposure caps. Pure. Clusters = instrument category; no correlation estimate."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal

from polyperps.execution.types import Intent, PositionView
from polyperps.risk.liquidation_guard import Allow, Reject, Resize, Verdict

_Q = Decimal("0.00000001")
_KNOWN = ("crypto", "equity", "index", "commodity")


@dataclass(frozen=True, slots=True, kw_only=True)
class ExposureLimits:
    gross: Decimal        # sum |notional| <= gross x equity
    cluster_net: Decimal  # |sum signed notional in a cluster| <= cluster_net x equity


EXPOSURE = ExposureLimits(gross=Decimal("1.0"), cluster_net=Decimal("0.6"))


def cluster_of(category: str) -> str:
    return category if category in _KNOWN else "other"


def vet_exposure(
    intent: Intent,
    *,
    positions: Sequence[PositionView],
    equity: Decimal,
    categories: Mapping[int, str],
    limits: ExposureLimits = EXPOSURE,
) -> Verdict:
    if equity <= 0:
        return Reject(reason="equity <= 0")

    sign = Decimal(1) if intent.side == "buy" else Decimal(-1)
    my_cluster = cluster_of(categories.get(intent.instrument_id, "other"))
    gross_existing = sum((p.notional for p in positions), Decimal(0))
    net_existing = sum(
        ((Decimal(1) if p.size > 0 else Decimal(-1)) * p.notional
         for p in positions if cluster_of(categories.get(p.instrument_id, "other")) == my_cluster),
        Decimal(0),
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
