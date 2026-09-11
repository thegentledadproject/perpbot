"""Execution cost model (spec 5.3). Pure Decimal arithmetic.

impact_bps is a pre-registered assumption (BAR.impact_bps), not a measurement.
"""

from __future__ import annotations

from decimal import Decimal

_BPS = Decimal(10_000)


def fill_cost(
    *,
    notional_delta: Decimal,
    notional: Decimal,
    spread_bps: Decimal,
    taker_fee_rate: Decimal,
    impact_bps: Decimal,
) -> Decimal:
    """Cost of changing exposure by notional_delta (absolute USD)."""
    if notional_delta == 0:
        return Decimal(0)
    turnover_fraction = notional_delta / notional
    rate = taker_fee_rate + spread_bps / (2 * _BPS) + impact_bps / _BPS * turnover_fraction
    return notional_delta * rate
