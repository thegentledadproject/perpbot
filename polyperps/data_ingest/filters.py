"""Pure tick validation. No I/O, no clocks - everything comes from the Tick.

Order of checks matters: the first failing rule names the rejection, so a
tick that is both stale and insane is reported as "stale".
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal

from polyperps.exchange.types import Tick


@dataclass(frozen=True, slots=True, kw_only=True)
class SanityBounds:
    max_staleness: timedelta
    max_abs_funding_rate: Decimal
    max_mark_index_divergence: Decimal  # fraction of index, e.g. 0.05 = 5%
    max_jump: Decimal  # fraction of previous mark, e.g. 0.10 = 10%
    max_baseline_age: timedelta  # skip price_jump if previous tick is older than this


@dataclass(frozen=True, slots=True)
class Rejection:
    reason: str
    detail: str


DEFAULT_BOUNDS = SanityBounds(
    max_staleness=timedelta(seconds=5),
    max_abs_funding_rate=Decimal("0.01"),
    max_mark_index_divergence=Decimal("0.05"),
    max_jump=Decimal("0.10"),
    max_baseline_age=timedelta(seconds=60),
)


def check_tick(tick: Tick, *, bounds: SanityBounds, previous: Tick | None) -> Rejection | None:
    age = tick.received_ts - tick.exchange_ts
    if age > bounds.max_staleness:
        return Rejection("stale", f"age={age.total_seconds():.3f}s > {bounds.max_staleness.total_seconds()}s")

    for name in ("mark_price", "index_price", "last_price"):
        if getattr(tick, name) <= 0:
            return Rejection("non_positive_price", f"{name}={getattr(tick, name)}")

    divergence = abs(tick.mark_price - tick.index_price) / tick.index_price
    if divergence > bounds.max_mark_index_divergence:
        return Rejection("mark_index_divergence", f"{divergence:.4f} > {bounds.max_mark_index_divergence}")

    if abs(tick.funding_rate) > bounds.max_abs_funding_rate:
        return Rejection("funding_out_of_bounds", f"|{tick.funding_rate}| > {bounds.max_abs_funding_rate}")

    if previous is not None:
        same_stream = (
            previous.source_type is tick.source_type
            and previous.sequence is not None
            and tick.sequence is not None
        )
        if same_stream and tick.sequence <= previous.sequence:
            return Rejection("out_of_order", f"sequence {tick.sequence} <= previous {previous.sequence}")
        baseline_age = tick.exchange_ts - previous.exchange_ts
        if baseline_age <= bounds.max_baseline_age:
            jump = abs(tick.mark_price - previous.mark_price) / previous.mark_price
            if jump > bounds.max_jump:
                return Rejection(
                    "price_jump", f"{jump:.4f} > {bounds.max_jump} vs previous mark {previous.mark_price}"
                )

    return None
