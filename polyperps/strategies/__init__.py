"""Pre-registered hypothesis strategies and their fixed grids (spec section 6).

Adding a grid point after seeing results is a spec amendment; record it in the
validation log's next record."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal

from polyperps.backtest.strategy import Strategy
from polyperps.strategies.basis import Basis
from polyperps.strategies.funding_reversion import FundingReversion
from polyperps.strategies.index_lag import IndexLag
from polyperps.strategies.lead_lag import LeadLag
from polyperps.strategies.overshoot import Overshoot

GRIDS: dict[str, list[dict]] = {
    "h1": [
        {"lookback": lb, "entry_z": ez, "exit_z": Decimal("0.5")}
        for lb in (48, 168)
        for ez in (Decimal("1.5"), Decimal("2.0"))
    ],
    "h2": [
        {"lookback": lb, "entry_z": ez}
        for lb in (24, 72)
        for ez in (Decimal("2.0"), Decimal("3.0"))
    ],
    "h3": [
        {"entry_bps": eb, "hold_bars": hb}
        for eb in (Decimal("10"), Decimal("25"))
        for hb in (1, 3)
    ],
    "h4": [
        {"gap_bps": gb, "hold_bars": hb}
        for gb in (Decimal("25"), Decimal("50"))
        for hb in (1, 3)
    ],
    "h5": [
        {"entry_z": ez, "hold_bars": hb}
        for ez in (Decimal("2.5"), Decimal("3.5"))
        for hb in (3, 6)
    ],
}


def build_strategy(
    hypothesis: str,
    params: Mapping,
    *,
    proxy_close_by_hour: Mapping[datetime, Decimal] | None = None,
) -> Strategy:
    if hypothesis == "h1":
        return FundingReversion(**params)
    if hypothesis == "h2":
        if proxy_close_by_hour is None:
            raise ValueError("h2 needs proxy_close_by_hour")
        return Basis(**params, proxy_close_by_hour=proxy_close_by_hour)
    if hypothesis == "h3":
        return IndexLag(**params)
    if hypothesis == "h4":
        if proxy_close_by_hour is None:
            raise ValueError("h4 needs proxy_close_by_hour")
        return LeadLag(**params, proxy_close_by_hour=proxy_close_by_hour)
    if hypothesis == "h5":
        return Overshoot(**params)
    raise ValueError(f"unknown hypothesis {hypothesis!r}")
