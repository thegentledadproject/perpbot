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
    raise ValueError(f"unknown hypothesis {hypothesis!r}")
