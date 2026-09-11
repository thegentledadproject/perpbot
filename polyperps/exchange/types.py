"""Exchange-neutral market data types.

Every record carries source_type (provenance) and two timestamps:
  exchange_ts  - what the exchange said (WS envelope / REST field)
  received_ts  - our wall clock at receipt
Staleness is received_ts - exchange_ts. REST tickers may lack an exchange
timestamp; the adapter then sets exchange_ts = received_ts, and the
POLYMARKET_REST source_type is how a consumer knows that happened.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


class SourceType(StrEnum):
    POLYMARKET_WS = "polymarket_ws"
    POLYMARKET_REST = "polymarket_rest"
    PROXY_HYPERLIQUID = "proxy_hyperliquid"  # Phase 1 backfill only; never confirms an edge
    PROXY_CEX = "proxy_cex"  # Phase 1 backfill only; never confirms an edge
    SYNTHETIC_TEST = "synthetic_test"


def _require_aware(obj: object) -> None:
    for f in fields(obj):  # type: ignore[arg-type]
        v = getattr(obj, f.name)
        if isinstance(v, datetime) and (v.tzinfo is None or v.utcoffset() is None):
            raise ValueError(f"{type(obj).__name__}.{f.name} must be timezone-aware")


def _require_source_type(obj: object) -> None:
    st = getattr(obj, "source_type", None)
    if not isinstance(st, SourceType):
        raise TypeError(f"{type(obj).__name__}.source_type must be a SourceType, got {st!r}")


@dataclass(frozen=True, slots=True, kw_only=True)
class Instrument:
    instrument_id: int
    symbol: str
    category: str
    funding_interval: str
    max_leverage: int
    price_decimals: int
    quantity_decimals: int
    min_notional: Decimal
    isolated_only: bool


@dataclass(frozen=True, slots=True, kw_only=True)
class Tick:
    instrument_id: int
    mark_price: Decimal
    index_price: Decimal
    last_price: Decimal
    funding_rate: Decimal
    next_funding: datetime
    exchange_ts: datetime
    received_ts: datetime
    source_type: SourceType
    sequence: int | None = None

    def __post_init__(self) -> None:
        _require_aware(self)
        _require_source_type(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class BookLevel:
    price: Decimal
    quantity: Decimal


@dataclass(frozen=True, slots=True, kw_only=True)
class BookSnapshot:
    instrument_id: int
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    exchange_ts: datetime
    received_ts: datetime
    source_type: SourceType
    sequence: int | None = None

    def __post_init__(self) -> None:
        _require_aware(self)
        _require_source_type(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class FundingObservation:
    instrument_id: int
    funding_rate: Decimal
    exchange_ts: datetime
    received_ts: datetime
    source_type: SourceType

    def __post_init__(self) -> None:
        _require_aware(self)
        _require_source_type(self)


@dataclass(frozen=True, slots=True, kw_only=True)
class Candle:
    instrument_id: int
    interval: str
    open_ts: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trades: int
    received_ts: datetime
    source_type: SourceType

    def __post_init__(self) -> None:
        _require_aware(self)
        _require_source_type(self)
