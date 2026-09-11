"""Non-secret runtime settings from environment. Secrets never live here -
see polyperps.security.key_management."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from polyperps.data_ingest.filters import DEFAULT_BOUNDS, SanityBounds


@dataclass(frozen=True, slots=True, kw_only=True)
class Settings:
    db_path: Path
    instrument_ids: tuple[int, ...]
    bounds: SanityBounds
    book_snapshot_interval_s: float
    health_log_interval_s: float
    rest_rate_per_sec: float
    rest_burst: int


def load_settings(env: Mapping[str, str] = os.environ) -> Settings:
    raw_ids = env.get("POLYPERPS_INSTRUMENT_IDS", "").strip()
    if not raw_ids:
        raise ValueError("POLYPERPS_INSTRUMENT_IDS is required (comma-separated instrument ids)")
    ids = tuple(int(x.strip()) for x in raw_ids.split(",") if x.strip())

    bounds = SanityBounds(
        max_staleness=timedelta(seconds=float(env.get("POLYPERPS_MAX_STALENESS_S", "5"))),
        max_abs_funding_rate=Decimal(env.get("POLYPERPS_MAX_ABS_FUNDING", str(DEFAULT_BOUNDS.max_abs_funding_rate))),
        max_mark_index_divergence=Decimal(env.get("POLYPERPS_MAX_MARK_INDEX_DIV", str(DEFAULT_BOUNDS.max_mark_index_divergence))),
        max_jump=Decimal(env.get("POLYPERPS_MAX_JUMP", str(DEFAULT_BOUNDS.max_jump))),
    )
    return Settings(
        db_path=Path(env.get("POLYPERPS_DB_PATH", "data/polyperps.sqlite3")),
        instrument_ids=ids,
        bounds=bounds,
        book_snapshot_interval_s=float(env.get("POLYPERPS_BOOK_INTERVAL_S", "5")),
        health_log_interval_s=float(env.get("POLYPERPS_HEALTH_LOG_S", "60")),
        rest_rate_per_sec=float(env.get("POLYPERPS_REST_RATE", "5")),
        rest_burst=int(env.get("POLYPERPS_REST_BURST", "10")),
    )
