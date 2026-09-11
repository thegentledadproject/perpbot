"""Parse SDK-style interval strings ("1h", "8h", "15m", ...) into timedeltas.

Pure function, no exceptions: an unparseable string returns None so callers
never fabricate a tolerance (see backfill.py's funding-gap check, which
skips the gap check entirely rather than guessing a fallback interval).
"""

from __future__ import annotations

import re
from datetime import timedelta

_PATTERN = re.compile(r"^(\d+)([smhdw])$", re.IGNORECASE)

_UNIT_TO_KWARG = {
    "s": "seconds",
    "m": "minutes",
    "h": "hours",
    "d": "days",
    "w": "weeks",
}


def parse_interval(text: str) -> timedelta | None:
    match = _PATTERN.match(text)
    if match is None:
        return None
    amount = int(match.group(1))
    unit = match.group(2).lower()
    return timedelta(**{_UNIT_TO_KWARG[unit]: amount})
