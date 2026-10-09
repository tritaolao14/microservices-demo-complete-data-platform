"""Horizontal date partitioning helpers for the Bronze layer."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timezone
from typing import Any

_METADATA_EVENT_DATE = "_event_date"
_TIMESTAMP_FIELDS = ("timestamp", "event_timestamp")


def parse_event_timestamp(value: Any) -> datetime | None:
    """Parse an RFC3339 / ISO-8601 timestamp into an aware UTC datetime."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def infer_event_date(record: Mapping[str, Any], default: date) -> date:
    """Return the UTC partition date for a record.

    Priority: explicit ``_event_date`` metadata, then the payload ``timestamp``
    / ``event_timestamp`` field (normalised to UTC), then ``default``.
    """
    explicit = record.get(_METADATA_EVENT_DATE)
    if isinstance(explicit, date) and not isinstance(explicit, datetime):
        return explicit
    if isinstance(explicit, str):
        parsed_explicit = parse_event_timestamp(explicit)
        if parsed_explicit is not None:
            return parsed_explicit.date()
    for field in _TIMESTAMP_FIELDS:
        parsed = parse_event_timestamp(record.get(field))
        if parsed is not None:
            return parsed.date()
    return default


def partition_path(prefix: str, event_date: date) -> str:
    """Build the date partition prefix ``<prefix>/<yyyy>/<mm>/<dd>``."""
    return (
        f"{prefix.strip('/')}/"
        f"{event_date.year:04d}/{event_date.month:02d}/{event_date.day:02d}"
    )
