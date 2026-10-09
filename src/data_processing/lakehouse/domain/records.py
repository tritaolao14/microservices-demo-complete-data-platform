"""Record contracts for the Bronze layer."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any


@dataclass(frozen=True)
class SourceRef:
    """Where a record came from, for lineage and auditing."""

    topic: str = ""
    partition: int = -1
    offset: int = -1


@dataclass(frozen=True)
class RawRecord:
    """One raw event plus ingestion metadata (Bronze fidelity).

    ``payload`` is kept verbatim; the remaining fields are the Metadata
    Decorator that lets the writer partition and readers trace lineage.
    """

    payload: Mapping[str, Any]
    event_date: date
    ingested_at: datetime
    source: SourceRef = field(default_factory=SourceRef)
