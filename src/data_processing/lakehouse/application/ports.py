"""Ports (interfaces) the Bronze use case depends on.

Adapters live in ``lakehouse.infrastructure``; the application layer only knows
these protocols, so dependencies point inwards (Dependency Inversion).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from lakehouse.domain.records import RawRecord


@runtime_checkable
class ObjectSink(Protocol):
    """Minimal object-store contract used by the writer."""

    def put(self, key: str, data: bytes) -> None:
        ...

    def get(self, key: str) -> bytes | None:
        ...

    def exists(self, key: str) -> bool:
        ...

    def copy(self, source_key: str, destination_key: str) -> None:
        ...

    def delete(self, key: str) -> None:
        ...

    def list_keys(self, prefix: str) -> list[str]:
        ...


@runtime_checkable
class RecordEncoder(Protocol):
    """Encode a batch of raw records into an immutable byte payload."""

    @property
    def schema_hash(self) -> str:
        ...

    def encode(self, records: Sequence[RawRecord]) -> bytes:
        ...
