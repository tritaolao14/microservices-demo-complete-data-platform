"""Test doubles for the Bronze writer (no pyarrow / boto3 required)."""

from __future__ import annotations

import json

from lakehouse.domain.errors import TransientStoreError
from lakehouse.infrastructure.sinks import InMemorySink


class FakeEncoder:
    """Deterministic encoder: the same records always produce the same bytes."""

    def __init__(self) -> None:
        self.batches: list[list] = []

    @property
    def schema_hash(self) -> str:
        return "fake-schema-hash"

    def encode(self, records) -> bytes:
        self.batches.append(list(records))
        payloads = [dict(record.payload) for record in records]
        return json.dumps(payloads, sort_keys=True).encode("utf-8")


class FlakySink(InMemorySink):
    """Sink whose ``put`` fails ``failures`` times before succeeding."""

    def __init__(self, failures: int) -> None:
        super().__init__()
        self.failures = failures

    def put(self, key: str, data: bytes) -> None:
        if self.failures > 0:
            self.failures -= 1
            raise TransientStoreError("simulated transient put failure")
        super().put(key, data)
