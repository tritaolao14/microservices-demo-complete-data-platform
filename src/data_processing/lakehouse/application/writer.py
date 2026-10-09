"""Bronze writer: land raw order events as snappy Parquet in MinIO.

Patterns (see `.agents/knowledge/data-engineering-design-patterns-index.md`):
- Horizontal Partitioner (Ch.8): partition by UTC event date ``yyyy/mm/dd``.
- Manifest (Ch.8): ``_manifest.json`` so readers avoid S3 listing.
- Transactional Writer (Ch.4): stage, then commit atomically.
- Proxy / Readiness Marker (Ch.6): publish ``_SUCCESS`` after the manifest.
- Circuit Breaker / Retry (Ch.3): backoff + jitter around object-store I/O.

This is a use case: it depends only on domain models and the ports in
``lakehouse.application.ports``; concrete sinks/encoders are injected.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from hashlib import sha256
from typing import Any, TypeVar

from lakehouse.application.layout import BronzeLayout
from lakehouse.application.options import WriterOptions
from lakehouse.application.ports import ObjectSink, RecordEncoder
from lakehouse.application.retry import RetryPolicy
from lakehouse.application.rollover import RolloverPolicy
from lakehouse.domain.errors import TransientStoreError
from lakehouse.domain.manifest import Manifest, ManifestFile
from lakehouse.domain.partitioning import infer_event_date
from lakehouse.domain.records import RawRecord, SourceRef

logger = logging.getLogger("data_processing.lakehouse")

T = TypeVar("T")


@dataclass(frozen=True)
class WriteResult:
    """Summary of one committed Parquet file."""

    partition: str
    path: str
    rows: int
    size_bytes: int
    schema_hash: str


class _RecordBuffer:
    """In-memory buffer of raw records with an estimated byte size."""

    def __init__(self) -> None:
        self._records: list[RawRecord] = []
        self._size_bytes = 0

    def __len__(self) -> int:
        return len(self._records)

    @property
    def size_bytes(self) -> int:
        return self._size_bytes

    def add(self, record: RawRecord) -> None:
        self._records.append(record)
        self._size_bytes += _estimate_size_bytes(record.payload)

    def drain(self) -> list[RawRecord]:
        records = self._records
        self._records = []
        self._size_bytes = 0
        return records


@dataclass
class BronzeOrdersWriter:
    """Land raw order events into the Bronze layer as Parquet files."""

    options: WriterOptions
    sink: ObjectSink
    encoder: RecordEncoder
    clock: Callable[[], float] = time.monotonic
    log: logging.Logger = field(default=logger)

    def __post_init__(self) -> None:
        self._layout = BronzeLayout(self.options.prefix)
        self._rollover = RolloverPolicy(
            max_rows=self.options.flush_rows,
            max_size_bytes=self.options.target_file_bytes,
            max_age_seconds=self.options.flush_interval_seconds,
        )
        self._retry = RetryPolicy(
            max_retries=self.options.max_retries,
            backoff_ms=self.options.backoff_ms,
            max_backoff_ms=self.options.max_backoff_ms,
        )
        self._dataset = self._layout.dataset
        self._schema_version = self.options.schema_version
        self._buffer = _RecordBuffer()
        self._opened_at = self.clock()

    @property
    def buffered_rows(self) -> int:
        return len(self._buffer)

    def add(
        self,
        payload: Mapping[str, Any],
        *,
        ingested_at: datetime | None = None,
        source: SourceRef | None = None,
        now: datetime | None = None,
    ) -> list[WriteResult]:
        """Buffer one raw record and flush if a rollover threshold is met."""
        reference = now or ingested_at or datetime.now(timezone.utc)
        self._buffer.add(
            RawRecord(
                payload=payload,
                event_date=infer_event_date(payload, reference.date()),
                ingested_at=ingested_at or reference,
                source=source or SourceRef(),
            )
        )
        return self.flush_if_needed()

    def add_many(
        self,
        payloads: Iterable[Mapping[str, Any]],
        *,
        source: SourceRef | None = None,
    ) -> list[WriteResult]:
        """Buffer many raw records, returning every triggered flush."""
        results: list[WriteResult] = []
        for payload in payloads:
            results.extend(self.add(payload, source=source))
        return results

    def flush_if_needed(self) -> list[WriteResult]:
        """Flush when the buffer crosses a rows, bytes, or age threshold."""
        should_flush = self._rollover.should_flush(
            rows=len(self._buffer),
            size_bytes=self._buffer.size_bytes,
            age_seconds=self.clock() - self._opened_at,
        )
        return self.flush() if should_flush else []

    def flush(self) -> list[WriteResult]:
        """Commit all buffered records, grouped by UTC event-date partition."""
        if len(self._buffer) == 0:
            return []
        records = self._buffer.drain()
        results = [
            self._commit_partition(event_date, group)
            for event_date, group in _group_by_event_date(records)
        ]
        self._opened_at = self.clock()
        return results

    def close(self) -> list[WriteResult]:
        """Flush any remaining buffered records."""
        return self.flush()

    def _commit_partition(
        self, event_date: date, records: Sequence[RawRecord]
    ) -> WriteResult:
        payload = self.encoder.encode(records)
        digest = _content_digest(payload)
        data_key = self._layout.data_key(event_date, digest)

        self._put_staged(data_key, digest, payload)
        self._append_manifest(event_date, records, data_key, payload)
        self._publish_ready(event_date)

        result = WriteResult(
            partition=self._layout.partition(event_date),
            path=data_key,
            rows=len(records),
            size_bytes=len(payload),
            schema_hash=self.encoder.schema_hash,
        )
        self.log.info(
            "Committed Bronze file %s (%d rows, %d bytes)",
            result.path,
            result.rows,
            result.size_bytes,
        )
        return result

    def _put_staged(self, data_key: str, digest: str, payload: bytes) -> None:
        staging_key = self._layout.staging_key(digest)
        self._call(lambda: self.sink.put(staging_key, payload))
        self._call(lambda: self.sink.copy(staging_key, data_key))
        self._call(lambda: self.sink.delete(staging_key))

    def _append_manifest(
        self,
        event_date: date,
        records: Sequence[RawRecord],
        data_key: str,
        payload: bytes,
    ) -> None:
        manifest_key = self._layout.manifest_key(event_date)
        existing = self._call(lambda: self.sink.get(manifest_key))
        manifest = Manifest.from_bytes(existing, self._dataset, self._schema_version)
        manifest.upsert(self._build_entry(records, data_key, payload))
        self._replace_atomic(
            manifest_key,
            self._layout.manifest_temp_key(event_date),
            manifest.to_bytes(),
        )

    def _publish_ready(self, event_date: date) -> None:
        self._call(lambda: self.sink.put(self._layout.success_key(event_date), b""))

    def _build_entry(
        self, records: Sequence[RawRecord], data_key: str, payload: bytes
    ) -> ManifestFile:
        event_dates = [record.event_date for record in records]
        return ManifestFile(
            path=data_key,
            rows=len(records),
            size_bytes=len(payload),
            schema_hash=self.encoder.schema_hash,
            min_event_date=min(event_dates).isoformat(),
            max_event_date=max(event_dates).isoformat(),
            created_at=datetime.now(timezone.utc).isoformat(),
        )

    def _replace_atomic(self, key: str, temp_key: str, payload: bytes) -> None:
        self._call(lambda: self.sink.put(temp_key, payload))
        self._call(lambda: self.sink.copy(temp_key, key))
        self._call(lambda: self.sink.delete(temp_key))

    def _call(self, operation: Callable[[], T]) -> T:
        return self._retry.run(
            operation, retryable=(TransientStoreError,), log=self.log
        )


def _group_by_event_date(
    records: Iterable[RawRecord],
) -> list[tuple[date, list[RawRecord]]]:
    groups: dict[date, list[RawRecord]] = {}
    for record in records:
        groups.setdefault(record.event_date, []).append(record)
    return [(event_date, groups[event_date]) for event_date in sorted(groups)]


def _content_digest(payload: bytes) -> str:
    return sha256(payload).hexdigest()[:16]


def _estimate_size_bytes(payload: Mapping[str, Any]) -> int:
    """Rough in-memory size of a record, used only to trigger rollover."""
    try:
        return len(json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"))
    except (TypeError, ValueError):
        return 0
