"""Parquet encoder adapter: raw records -> columnar bytes (snappy)."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from typing import Any

from lakehouse.domain.records import RawRecord

_METADATA_COLUMNS = (
    "raw",
    "_ingested_at",
    "_event_date",
    "_source_topic",
    "_kafka_partition",
    "_kafka_offset",
    "_schema_version",
)

_RAW_COLUMN = "raw"


class ParquetEncoder:
    """Encode records as snappy Parquet with a raw JSON column.

    Implements the ``RecordEncoder`` port. Stores the event payload verbatim in
    ``raw`` (Bronze fidelity) plus metadata columns. pyarrow is imported lazily
    so importing this module never requires the optional dependency.
    """

    def __init__(self, compression: str = "snappy", schema_version: str = "1") -> None:
        self._compression = compression
        self._schema_version = schema_version
        self._schema_hash = _schema_hash(schema_version)

    @property
    def schema_hash(self) -> str:
        return self._schema_hash

    def encode(self, records: Sequence[RawRecord]) -> bytes:
        if not records:
            return b""

        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table(self._to_columns(records))
        buffer = pa.BufferOutputStream()
        pq.write_table(table, buffer, compression=self._compression, version="2.6")
        return buffer.getvalue().to_pybytes()

    def _to_columns(self, records: Sequence[RawRecord]) -> dict[str, list[Any]]:
        columns: dict[str, list[Any]] = {name: [] for name in _METADATA_COLUMNS}
        for record in records:
            columns[_RAW_COLUMN].append(
                json.dumps(record.payload, ensure_ascii=False, default=str)
            )
            columns["_ingested_at"].append(record.ingested_at.isoformat())
            columns["_event_date"].append(record.event_date.isoformat())
            columns["_source_topic"].append(record.source.topic)
            columns["_kafka_partition"].append(record.source.partition)
            columns["_kafka_offset"].append(record.source.offset)
            columns["_schema_version"].append(self._schema_version)
        return columns


def _schema_hash(schema_version: str) -> str:
    signature = "|".join(_METADATA_COLUMNS) + f"|v{schema_version}"
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()
