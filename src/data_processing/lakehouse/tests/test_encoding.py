import io
import json
from datetime import date, datetime, timezone

import pytest

from lakehouse.domain.records import RawRecord, SourceRef
from lakehouse.infrastructure.parquet_encoder import ParquetEncoder

pq = pytest.importorskip("pyarrow.parquet")


def _record(payload):
    return RawRecord(
        payload=payload,
        event_date=date(2026, 10, 9),
        ingested_at=datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc),
        source=SourceRef(topic="orders", partition=0, offset=1),
    )


def test_parquet_encoder_roundtrip():
    encoder = ParquetEncoder(compression="snappy", schema_version="1")
    records = [_record({"order_id": "o1", "items": [1, 2]}), _record({"order_id": "o2"})]

    data = encoder.encode(records)
    table = pq.read_table(io.BytesIO(data))

    assert table.num_rows == 2
    raw = table.column("raw").to_pylist()
    assert json.loads(raw[0])["order_id"] == "o1"
    assert table.column("_event_date").to_pylist() == ["2026-10-09", "2026-10-09"]
    assert table.column("_source_topic").to_pylist() == ["orders", "orders"]
    assert encoder.schema_hash


def test_parquet_encoder_empty_batch():
    assert ParquetEncoder().encode([]) == b""
