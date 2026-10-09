import pytest

from lakehouse.application.layout import MANIFEST_NAME, SUCCESS_MARKER
from lakehouse.application.options import WriterOptions
from lakehouse.application.writer import BronzeOrdersWriter
from lakehouse.domain.errors import TransientStoreError
from lakehouse.domain.manifest import Manifest
from lakehouse.infrastructure.sinks import InMemorySink
from lakehouse.tests.fakes import FakeEncoder, FlakySink

DATASET = "bronze/orders"


def make_config(**overrides):
    values = {
        "prefix": DATASET,
        "flush_rows": 100,
        "flush_interval_seconds": 0,
        "target_file_bytes": 1024 * 1024,
        "max_retries": 3,
        "backoff_ms": 1,
        "max_backoff_ms": 2,
    }
    values.update(overrides)
    return WriterOptions(**values)


def make_writer(sink, **overrides):
    return BronzeOrdersWriter(make_config(**overrides), sink, encoder=FakeEncoder())


def test_flush_lands_partition_manifest_and_marker():
    sink = InMemorySink()
    writer = make_writer(sink, flush_rows=2)

    assert writer.add({"order_id": "o1", "timestamp": "2026-10-09T10:00:00Z"}) == []
    results = writer.add({"order_id": "o2", "timestamp": "2026-10-09T11:00:00Z"})

    assert len(results) == 1
    result = results[0]
    assert result.partition == f"{DATASET}/2026/10/09"
    assert result.rows == 2
    assert result.path.startswith(f"{DATASET}/2026/10/09/part-")
    assert sink.exists(result.path)
    assert sink.exists(f"{DATASET}/2026/10/09/{SUCCESS_MARKER}")
    assert sink.list_keys(f"{DATASET}/_staging/") == []

    manifest = Manifest.from_bytes(
        sink.get(f"{DATASET}/2026/10/09/{MANIFEST_NAME}"), DATASET, "1"
    )
    assert manifest.total_rows() == 2
    assert [entry.path for entry in manifest.files] == [result.path]


def test_records_split_by_event_date():
    sink = InMemorySink()
    writer = make_writer(sink)
    writer.add({"order_id": "a", "timestamp": "2026-10-09T23:00:00Z"})
    writer.add({"order_id": "b", "timestamp": "2026-10-10T01:00:00Z"})

    results = writer.flush()
    assert sorted(result.partition for result in results) == [
        f"{DATASET}/2026/10/09",
        f"{DATASET}/2026/10/10",
    ]


def test_reflush_same_records_is_idempotent():
    sink = InMemorySink()
    payload = {"order_id": "o1", "timestamp": "2026-10-09T10:00:00Z"}

    first = make_writer(sink, flush_rows=1).add(dict(payload))[0]
    second = make_writer(sink, flush_rows=1).add(dict(payload))[0]

    assert first.path == second.path
    manifest = Manifest.from_bytes(
        sink.get(f"{first.partition}/{MANIFEST_NAME}"), DATASET, "1"
    )
    assert len(manifest.files) == 1


def test_transient_errors_are_retried():
    sink = FlakySink(failures=2)
    writer = make_writer(sink, flush_rows=1)
    results = writer.add({"order_id": "o1", "timestamp": "2026-10-09T10:00:00Z"})
    assert len(results) == 1
    assert sink.exists(results[0].path)


def test_transient_errors_raise_after_exhaustion():
    sink = FlakySink(failures=10)
    writer = make_writer(sink, flush_rows=1, max_retries=2)
    with pytest.raises(TransientStoreError):
        writer.add({"order_id": "o1", "timestamp": "2026-10-09T10:00:00Z"})


def test_close_flushes_buffer():
    sink = InMemorySink()
    writer = make_writer(sink)
    writer.add({"order_id": "o1", "timestamp": "2026-10-09T10:00:00Z"})
    assert writer.buffered_rows == 1

    results = writer.close()
    assert len(results) == 1
    assert writer.buffered_rows == 0
