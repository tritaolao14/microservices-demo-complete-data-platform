"""Tests for flush durability in ``BronzeOrdersWriter``.

The buffer must only be cleared once every partition has been committed
(DPFMD-48). Draining first and committing second drops the records outright
when a partition fails, with nothing left to retry from.
"""

import pytest

from lakehouse.application.options import WriterOptions
from lakehouse.application.writer import BronzeOrdersWriter
from lakehouse.domain.errors import TransientStoreError
from lakehouse.infrastructure.sinks import InMemorySink
from lakehouse.tests.fakes import FakeEncoder, FlakySink

DATASET = "bronze/orders"


def make_writer(sink=None, flush_rows=100, flush_interval_seconds=0):
    options = WriterOptions(
        prefix=DATASET,
        flush_rows=flush_rows,
        flush_interval_seconds=flush_interval_seconds,
    )
    return BronzeOrdersWriter(options, sink or InMemorySink(), encoder=FakeEncoder())


def payload(i, day="2026-10-09"):
    return {"order_id": f"o{i}", "timestamp": f"{day}T10:00:00Z"}


def test_failed_flush_keeps_records_in_the_buffer(monkeypatch):
    """The core guarantee: a failed flush loses nothing."""
    writer = make_writer()

    def boom(*_args, **_kwargs):
        raise RuntimeError("object store unavailable")

    monkeypatch.setattr(writer, "_commit_partition", boom)

    writer.add(payload(1))
    writer.add(payload(2))
    assert writer.buffered_rows == 2

    with pytest.raises(RuntimeError):
        writer.flush()

    assert writer.buffered_rows == 2, "records were dropped by the failed flush"


def test_records_survive_across_repeated_failures(monkeypatch):
    """Repeated failures must not erode the buffer."""
    writer = make_writer()

    def boom(*_args, **_kwargs):
        raise RuntimeError("object store unavailable")

    monkeypatch.setattr(writer, "_commit_partition", boom)
    writer.add(payload(1))

    for _ in range(3):
        with pytest.raises(RuntimeError):
            writer.flush()

    assert writer.buffered_rows == 1


def test_retry_after_failure_writes_every_record(monkeypatch):
    """Once the store recovers, the retained records are all committed."""
    sink = InMemorySink()
    writer = make_writer(sink)

    original = writer._commit_partition
    state = {"fail": True}

    def flaky(*args, **kwargs):
        if state["fail"]:
            raise RuntimeError("object store unavailable")
        return original(*args, **kwargs)

    monkeypatch.setattr(writer, "_commit_partition", flaky)

    writer.add(payload(1))
    writer.add(payload(2))

    with pytest.raises(RuntimeError):
        writer.flush()
    assert sink.list_keys(f"{DATASET}/") == []

    state["fail"] = False
    results = writer.flush()

    assert len(results) == 1
    assert results[0].rows == 2
    assert writer.buffered_rows == 0
    assert len(sink.list_keys(f"{DATASET}/2026/10/09/part-")) == 1


def test_successful_flush_clears_the_buffer():
    writer = make_writer()
    writer.add(payload(1))
    writer.flush()
    assert writer.buffered_rows == 0


def test_partial_failure_then_retry_does_not_duplicate(monkeypatch):
    """A partition committed before the failure is rewritten harmlessly.

    Data files are named by content hash and the manifest upsert dedupes by
    path, so retrying an already-written partition produces the same object
    rather than a second copy.
    """
    sink = InMemorySink()
    writer = make_writer(sink)
    original = writer._commit_partition
    calls: list[str] = []

    def fail_on_second(*args, **kwargs):
        event_date = args[0] if args else kwargs.get("event_date")
        calls.append(str(event_date))
        if len(calls) == 2:
            raise RuntimeError("object store unavailable")
        return original(*args, **kwargs)

    monkeypatch.setattr(writer, "_commit_partition", fail_on_second)

    writer.add(payload(1, day="2026-10-09"))
    writer.add(payload(2, day="2026-10-10"))

    with pytest.raises(RuntimeError):
        writer.flush()

    first_run_files = sink.list_keys(f"{DATASET}/2026/10/09/part-")
    assert len(first_run_files) == 1

    monkeypatch.setattr(writer, "_commit_partition", original)
    writer.flush()

    # The already-committed partition kept exactly one data file.
    assert sink.list_keys(f"{DATASET}/2026/10/09/part-") == first_run_files
    assert len(sink.list_keys(f"{DATASET}/2026/10/10/part-")) == 1
    assert writer.buffered_rows == 0


def test_size_estimate_is_restored_with_the_records(monkeypatch):
    """restore() must also put back the byte total used by rollover."""
    writer = make_writer()
    monkeypatch.setattr(
        writer, "_commit_partition", lambda *a, **k: (_ for _ in ()).throw(RuntimeError())
    )

    writer.add(payload(1))
    before = writer._buffer.size_bytes
    assert before > 0

    with pytest.raises(RuntimeError):
        writer.flush()

    assert writer._buffer.size_bytes == before


def test_flaky_sink_failure_is_recoverable():
    """End-to-end through the real writer against a sink that fails then works.

    FlakySink raises TransientStoreError, so RetryPolicy burns its retries
    first; enough failures are needed to exhaust them for the flush to give up.
    """
    sink = FlakySink(failures=50)
    writer = make_writer(sink)
    writer.add(payload(1))

    with pytest.raises(TransientStoreError):
        writer.flush()

    assert writer.buffered_rows == 1

    sink.failures = 0
    results = writer.flush()
    assert results[0].rows == 1
    assert writer.buffered_rows == 0