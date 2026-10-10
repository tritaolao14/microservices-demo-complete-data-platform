"""Tests for the Kafka consumption loop in ``runner.run_kafka``.

The commit-ordering behaviour is the point of DPFMD-47: an offset may only be
acknowledged after the records it covers are durably in the Bronze zone. These
tests use a fake consumer so they can drive flushes and failures directly
instead of standing up a broker.
"""

import sys
import types

import pytest

from lakehouse.application.options import WriterOptions
from lakehouse.application.writer import BronzeOrdersWriter
from lakehouse.infrastructure.sinks import InMemorySink
from lakehouse.runner import run_kafka
from lakehouse.tests.fakes import FakeEncoder

DATASET = "bronze/orders"


class FakeMessage:
    def __init__(self, value, partition=0, offset=0, topic="orders"):
        self.value = value
        self.partition = partition
        self.offset = offset
        self.topic = topic


class FakeTopicPartition:
    def __init__(self, topic, partition):
        self.topic = topic
        self.partition = partition

    def __eq__(self, other):
        return isinstance(other, FakeTopicPartition) and (
            self.topic,
            self.partition,
        ) == (other.topic, other.partition)

    def __hash__(self):
        return hash((self.topic, self.partition))

    def __repr__(self):
        return f"TP({self.topic},{self.partition})"


class FakeOffsetAndMetadata:
    """Mirrors kafka-python 2.x, where OffsetAndMetadata.__new__ takes all three
    arguments positionally with no defaults. The fake deliberately has none
    either: with defaults it would accept calls the real client rejects, and
    the suite would stay green while production code crashes.
    """

    def __init__(self, offset, metadata, leader_epoch):
        self.offset = offset
        self.metadata = metadata
        self.leader_epoch = leader_epoch


class FakeConsumer:
    """Yields a fixed message list and records every commit."""

    def __init__(self, messages):
        self._messages = messages
        self.commits = []
        self.closed = False

    def __iter__(self):
        return iter(self._messages)

    def commit(self, offsets):
        self.commits.append(dict(offsets))

    def close(self):
        self.closed = True


@pytest.fixture
def fake_consumer(monkeypatch):
    """Replace kafka-python with fakes and hand back a consumer factory."""
    module = types.ModuleType("kafka")
    module.TopicPartition = FakeTopicPartition
    module.KafkaConsumer = lambda *args, **kwargs: None
    structs = types.ModuleType("kafka.structs")
    structs.OffsetAndMetadata = FakeOffsetAndMetadata
    module.structs = structs
    monkeypatch.setitem(sys.modules, "kafka", module)
    monkeypatch.setitem(sys.modules, "kafka.structs", structs)

    def _make(messages):
        consumer = FakeConsumer(messages)
        monkeypatch.setattr(module, "KafkaConsumer", lambda *a, **k: consumer)
        return consumer

    return _make


def make_writer(flush_rows=100):
    options = WriterOptions(prefix=DATASET, flush_rows=flush_rows, flush_interval_seconds=0)
    return BronzeOrdersWriter(options, InMemorySink(), encoder=FakeEncoder())


def msgs(n, partition=0):
    return [
        FakeMessage(
            {"order_id": f"o{i}", "timestamp": "2026-10-09T10:00:00Z"},
            partition=partition,
            offset=i,
        )
        for i in range(n)
    ]


def run(writer, consumer, **kwargs):
    return run_kafka(writer, "broker:9092", "orders", "test-group", **kwargs)


def committed_offsets(consumer, partition=0):
    return [c[FakeTopicPartition("orders", partition)].offset for c in consumer.commits]


def test_nothing_is_committed_while_records_stay_buffered(fake_consumer):
    """Below the rollover threshold no offset may be acknowledged mid-loop."""
    writer = make_writer(flush_rows=100)
    consumer = fake_consumer(msgs(5))

    assert run(writer, consumer) == 5
    # Only the flush from close(); the loop itself committed nothing.
    assert committed_offsets(consumer) == [5]


def test_offsets_advance_only_on_flush(fake_consumer):
    """Commits track completed flushes, not consumed messages."""
    writer = make_writer(flush_rows=2)
    consumer = fake_consumer(msgs(5))

    run(writer, consumer)
    # Flushes at 2 rows and 4 rows, then close() flushes the last one.
    assert committed_offsets(consumer) == [2, 4, 5]


def test_failed_flush_commits_nothing(fake_consumer, monkeypatch):
    """A flush that raises must leave every offset uncommitted."""
    writer = make_writer(flush_rows=2)
    consumer = fake_consumer(msgs(3))

    def boom():
        raise RuntimeError("object store unavailable")

    monkeypatch.setattr(writer, "flush", boom)

    with pytest.raises(RuntimeError):
        run(writer, consumer)

    assert consumer.commits == []


def test_failed_close_commits_nothing(fake_consumer, monkeypatch):
    """Same guarantee for the final flush in the finally block."""
    writer = make_writer(flush_rows=100)
    consumer = fake_consumer(msgs(3))

    def boom():
        raise RuntimeError("object store unavailable")

    monkeypatch.setattr(writer, "close", boom)

    with pytest.raises(RuntimeError):
        run(writer, consumer)

    assert consumer.commits == []


def test_offset_and_metadata_requires_leader_epoch(fake_consumer):
    """Guard against a fake that drifts from the real kafka-python 2.x signature.

    The client calls OffsetAndMetadata(offset, metadata, leader_epoch)
    positionally; kafka-python 2.x has no default for the third argument.
    """
    writer = make_writer(flush_rows=100)
    consumer = fake_consumer(msgs(2))

    run(writer, consumer)

    with pytest.raises(TypeError):
        FakeOffsetAndMetadata(1, None)  # type: ignore[call-arg]
    assert consumer.commits  # the real path constructed it successfully


def test_consumer_is_closed(fake_consumer):
    writer = make_writer(flush_rows=100)
    consumer = fake_consumer(msgs(3))

    run(writer, consumer)
    assert consumer.closed is True


def test_replay_after_restart_is_idempotent(fake_consumer):
    """Kafka replays the uncommitted batch after a crash.

    The writer names files by content hash, so rewritten records land on the
    same object key instead of duplicating the data file.
    """
    sink = InMemorySink()

    first = make_writer(flush_rows=100)
    first.sink = sink  # type: ignore[assignment]
    run(first, fake_consumer(msgs(4)))
    after_first = sink.list_keys(f"{DATASET}/2026/10/09/part-")

    second = make_writer(flush_rows=100)
    second.sink = sink  # type: ignore[assignment]
    run(second, fake_consumer(msgs(4)))
    after_replay = sink.list_keys(f"{DATASET}/2026/10/09/part-")

    assert after_first == after_replay


def test_partition_offsets_are_tracked_separately(fake_consumer):
    """Each Kafka partition gets its own commit position."""
    writer = make_writer(flush_rows=100)
    messages = msgs(2, partition=0) + msgs(2, partition=1)
    consumer = fake_consumer(messages)

    run(writer, consumer)

    commit = consumer.commits[0]
    assert commit[FakeTopicPartition("orders", 0)].offset == 2
    assert commit[FakeTopicPartition("orders", 1)].offset == 2