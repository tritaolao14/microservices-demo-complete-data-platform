"""Tests for the flush timer and graceful shutdown in the runner.

Both were gaps found while deploying the writer:

- The rollover timer was only evaluated inside ``add()``, so a topic that went
  quiet kept its records buffered indefinitely and ``BRONZE_FLUSH_INTERVAL_SECONDS``
  bounded nothing.
- SIGTERM killed the process without running ``finally``, so ``writer.close()``
  never flushed and Kubernetes' terminationGracePeriodSeconds was wasted.
"""

import signal
import sys
import threading
import time
import types

import pytest

from lakehouse.application.options import WriterOptions
from lakehouse.application.writer import BronzeOrdersWriter
from lakehouse.infrastructure.sinks import InMemorySink
from lakehouse.runner import _exit_on_sigterm, _install_sigterm_handler, run_kafka
from lakehouse.tests.fakes import FakeEncoder

DATASET = "bronze/orders"


class FakeMessage:
    def __init__(self, value, offset, topic="orders", partition=0):
        self.value = value
        self.offset = offset
        self.topic = topic
        self.partition = partition


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


class FakeOffsetAndMetadata:
    def __init__(self, offset, metadata, leader_epoch):
        self.offset = offset


class PollLimitReached(Exception):
    """Raised by the fake to end a loop that production would never end."""


class IdleConsumer:
    """Delivers its messages, then returns empty polls like a quiet topic.

    ``max_polls`` bounds the loop for tests that need it to keep polling past
    the last message; production runs until a shutdown signal.
    """

    def __init__(self, messages, max_polls=None, on_poll=None):
        self._messages = list(messages)
        self._sent = False
        self.commits = []
        self.closed = False
        self.polls = 0
        self.max_polls = max_polls
        self.on_poll = on_poll

    @property
    def message_count(self):
        return len(self._messages)

    def poll(self, timeout_ms=None):
        self.polls += 1
        if self.on_poll is not None:
            self.on_poll(self.polls)
        if self.max_polls is not None and self.polls > self.max_polls:
            raise PollLimitReached
        if self._sent or not self._messages:
            return {}
        self._sent = True
        return {FakeTopicPartition("orders", 0): list(self._messages)}

    def commit(self, offsets):
        self.commits.append(dict(offsets))

    def close(self):
        self.closed = True


@pytest.fixture
def fake_kafka(monkeypatch):
    module = types.ModuleType("kafka")
    module.TopicPartition = FakeTopicPartition
    module.KafkaConsumer = lambda *a, **k: None
    structs = types.ModuleType("kafka.structs")
    structs.OffsetAndMetadata = FakeOffsetAndMetadata
    module.structs = structs
    monkeypatch.setitem(sys.modules, "kafka", module)
    monkeypatch.setitem(sys.modules, "kafka.structs", structs)

    def _make(messages, max_polls=None, on_poll=None):
        consumer = IdleConsumer(messages, max_polls=max_polls, on_poll=on_poll)
        monkeypatch.setattr(module, "KafkaConsumer", lambda *a, **k: consumer)
        return consumer

    return _make


def make_writer(sink, flush_interval_seconds=0.05, flush_rows=10**6, step=0.02):
    options = WriterOptions(
        prefix=DATASET,
        flush_rows=flush_rows,
        flush_interval_seconds=flush_interval_seconds,
    )
    # A clock that advances on every call, so the age rollover becomes
    # reachable without the test actually sleeping.
    state = {"t": 0.0}

    def clock() -> float:
        state["t"] += step
        return state["t"]

    return BronzeOrdersWriter(options, sink, encoder=FakeEncoder(), clock=clock)


def msgs(n):
    return [
        FakeMessage({"order_id": f"o{i}", "timestamp": "2026-10-09T10:00:00Z"}, offset=i)
        for i in range(n)
    ]


def data_files(sink):
    return sink.list_keys(f"{DATASET}/2026/10/09/part-")


def test_idle_topic_flushes_before_shutdown(fake_kafka):
    """The interval must fire while the loop is still running.

    run_kafka flushes in its finally block, so "something reached the sink" is
    not evidence. The distinguishing question is whether the data was written
    before the loop ended -- in production the loop never ends, so a flush that
    only happens at shutdown is a flush that never happens.
    """
    sink = InMemorySink()
    writer = make_writer(sink, flush_interval_seconds=0.05)
    written_at: dict[int, int] = {}
    fake_kafka(
        msgs(2),
        max_polls=25,
        on_poll=lambda n: written_at.__setitem__(n, len(data_files(sink))),
    )

    with pytest.raises(PollLimitReached):
        run_kafka(writer, "b:9092", "orders", "g", max_messages=10**6)

    # The last poll is the one that raised and triggered shutdown; everything
    # before it happened while the loop was still running.
    during_loop = [written_at[n] for n in sorted(written_at)[:-1]]
    assert during_loop, "loop never polled"
    assert any(count > 0 for count in during_loop), (
        "records were only written by the shutdown flush, never by the timer"
    )


def test_idle_flush_also_commits_offsets(fake_kafka):
    """Records written by the timer must be acknowledged too.

    The timer-driven flush is a different code path from the one inside add();
    if its result is not committed the data is written but Kafka replays it on
    every restart.
    """
    sink = InMemorySink()
    writer = make_writer(sink, flush_interval_seconds=0.05)
    consumer = fake_kafka(msgs(2), max_polls=25)

    with pytest.raises(PollLimitReached):
        run_kafka(writer, "b:9092", "orders", "g", max_messages=10**6)

    # Committed during the loop, not by the shutdown path in finally.
    assert consumer.commits, "the timer-driven flush never acknowledged offsets"
    assert consumer.commits[0][FakeTopicPartition("orders", 0)].offset == 2


def test_rollover_is_checked_on_empty_polls(fake_kafka):
    """The timer must be evaluated between polls, not only inside add()."""
    sink = InMemorySink()
    writer = make_writer(sink, flush_interval_seconds=0.05)
    buffered_when_checked: list[int] = []
    original = writer.flush_if_needed

    def spy():
        buffered_when_checked.append(len(writer._buffer))
        return original()

    writer.flush_if_needed = spy  # type: ignore[method-assign]

    consumer = fake_kafka(msgs(2), max_polls=25)
    with pytest.raises(PollLimitReached):
        run_kafka(writer, "b:9092", "orders", "g", max_messages=10**6)

    assert consumer.polls > 3
    # add() ran twice; every later check came from an empty poll, which is where
    # the idle flush has to come from.
    assert len(buffered_when_checked) > 2
    assert any(n > 0 for n in buffered_when_checked[2:])
    assert writer.buffered_rows == 0
    assert data_files(sink)


def test_sigterm_handler_raises_system_exit():
    """SIGTERM must unwind through finally rather than kill the process."""
    with pytest.raises(SystemExit) as exc:
        _exit_on_sigterm(signal.SIGTERM, None)
    assert exc.value.code == 128 + signal.SIGTERM


def test_sigterm_handler_runs_cleanup(fake_kafka):
    """The finally block must still execute when SIGTERM arrives."""
    sink = InMemorySink()
    # A long interval: only the shutdown flush can empty the buffer.
    writer = make_writer(sink, flush_interval_seconds=10**6)
    fake_kafka(msgs(4), max_polls=10**6)

    def send_term():
        time.sleep(0.2)
        signal.raise_signal(signal.SIGTERM)

    threading.Thread(target=send_term, daemon=True).start()

    previous = _install_sigterm_handler()
    try:
        with pytest.raises(SystemExit):
            run_kafka(writer, "b:9092", "orders", "g", max_messages=10**6)
    finally:
        previous()

    # The shutdown path flushed instead of dropping the batch.
    assert sink.list_keys(f"{DATASET}/2026/10/09/part-")
    assert writer.buffered_rows == 0


def test_sigterm_handler_is_restored(fake_kafka):
    before = signal.getsignal(signal.SIGTERM)
    restore = _install_sigterm_handler()
    assert signal.getsignal(signal.SIGTERM) is not before
    restore()
    assert signal.getsignal(signal.SIGTERM) is before