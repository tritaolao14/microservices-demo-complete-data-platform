"""Runner: land raw ``orders`` events into the Bronze layer.

Composition root: wires infrastructure adapters into the writer use case.

Two sources:
- Kafka topic ``orders`` (default, matches the checkoutservice producer).
- Newline-delimited JSON file via ``--input`` (offline dry-run / E2E).
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
from datetime import datetime, timezone

from lakehouse.application.ports import ObjectSink, RecordEncoder
from lakehouse.application.writer import BronzeOrdersWriter
from lakehouse.domain.records import SourceRef
from lakehouse.infrastructure.config import BronzeConfig
from lakehouse.infrastructure.parquet_encoder import ParquetEncoder
from lakehouse.infrastructure.sinks import Boto3Sink

# How long one poll may block. It bounds how late a shutdown can be noticed and
# how late the rollover timer can be evaluated, since both are only checked
# between polls.
POLL_TIMEOUT_MS = 1000


def build_writer(
    config: BronzeConfig,
    sink: ObjectSink | None = None,
    encoder: RecordEncoder | None = None,
) -> BronzeOrdersWriter:
    """Build a Bronze writer from config, defaulting to a real MinIO sink."""
    store = sink or Boto3Sink(
        endpoint_url=config.endpoint_url,
        access_key=config.access_key,
        secret_key=config.secret_key,
        bucket=config.bucket,
        region=config.region,
    )
    return BronzeOrdersWriter(
        config.writer,
        store,
        encoder=encoder
        or ParquetEncoder(config.compression, config.writer.schema_version),
    )


def run_jsonl(path: str, writer: BronzeOrdersWriter, source_name: str = "file") -> int:
    """Land every JSON object in a newline-delimited file, then flush."""
    count = 0
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            writer.add(json.loads(stripped), source=SourceRef(topic=source_name))
            count += 1
    writer.close()
    return count


def run_kafka(
    writer: BronzeOrdersWriter,
    broker: str,
    topic: str,
    group_id: str,
    auto_offset_reset: str = "latest",
    max_messages: int | None = None,
) -> int:
    """Consume the Kafka topic and land each message until interrupted.

    Offsets are committed only once the records they cover are durably in the
    Bronze zone. With ``enable_auto_commit=True`` the offset advanced the moment
    a message was consumed, while the writer still held that message in memory
    until rollover (60s / 50k rows / 128 MiB by default). A restart inside that
    window lost the batch permanently: Kafka would not redeliver it because the
    group offset had moved, and Bronze never received it.

    Committing after the flush trades that for at-least-once delivery -- a crash
    replays the buffered batch instead of dropping it. Replay is safe because
    the Bronze writer names files by content hash, so rewritten rows land on the
    same object rather than duplicating it.

    The loop polls rather than iterating the consumer directly. ``add()`` is the
    only place that evaluates the age-based rollover, so a plain
    ``for message in consumer`` never checks that timer while the topic is quiet
    and holds whatever it buffered indefinitely. Polling also gives the loop a
    regular point at which a shutdown signal can be noticed.
    """
    from kafka import KafkaConsumer, TopicPartition
    from kafka.structs import OffsetAndMetadata

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=False,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )

    # Offset to commit next (Kafka semantics: last seen + 1) for each partition
    # whose records are currently sitting in the writer's buffer. Emptied once
    # a flush covering them has succeeded.
    buffered: dict[tuple[str, int], int] = {}
    count = 0

    def commit_durable() -> None:
        """Advance the committed offsets to cover everything written so far."""
        if not buffered:
            return
        # kafka-python 2.x takes all three positionally (offset, metadata,
        # leader_epoch) with no defaults; 3.x gives the last two defaults.
        consumer.commit(
            {
                TopicPartition(name, partition): OffsetAndMetadata(offset, None, -1)
                for (name, partition), offset in buffered.items()
            }
        )
        buffered.clear()

    try:
        while True:
            for messages in consumer.poll(timeout_ms=POLL_TIMEOUT_MS).values():
                for message in messages:
                    flushed = writer.add(
                        message.value,
                        ingested_at=datetime.now(timezone.utc),
                        source=SourceRef(
                            topic=message.topic,
                            partition=message.partition,
                            offset=message.offset,
                        ),
                    )
                    buffered[(message.topic, message.partition)] = message.offset + 1
                    count += 1
                    # Non-empty result means a rollover committed those records;
                    # only now are the offsets covering them safe to acknowledge.
                    if flushed:
                        commit_durable()
                    if max_messages is not None and count >= max_messages:
                        break
            # Checked on every poll, including an empty one. Iterating the
            # consumer instead evaluated the rollover only inside add(), so a
            # topic that went quiet kept its records buffered indefinitely and
            # BRONZE_FLUSH_INTERVAL_SECONDS bounded nothing. The commit has to
            # follow this flush too -- without it the records are written but
            # the offsets stay unacknowledged, and get replayed on restart.
            if writer.buffered_rows and writer.flush_if_needed():
                commit_durable()
            if max_messages is not None and count >= max_messages:
                break
    finally:
        # close() flushes whatever is left. If it raises, the offsets stay
        # uncommitted and Kafka replays the batch on the next start.
        writer.close()
        commit_durable()
        consumer.close()
    return count


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Land raw order events into the Bronze (MinIO Parquet) layer."
    )
    parser.add_argument(
        "--input", help="Read newline-delimited JSON events instead of Kafka."
    )
    parser.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "kafka:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "orders"))
    parser.add_argument(
        "--group", default=os.getenv("KAFKA_CONSUMER_GROUP", "bronze-orders-writer")
    )
    parser.add_argument(
        "--auto-offset-reset", default=os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")
    )
    parser.add_argument("--max-messages", type=int, default=None)
    return parser.parse_args(argv)


def _exit_on_sigterm(signum, _frame):
    """Turn SIGTERM into an exception so ``finally`` blocks still run.

    Python's default SIGTERM action ends the process immediately; ``finally``
    blocks do not execute and any buffered records are lost from memory.
    Kubernetes sends SIGTERM on pod deletion and waits
    ``terminationGracePeriodSeconds`` before SIGKILL, so raising instead buys
    that window real meaning: the writer flushes and the offsets are committed.
    """
    raise SystemExit(128 + signum)


def _install_sigterm_handler():
    """Handle SIGTERM for the duration of main(); returns a restore callable."""
    if threading.current_thread() is not threading.main_thread():
        # signal.signal only works on the main thread; tests call main() from
        # the main thread so this is a guard, not an expected path.
        return lambda: None
    previous = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, _exit_on_sigterm)
    return lambda: signal.signal(signal.SIGTERM, previous)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = BronzeConfig.from_env()
    writer = build_writer(config)
    restore_sigterm = _install_sigterm_handler()
    try:
        if args.input:
            count = run_jsonl(args.input, writer)
        else:
            count = run_kafka(
                writer,
                args.broker,
                args.topic,
                args.group,
                args.auto_offset_reset,
                args.max_messages,
            )
    finally:
        restore_sigterm()
    print(
        f"Landed {count} record(s) to {config.bucket}/{config.writer.prefix}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
