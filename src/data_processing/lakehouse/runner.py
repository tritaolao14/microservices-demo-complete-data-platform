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
import sys
from datetime import datetime, timezone

from lakehouse.application.ports import ObjectSink, RecordEncoder
from lakehouse.application.writer import BronzeOrdersWriter
from lakehouse.domain.records import SourceRef
from lakehouse.infrastructure.config import BronzeConfig
from lakehouse.infrastructure.parquet_encoder import ParquetEncoder
from lakehouse.infrastructure.sinks import Boto3Sink


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
    """Consume the Kafka topic and land each message until interrupted."""
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )
    count = 0
    try:
        for message in consumer:
            writer.add(
                message.value,
                ingested_at=datetime.now(timezone.utc),
                source=SourceRef(
                    topic=message.topic,
                    partition=message.partition,
                    offset=message.offset,
                ),
            )
            count += 1
            if max_messages is not None and count >= max_messages:
                break
    finally:
        writer.close()
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


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    config = BronzeConfig.from_env()
    writer = build_writer(config)
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
    print(
        f"Landed {count} record(s) to {config.bucket}/{config.writer.prefix}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
