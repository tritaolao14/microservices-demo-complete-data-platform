"""Kafka consumer adapter.

Responsibility: Kafka I/O only (connect, consume, close).
Data mapping delegated to OrderDeserializer.
"""

import json
from collections.abc import Iterator

from common.logging import get_logger
from kafka import KafkaConsumer as _KafkaConsumer

from infrastructure.config import InfraConfig

logger = get_logger("dataingestion")


class KafkaConsumerAdapter:
    """Wrap kafka-python. Kafka I/O only — no data mapping."""

    def __init__(self, config: InfraConfig) -> None:
        self._config = config
        topics = [config.kafka_topic]
        if config.kafka_retry_topic and config.kafka_retry_topic != config.kafka_topic:
            topics.append(config.kafka_retry_topic)
        self._consumer = _KafkaConsumer(
            *topics,
            bootstrap_servers=[config.kafka_broker],
            group_id=config.kafka_consumer_group,
            auto_offset_reset=config.kafka_auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            key_deserializer=lambda b: b.decode("utf-8") if b else None,
        )

    def consume(self) -> Iterator[dict]:
        """Yield raw deserialized JSON messages from Kafka."""
        for message in self._consumer:
            yield message.value

    def close(self) -> None:
        """Close Kafka consumer connection."""
        self._consumer.close()
