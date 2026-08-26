"""Kafka consumer adapter.

Responsibility: Kafka I/O only (connect, consume, close).
Data mapping delegated to OrderDeserializer.
"""

import json
import logging
from collections.abc import Iterator

from kafka import KafkaConsumer as _KafkaConsumer

from infrastructure.config import InfraConfig

logger = logging.getLogger("dataingestion")


class KafkaConsumerAdapter:
    """Wrap kafka-python. Kafka I/O only — no data mapping."""

    def __init__(self, config: InfraConfig) -> None:
        self._config = config
        self._consumer = _KafkaConsumer(
            config.kafka_topic,
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
