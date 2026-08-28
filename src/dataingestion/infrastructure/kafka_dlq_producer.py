"""Kafka dead-letter-queue producer adapter.

Responsibility: publish poison / non-recoverable messages to a DLQ topic
so they can be inspected later. Pure Kafka (kafka-python), mirroring the
Flink side-output -> KafkaSink pattern.
"""

import json
from datetime import datetime, timezone

from common.logging import get_logger
from kafka import KafkaProducer

logger = get_logger("dataingestion")


class KafkaDlqProducer:
    """Publish non-recoverable messages to the DLQ topic."""

    def __init__(self, broker: str, topic: str = "orders_dlq") -> None:
        self._topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=2,
        )

    def send(self, raw: dict, error: str, attempt: int = 0) -> None:
        """Send a poison message to the DLQ topic."""
        payload = {
            "original": raw,
            "error": error,
            "attempt": attempt,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        try:
            key = raw.get("order_id") if isinstance(raw, dict) else None
            self._producer.send(self._topic, value=payload, key=key)
            self._producer.flush(timeout=5)
            logger.warning("Sent to DLQ %s (attempt %d): %s", self._topic, attempt, error)
        except Exception:
            logger.exception("Failed to send to DLQ %s", self._topic)

    def close(self) -> None:
        try:
            self._producer.close()
        except Exception:
            logger.exception("Error closing DLQ producer")
