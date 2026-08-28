"""Kafka retry-producer adapter.

Responsibility: publish messages that failed transient DB errors to a
retry topic, so the consumer can re-process them before they hit the
dead-letter queue. Pure Kafka (kafka-python), mirroring the Flink
side-output -> retry topic pattern without external dependencies.
"""

import json
from datetime import datetime, timezone

from common.logging import get_logger
from kafka import KafkaProducer

logger = get_logger("dataingestion")


class KafkaRetryProducer:
    """Publish failed messages to the retry topic."""

    def __init__(self, broker: str, topic: str = "orders_retry") -> None:
        self._topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=2,
        )

    def send(self, raw: dict, error: str, attempt: int) -> None:
        """Send a failed message to the retry topic with attempt count."""
        payload = {
            "original": raw,
            "error": error,
            "attempt": attempt,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        try:
            key = raw.get("order_id") if isinstance(raw, dict) else None
            self._producer.send(
                self._topic,
                value=payload,
                key=key,
                headers=[("x-retry-count", str(attempt).encode("utf-8"))],
            )
            self._producer.flush(timeout=5)
            logger.warning("Sent to retry topic %s (attempt %d): %s", self._topic, attempt, error)
        except Exception:
            logger.exception("Failed to send to retry topic %s", self._topic)

    def close(self) -> None:
        try:
            self._producer.close()
        except Exception:
            logger.exception("Error closing retry producer")
