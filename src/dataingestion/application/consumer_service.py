"""Application layer: use case consume -> deserialize -> transform -> output.

Routing on failure:
- Invalid / poison messages -> DLQ topic directly
- Non-retryable DB errors (e.g. missing table) -> DLQ topic
- Retryable DB errors -> retry topic first; after RETRY_MAX_ATTEMPT
  the same message is re-consumed from the retry topic and, on repeated
  failure, finally moves to the DLQ topic.
"""

import time

import psycopg2
from common import metrics
from common.logging import get_logger
from domain.exceptions import InvalidOrderError
from domain.models import TransformedOrderItem
from domain.transform import transform_order
from presentation.order_formatter import format_order_item

logger = get_logger("dataingestion")

RETRYABLE = (psycopg2.OperationalError, psycopg2.InterfaceError, psycopg2.InternalError)
NON_RETRYABLE = (psycopg2.ProgrammingError, psycopg2.DataError)

_RETRY_KEY = "__retry_attempt"


class ConsumerService:
    """Orchestrate Kafka messages: deserialize -> validate/transform -> log.

    Depends on abstractions (DIP):
    - consumer: object with consume() -> Iterator[dict]
    - deserializer: OrderDeserializer
    - writer: object with save(tuple[TransformedOrderItem]) -> None
    - retry_writer: optional, object with send(raw, error, attempt) -> None
    - dlq_writer: optional, object with send(raw, error) -> None
    Does not import kafka-python.
    """

    def __init__(
        self,
        consumer,
        deserializer,
        writer=None,
        retry_writer=None,
        dlq_writer=None,
        retry_max_attempt: int = 3,
        log=logger,
    ) -> None:
        self._consumer = consumer
        self._deserializer = deserializer
        self._writer = writer
        self._retry_writer = retry_writer
        self._dlq_writer = dlq_writer
        self._retry_max_attempt = retry_max_attempt
        self._log = log

    def process(self, raw: dict) -> tuple[TransformedOrderItem, ...]:
        """Process a single raw message: deserialize -> transform."""
        order = self._deserializer.deserialize(raw)
        return transform_order(order)

    def _unwrap(self, raw: dict) -> tuple[dict, int]:
        """Extract (message, retry_attempt) from a possibly-wrapped retry message."""
        if isinstance(raw, dict) and "original" in raw:
            original = raw.get("original", {})
            attempt = int(raw.get("attempt", 0))
            return original, attempt
        return raw, 0

    def _send_retry_or_dlq(self, raw: dict, error: str, attempt: int) -> str:
        """Route a retryable failure to the retry topic, or DLQ when exhausted."""
        if attempt < self._retry_max_attempt:
            if self._retry_writer:
                self._retry_writer.send(raw, error, attempt + 1)
                metrics.RETRY_MESSAGES.inc()
                return "retry"
            self._log.error("Retry writer not configured; skipping retry for: %s", error)
            return "dropped"
        if self._dlq_writer:
            self._dlq_writer.send(raw, error, attempt)
            metrics.DLQ_MESSAGES.inc()
            return "dlq"
        self._log.error("DLQ writer not configured; dropping message: %s", error)
        return "dropped"

    @staticmethod
    def _elapsed_ms(started: float) -> float:
        return round((time.perf_counter() - started) * 1000, 2)

    def run(self) -> None:
        """Consume loop: process each message, persist, log results, route failures."""
        for envelope in self._consumer.consume():
            raw, attempt = self._unwrap(envelope)
            started = time.perf_counter()
            try:
                items = self.process(raw)
                if self._writer:
                    self._writer.save(items)
                duration_ms = self._elapsed_ms(started)
                metrics.MESSAGES_PROCESSED.labels(status="success").inc()
                metrics.LAST_PROCESSED.set(time.time())
                for item in items:
                    self._log.info(
                        format_order_item(item),
                        extra={
                            "stage": "persist",
                            "record_count": len(items),
                            "duration_ms": duration_ms,
                        },
                    )
            except InvalidOrderError as exc:
                metrics.MESSAGES_PROCESSED.labels(status="invalid").inc()
                self._log.warning(
                    "Skipping invalid order: %s",
                    exc,
                    extra={
                        "stage": "dlq",
                        "record_count": 1,
                        "error": str(exc),
                    },
                )
                if self._dlq_writer:
                    self._dlq_writer.send(raw, f"InvalidOrder: {exc}", attempt)
                    metrics.DLQ_MESSAGES.inc()
            except NON_RETRYABLE as exc:
                metrics.MESSAGES_PROCESSED.labels(status="non_retryable").inc()
                self._log.error(
                    "Non-retryable DB error, sending to DLQ: %s",
                    exc,
                    extra={
                        "stage": "dlq",
                        "record_count": 1,
                        "error": str(exc),
                    },
                )
                if self._dlq_writer:
                    self._dlq_writer.send(raw, f"NonRetryable DB: {exc}", attempt)
                    metrics.DLQ_MESSAGES.inc()
            except RETRYABLE as exc:
                status = self._send_retry_or_dlq(
                    raw, f"Retryable DB: {exc}", attempt
                )
                metrics.MESSAGES_PROCESSED.labels(status=status).inc()
                self._log.error(
                    "Retryable DB error after in-memory retries: %s",
                    exc,
                    extra={
                        "stage": status,
                        "record_count": 1,
                        "duration_ms": self._elapsed_ms(started),
                        "error": str(exc),
                    },
                )
            except Exception as exc:  # noqa: BLE001 - unbounded consumer loop
                metrics.MESSAGES_PROCESSED.labels(status="unexpected").inc()
                self._log.exception(
                    "Unexpected error processing message: %s",
                    exc,
                    extra={
                        "stage": "dlq",
                        "record_count": 1,
                        "duration_ms": self._elapsed_ms(started),
                        "error": str(exc),
                    },
                )
                if self._dlq_writer:
                    self._dlq_writer.send(raw, f"Unexpected: {exc}", attempt)
                    metrics.DLQ_MESSAGES.inc()
