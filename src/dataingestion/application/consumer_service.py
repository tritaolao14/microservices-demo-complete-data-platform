"""Application layer: use case consume -> deserialize -> transform -> output.

Routing on failure:
- Invalid / poison messages -> DLQ topic directly
- Non-retryable DB errors (e.g. missing table) -> DLQ topic
- Retryable DB errors -> retry topic first; after RETRY_MAX_ATTEMPT
  the same message is re-consumed from the retry topic and, on repeated
  failure, finally moves to the DLQ topic.
"""

import psycopg2
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

    def _send_retry_or_dlq(self, raw: dict, error: str, attempt: int) -> None:
        """Route a retryable failure to the retry topic, or DLQ when exhausted."""
        if attempt < self._retry_max_attempt:
            if self._retry_writer:
                self._retry_writer.send(raw, error, attempt + 1)
            else:
                self._log.error("Retry writer not configured; skipping retry for: %s", error)
        else:
            if self._dlq_writer:
                self._dlq_writer.send(raw, error, attempt)
            else:
                self._log.error("DLQ writer not configured; dropping message: %s", error)

    def run(self) -> None:
        """Consume loop: process each message, persist, log results, route failures."""
        for envelope in self._consumer.consume():
            raw, attempt = self._unwrap(envelope)
            try:
                items = self.process(raw)
                if self._writer:
                    self._writer.save(items)
                for item in items:
                    self._log.info(format_order_item(item))
            except InvalidOrderError as exc:
                self._log.warning("Skipping invalid order: %s", exc)
                if self._dlq_writer:
                    self._dlq_writer.send(raw, f"InvalidOrder: {exc}", attempt)
            except NON_RETRYABLE as exc:
                self._log.error("Non-retryable DB error, sending to DLQ: %s", exc)
                if self._dlq_writer:
                    self._dlq_writer.send(raw, f"NonRetryable DB: {exc}", attempt)
            except RETRYABLE as exc:
                self._log.error("Retryable DB error after in-memory retries: %s", exc)
                self._send_retry_or_dlq(raw, f"Retryable DB: {exc}", attempt)
            except Exception as exc:  # noqa: BLE001 - unbounded consumer loop
                self._log.exception("Unexpected error processing message: %s", exc)
                if self._dlq_writer:
                    self._dlq_writer.send(raw, f"Unexpected: {exc}", attempt)
