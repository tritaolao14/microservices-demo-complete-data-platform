"""Application layer: use case consume -> deserialize -> transform -> output."""

import logging

from domain.exceptions import InvalidOrderError
from domain.models import TransformedOrderItem
from domain.transform import transform_order
from presentation.order_formatter import format_order_item

logger = logging.getLogger(__name__)


class ConsumerService:
    """Orchestrate Kafka messages: deserialize -> validate/transform -> log.

    Depends on abstractions (DIP):
    - consumer: object with consume() -> Iterator[dict]
    - deserializer: OrderDeserializer
    Does not import kafka-python.
    """

    def __init__(self, consumer, deserializer, log=logger) -> None:
        self._consumer = consumer
        self._deserializer = deserializer
        self._log = log

    def process(self, raw: dict) -> tuple[TransformedOrderItem, ...]:
        """Process a single raw message: deserialize -> transform."""
        order = self._deserializer.deserialize(raw)
        return transform_order(order)

    def run(self) -> None:
        """Consume loop: process each message, log results, skip invalid."""
        for raw in self._consumer.consume():
            try:
                for item in self.process(raw):
                    self._log.info(format_order_item(item))
            except InvalidOrderError as exc:
                self._log.warning("Skipping invalid order: %s", exc)
