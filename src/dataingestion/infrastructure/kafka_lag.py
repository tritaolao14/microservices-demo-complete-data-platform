"""Consumer-lag collector (Online Observer) for the ingestion topics."""

from collections.abc import Iterable

from common.logging import get_logger
from kafka import KafkaConsumer, TopicPartition

logger = get_logger("dataingestion")


class KafkaLagCollector:
    """Compute total consumer lag per topic for one consumer group."""

    def __init__(
        self,
        broker: str,
        group_id: str,
        topics: Iterable[str],
        timeout_ms: int = 11000,
    ) -> None:
        self._broker = broker
        self._group_id = group_id
        self._topics = list(topics)
        self._timeout_ms = timeout_ms

    def collect(self) -> dict[str, int]:
        """Return topic -> total lag (end offset - committed offset)."""
        consumer = KafkaConsumer(
            bootstrap_servers=[self._broker],
            group_id=self._group_id,
            enable_auto_commit=False,
            session_timeout_ms=10000,
            request_timeout_ms=self._timeout_ms,
            consumer_timeout_ms=self._timeout_ms,
        )
        try:
            lag: dict[str, int] = {}
            for topic in self._topics:
                partitions = consumer.partitions_for_topic(topic) or set()
                topic_partitions = [
                    TopicPartition(topic, partition)
                    for partition in sorted(partitions)
                ]
                if not topic_partitions:
                    lag[topic] = 0
                    continue
                end_offsets = consumer.end_offsets(topic_partitions)
                committed = consumer.committed(topic_partitions)
                lag[topic] = sum(
                    max(0, end_offsets.get(tp, 0) - (committed.get(tp) or 0))
                    for tp in topic_partitions
                )
            return lag
        finally:
            consumer.close()
