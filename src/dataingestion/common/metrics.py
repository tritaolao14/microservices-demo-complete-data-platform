"""Prometheus metrics for the orders ingestion pipeline."""

from prometheus_client import Counter, Gauge

MESSAGES_PROCESSED = Counter(
    "dataingestion_messages_processed",
    "Kafka messages processed, labelled by outcome",
    ["status"],
)
RETRY_MESSAGES = Counter(
    "dataingestion_retry_messages",
    "Messages routed to the retry topic",
)
DLQ_MESSAGES = Counter(
    "dataingestion_dlq_messages",
    "Messages routed to the dead-letter queue",
)
CONSUMER_LAG = Gauge(
    "dataingestion_consumer_lag",
    "Consumer lag in messages",
    ["topic"],
)
LAST_PROCESSED = Gauge(
    "dataingestion_last_processed_timestamp_seconds",
    "Unix timestamp of the last successfully processed message",
)
PIPELINE_UP = Gauge(
    "dataingestion_pipeline_up",
    "1 while the ingestion service is running",
)
