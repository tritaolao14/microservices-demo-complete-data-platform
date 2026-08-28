import os
from dataclasses import dataclass


@dataclass(frozen=True)
class InfraConfig:
    kafka_broker: str = os.getenv("KAFKA_BROKER", "kafka:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "orders")
    kafka_retry_topic: str = os.getenv("KAFKA_RETRY_TOPIC", "orders_retry")
    kafka_dlq_topic: str = os.getenv("KAFKA_DLQ_TOPIC", "orders_dlq")
    kafka_consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "dataingestion")
    kafka_auto_offset_reset: str = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")
    kafka_retry_max_attempt: int = int(os.getenv("RETRY_MAX_ATTEMPT", "3"))
    postgres_dsn: str = os.getenv(
        "POSTGRES_DSN",
        "postgresql://boutique:boutique_pass@postgres:5432/product_catalog",
    )
    postgres_max_retries: int = int(os.getenv("POSTGRES_MAX_RETRIES", "3"))
    postgres_retry_backoff_ms: int = int(os.getenv("POSTGRES_RETRY_BACKOFF_MS", "5000"))


