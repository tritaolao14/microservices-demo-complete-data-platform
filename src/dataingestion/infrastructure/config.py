import os
from dataclasses import dataclass


@dataclass(frozen=True)
class InfraConfig:
    kafka_broker: str = os.getenv("KAFKA_BROKER", "kafka:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "orders")
    kafka_consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "dataingestion")
    kafka_auto_offset_reset: str = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")
    

