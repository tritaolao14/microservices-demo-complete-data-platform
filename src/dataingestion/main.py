
from application.consumer_service import ConsumerService
from common.logging import setup_logging
from infrastructure.config import InfraConfig
from infrastructure.deserializer import OrderDeserializer
from infrastructure.kafka_consumer import KafkaConsumerAdapter
from infrastructure.kafka_dlq_producer import KafkaDlqProducer
from infrastructure.kafka_retry_producer import KafkaRetryProducer
from infrastructure.postgres_writer import PostgresWriter


def main():
    logger = setup_logging()
    config = InfraConfig()
    logger.info(
        "Starting data ingestion service: broker=%s topic=%s group=%s offset=%s "
        "retry_topic=%s dlq_topic=%s retry_max_attempt=%d",
        config.kafka_broker,
        config.kafka_topic,
        config.kafka_consumer_group,
        config.kafka_auto_offset_reset,
        config.kafka_retry_topic,
        config.kafka_dlq_topic,
        config.kafka_retry_max_attempt,
    )

    consumer = KafkaConsumerAdapter(config)
    deserializer = OrderDeserializer()
    writer = PostgresWriter(
        config.postgres_dsn,
        max_retries=config.postgres_max_retries,
        backoff_ms=config.postgres_retry_backoff_ms,
    )
    retry_writer = KafkaRetryProducer(config.kafka_broker, config.kafka_retry_topic)
    dlq_writer = KafkaDlqProducer(config.kafka_broker, config.kafka_dlq_topic)
    service = ConsumerService(
        consumer,
        deserializer,
        writer,
        retry_writer=retry_writer,
        dlq_writer=dlq_writer,
        retry_max_attempt=config.kafka_retry_max_attempt,
        log=logger,
    )

    try:
        service.run()
    except KeyboardInterrupt:
        logger.info("Shutting down data ingestion service")
    finally:
        retry_writer.close()
        dlq_writer.close()
        writer.close()
        consumer.close()


if __name__ == "__main__":
    main()
