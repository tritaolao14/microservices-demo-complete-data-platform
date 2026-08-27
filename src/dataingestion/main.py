
from application.consumer_service import ConsumerService
from common.logging import setup_logging
from infrastructure.config import InfraConfig
from infrastructure.deserializer import OrderDeserializer
from infrastructure.kafka_consumer import KafkaConsumerAdapter
from infrastructure.postgres_writer import PostgresWriter


def main():
    logger = setup_logging()
    config = InfraConfig()
    logger.info(
        "Starting data ingestion service: broker=%s topic=%s group=%s offset=%s",
        config.kafka_broker,
        config.kafka_topic,
        config.kafka_consumer_group,
        config.kafka_auto_offset_reset
    )

    consumer = KafkaConsumerAdapter(config)
    deserializer = OrderDeserializer()
    writer = PostgresWriter(config.postgres_dsn)
    service = ConsumerService(consumer, deserializer, writer, logger)
    
    try:
        service.run()
    except KeyboardInterrupt:
        logger.info("Shutting down data ingestion service")
    finally:
        writer.close()
        consumer.close()

if __name__ == "__main__":
    main()
