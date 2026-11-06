import threading

from application.consumer_service import ConsumerService
from common import metrics
from common.logging import setup_logging
from infrastructure.config import InfraConfig
from infrastructure.deserializer import OrderDeserializer
from infrastructure.kafka_consumer import KafkaConsumerAdapter
from infrastructure.kafka_dlq_producer import KafkaDlqProducer
from infrastructure.kafka_lag import KafkaLagCollector
from infrastructure.kafka_retry_producer import KafkaRetryProducer
from infrastructure.postgres_writer import PostgresWriter
from prometheus_client import start_http_server


def _start_lag_reporter(config: InfraConfig, log) -> tuple[threading.Event, threading.Thread]:
    collector = KafkaLagCollector(
        config.kafka_broker,
        config.kafka_consumer_group,
        [config.kafka_topic, config.kafka_retry_topic],
    )
    stop = threading.Event()

    def loop() -> None:
        while not stop.wait(config.lag_poll_seconds):
            try:
                for topic, lag in collector.collect().items():
                    metrics.CONSUMER_LAG.labels(topic=topic).set(lag)
            except Exception as exc:  # noqa: BLE001 - lag is best-effort
                log.warning("Consumer lag collection failed: %s", exc)

    thread = threading.Thread(target=loop, name="consumer-lag-reporter", daemon=True)
    thread.start()
    return stop, thread


def main():
    logger = setup_logging()
    config = InfraConfig()
    start_http_server(config.metrics_port)
    metrics.PIPELINE_UP.set(1)
    logger.info(
        "Starting data ingestion service: pipeline_id=%s broker=%s topic=%s "
        "group=%s offset=%s retry_topic=%s dlq_topic=%s retry_max_attempt=%d "
        "metrics_port=%d",
        config.pipeline_id,
        config.kafka_broker,
        config.kafka_topic,
        config.kafka_consumer_group,
        config.kafka_auto_offset_reset,
        config.kafka_retry_topic,
        config.kafka_dlq_topic,
        config.kafka_retry_max_attempt,
        config.metrics_port,
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
    stop_lag, lag_thread = _start_lag_reporter(config, logger)

    try:
        service.run()
    except KeyboardInterrupt:
        logger.info("Shutting down data ingestion service")
    finally:
        metrics.PIPELINE_UP.set(0)
        stop_lag.set()
        lag_thread.join(timeout=5)
        retry_writer.close()
        dlq_writer.close()
        writer.close()
        consumer.close()


if __name__ == "__main__":
    main()
