"""Unit tests for structured logging, metrics and consumer-lag collection."""

import io
import json
import logging
from unittest.mock import MagicMock, patch

from application.consumer_service import ConsumerService
from common import metrics
from common.logging import JsonFormatter
from infrastructure.deserializer import OrderDeserializer
from infrastructure.kafka_lag import KafkaLagCollector
from kafka import TopicPartition
from prometheus_client import REGISTRY


def _sample(name: str, labels: dict[str, str]) -> float:
    value = REGISTRY.get_sample_value(name, labels)
    return value or 0.0


class FakeConsumer:
    def __init__(self, messages: list[dict]) -> None:
        self._messages = messages

    def consume(self):
        return iter(self._messages)

    def close(self) -> None:
        pass


class FakeWriter:
    def __init__(self, raise_on_save=None) -> None:
        self.saved: list[tuple] = []
        self._raise_on_save = raise_on_save

    def save(self, items: tuple) -> None:
        if self._raise_on_save:
            raise self._raise_on_save
        self.saved.append(items)

    def close(self) -> None:
        pass


class FakeRetryWriter:
    def __init__(self) -> None:
        self.sent: list[tuple] = []

    def send(self, raw: dict, error: str, attempt: int) -> None:
        self.sent.append((raw, error, attempt))


class FakeDlqWriter:
    def __init__(self) -> None:
        self.sent: list[tuple] = []

    def send(self, raw: dict, error: str, attempt: int = 0) -> None:
        self.sent.append((raw, error, attempt))


class FakeLogger:
    def info(self, msg: str, *args, **kwargs) -> None:
        pass

    def warning(self, msg: str, *args, **kwargs) -> None:
        pass

    def error(self, msg: str, *args, **kwargs) -> None:
        pass

    def exception(self, msg: str, *args, **kwargs) -> None:
        pass


def _valid_message(order_id: str = "uuid-1") -> dict:
    return {
        "order_id": order_id,
        "shipping_tracking_id": "TRACK-123",
        "shipping_cost": {"currency_code": "USD", "units": 5, "nanos": 0},
        "shipping_address": {
            "street_address": "123 Main St",
            "city": "City",
            "state": "State",
            "country": "US",
            "zip_code": 12345,
        },
        "items": [
            {
                "item": {"product_id": "SKU1", "quantity": 2},
                "cost": {"currency_code": "USD", "units": 10, "nanos": 0},
            },
        ],
        "timestamp": "2026-01-01T00:00:00Z",
    }


def test_json_formatter_includes_context_fields():
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    log = logging.getLogger("test.dataingestion.json")
    log.handlers = [handler]
    log.propagate = False
    log.setLevel(logging.INFO)

    log.info(
        "processed order",
        extra={"stage": "persist", "record_count": 2, "duration_ms": 1.5},
    )

    payload = json.loads(stream.getvalue())
    assert payload["message"] == "processed order"
    assert payload["level"] == "INFO"
    assert payload["pipeline_id"]
    assert payload["run_id"]
    assert payload["stage"] == "persist"
    assert payload["record_count"] == 2
    assert payload["duration_ms"] == 1.5
    assert payload["error"] is None


def test_success_increments_processed_and_freshness():
    before = _sample(
        "dataingestion_messages_processed_total", {"status": "success"}
    )
    service = ConsumerService(
        FakeConsumer([_valid_message()]),
        OrderDeserializer(),
        FakeWriter(),
        log=FakeLogger(),
    )

    service.run()

    after = _sample(
        "dataingestion_messages_processed_total", {"status": "success"}
    )
    assert after == before + 1
    freshness = _sample(
        "dataingestion_last_processed_timestamp_seconds", {}
    )
    assert freshness > 0


def test_invalid_message_increments_dlq_metrics():
    processed_before = _sample(
        "dataingestion_messages_processed_total", {"status": "invalid"}
    )
    dlq_before = _sample("dataingestion_dlq_messages_total", {})
    service = ConsumerService(
        FakeConsumer([_valid_message(order_id="")]),
        OrderDeserializer(),
        FakeWriter(),
        dlq_writer=FakeDlqWriter(),
        log=FakeLogger(),
    )

    service.run()

    assert (
        _sample("dataingestion_messages_processed_total", {"status": "invalid"})
        == processed_before + 1
    )
    assert _sample("dataingestion_dlq_messages_total", {}) == dlq_before + 1


def test_retryable_error_increments_retry_metric():
    before = _sample("dataingestion_retry_messages_total", {})
    import psycopg2

    service = ConsumerService(
        FakeConsumer([_valid_message()]),
        OrderDeserializer(),
        FakeWriter(raise_on_save=psycopg2.OperationalError("down")),
        retry_writer=FakeRetryWriter(),
        dlq_writer=FakeDlqWriter(),
        retry_max_attempt=3,
        log=FakeLogger(),
    )

    service.run()

    assert _sample("dataingestion_retry_messages_total", {}) == before + 1


@patch("infrastructure.kafka_lag.KafkaConsumer")
def test_lag_collector_sums_partitions(mock_consumer_cls):
    consumer = MagicMock()
    mock_consumer_cls.return_value = consumer
    consumer.partitions_for_topic.return_value = {0, 1}
    tp0 = TopicPartition("orders", 0)
    tp1 = TopicPartition("orders", 1)
    consumer.end_offsets.return_value = {tp0: 100, tp1: 50}
    consumer.committed.return_value = {tp0: 90, tp1: None}

    collector = KafkaLagCollector("kafka:9092", "dataingestion", ["orders"])

    assert collector.collect() == {"orders": 60}
    consumer.close.assert_called_once()

    kwargs = mock_consumer_cls.call_args.kwargs
    assert kwargs["request_timeout_ms"] > kwargs["session_timeout_ms"]


@patch("infrastructure.kafka_lag.KafkaConsumer")
def test_lag_collector_handles_missing_topic(mock_consumer_cls):
    consumer = MagicMock()
    mock_consumer_cls.return_value = consumer
    consumer.partitions_for_topic.return_value = None

    collector = KafkaLagCollector("kafka:9092", "dataingestion", ["orders_dlq"])

    assert collector.collect() == {"orders_dlq": 0}
    consumer.close.assert_called_once()


def test_metrics_are_exposed_under_expected_names():
    assert metrics.MESSAGES_PROCESSED._name == "dataingestion_messages_processed"
    assert metrics.RETRY_MESSAGES._name == "dataingestion_retry_messages"
    assert metrics.DLQ_MESSAGES._name == "dataingestion_dlq_messages"
    assert metrics.CONSUMER_LAG._name == "dataingestion_consumer_lag"
