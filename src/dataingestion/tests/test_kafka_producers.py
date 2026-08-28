"""Unit tests for Kafka retry & DLQ producers (infrastructure layer)."""

from unittest.mock import patch

from infrastructure.kafka_dlq_producer import KafkaDlqProducer
from infrastructure.kafka_retry_producer import KafkaRetryProducer


@patch("infrastructure.kafka_retry_producer.KafkaProducer")
class TestKafkaRetryProducer:
    def _producer(self, mock_cls):
        instance = mock_cls.return_value
        producer = KafkaRetryProducer("test-kafka:9093", "orders_retry")
        return producer, instance

    def test_init_serializers(self, mock_cls):
        KafkaRetryProducer("test-kafka:9093", "orders_retry")
        _, kwargs = mock_cls.call_args
        assert kwargs["bootstrap_servers"] == ["test-kafka:9093"]
        assert kwargs["acks"] == "all"

    def test_send_payload_and_header(self, mock_cls):
        producer, instance = self._producer(mock_cls)
        raw = {"order_id": "o1"}
        producer.send(raw, "some error", 2)

        instance.send.assert_called_once()
        _, kwargs = instance.send.call_args
        payload = kwargs["value"]
        assert payload["original"] == raw
        assert payload["error"] == "some error"
        assert payload["attempt"] == 2
        assert kwargs["key"] == "o1"
        assert kwargs["headers"] == [("x-retry-count", b"2")]
        instance.flush.assert_called_once()

    def test_send_survives_producer_failure(self, mock_cls):
        producer, instance = self._producer(mock_cls)
        instance.send.side_effect = Exception("broker down")

        producer.send({"order_id": "o1"}, "err", 1)  # should not raise

    def test_close(self, mock_cls):
        producer, instance = self._producer(mock_cls)
        producer.close()
        instance.close.assert_called_once()


@patch("infrastructure.kafka_dlq_producer.KafkaProducer")
class TestKafkaDlqProducer:
    def _producer(self, mock_cls):
        instance = mock_cls.return_value
        producer = KafkaDlqProducer("test-kafka:9093", "orders_dlq")
        return producer, instance

    def test_init(self, mock_cls):
        KafkaDlqProducer("test-kafka:9093", "orders_dlq")
        _, kwargs = mock_cls.call_args
        assert kwargs["bootstrap_servers"] == ["test-kafka:9093"]
        assert kwargs["acks"] == "all"

    def test_send_payload(self, mock_cls):
        producer, instance = self._producer(mock_cls)
        raw = {"order_id": "o1"}
        producer.send(raw, "poison", 3)

        instance.send.assert_called_once()
        _, kwargs = instance.send.call_args
        payload = kwargs["value"]
        assert payload["original"] == raw
        assert payload["error"] == "poison"
        assert payload["attempt"] == 3
        assert kwargs["key"] == "o1"
        instance.flush.assert_called_once()

    def test_send_survives_producer_failure(self, mock_cls):
        producer, instance = self._producer(mock_cls)
        instance.send.side_effect = Exception("broker down")

        producer.send({"order_id": "o1"}, "err", 1)  # should not raise

    def test_close(self, mock_cls):
        producer, instance = self._producer(mock_cls)
        producer.close()
        instance.close.assert_called_once()
