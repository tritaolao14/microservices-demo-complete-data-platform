from unittest.mock import MagicMock

import pytest
from infrastructure.config import InfraConfig
from infrastructure.kafka_consumer import KafkaConsumerAdapter


class FakeMessage:
    def __init__(self, value):
        self.value = value

@pytest.fixture
def mock_kafka_cls(monkeypatch):
    mock_cls = MagicMock()
    monkeypatch.setattr("infrastructure.kafka_consumer._KafkaConsumer", mock_cls)
    return mock_cls

class TestKafkaConsumerAdapter:
    def test_init(self, mock_kafka_cls):
        config = InfraConfig(
            kafka_broker="test-kafka:9093",
            kafka_topic="orders",
            kafka_consumer_group="test-group",
            kafka_auto_offset_reset='earliest',
        )
        KafkaConsumerAdapter(config)
        mock_kafka_cls.assert_called_once()
        _, kwargs = mock_kafka_cls.call_args
        assert kwargs['bootstrap_servers'] == ['test-kafka:9093']
        assert kwargs['group_id'] == 'test-group'
        assert kwargs['auto_offset_reset'] == 'earliest'

    def test_consume_yields_message_values(self, mock_kafka_cls):
        fake_consumer = [FakeMessage({"order_id":"a"}),FakeMessage({"order_id":"b"})]
        mock_kafka_cls.return_value.__iter__.return_value = iter(fake_consumer)
        
        adapter = KafkaConsumerAdapter(InfraConfig())

        assert list(adapter.consume()) == [{"order_id":"a"}, {"order_id":"b"}]
    
    def test_consume_empty(self, mock_kafka_cls):
        mock_kafka_cls.return_value.__iter__.return_value = iter([])
        
        adapter = KafkaConsumerAdapter(InfraConfig())

        assert list(adapter.consume()) == []

    
    def test_close_delegates(self, mock_kafka_cls):
        mock_instance = mock_kafka_cls.return_value
        adapter = KafkaConsumerAdapter(InfraConfig())
        adapter.close()
        mock_instance.close.assert_called_once()


    