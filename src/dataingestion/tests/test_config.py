import importlib

from infrastructure import config
from infrastructure.config import InfraConfig


class TestInfraConfigDefaults:
    def test_default_without_env(self, monkeypatch):
        for var in [
            "KAFKA_BROKER", 
            "KAFKA_TOPIC",
            "KAFKA_CONSUMER_GROUP",
            "KAFKA_AUTO_OFFSET_RESET",
            "POSTGRES_DSN",
        ]:
            monkeypatch.delenv(var, raising=False)
        importlib.reload(config)
        cfg = InfraConfig()
        
        assert cfg.kafka_broker == "kafka:9092"
        assert cfg.kafka_topic == "orders"
        assert cfg.kafka_consumer_group == "dataingestion"
        assert cfg.kafka_auto_offset_reset == "latest"
        assert cfg.postgres_dsn == "postgresql://boutique:boutique_pass@postgres:5432/product_catalog"
        assert cfg.kafka_retry_topic == "orders_retry"
        assert cfg.kafka_dlq_topic == "orders_dlq"
        assert cfg.kafka_retry_max_attempt == 3
        assert cfg.postgres_max_retries == 3
        assert cfg.postgres_retry_backoff_ms == 5000

class TestInfraConfigOverride:
    def test_override_broker(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BROKER", "test-kafka:9093")
        importlib.reload(config)
        cfg = config.InfraConfig()
        assert cfg.kafka_broker == "test-kafka:9093"

    def test_override_all(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BROKER", "test-kafka:9093")
        monkeypatch.setenv("KAFKA_TOPIC", "test-orders")
        monkeypatch.setenv("KAFKA_CONSUMER_GROUP", "test-dataingestion")
        monkeypatch.setenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
        monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost/testdb")
        importlib.reload(config)
        cfg = config.InfraConfig()
        assert cfg.kafka_broker == "test-kafka:9093"
        assert cfg.kafka_topic == "test-orders"
        assert cfg.kafka_consumer_group == "test-dataingestion"
        assert cfg.kafka_auto_offset_reset == "earliest"
        assert cfg.postgres_dsn == "postgresql://test:test@localhost/testdb"