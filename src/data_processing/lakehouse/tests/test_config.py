from lakehouse.infrastructure.config import BronzeConfig


def test_from_env_overrides(monkeypatch):
    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio.local:9000")
    monkeypatch.setenv("LAKEHOUSE_BUCKET", "lake")
    monkeypatch.setenv("BRONZE_FLUSH_ROWS", "7")
    monkeypatch.setenv("BRONZE_FLUSH_INTERVAL_SECONDS", "1.5")
    config = BronzeConfig.from_env()
    assert config.endpoint_url == "http://minio.local:9000"
    assert config.bucket == "lake"
    assert config.writer.flush_rows == 7
    assert config.writer.flush_interval_seconds == 1.5


def test_defaults():
    config = BronzeConfig()
    assert config.writer.prefix == "bronze/orders"
    assert config.compression == "snappy"
    assert config.writer.flush_rows == 50_000
