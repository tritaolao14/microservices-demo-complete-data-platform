"""Configuration for the Bronze MinIO/S3 Parquet writer (env-driven)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from lakehouse.application.options import WriterOptions


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw else default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw else default


@dataclass(frozen=True)
class BronzeConfig:
    """Connection plus writer options, composed for the composition root.

    Reads object-store connection from the same ``MINIO_*`` variables used by
    the other ``data_processing`` scripts; ``BRONZE_*`` tune file rollover.
    """

    endpoint_url: str = "http://minio:9000"
    access_key: str = "admin"
    secret_key: str = "admin123"
    region: str = "us-east-1"
    bucket: str = "lakehouse"
    compression: str = "snappy"
    writer: WriterOptions = field(default_factory=WriterOptions)

    @classmethod
    def from_env(cls) -> BronzeConfig:
        """Build a config, letting environment variables override defaults."""
        return cls(
            endpoint_url=os.getenv("MINIO_ENDPOINT", cls.endpoint_url),
            access_key=os.getenv("MINIO_ACCESS_KEY", cls.access_key),
            secret_key=os.getenv("MINIO_SECRET_KEY", cls.secret_key),
            region=os.getenv("MINIO_REGION", cls.region),
            bucket=os.getenv("LAKEHOUSE_BUCKET", cls.bucket),
            compression=os.getenv("BRONZE_COMPRESSION", cls.compression),
            writer=WriterOptions(
                prefix=os.getenv("BRONZE_PREFIX", WriterOptions.prefix),
                schema_version=os.getenv(
                    "BRONZE_SCHEMA_VERSION", WriterOptions.schema_version
                ),
                target_file_bytes=_env_int(
                    "BRONZE_TARGET_FILE_BYTES", WriterOptions.target_file_bytes
                ),
                flush_rows=_env_int("BRONZE_FLUSH_ROWS", WriterOptions.flush_rows),
                flush_interval_seconds=_env_float(
                    "BRONZE_FLUSH_INTERVAL_SECONDS",
                    WriterOptions.flush_interval_seconds,
                ),
                max_retries=_env_int("BRONZE_MAX_RETRIES", WriterOptions.max_retries),
                backoff_ms=_env_int("BRONZE_BACKOFF_MS", WriterOptions.backoff_ms),
                max_backoff_ms=_env_int(
                    "BRONZE_MAX_BACKOFF_MS", WriterOptions.max_backoff_ms
                ),
            ),
        )
