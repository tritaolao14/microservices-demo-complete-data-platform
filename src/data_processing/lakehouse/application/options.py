"""Tuning options for the Bronze writer (pure, env-agnostic)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WriterOptions:
    """Rollover, schema, and retry settings for one Bronze writer instance."""

    prefix: str = "bronze/orders"
    schema_version: str = "1"
    target_file_bytes: int = 128 * 1024 * 1024
    flush_rows: int = 50_000
    flush_interval_seconds: float = 60.0
    max_retries: int = 3
    backoff_ms: int = 500
    max_backoff_ms: int = 5_000
