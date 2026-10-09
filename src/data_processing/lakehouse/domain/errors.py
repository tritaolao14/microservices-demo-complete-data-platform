"""Domain errors for the Bronze lakehouse layer."""

from __future__ import annotations


class DataPlatformError(Exception):
    """Base class for data_processing errors."""


class TransientStoreError(DataPlatformError):
    """Retryable object-store I/O failure (network, throttling, 5xx)."""
