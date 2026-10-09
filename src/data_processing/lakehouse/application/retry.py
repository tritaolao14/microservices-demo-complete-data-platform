"""Retry policy (circuit breaker / retry pattern) for transient I/O."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    """Retry an operation with exponential backoff plus jitter."""

    max_retries: int = 3
    backoff_ms: int = 500
    max_backoff_ms: int = 5_000

    def run(
        self,
        operation: Callable[[], T],
        *,
        retryable: tuple[type[BaseException], ...],
        log: logging.Logger | None = None,
    ) -> T:
        """Run ``operation``, retrying transient failures until exhausted."""
        for attempt in range(self.max_retries + 1):
            try:
                return operation()
            except retryable as exc:
                if attempt >= self.max_retries:
                    raise
                delay = self._delay_seconds(attempt)
                if log is not None:
                    log.warning(
                        "Transient object-store failure (attempt %d), "
                        "retrying in %.2fs: %s",
                        attempt + 1,
                        delay,
                        exc,
                    )
                time.sleep(delay)
        raise AssertionError("unreachable: retry loop must return or raise")

    def _delay_seconds(self, attempt: int) -> float:
        capped_ms = min(self.backoff_ms * (2 ** attempt), self.max_backoff_ms)
        return (capped_ms / 1000.0) * (0.9 + random.random() * 0.2)
