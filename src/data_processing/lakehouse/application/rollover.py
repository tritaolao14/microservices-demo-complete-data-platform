"""File rollover policy for the Bronze writer (rows / bytes / age)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RolloverPolicy:
    """Decide when buffered records should become a new file."""

    max_rows: int
    max_size_bytes: int
    max_age_seconds: float

    def should_flush(self, *, rows: int, size_bytes: int, age_seconds: float) -> bool:
        if rows == 0:
            return False
        if rows >= self.max_rows or size_bytes >= self.max_size_bytes:
            return True
        return self.max_age_seconds > 0 and age_seconds >= self.max_age_seconds
