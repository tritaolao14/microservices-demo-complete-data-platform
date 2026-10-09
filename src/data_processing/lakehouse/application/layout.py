"""Object-key layout for the Bronze layer (single source of truth for paths)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from lakehouse.domain.partitioning import partition_path

MANIFEST_NAME = "_manifest.json"
SUCCESS_MARKER = "_SUCCESS"
STAGING_DIR = "_staging"


@dataclass(frozen=True)
class BronzeLayout:
    """Build every object key written by the Bronze writer."""

    prefix: str

    @property
    def dataset(self) -> str:
        return self.prefix.strip("/")

    def partition(self, event_date: date) -> str:
        return partition_path(self.dataset, event_date)

    def data_key(self, event_date: date, digest: str) -> str:
        return f"{self.partition(event_date)}/part-{digest}.parquet"

    def staging_key(self, digest: str) -> str:
        return f"{self.dataset}/{STAGING_DIR}/{digest}.parquet"

    def manifest_key(self, event_date: date) -> str:
        return f"{self.partition(event_date)}/{MANIFEST_NAME}"

    def manifest_temp_key(self, event_date: date) -> str:
        return f"{self.manifest_key(event_date)}.tmp"

    def success_key(self, event_date: date) -> str:
        return f"{self.partition(event_date)}/{SUCCESS_MARKER}"
