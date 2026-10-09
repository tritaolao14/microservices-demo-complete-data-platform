"""Manifest contract: readers use it instead of expensive object listing."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ManifestFile:
    """One committed Parquet data file tracked by the manifest."""

    path: str
    rows: int
    size_bytes: int
    schema_hash: str
    min_event_date: str = ""
    max_event_date: str = ""
    created_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "rows": self.rows,
            "size_bytes": self.size_bytes,
            "schema_hash": self.schema_hash,
            "min_event_date": self.min_event_date,
            "max_event_date": self.max_event_date,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ManifestFile:
        return cls(
            path=str(data["path"]),
            rows=int(data.get("rows", 0)),
            size_bytes=int(data.get("size_bytes", 0)),
            schema_hash=str(data.get("schema_hash", "")),
            min_event_date=str(data.get("min_event_date", "")),
            max_event_date=str(data.get("max_event_date", "")),
            created_at=str(data.get("created_at", "")),
        )


@dataclass
class Manifest:
    """The per-partition list of data files, deduplicated by path."""

    dataset: str
    schema_version: str
    files: list[ManifestFile] = field(default_factory=list)

    def upsert(self, entry: ManifestFile) -> None:
        """Insert or replace the entry whose ``path`` matches ``entry.path``."""
        for index, existing in enumerate(self.files):
            if existing.path == entry.path:
                self.files[index] = entry
                return
        self.files.append(entry)

    def total_rows(self) -> int:
        return sum(entry.rows for entry in self.files)

    def to_bytes(self) -> bytes:
        document = {
            "dataset": self.dataset,
            "schema_version": self.schema_version,
            "files": [entry.to_dict() for entry in self.files],
        }
        return json.dumps(document, ensure_ascii=False, indent=2).encode("utf-8")

    @classmethod
    def from_bytes(
        cls, data: bytes | None, dataset: str, schema_version: str
    ) -> Manifest:
        if not data:
            return cls(dataset=dataset, schema_version=schema_version)
        document = json.loads(data.decode("utf-8"))
        files = [ManifestFile.from_dict(item) for item in document.get("files", [])]
        return cls(dataset=dataset, schema_version=schema_version, files=files)
