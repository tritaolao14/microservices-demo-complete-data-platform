"""Bronze (raw) lakehouse layer: land raw order events as MinIO Parquet.

Clean-architecture layout: ``domain`` (entities/pure logic) <- ``application``
(use case + ports) <- ``infrastructure`` (adapters). ``runner`` is the
composition root.
"""

from lakehouse.application.layout import BronzeLayout
from lakehouse.application.options import WriterOptions
from lakehouse.application.ports import ObjectSink, RecordEncoder
from lakehouse.application.writer import BronzeOrdersWriter, WriteResult
from lakehouse.domain.records import RawRecord, SourceRef
from lakehouse.infrastructure.config import BronzeConfig

__all__ = [
    "BronzeConfig",
    "BronzeLayout",
    "BronzeOrdersWriter",
    "ObjectSink",
    "RawRecord",
    "RecordEncoder",
    "SourceRef",
    "WriteResult",
    "WriterOptions",
]
