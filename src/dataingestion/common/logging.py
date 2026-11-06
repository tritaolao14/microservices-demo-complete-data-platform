import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone

PIPELINE_ID = os.getenv("PIPELINE_ID", "orders_ingestion")
RUN_ID = uuid.uuid4().hex


class JsonFormatter(logging.Formatter):
    """Render every log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        error = getattr(record, "error", None)
        if record.exc_info:
            error = self.formatException(record.exc_info)
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "pipeline_id": getattr(record, "pipeline_id", PIPELINE_ID),
            "run_id": getattr(record, "run_id", RUN_ID),
            "stage": getattr(record, "stage", None),
            "record_count": getattr(record, "record_count", None),
            "duration_ms": getattr(record, "duration_ms", None),
            "error": error,
        }
        return json.dumps(payload, ensure_ascii=False, default=str)


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    for noisy in ("kafka", "kafka.conn", "kafka.client"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return logging.getLogger("dataingestion")


def get_logger(name: str = "dataingestion") -> logging.Logger:
    return logging.getLogger(name)
