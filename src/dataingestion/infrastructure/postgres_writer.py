"""PostgreSQL writer adapter.

Responsibility: persist TransformedOrderItem to analytics.order_items.
Idempotency: INSERT ... ON CONFLICT (order_id, product_id) DO NOTHING.
"""

import logging
from datetime import datetime

import psycopg2
import psycopg2.extras
from domain.models import TransformedOrderItem

logger = logging.getLogger(__name__)

_INSERT_SQL = """
INSERT INTO analytics.order_items (
    order_id, product_id, quantity, currency_code,
    unit_price, total_price, event_timestamp, status
) VALUES %s
ON CONFLICT (order_id, product_id) DO NOTHING
"""


class PostgresWriter:
    """Batch-writes TransformedOrderItem rows to PostgreSQL."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = True

    def _parse_timestamp(self, ts: str) -> datetime:
        """Parse RFC3339 timestamp string to timezone-aware datetime."""
        ts = ts.replace("Z", "+00:00")
        return datetime.fromisoformat(ts)

    def _to_row(self, item: TransformedOrderItem) -> tuple:
        """Convert TransformedOrderItem to tuple for execute_values."""
        return (
            item.order_id,
            item.product_id,
            item.quantity,
            item.currency_code,
            float(item.unit_price),
            float(item.total_price),
            self._parse_timestamp(item.event_timestamp),
            item.status.value,
        )

    def save(self, items: tuple[TransformedOrderItem, ...]) -> None:
        """Persist items to analytics.order_items. Empty tuple is a no-op."""
        if not items:
            return
        rows = [self._to_row(item) for item in items]
        try:
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, _INSERT_SQL, rows)
            self._conn.commit()
            logger.info("Wrote %d item(s) to analytics.order_items", len(rows))
        except psycopg2.Error:
            logger.exception("Failed to write to PostgreSQL")
            raise

    def close(self) -> None:
        """Close database connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
