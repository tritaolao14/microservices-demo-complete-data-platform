"""PostgreSQL writer adapter.

Responsibility: persist TransformedOrderItem to analytics.order_items.
Idempotency: INSERT ... ON CONFLICT (order_id, product_id) DO NOTHING.
"""

import random
import time
from datetime import datetime

import psycopg2
import psycopg2.extras
from common.logging import get_logger
from domain.models import TransformedOrderItem

logger = get_logger("dataingestion")

_INSERT_SQL = """
INSERT INTO analytics.order_items (
    order_id, product_id, quantity, currency_code,
    unit_price, total_price, event_timestamp, status
) VALUES %s
ON CONFLICT (order_id, product_id) DO NOTHING
"""

RETRYABLE = (
    psycopg2.OperationalError,
    psycopg2.InterfaceError,
    psycopg2.InternalError,
)

NON_RETRYABLE = (
    psycopg2.ProgrammingError,
    psycopg2.DataError,
)

class PostgresWriter:
    """Batch-writes TransformedOrderItem rows to PostgreSQL."""

    def __init__(
        self, 
        dsn: str,
        max_retries: int = 3,
        backoff_ms: int = 500,
        max_backoff_ms: int = 5000
    ) -> None:
        self._dsn = dsn
        self._max_retries = max_retries
        self._backoff_ms = backoff_ms
        self._max_backoff_ms = max_backoff_ms
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = True

    def _reconnect(self) -> None:
        if self._conn and not self._conn.closed:
            try:
                self._conn.close()
            except psycopg2.Error as e:
                logger.debug("Failed to close old connection: %s", e)
        self._conn = psycopg2.connect(self._dsn)
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

        for attempt in range(self._max_retries + 1):
            try:
                with self._conn.cursor() as cur:
                    psycopg2.extras.execute_values(cur, _INSERT_SQL, rows)
                self._conn.commit()
                logger.info("Wrote %d item(s) to analytics.order_items", len(rows))
                return
            except NON_RETRYABLE:
                logger.exception("Non-retryable error writing to PostgreSQL")
                raise
            except RETRYABLE as e:
                if attempt >= self._max_retries:
                    logger.exception("Max retries exceeded for PostgreSQL")
                    raise
                if self._conn.closed:
                    self._reconnect()
                else:
                    try:
                        self._conn.rollback()
                    except psycopg2.Error as rb_err:
                        logger.debug("Failed to rollback connection: %s", rb_err)

                backoff = min(self._backoff_ms * (2 ** attempt), self._max_backoff_ms) / 1000.0
                backoff *= (0.9 + random.random() * 0.2)
                logger.warning(
                    "PostgreSQL write attempt %d failed, retrying in %.2fs: %s",
                    attempt + 1, backoff, e,
                )
                time.sleep(backoff)
            except psycopg2.Error:
                logger.exception("PostgreSQL error writing to PostgreSQL")
                raise
    def close(self) -> None:
        """Close database connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
