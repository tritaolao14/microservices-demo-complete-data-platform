"""Unit tests for PostgresWriter (infrastructure layer)."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import psycopg2
import pytest
from domain.models import OrderStatus, TransformedOrderItem
from infrastructure.postgres_writer import _INSERT_SQL, PostgresWriter


def _sample_item(
    order_id: str = "order-1",
    product_id: str = "SKU1",
    quantity: int = 2,
    currency_code: str = "USD",
    unit_price: float = 10.0,
    total_price: float = 20.0,
    event_timestamp: str = "2026-01-01T00:00:00Z",
    status: OrderStatus = OrderStatus.SUCCEED,
) -> TransformedOrderItem:
    return TransformedOrderItem(
        order_id=order_id,
        product_id=product_id,
        quantity=quantity,
        currency_code=currency_code,
        unit_price=Decimal(str(unit_price)),
        total_price=Decimal(str(total_price)),
        event_timestamp=event_timestamp,
        status=status,
    )


@patch("infrastructure.postgres_writer.psycopg2.extras.execute_values")
@patch("infrastructure.postgres_writer.psycopg2.connect")
class TestPostgresWriter:
    def test_save_writes_items(self, mock_connect, mock_exec):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        writer = PostgresWriter("postgresql://localhost/test")
        items = (_sample_item(),)
        writer.save(items)

        mock_exec.assert_called_once()
        args = mock_exec.call_args
        assert args[0][1] == _INSERT_SQL
        assert len(args[0][2]) == 1
        mock_conn.commit.assert_called_once()
        assert mock_conn.autocommit is True

    def test_save_empty_tuple_noop(self, mock_connect, mock_exec):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        writer = PostgresWriter("postgresql://localhost/test")
        writer.save(())

        mock_exec.assert_not_called()
        mock_conn.commit.assert_not_called()

    def test_save_idempotent_no_error(self, mock_connect, mock_exec):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        writer = PostgresWriter("postgresql://localhost/test")
        items = (_sample_item(), _sample_item())  # duplicate
        writer.save(items)

        mock_exec.assert_called_once()
        assert len(mock_exec.call_args[0][2]) == 2
        mock_conn.commit.assert_called_once()

    def test_parse_timestamp_utc(self, mock_connect, mock_exec):
        mock_connect.return_value = MagicMock()
        writer = PostgresWriter("postgresql://localhost/test")

        result = writer._parse_timestamp("2026-01-01T00:00:00Z")
        assert result == datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    def test_parse_timestamp_with_offset(self, mock_connect, mock_exec):
        mock_connect.return_value = MagicMock()
        writer = PostgresWriter("postgresql://localhost/test")

        result = writer._parse_timestamp("2026-06-15T12:30:00+07:00")
        assert result.year == 2026
        assert result.month == 6
        assert result.hour == 12

    def test_to_row_conversion(self, mock_connect, mock_exec):
        mock_connect.return_value = MagicMock()
        writer = PostgresWriter("postgresql://localhost/test")

        item = _sample_item(unit_price=19.99, total_price=39.98)
        row = writer._to_row(item)

        assert row[0] == "order-1"
        assert row[1] == "SKU1"
        assert row[2] == 2
        assert row[3] == "USD"
        assert row[4] == 19.99
        assert row[5] == 39.98
        assert isinstance(row[6], datetime)
        assert row[7] == "SUCCEED"

    def test_to_row_status_cancelled(self, mock_connect, mock_exec):
        mock_connect.return_value = MagicMock()
        writer = PostgresWriter("postgresql://localhost/test")

        item = _sample_item(status=OrderStatus.CANCELLED)
        row = writer._to_row(item)

        assert row[7] == "CANCELLED"

    def test_save_db_error_raises(self, mock_connect, mock_exec):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_exec.side_effect = psycopg2.Error("db error")

        writer = PostgresWriter("postgresql://localhost/test")
        with pytest.raises(psycopg2.Error):
            writer.save((_sample_item(),))

    def test_save_retries_then_succeeds(self, mock_connect, mock_exec):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        # first call raises OperationalError, second succeeds
        mock_exec.side_effect = [psycopg2.OperationalError("conn lost"), None]

        writer = PostgresWriter("postgresql://localhost/test", max_retries=3, backoff_ms=10)
        with patch("infrastructure.postgres_writer.time.sleep"):
            writer.save((_sample_item(),))

        assert mock_exec.call_count == 2
        mock_conn.commit.assert_called_once()

    def test_save_retryable_exhausted_raises(self, mock_connect, mock_exec):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_exec.side_effect = psycopg2.OperationalError("conn lost")

        writer = PostgresWriter("postgresql://localhost/test", max_retries=1, backoff_ms=10)
        with patch("infrastructure.postgres_writer.time.sleep"):
            pytest.raises(psycopg2.OperationalError, writer.save, (_sample_item(),))

        # attempt 0 (fail, retry) + attempt 1 (fail, exhausted) = 2
        assert mock_exec.call_count == 2

    def test_save_non_retryable_no_retry(self, mock_connect, mock_exec):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_exec.side_effect = psycopg2.ProgrammingError("no table")

        writer = PostgresWriter("postgresql://localhost/test", max_retries=3)
        with pytest.raises(psycopg2.ProgrammingError):
            writer.save((_sample_item(),))

        mock_exec.assert_called_once()  # non-retryable must not retry

    def test_save_reconnects_when_conn_closed(self, mock_connect, mock_exec):
        mock_conn = MagicMock()
        mock_conn.closed = True
        mock_connect.return_value = mock_conn
        # first attempt raises OperationalError, second (after reconnect) succeeds
        mock_exec.side_effect = [psycopg2.OperationalError("conn lost"), None]

        writer = PostgresWriter("postgresql://localhost/test", max_retries=3, backoff_ms=10)
        with patch("infrastructure.postgres_writer.time.sleep"):
            writer.save((_sample_item(),))

        # reconnect() calls psycopg2.connect again
        assert mock_connect.call_count >= 2
        assert mock_exec.call_count == 2

    def test_close_connection(self, mock_connect, mock_exec):
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_connect.return_value = mock_conn

        writer = PostgresWriter("postgresql://localhost/test")
        writer.close()

        mock_conn.close.assert_called_once()

    def test_close_already_closed(self, mock_connect, mock_exec):
        mock_conn = MagicMock()
        mock_conn.closed = True
        mock_connect.return_value = mock_conn

        writer = PostgresWriter("postgresql://localhost/test")
        writer.close()

        mock_conn.close.assert_not_called()
