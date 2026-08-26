"""Unit tests for ConsumerService (application layer)."""

from decimal import Decimal

import pytest
from application.consumer_service import ConsumerService
from domain.exceptions import InvalidOrderError
from infrastructure.deserializer import OrderDeserializer


class FakeConsumer:
    """In-memory consumer for testing."""

    def __init__(self, messages: list[dict]) -> None:
        self._messages = messages

    def consume(self):
        return iter(self._messages)

    def close(self) -> None:
        pass


class FakeLogger:
    """Capture log calls for assertions."""

    def __init__(self) -> None:
        self.infos: list[str] = []
        self.warnings: list[str] = []

    def info(self, msg: str, *args) -> None:
        self.infos.append(msg % args if args else msg)

    def warning(self, msg: str, *args) -> None:
        self.warnings.append(msg % args if args else msg)


def _valid_message(
    order_id: str = "test-uuid",
    product_id: str = "SKU1",
    quantity: int = 2,
    currency: str = "USD",
    units: int = 10,
    nanos: int = 0,
) -> dict:
    return {
        "order_id": order_id,
        "shipping_tracking_id": "TRACK-123",
        "shipping_cost": {"currency_code": "USD", "units": 5, "nanos": 0},
        "shipping_address": {
            "street_address": "123 Main St",
            "city": "City",
            "state": "State",
            "country": "US",
            "zip_code": 12345,
        },
        "items": [
            {
                "item": {"product_id": product_id, "quantity": quantity},
                "cost": {"currency_code": currency, "units": units, "nanos": nanos},
            },
        ],
        "timestamp": "2026-01-01T00:00:00Z",
    }


# --- process() ---
class TestProcess:
    def test_process_valid_message(self):
        consumer = FakeConsumer([])
        deserializer = OrderDeserializer()
        service = ConsumerService(consumer, deserializer, FakeLogger())

        result = service.process(_valid_message())

        assert len(result) == 1
        assert result[0].order_id == "test-uuid"
        assert result[0].product_id == "SKU1"
        assert result[0].quantity == 2
        assert result[0].currency_code == "USD"
        assert result[0].unit_price == Decimal(10)
        assert result[0].total_price == Decimal(20)
        assert result[0].event_timestamp == "2026-01-01T00:00:00Z"

    def test_process_invalid_message_raises(self):
        consumer = FakeConsumer([])
        deserializer = OrderDeserializer()
        service = ConsumerService(consumer, deserializer, FakeLogger())

        # Missing order_id -> InvalidOrderError
        raw = _valid_message(order_id="")
        with pytest.raises(InvalidOrderError):
            service.process(raw)


# --- run() ---
class TestRun:
    def test_run_logs_transformed_items(self):
        messages = [_valid_message(order_id="uuid-1")]
        consumer = FakeConsumer(messages)
        deserializer = OrderDeserializer()
        fake_log = FakeLogger()
        service = ConsumerService(consumer, deserializer, fake_log)

        service.run()

        assert len(fake_log.infos) == 1
        assert "uuid-1" in fake_log.infos[0]
        assert "SKU1" in fake_log.infos[0]

    def test_run_skips_invalid_order(self):
        messages = [_valid_message(order_id="")]  # invalid
        consumer = FakeConsumer(messages)
        deserializer = OrderDeserializer()
        fake_log = FakeLogger()
        service = ConsumerService(consumer, deserializer, fake_log)

        service.run()  # should not raise

        assert len(fake_log.warnings) == 1
        assert "Skipping invalid order" in fake_log.warnings[0]
        assert len(fake_log.infos) == 0

    def test_run_continues_after_invalid(self):
        messages = [
            _valid_message(order_id=""),  # invalid -> warning
            _valid_message(order_id="uuid-valid"),  # valid -> info
        ]
        consumer = FakeConsumer(messages)
        deserializer = OrderDeserializer()
        fake_log = FakeLogger()
        service = ConsumerService(consumer, deserializer, fake_log)

        service.run()

        assert len(fake_log.warnings) == 1
        assert len(fake_log.infos) == 1
        assert "uuid-valid" in fake_log.infos[0]

    def test_run_multiple_messages(self):
        messages = [
            _valid_message(order_id="uuid-1", product_id="SKU1"),
            _valid_message(order_id="uuid-2", product_id="SKU2"),
            _valid_message(order_id="uuid-3", product_id="SKU3"),
        ]
        consumer = FakeConsumer(messages)
        deserializer = OrderDeserializer()
        fake_log = FakeLogger()
        service = ConsumerService(consumer, deserializer, fake_log)

        service.run()

        assert len(fake_log.infos) == 3
        assert "uuid-1" in fake_log.infos[0]
        assert "uuid-2" in fake_log.infos[1]
        assert "uuid-3" in fake_log.infos[2]

    def test_run_multiple_items_per_order(self):
        raw = {
            "order_id": "uuid-multi",
            "shipping_tracking_id": "TRACK-123",
            "shipping_cost": {"currency_code": "USD", "units": 5, "nanos": 0},
            "shipping_address": {
                "street_address": "123 Main St",
                "city": "City",
                "state": "State",
                "country": "US",
                "zip_code": 12345,
            },
            "items": [
                {
                    "item": {"product_id": "SKU1", "quantity": 1},
                    "cost": {"currency_code": "USD", "units": 10, "nanos": 0},
                },
                {
                    "item": {"product_id": "SKU2", "quantity": 3},
                    "cost": {"currency_code": "USD", "units": 5, "nanos": 0},
                },
            ],
            "timestamp": "2026-01-01T00:00:00Z",
        }
        consumer = FakeConsumer([raw])
        deserializer = OrderDeserializer()
        fake_log = FakeLogger()
        service = ConsumerService(consumer, deserializer, fake_log)

        service.run()

        assert len(fake_log.infos) == 2
        assert "SKU1" in fake_log.infos[0]
        assert "SKU2" in fake_log.infos[1]
