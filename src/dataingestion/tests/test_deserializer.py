"""Unit tests for OrderDeserializer."""

import pytest
from domain.models import Money, OrderAddress, OrderItem, OrderStatus
from infrastructure.deserializer import OrderDeserializer


@pytest.fixture
def deserializer():
    return OrderDeserializer()


# --- Happy path ---
class TestDeserializeHappyPath:
    def test_full_message(self, deserializer):
        message = {
            "order_id": "test-uuid",
            "shipping_tracking_id": "TRACK-123",
            "shipping_cost": {
                "currency_code": "USD",
                "units": 8,
                "nanos": 990000000,
            },
            "shipping_address": {
                "street_address": "123 Main St",
                "city": "Mountain View",
                "state": "CA",
                "country": "US",
                "zip_code": 94043,
            },
            "items": [
                {
                    "item": {"product_id": "SKU1", "quantity": 2},
                    "cost": {
                        "currency_code": "USD",
                        "units": 18,
                        "nanos": 490000000,
                    },
                },
            ],
            "timestamp": "2026-08-23T14:05:09Z",
        }
        order = deserializer.deserialize(message)

        assert order.order_id == "test-uuid"
        assert order.shipping_tracking_id == "TRACK-123"
        assert order.shipping_cost == Money("USD", 8, 990000000)
        assert order.shipping_address == OrderAddress(
            "123 Main St", "Mountain View", "CA", "US", 94043
        )
        assert len(order.items) == 1
        assert order.items[0] == OrderItem(
            product_id="SKU1",
            quantity=2,
            cost=Money("USD", 18, 490000000),
        )
        assert order.timestamp == "2026-08-23T14:05:09Z"
        assert order.status == OrderStatus.SUCCEED

    def test_multiple_items(self, deserializer):
        message = {
            "order_id": "uuid-2",
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
            "timestamp": "2026-08-23T14:05:09Z",
        }
        order = deserializer.deserialize(message)

        assert len(order.items) == 2
        assert order.items[0].product_id == "SKU1"
        assert order.items[1].product_id == "SKU2"


# --- Omitempty (missing fields from Go json.Marshal) ---
class TestDeserializeOmitempty:
    def test_missing_shipping_tracking_id(self, deserializer):
        message = {
            "order_id": "uuid-3",
            "items": [
                {
                    "item": {"product_id": "SKU1", "quantity": 1},
                    "cost": {"currency_code": "USD", "units": 10, "nanos": 0},
                },
            ],
            "timestamp": "2026-08-23T14:05:09Z",
        }
        order = deserializer.deserialize(message)
        assert order.shipping_tracking_id == ""

    def test_missing_shipping_cost(self, deserializer):
        message = {
            "order_id": "uuid-4",
            "items": [
                {
                    "item": {"product_id": "SKU1", "quantity": 1},
                    "cost": {"currency_code": "USD", "units": 10, "nanos": 0},
                },
            ],
            "timestamp": "2026-08-23T14:05:09Z",
        }
        order = deserializer.deserialize(message)
        assert order.shipping_cost.currency_code == ""
        assert order.shipping_cost.units == 0
        assert order.shipping_cost.nanos == 0

    def test_missing_shipping_address(self, deserializer):
        message = {
            "order_id": "uuid-5",
            "items": [
                {
                    "item": {"product_id": "SKU1", "quantity": 1},
                    "cost": {"currency_code": "USD", "units": 10, "nanos": 0},
                },
            ],
            "timestamp": "2026-08-23T14:05:09Z",
        }
        order = deserializer.deserialize(message)
        assert order.shipping_address.street_address == ""
        assert order.shipping_address.city == ""
        assert order.shipping_address.zip_code == 0

    def test_money_omitempty_nanos(self, deserializer):
        """nanos=0 dropped by Go json.Marshal + omitempty."""
        message = {
            "order_id": "uuid-6",
            "items": [
                {
                    "item": {"product_id": "SKU1", "quantity": 2},
                    "cost": {"currency_code": "USD", "units": 10},
                },
            ],
            "timestamp": "2026-08-23T14:05:09Z",
        }
        order = deserializer.deserialize(message)
        assert order.items[0].cost.nanos == 0

    def test_cartitem_omitempty_quantity(self, deserializer):
        """quantity=0 dropped by Go json.Marshal + omitempty."""
        message = {
            "order_id": "uuid-7",
            "items": [
                {
                    "item": {"product_id": "SKU1"},
                    "cost": {"currency_code": "USD", "units": 10, "nanos": 0},
                },
            ],
            "timestamp": "2026-08-23T14:05:09Z",
        }
        order = deserializer.deserialize(message)
        assert order.items[0].quantity == 0

    def test_empty_fields(self, deserializer):
        """All zero values — still parseable."""
        message = {
            "order_id": "uuid-8",
            "items": [
                {
                    "item": {"product_id": "SKU1", "quantity": 0},
                    "cost": {"currency_code": "", "units": 0, "nanos": 0},
                },
            ],
            "timestamp": "2026-08-23T14:05:09Z",
        }
        order = deserializer.deserialize(message)
        assert order.items[0].quantity == 0
        assert order.items[0].cost.currency_code == ""


# --- Edge cases ---
class TestDeserializeEdgeCases:
    def test_empty_items_list(self, deserializer):
        message = {
            "order_id": "uuid-9",
            "items": [],
            "timestamp": "2026-08-23T14:05:09Z",
        }
        order = deserializer.deserialize(message)
        assert len(order.items) == 0

    def test_missing_items_key(self, deserializer):
        message = {
            "order_id": "uuid-10",
            "timestamp": "2026-08-23T14:05:09Z",
        }
        order = deserializer.deserialize(message)
        assert len(order.items) == 0

    def test_negative_money(self, deserializer):
        message = {
            "order_id": "uuid-11",
            "items": [
                {
                    "item": {"product_id": "SKU1", "quantity": 1},
                    "cost": {
                        "currency_code": "USD",
                        "units": -1,
                        "nanos": -750000000,
                    },
                },
            ],
            "timestamp": "2026-08-23T14:05:09Z",
        }
        order = deserializer.deserialize(message)
        assert order.items[0].cost.units == -1
        assert order.items[0].cost.nanos == -750000000

    def test_status_always_succeed(self, deserializer):
        """Producer doesn't send status — always default SUCCEED."""
        message = {
            "order_id": "uuid-12",
            "items": [
                {
                    "item": {"product_id": "SKU1", "quantity": 1},
                    "cost": {"currency_code": "USD", "units": 10, "nanos": 0},
                },
            ],
            "timestamp": "2026-08-23T14:05:09Z",
        }
        order = deserializer.deserialize(message)
        assert order.status == OrderStatus.SUCCEED
