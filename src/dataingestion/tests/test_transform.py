from decimal import Decimal

import pytest
from domain.exceptions import InvalidOrderError
from domain.models import Money, Order, OrderAddress, OrderItem, OrderStatus
from domain.transform import transform_order, validate_order


# --- Money ---
class TestMoneyToDecimal:
    def test_with_nanos(self):
        assert Money("USD", 18, 490000000).to_decimal() == Decimal("18.49")

    def test_zero_nanos(self):
        assert Money("USD", 5, 0).to_decimal() == Decimal(5)

    def test_zero_all(self):
        assert Money("", 0, 0).to_decimal() == Decimal(0)

    def test_negative(self):
        assert Money("USD", -1, -500000000).to_decimal() == Decimal("-1.5")

# --- transform_order ---
class TestTransformOrder:
    def test_single_item(self):
        order = _make_order(items=[_make_item("SKU1", 2, "USD", 10, 0)])
        result = transform_order(order)
        assert len(result) == 1
        assert result[0].total_price == Decimal(20)

    def test_multiple_items(self):
        order = _make_order(items=[
            _make_item("SKU1", 1, "USD", 10, 0),
            _make_item("SKU2", 3, "USD", 5, 0),
        ])
        result = transform_order(order)
        assert len(result) == 2
        assert result[0].total_price == Decimal(10)
        assert result[1].total_price == Decimal(15)

    def test_decimal_precision(self):
        order = _make_order(items=[_make_item("SKU1", 3, "USD", 18, 490000000)])
        result = transform_order(order)
        assert result[0].unit_price == Decimal("18.49")
        assert result[0].total_price == Decimal("55.47")

# --- validate_order ---
class TestValidateOrder:
    def test_missing_order_id(self):
        order = _make_order(order_id="")
        with pytest.raises(InvalidOrderError, match="Missing order_id"):
            validate_order(order)

    def test_empty_items(self):
        order = _make_order(items=())
        with pytest.raises(InvalidOrderError, match="no items"):
            validate_order(order)

    def test_missing_product_id(self):
        order = _make_order(items=[_make_item("", 1, "USD", 10, 0)])
        with pytest.raises(InvalidOrderError, match="missing product_id"):
            validate_order(order)

    def test_missing_timestamp(self):
        order = _make_order(timestamp="")
        with pytest.raises(InvalidOrderError, match="missing timestamp"):
            validate_order(order)

# --- Helpers ---
def _make_order(order_id="test-uuid", items=None, timestamp="2026-01-01T00:00:00Z"):
    return Order(
        order_id=order_id,
        shipping_tracking_id="TRACK-123",
        shipping_cost=Money("USD", 5, 0),
        shipping_address=OrderAddress("123 Main St", "City", "State", "US", 12345),
        items=items if items is not None else (_make_item("SKU1", 1, "USD", 10, 0),),
        timestamp=timestamp,
        status=OrderStatus.SUCCEED
    )

def _make_item(product_id="SKU1", quantity=1, currency="USD", units=10, nanos=0):
    return OrderItem(
        product_id=product_id,
        quantity=quantity,
        cost=Money(currency, units, nanos),
    )
