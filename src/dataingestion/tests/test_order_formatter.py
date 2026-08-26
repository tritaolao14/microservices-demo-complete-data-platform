from decimal import Decimal

from domain.models import OrderStatus, TransformedOrderItem
from presentation.order_formatter import format_order_item


def _make_item(**overrides):
    base = {
        "order_id": "test-uuid",
        "product_id": "SKU1",
        "quantity": 2,
        "currency_code": "USD",
        "unit_price": Decimal("18.49"),
        "total_price": Decimal("36.98"),
        "event_timestamp": "2026-01-01T00:00:00Z",
        "status": OrderStatus.SUCCEED,
    }
    base.update(overrides)
    return TransformedOrderItem(**base)


class TestFormatOrderItem:
    def test_contains_all_fields(self):
        item = _make_item()
        out = format_order_item(item)

        assert "test-uuid" in out
        assert "SKU1" in out
        assert "quantity=2" in out
        assert "USD" in out
        assert "18.49" in out
        assert "36.98" in out
        assert "2026-01-01T00:00:00Z" in out
        assert "SUCCEED" in out

    def test_decimal_precision(self):
        item = _make_item(unit_price=Decimal("18.49"), total_price=Decimal("55.47"))
        out = format_order_item(item)

        assert "18.49" in out
        assert "55.47" in out
        assert "18.490000000" not in out

    def test_different_status(self):
        item = _make_item(status=OrderStatus.CANCELLED)
        out = format_order_item(item)

        assert "CANCELLED" in out