"""Data mapping: raw JSON dict -> domain models.

Responsibility: adapt external format (JSON from Kafka) to domain objects.
Handles omitempty behavior from Go json.Marshal (missing fields -> defaults).
"""

from domain.models import Money, Order, OrderAddress, OrderItem, OrderStatus


class OrderDeserializer:
    """Map raw JSON dict from Kafka 'orders' topic to Order domain model."""

    def deserialize(self, message: dict) -> Order:
        """Parse raw JSON dict -> Order domain model.

        Handles omitempty: missing fields -> safe defaults.
        """
        return Order(
            order_id=message.get("order_id", ""),
            shipping_tracking_id=message.get("shipping_tracking_id", ""),
            shipping_cost=self._parse_money(message.get("shipping_cost", {})),
            shipping_address=self._parse_address(
                message.get("shipping_address", {})
            ),
            items=tuple(
                self._parse_order_item(item)
                for item in message.get("items", [])
            ),
            timestamp=message.get("timestamp", ""),
            status=OrderStatus.SUCCEED,
        )

    def _parse_money(self, data: dict) -> Money:
        """Parse Money from JSON dict. Handle omitempty."""
        return Money(
            currency_code=data.get("currency_code", ""),
            units=data.get("units", 0),
            nanos=data.get("nanos", 0),
        )

    def _parse_address(self, data: dict) -> OrderAddress:
        """Parse Address from JSON dict. Handle omitempty."""
        return OrderAddress(
            street_address=data.get("street_address", ""),
            city=data.get("city", ""),
            state=data.get("state", ""),
            country=data.get("country", ""),
            zip_code=data.get("zip_code", 0),
        )

    def _parse_order_item(self, data: dict) -> OrderItem:
        """Parse OrderItem from JSON dict. Handle nested {item, cost}."""
        item = data.get("item", {})
        return OrderItem(
            product_id=item.get("product_id", ""),
            quantity=item.get("quantity", 0),
            cost=self._parse_money(data.get("cost", {})),
        )
