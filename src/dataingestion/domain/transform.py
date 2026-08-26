""" 
Order transformation logic.

Input: order message -> tuple of transformed 
Workflow: validate order -> transform order -> return tuple of transformed orders
"""

from domain.exceptions import InvalidOrderError
from domain.models import Order, TransformedOrderItem


def validate_order(order: Order) -> None:
    """Validate required fields in Order."""
    if not order.order_id:
        raise InvalidOrderError("Missing order_id")
    if not order.items:
        raise InvalidOrderError(f"Order {order.order_id} has no items")
    if not order.timestamp:
        raise InvalidOrderError(f"Order {order.order_id} is missing timestamp")
    for item in order.items:
        if not item.product_id:
            raise InvalidOrderError(f"Order {order.order_id} has item with missing product_id")


def transform_order(order: Order) -> tuple[TransformedOrderItem, ...]:
    """Transform Order to tuple """
    validate_order(order)
    return tuple(
        TransformedOrderItem(
            order_id=order.order_id,
            product_id=item.product_id,
            quantity=item.quantity,
            currency_code=item.cost.currency_code,
            unit_price=item.cost.to_decimal(),
            total_price=item.cost.to_decimal() * item.quantity,
            event_timestamp=order.timestamp,
            status=order.status
        )
        for item in order.items
    )

