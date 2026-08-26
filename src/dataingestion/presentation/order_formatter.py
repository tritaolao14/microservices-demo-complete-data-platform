"""Presentation layer: format transformed items for output."""

from domain.models import TransformedOrderItem


def format_order_item(item: TransformedOrderItem) -> str:
    """Format TransformedOrderItem into a log line."""
    return (
        f"Transformed: order_id={item.order_id} product_id={item.product_id} "
        f"quantity={item.quantity} currency={item.currency_code} "
        f"unit_price={item.unit_price} total_price={item.total_price} "
        f"event_timestamp={item.event_timestamp} status={item.status.value}"
    )
