"""
Data models cho order events.
source schema: checkoutservice publishOrderEvent()
Traceback schema: protos/demo.proto -> go struct -> json, struct tags
-> serialization function -> check field included

CartItem struct (take)
AddItemRequest struct
EmptyCartRequest struct
GetCartRequest struct
Cart struct
Empty struct

ListRecommendationsRequest struct
ListRecommendationsResponse struct

Product struct
ListProductsResponse struct
GetProductRequest struct
SearchProductsRequest struct
SearchProductsResponse struct

GetQuoteRequest struct
GetQuoteResponse struct

ShipOrderRequest struct
ShipOrderResponse struct
Address struct

Money struct (take)
GetSupportedCurrenciesResponse struct
CurrencyConversionRequest struct
CreditCardInfo struct
ChargeRequest struct
ChargeResponse struct

OrderItem struct (take)
OrderResult struct
SendOrderConfirmationRequest struct
PlaceOrderRequest struct
PlaceOrderResponse struct

AdRequest struct
AdResponse struct
Ad struct
x struct

Note: 
- (take) means take the field from the source schema
- Only take the fields that needed for business analytics
"""

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum


class OrderStatus(Enum):
    """Order status"""
    # mimic with simple status
    SUCCEED = "SUCCEED"
    CANCELLED = "CANCELLED"

@dataclass(frozen=True)
class Money:
    """Money amount (mirror from proto)"""
    currency_code: str
    units: int # whole units
    nanos: int # nano = 10^-9 (eg. 10.49, nanos = 490_000_000)

    def to_decimal(self) -> Decimal:
        """Convert to decimal"""
        return Decimal(self.units) + Decimal(self.nanos) / Decimal(10**9)


@dataclass(frozen=True)
class OrderAddress:
    """Address (mirror from proto)"""
    street_address: str
    city: str
    state: str
    country: str
    zip_code: int


@dataclass(frozen=True)
class OrderItem:
    """Order item (mirror from proto)"""
    product_id: str
    quantity: int
    cost: Money
    


@dataclass(frozen=True)
class Order:
    """Incoming order event for kafka topic `orders`"""
    order_id: str
    shipping_tracking_id: str
    shipping_cost: Money
    shipping_address: OrderAddress
    items: tuple[OrderItem, ...]
    timestamp: str
    status: OrderStatus


@dataclass(frozen=True)
class TransformedOrderItem:
    """Flattened for analytics -> target analytics.order_items table."""
    order_id: str
    product_id: str
    quantity: int
    currency_code: str
    unit_price: Decimal
    total_price: Decimal
    event_timestamp: str
    status: OrderStatus








