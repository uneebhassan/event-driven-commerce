from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel


class OrderCreated(BaseModel):
    event_id: UUID
    event_type: str = "OrderCreated"
    occurred_at: datetime
    order_id: UUID
    customer_id: UUID
    total_amount: Decimal
    currency: str


class PaymentSucceeded(BaseModel):
    event_id: UUID
    event_type: str = "PaymentSucceeded"
    occurred_at: datetime
    payment_id: UUID
    order_id: UUID
    customer_id: UUID
    amount: Decimal
    currency: str


class PaymentFailed(BaseModel):
    event_id: UUID
    event_type: str = "PaymentFailed"
    occurred_at: datetime
    payment_id: UUID
    order_id: UUID
    customer_id: UUID
    amount: Decimal
    currency: str
    reason: str
