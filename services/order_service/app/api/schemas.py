from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, Field


class CreateOrderRequest(BaseModel):
    customer_id: UUID
    total_amount: Decimal = Field(gt=0)
    currency: str = "EUR"


class OrderResponse(BaseModel):
    id: UUID
    customer_id: UUID
    total_amount: Decimal
    currency: str