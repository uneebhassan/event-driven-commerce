from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class OrderCreated(BaseModel):
    event_id: UUID
    event_type: str = "OrderCreated"
    occurred_at: datetime
    order_id: UUID
    customer_id: UUID