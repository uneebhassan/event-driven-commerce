from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID


@dataclass
class Order:
    id: UUID
    customer_id: UUID
    total_amount: Decimal
    currency: str