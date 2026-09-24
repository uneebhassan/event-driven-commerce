from datetime import datetime, timezone
from uuid import uuid4

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.events.schemas import OrderCreated
from app.infrastructure.database import get_db_session
from app.infrastructure.kafka import KafkaEventPublisher
from app.infrastructure.models import OrderModel

from .schemas import CreateOrderRequest, OrderResponse

router = APIRouter()

ORDER_CREATED_TOPIC = "order.created"


def get_event_publisher(request: Request) -> KafkaEventPublisher:
    return request.app.state.event_publisher


@router.post("/orders", response_model=OrderResponse)
async def create_order(
    request: CreateOrderRequest,
    session: AsyncSession = Depends(get_db_session),
    publisher: KafkaEventPublisher = Depends(get_event_publisher),
) -> OrderResponse:
    order = OrderModel(
        id=uuid4(),
        customer_id=request.customer_id,
        total_amount=request.total_amount,
        currency=request.currency,
    )

    session.add(order)
    await session.commit()
    await session.refresh(order)

    event = OrderCreated(
        event_id=uuid4(),
        occurred_at=datetime.now(timezone.utc),
        order_id=order.id,
        customer_id=order.customer_id,
    )
    await publisher.publish(
        ORDER_CREATED_TOPIC,
        event.model_dump_json().encode("utf-8"),
    )

    return OrderResponse(
        id=order.id,
        customer_id=order.customer_id,
        total_amount=order.total_amount,
        currency=order.currency,
    )
