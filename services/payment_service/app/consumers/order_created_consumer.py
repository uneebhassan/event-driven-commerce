import asyncio
import json
import logging
from datetime import datetime, timezone
from uuid import uuid4

from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError

from app.events.schemas import OrderCreated, PaymentFailed, PaymentSucceeded
from app.infrastructure.kafka import KafkaEventPublisher
from app.infrastructure.models import PaymentModel
from app.infrastructure.payment_gateway import SimulatedPaymentGateway
from app.infrastructure.repository import PaymentRepository

logger = logging.getLogger(__name__)


async def handle_order_created(
    event: OrderCreated,
    gateway: SimulatedPaymentGateway,
    publisher: KafkaEventPublisher,
    repository: PaymentRepository,
    payments_topic: str,
) -> None:
    payment = await repository.get_by_order_id(event.order_id)
    if payment is None:
        result = await gateway.charge(event.total_amount, event.currency)
        payment = PaymentModel(
            id=uuid4(),
            order_id=event.order_id,
            customer_id=event.customer_id,
            amount=event.total_amount,
            currency=event.currency,
            status="succeeded" if result.success else "failed",
            failure_reason=result.reason,
        )
        await repository.add(payment)

    common = dict(
        event_id=uuid4(),
        occurred_at=datetime.now(timezone.utc),
        payment_id=payment.id,
        order_id=payment.order_id,
        customer_id=payment.customer_id,
        amount=payment.amount,
        currency=payment.currency,
    )
    if payment.status == "succeeded":
        outcome = PaymentSucceeded(**common)
    else:
        outcome = PaymentFailed(**common, reason=payment.failure_reason or "unknown")

    await publisher.publish(
        payments_topic,
        key=str(payment.order_id).encode("utf-8"),
        value=outcome.model_dump_json().encode("utf-8"),
    )


class OrderCreatedConsumer:

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        payments_topic: str,
        gateway: SimulatedPaymentGateway,
        publisher: KafkaEventPublisher,
        repository: PaymentRepository,
    ) -> None:
        self.payments_topic = payments_topic
        self.repository = repository
        self.gateway = gateway
        self.publisher = publisher
        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        self._task: asyncio.Task | None = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        await self.consumer.start()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self.consumer.stop()

    async def _run(self) -> None:
        async for message in self.consumer:
            try:
                body = json.loads(message.value)
                if body.get("event_type") != "OrderCreated":
                    await self.consumer.commit()
                    continue
                event = OrderCreated.model_validate(body)
                await handle_order_created(
                    event, self.gateway, self.publisher, self.repository, self.payments_topic
                )
                await self.consumer.commit()
            except (json.JSONDecodeError, ValidationError):
                logger.exception("Skipping invalid message at offset %s", message.offset)
                await self.consumer.commit()
            except Exception:
                logger.exception("Failed to process message at offset %s", message.offset)
