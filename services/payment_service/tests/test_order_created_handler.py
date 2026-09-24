import json
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID, uuid4

from app.consumers.order_created_consumer import handle_order_created
from app.events.schemas import OrderCreated
from app.infrastructure.models import PaymentModel
from app.infrastructure.payment_gateway import PaymentResult, SimulatedPaymentGateway


class FakePublisher:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes, dict]] = []

    async def publish(self, topic: str, key: bytes, value: bytes) -> None:
        self.sent.append((topic, key, json.loads(value)))


class FakeRepository:
    def __init__(self) -> None:
        self.rows: dict[UUID, PaymentModel] = {}

    async def get_by_order_id(self, order_id: UUID) -> PaymentModel | None:
        return self.rows.get(order_id)

    async def add(self, payment: PaymentModel) -> None:
        self.rows[payment.order_id] = payment


class CountingGateway(SimulatedPaymentGateway):
    def __init__(self, failure_threshold: float) -> None:
        super().__init__(failure_threshold)
        self.charges = 0

    async def charge(self, amount: Decimal, currency: str) -> PaymentResult:
        self.charges += 1
        return await super().charge(amount, currency)


def make_event(amount: str) -> OrderCreated:
    return OrderCreated(
        event_id=uuid4(),
        occurred_at=datetime.now(timezone.utc),
        order_id=uuid4(),
        customer_id=uuid4(),
        total_amount=Decimal(amount),
        currency="EUR",
    )


async def test_payment_succeeds_within_limit_and_is_saved():
    publisher, repo = FakePublisher(), FakeRepository()
    event = make_event("49.99")

    await handle_order_created(
        event, SimulatedPaymentGateway(1000), publisher, repo, "payments.events"
    )

    topic, key, body = publisher.sent[0]
    assert topic == "payments.events"
    assert key == str(event.order_id).encode()
    assert body["event_type"] == "PaymentSucceeded"
    saved = repo.rows[event.order_id]
    assert saved.status == "succeeded"
    assert saved.failure_reason is None
    assert str(saved.id) == body["payment_id"]


async def test_payment_fails_above_limit_and_is_saved():
    publisher, repo = FakePublisher(), FakeRepository()
    event = make_event("5000")

    await handle_order_created(
        event, SimulatedPaymentGateway(1000), publisher, repo, "payments.events"
    )

    _, _, body = publisher.sent[0]
    assert body["event_type"] == "PaymentFailed"
    assert body["reason"] == "amount_exceeds_limit"
    saved = repo.rows[event.order_id]
    assert saved.status == "failed"
    assert saved.failure_reason == "amount_exceeds_limit"


async def test_redelivered_order_is_not_charged_twice():
    publisher, repo = FakePublisher(), FakeRepository()
    gateway = CountingGateway(1000)
    event = make_event("49.99")

    await handle_order_created(event, gateway, publisher, repo, "payments.events")
    await handle_order_created(event, gateway, publisher, repo, "payments.events")

    assert gateway.charges == 1
    assert len(repo.rows) == 1
    assert publisher.sent[0][2]["payment_id"] == publisher.sent[1][2]["payment_id"]
