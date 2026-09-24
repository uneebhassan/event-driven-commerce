from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.consumers.order_created_consumer import OrderCreatedConsumer
from app.infrastructure.database import SessionLocal, engine
from app.infrastructure.kafka import KafkaEventPublisher, ensure_topics
from app.infrastructure.payment_gateway import SimulatedPaymentGateway
from app.infrastructure.repository import PaymentRepository


@asynccontextmanager
async def lifespan(app: FastAPI):
    servers = settings.kafka_bootstrap_servers
    await ensure_topics(servers, [settings.orders_topic, settings.payments_topic])

    publisher = KafkaEventPublisher(servers)
    await publisher.start()

    consumer = OrderCreatedConsumer(
        bootstrap_servers=servers,
        topic=settings.orders_topic,
        group_id=settings.consumer_group,
        payments_topic=settings.payments_topic,
        gateway=SimulatedPaymentGateway(settings.payment_failure_threshold),
        publisher=publisher,
        repository=PaymentRepository(SessionLocal),
    )
    await consumer.start()
    app.state.consumer = consumer
    try:
        yield
    finally:
        await consumer.stop()
        await publisher.stop()
        await engine.dispose()


app = FastAPI(
    title="Payment Service",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict[str, str]:
    running = app.state.consumer.running
    return {
        "status": "ok" if running else "degraded",
        "consumer": "running" if running else "stopped",
    }
