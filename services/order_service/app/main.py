from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.infrastructure.kafka import KafkaEventPublisher, ensure_topics

from .api.routes import ORDER_CREATED_TOPIC, router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await ensure_topics(settings.kafka_bootstrap_servers, [ORDER_CREATED_TOPIC])
    publisher = KafkaEventPublisher(settings.kafka_bootstrap_servers)
    await publisher.start()
    app.state.event_publisher = publisher
    try:
        yield
    finally:
        await publisher.stop()


app = FastAPI(
    title="Order Service",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
