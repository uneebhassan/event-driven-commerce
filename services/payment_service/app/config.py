from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_ENV = next(
    (p / ".env" for p in Path(__file__).resolve().parents if (p / ".env").is_file()),
    None,
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ROOT_ENV, extra="ignore")

    postgres_user: str
    postgres_password: str
    postgres_db: str
    postgres_port: int = 5432
    postgres_host: str = "localhost"

    kafka_advertised_host: str = "localhost"
    kafka_port: int = 9092
    kafka_bootstrap_override: str | None = None

    orders_topic: str = "orders.events"
    payments_topic: str = "payments.events"
    consumer_group: str = "payment-service"

    payment_failure_threshold: float = 1000.0

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def kafka_bootstrap_servers(self) -> str:
        return self.kafka_bootstrap_override or f"{self.kafka_advertised_host}:{self.kafka_port}"


settings = Settings()
