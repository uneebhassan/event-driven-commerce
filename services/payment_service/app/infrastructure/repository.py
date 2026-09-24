from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.infrastructure.models import PaymentModel


class PaymentRepository:

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get_by_order_id(self, order_id: UUID) -> PaymentModel | None:
        async with self._session_factory() as session:
            result = await session.execute(
                select(PaymentModel).where(PaymentModel.order_id == order_id)
            )
            return result.scalar_one_or_none()

    async def add(self, payment: PaymentModel) -> None:
        async with self._session_factory() as session:
            session.add(payment)
            await session.commit()
