from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class PaymentResult:
    success: bool
    reason: str | None = None


class SimulatedPaymentGateway:
    """Approves payments up to a limit; declines anything above it."""

    def __init__(self, failure_threshold: float) -> None:
        self.failure_threshold = Decimal(str(failure_threshold))

    async def charge(self, amount: Decimal, currency: str) -> PaymentResult:
        if amount > self.failure_threshold:
            return PaymentResult(success=False, reason="amount_exceeds_limit")
        return PaymentResult(success=True)
