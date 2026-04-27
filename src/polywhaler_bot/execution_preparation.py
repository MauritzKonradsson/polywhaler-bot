from __future__ import annotations

from typing import Any

from polywhaler_bot.models import ExecutionReadyIntent, PreExecutionOrder


class ExecutionPreparer:
    """
    Read-only pre-execution order builder.

    Responsibilities:
    - accept ExecutionReadyIntent
    - require allowed == True
    - require intended_notional > 0
    - use placeholder mock price
    - convert notional -> approximate size
    - generate deterministic client_order_id

    This module must NEVER:
    - submit orders
    - call authenticated APIs
    - write to the DB
    - create fills
    - update positions
    """

    MOCK_PRICE = 0.5
    MINIMUM_NOTIONAL = 2.0

    def build(
        self,
        *,
        execution_ready_intent: ExecutionReadyIntent,
    ) -> PreExecutionOrder:
        if not execution_ready_intent.allowed:
            raise ValueError("execution_ready_intent is not allowed")

        intended_notional = execution_ready_intent.intended_notional
        if intended_notional is None:
            raise ValueError("intended_notional is missing")
        if intended_notional < self.MINIMUM_NOTIONAL:
            raise ValueError(
                f"intended_notional below minimum practical order size ({self.MINIMUM_NOTIONAL})"
            )

        price = self.MOCK_PRICE
        if price <= 0:
            raise ValueError("mock price must be positive")

        size = intended_notional / price
        client_order_id = self.build_client_order_id(
            intent_key=execution_ready_intent.intent_key
        )

        return PreExecutionOrder(
            intent_id=execution_ready_intent.intent_id,
            intent_key=execution_ready_intent.intent_key,
            position_key=execution_ready_intent.position_key,
            client_order_id=client_order_id,
            condition_id=execution_ready_intent.condition_id,
            token_id=execution_ready_intent.token_id,
            outcome=execution_ready_intent.outcome,
            side=execution_ready_intent.side,
            price=round(price, 6),
            size=round(size, 8),
            notional=round(float(intended_notional), 2),
            pricing_source="mock_static",
            sizing_source="notional_to_size_estimate",
        )

    @staticmethod
    def build_client_order_id(*, intent_key: str) -> str:
        return f"{intent_key.strip()}|pre"
