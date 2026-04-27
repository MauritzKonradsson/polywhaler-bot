from __future__ import annotations

from typing import Any

from polywhaler_bot.models import ExecutionValidationResult, PreExecutionOrder


class ExecutionValidator:
    """
    Final pre-submit safety validator for pre-execution orders.

    Responsibilities:
    - accept PreExecutionOrder
    - validate required execution fields
    - fail closed on any invalid field
    - return structured ExecutionValidationResult

    This module must NEVER:
    - write to the DB
    - call external APIs
    - submit orders
    """

    MINIMUM_NOTIONAL = 2.0
    VALID_SIDES = {"BUY", "SELL"}

    def validate(
        self,
        *,
        pre_execution_order: PreExecutionOrder,
    ) -> ExecutionValidationResult:
        reasons: list[str] = []

        price = self._float_or_zero(pre_execution_order.price)
        size = self._float_or_zero(pre_execution_order.size)
        notional = self._float_or_zero(pre_execution_order.notional)

        if price <= 0:
            reasons.append("invalid_price_non_positive")
        if size <= 0:
            reasons.append("invalid_size_non_positive")
        if notional < self.MINIMUM_NOTIONAL:
            reasons.append("notional_below_minimum_order_size")

        condition_id = self._string_or_none(pre_execution_order.condition_id)
        token_id = self._string_or_none(pre_execution_order.token_id)
        outcome = self._string_or_none(pre_execution_order.outcome)
        side = self._string_or_none(pre_execution_order.side)

        if not condition_id:
            reasons.append("missing_condition_id")
        if not token_id:
            reasons.append("missing_token_id")
        if not outcome:
            reasons.append("missing_outcome")
        if not side:
            reasons.append("missing_side")
        elif side.upper() not in self.VALID_SIDES:
            reasons.append(f"invalid_side({side})")

        client_order_id = self._string_or_none(pre_execution_order.client_order_id)
        if not client_order_id:
            reasons.append("missing_client_order_id")

        return ExecutionValidationResult(
            intent_id=int(pre_execution_order.intent_id),
            intent_key=pre_execution_order.intent_key,
            position_key=pre_execution_order.position_key,
            client_order_id=pre_execution_order.client_order_id,
            valid=(len(reasons) == 0),
            reasons=reasons,
            price=pre_execution_order.price,
            size=pre_execution_order.size,
            notional=pre_execution_order.notional,
            condition_id=pre_execution_order.condition_id,
            token_id=pre_execution_order.token_id,
            outcome=pre_execution_order.outcome,
            side=pre_execution_order.side,
        )

    def _string_or_none(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None

    def _float_or_zero(self, value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
