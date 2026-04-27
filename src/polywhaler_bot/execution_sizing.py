from __future__ import annotations

from typing import Any

from polywhaler_bot.models import ExecutionSizingResult


class ExecutionSizer:
    """
    Read-only execution sizing / safety calculator.

    This step is intentionally narrow:
    - no live order submission
    - no DB writes
    - no authenticated trading calls
    - no order_attempt creation/update
    - no fills
    - no position updates
    """

    SOFT_CEILING_FRACTION = 0.25
    MINIMUM_ORDER_NOTIONAL = 2.0

    def evaluate(
        self,
        *,
        execution_intent: dict[str, Any],
        available_capital: float | int | None,
        exposure_snapshot: dict[str, float] | None,
    ) -> ExecutionSizingResult:
        intent_id = self._int_or_zero(execution_intent.get("id"))
        intent_key = self._string_or_none(execution_intent.get("intent_key")) or ""
        position_key = self._string_or_none(execution_intent.get("position_key")) or ""

        reasons: list[str] = []

        if intent_id <= 0:
            reasons.append("missing_or_invalid_intent_id")
        if not intent_key:
            reasons.append("missing_intent_key")
        if not position_key:
            reasons.append("missing_position_key")

        intent_status = self._string_or_none(execution_intent.get("intent_status"))
        decision = self._string_or_none(execution_intent.get("decision"))
        execution_eligible = self._boolish(execution_intent.get("execution_eligible"))

        if intent_status != "pending":
            reasons.append(f"intent_not_pending(status={intent_status!r})")
        if decision != "pass":
            reasons.append(f"intent_decision_not_pass(decision={decision!r})")
        if not execution_eligible:
            reasons.append("intent_not_execution_eligible")

        if not self._string_or_none(execution_intent.get("condition_id")):
            reasons.append("missing_condition_id")
        if not self._string_or_none(execution_intent.get("token_id")):
            reasons.append("missing_token_id")
        if not self._string_or_none(execution_intent.get("outcome")):
            reasons.append("missing_outcome")
        if not self._string_or_none(execution_intent.get("side")):
            reasons.append("missing_side")

        available_capital_value = self._float_or_none(available_capital)
        if available_capital_value is None:
            reasons.append("available_capital_unavailable")
            return self._result(
                intent_id=intent_id,
                intent_key=intent_key,
                position_key=position_key,
                allowed=False,
                intended_notional=None,
                intended_size=None,
                available_capital=0.0,
                ceiling_notional=0.0,
                existing_local_exposure=0.0,
                remaining_capacity=0.0,
                exposure_snapshot={},
                reasons=reasons,
            )

        if available_capital_value <= 0:
            reasons.append("available_capital_non_positive")
            return self._result(
                intent_id=intent_id,
                intent_key=intent_key,
                position_key=position_key,
                allowed=False,
                intended_notional=None,
                intended_size=None,
                available_capital=available_capital_value,
                ceiling_notional=0.0,
                existing_local_exposure=0.0,
                remaining_capacity=0.0,
                exposure_snapshot={},
                reasons=reasons,
            )

        if exposure_snapshot is None:
            reasons.append("local_exposure_unavailable")
            return self._result(
                intent_id=intent_id,
                intent_key=intent_key,
                position_key=position_key,
                allowed=False,
                intended_notional=None,
                intended_size=None,
                available_capital=available_capital_value,
                ceiling_notional=available_capital_value * self.SOFT_CEILING_FRACTION,
                existing_local_exposure=0.0,
                remaining_capacity=0.0,
                exposure_snapshot={},
                reasons=reasons,
            )

        normalized_snapshot = {
            "intent_notional": self._float_or_zero(exposure_snapshot.get("intent_notional")),
            "order_attempt_notional": self._float_or_zero(
                exposure_snapshot.get("order_attempt_notional")
            ),
            "position_reserved_notional": self._float_or_zero(
                exposure_snapshot.get("position_reserved_notional")
            ),
            "position_filled_notional": self._float_or_zero(
                exposure_snapshot.get("position_filled_notional")
            ),
        }
        existing_local_exposure = (
            normalized_snapshot["intent_notional"]
            + normalized_snapshot["order_attempt_notional"]
            + normalized_snapshot["position_reserved_notional"]
            + normalized_snapshot["position_filled_notional"]
        )

        ceiling_notional = available_capital_value * self.SOFT_CEILING_FRACTION
        remaining_capacity = ceiling_notional - existing_local_exposure

        if remaining_capacity <= 0:
            reasons.append("single_prediction_ceiling_fully_used_or_exceeded")

        if 0 < remaining_capacity < self.MINIMUM_ORDER_NOTIONAL:
            reasons.append("remaining_capacity_below_minimum_order")

        if reasons:
            return self._result(
                intent_id=intent_id,
                intent_key=intent_key,
                position_key=position_key,
                allowed=False,
                intended_notional=None,
                intended_size=None,
                available_capital=available_capital_value,
                ceiling_notional=ceiling_notional,
                existing_local_exposure=existing_local_exposure,
                remaining_capacity=max(remaining_capacity, 0.0),
                exposure_snapshot=normalized_snapshot,
                reasons=reasons,
            )

        intended_notional = round(remaining_capacity, 2)

        # Step 10.4 is safety/notional-only. Share sizing is not introduced yet.
        return self._result(
            intent_id=intent_id,
            intent_key=intent_key,
            position_key=position_key,
            allowed=True,
            intended_notional=intended_notional,
            intended_size=None,
            available_capital=available_capital_value,
            ceiling_notional=ceiling_notional,
            existing_local_exposure=existing_local_exposure,
            remaining_capacity=remaining_capacity,
            exposure_snapshot=normalized_snapshot,
            reasons=[],
        )

    def _result(
        self,
        *,
        intent_id: int,
        intent_key: str,
        position_key: str,
        allowed: bool,
        intended_notional: float | None,
        intended_size: float | None,
        available_capital: float,
        ceiling_notional: float,
        existing_local_exposure: float,
        remaining_capacity: float,
        exposure_snapshot: dict[str, float],
        reasons: list[str],
    ) -> ExecutionSizingResult:
        return ExecutionSizingResult(
            intent_id=intent_id,
            intent_key=intent_key,
            position_key=position_key,
            allowed=allowed,
            intended_notional=intended_notional,
            intended_size=intended_size,
            available_capital=available_capital,
            ceiling_fraction=self.SOFT_CEILING_FRACTION,
            ceiling_notional=round(ceiling_notional, 2),
            existing_local_exposure=round(existing_local_exposure, 2),
            remaining_capacity=round(remaining_capacity, 2),
            minimum_order_notional=self.MINIMUM_ORDER_NOTIONAL,
            exposure_snapshot={
                key: round(value, 2) for key, value in exposure_snapshot.items()
            },
            reasons=reasons,
        )

    def _string_or_none(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None

    def _float_or_none(self, value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _float_or_zero(self, value: Any) -> float:
        parsed = self._float_or_none(value)
        return parsed if parsed is not None else 0.0

    def _int_or_zero(self, value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def _boolish(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            return value != 0
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return False
