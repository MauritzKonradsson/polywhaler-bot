from __future__ import annotations

from typing import Any

from polywhaler_bot.execution_sizing import ExecutionSizer
from polywhaler_bot.models import ExecutionReadyIntent


class ExecutionReadyBuilder:
    """
    Read-only builder that combines an execution_intent with a sizing result.

    No side effects:
    - no DB writes
    - no order submission
    - no authenticated trading calls
    """

    def __init__(self) -> None:
        self.sizer = ExecutionSizer()

    def build(
        self,
        *,
        execution_intent: dict[str, Any],
        available_capital: float | int | None,
        exposure_snapshot: dict[str, float] | None,
    ) -> ExecutionReadyIntent:
        sizing = self.sizer.evaluate(
            execution_intent=execution_intent,
            available_capital=available_capital,
            exposure_snapshot=exposure_snapshot,
        )

        return ExecutionReadyIntent(
            intent_id=self._require_int(
                execution_intent.get("id"),
                field_name="execution_intent.id",
            ),
            intent_key=self._require_text(
                execution_intent.get("intent_key"),
                field_name="execution_intent.intent_key",
            ),
            position_key=self._require_text(
                execution_intent.get("position_key"),
                field_name="execution_intent.position_key",
            ),
            allowed=sizing.allowed,
            intended_notional=sizing.intended_notional,
            intended_size=sizing.intended_size,
            condition_id=self._require_text(
                execution_intent.get("condition_id"),
                field_name="execution_intent.condition_id",
            ),
            token_id=self._require_text(
                execution_intent.get("token_id"),
                field_name="execution_intent.token_id",
            ),
            outcome=self._require_text(
                execution_intent.get("outcome"),
                field_name="execution_intent.outcome",
            ),
            side=self._require_text(
                execution_intent.get("side"),
                field_name="execution_intent.side",
            ),
            market_slug=self._string_or_none(execution_intent.get("market_slug")),
            source_timestamp_utc=self._string_or_none(
                execution_intent.get("source_timestamp_utc")
            ),
            sizing_reasons=list(sizing.reasons),
        )

    def _require_text(self, value: Any, *, field_name: str) -> str:
        text = self._string_or_none(value)
        if not text:
            raise ValueError(f"Missing required field: {field_name}")
        return text

    def _require_int(self, value: Any, *, field_name: str) -> int:
        try:
            result = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Missing or invalid required int field: {field_name}") from exc
        if result <= 0:
            raise ValueError(f"Missing or invalid required int field: {field_name}")
        return result

    def _string_or_none(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None
