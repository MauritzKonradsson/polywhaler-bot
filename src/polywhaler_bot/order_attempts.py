from __future__ import annotations

import json
from typing import Any

from polywhaler_bot.models import OrderAttempt


class OrderAttemptBuilder:
    """
    Builds planned OrderAttempt rows from pending ExecutionIntent rows.

    This step is intentionally narrow:
    - no live order submission
    - no authenticated trading calls
    - no fills
    - no position updates
    """

    ATTEMPT_STATUS_PLANNED = "planned"

    def build(
        self,
        *,
        execution_intent: dict[str, Any],
    ) -> OrderAttempt:
        """
        Build a planned OrderAttempt from one execution_intent row.

        Raises ValueError if required execution-intent fields are missing.
        """
        intent_id = self._require_int(
            execution_intent.get("id"),
            field_name="execution_intent.id",
        )
        intent_key = self._require_text(
            execution_intent.get("intent_key"),
            field_name="execution_intent.intent_key",
        )
        position_key = self._require_text(
            execution_intent.get("position_key"),
            field_name="execution_intent.position_key",
        )
        token_id = self._require_text(
            execution_intent.get("token_id"),
            field_name="execution_intent.token_id",
        )
        condition_id = self._require_text(
            execution_intent.get("condition_id"),
            field_name="execution_intent.condition_id",
        )
        outcome = self._require_text(
            execution_intent.get("outcome"),
            field_name="execution_intent.outcome",
        )
        side = self._require_text(
            execution_intent.get("side"),
            field_name="execution_intent.side",
        )

        order_attempt_key = self.build_order_attempt_key(intent_key=intent_key)

        planned_request = {
            "mode": "planned_only",
            "intent_id": intent_id,
            "intent_key": intent_key,
            "position_key": position_key,
            "token_id": token_id,
            "condition_id": condition_id,
            "outcome": outcome,
            "side": side,
            "market_slug": self._string_or_none(execution_intent.get("market_slug")),
            "source_timestamp_utc": self._string_or_none(
                execution_intent.get("source_timestamp_utc")
            ),
            "requested_size": self._float_or_none(
                execution_intent.get("intended_size")
            ),
            "requested_notional": self._float_or_none(
                execution_intent.get("intended_notional")
            ),
            "submit_ready": False,
            "submit_blocked_reason": "step_10_3_planned_only_no_live_submission",
        }

        return OrderAttempt(
            order_attempt_key=order_attempt_key,
            intent_id=intent_id,
            intent_key=intent_key,
            position_key=position_key,
            client_order_id=None,
            exchange_order_id=None,
            side=side,
            token_id=token_id,
            condition_id=condition_id,
            outcome=outcome,
            limit_price=None,
            requested_size=self._float_or_none(execution_intent.get("intended_size")),
            requested_notional=self._float_or_none(
                execution_intent.get("intended_notional")
            ),
            attempt_status=self.ATTEMPT_STATUS_PLANNED,
            raw_request_json=json.dumps(
                planned_request,
                ensure_ascii=False,
                sort_keys=True,
            ),
            raw_response_json=None,
            error_text=None,
            submitted_at_utc=None,
        )

    @staticmethod
    def build_order_attempt_key(*, intent_key: str) -> str:
        return f"{intent_key.strip()}|attempt-1".lower()

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

    def _float_or_none(self, value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
