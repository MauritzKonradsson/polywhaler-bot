from __future__ import annotations

import json
from typing import Any

from polywhaler_bot.models import ExecutionIntent, GateDecision


class ExecutionIntentBuilder:
    """
    Builds ExecutionIntent rows from Milestone 4 pass GateDecision objects.

    This module is intentionally narrow:
    - no live order submission
    - no order_attempt creation
    - no fill creation
    - no position updates
    """

    ACTION_TYPE_ENTRY = "entry"
    INTENT_STATUS_PENDING = "pending"

    def build(
        self,
        *,
        canonical_event: dict[str, Any],
        gate_decision: GateDecision,
    ) -> ExecutionIntent | None:
        """
        Returns an ExecutionIntent only when the gate decision is execution-eligible.

        Returns None for blocked/ambiguous/non-eligible decisions.
        """
        if gate_decision.decision != "pass":
            return None
        if not gate_decision.execution_eligible:
            return None

        resolved_market = gate_decision.resolved_market
        visibility = gate_decision.visibility

        canonical_event_id = self._require_int(
            gate_decision.canonical_event_id,
            field_name="canonical_event_id",
        )
        lifecycle_key = self._require_text(
            gate_decision.lifecycle_key,
            field_name="lifecycle_key",
        )
        condition_id = self._require_text(
            resolved_market.condition_id,
            field_name="resolved_market.condition_id",
        )
        token_id = self._require_text(
            resolved_market.token_id,
            field_name="resolved_market.token_id",
        )
        outcome = self._require_text(
            resolved_market.outcome,
            field_name="resolved_market.outcome",
        )
        side = self._require_text(
            resolved_market.replication_side,
            field_name="resolved_market.replication_side",
        )

        position_key = self.build_position_key(
            condition_id=condition_id,
            token_id=token_id,
            outcome=outcome,
            side=side,
        )
        intent_key = self.build_intent_key(
            canonical_event_id=canonical_event_id,
            lifecycle_key=lifecycle_key,
            position_key=position_key,
            action_type=self.ACTION_TYPE_ENTRY,
        )

        source_timestamp_utc = self._string_or_none(
            canonical_event.get("source_timestamp_utc")
        )
        insider_address = (
            self._string_or_none(canonical_event.get("insider_address"))
            or self._string_or_none(visibility.insider_address)
        )

        return ExecutionIntent(
            intent_key=intent_key,
            canonical_event_id=canonical_event_id,
            lifecycle_key=lifecycle_key,
            position_key=position_key,
            action_type=self.ACTION_TYPE_ENTRY,
            intent_status=self.INTENT_STATUS_PENDING,
            decision=gate_decision.decision,
            execution_eligible=gate_decision.execution_eligible,
            market_slug=self._string_or_none(resolved_market.market_slug),
            condition_id=condition_id,
            token_id=token_id,
            asset=self._string_or_none(resolved_market.asset),
            outcome=outcome,
            side=side,
            insider_address=insider_address,
            source_timestamp_utc=source_timestamp_utc,
            intended_notional=None,
            intended_size=None,
            gate_results_json=json.dumps(
                gate_decision.gate_results,
                ensure_ascii=False,
                sort_keys=True,
            ),
            gate_reasons_json=json.dumps(
                gate_decision.reasons,
                ensure_ascii=False,
            ),
            resolved_market_json=json.dumps(
                gate_decision.resolved_market.model_dump(mode="json"),
                ensure_ascii=False,
                sort_keys=True,
            ),
            visibility_json=json.dumps(
                gate_decision.visibility.model_dump(mode="json"),
                ensure_ascii=False,
                sort_keys=True,
            ),
        )

    @staticmethod
    def build_position_key(
        *,
        condition_id: str,
        token_id: str,
        outcome: str,
        side: str,
    ) -> str:
        return (
            f"{condition_id.strip()}|{token_id.strip()}|{outcome.strip()}|{side.strip()}"
            .lower()
        )

    @staticmethod
    def build_intent_key(
        *,
        canonical_event_id: int,
        lifecycle_key: str,
        position_key: str,
        action_type: str,
    ) -> str:
        return (
            f"{canonical_event_id}|{lifecycle_key.strip()}|"
            f"{position_key.strip()}|{action_type.strip()}"
        ).lower()

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
