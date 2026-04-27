from __future__ import annotations

from typing import Any

from polywhaler_bot.models import ExecutionReadinessResult, SubmissionPlan


class SubmissionPlanner:
    """
    Read-only submission plan builder.

    Responsibilities:
    - consume ExecutionReadinessResult
    - produce SubmissionPlan
    - preserve reasons and readiness snapshot

    Hard constraints:
    - NO order submission
    - NO authenticated trading calls
    - NO DB writes
    - NO fills
    - NO position updates
    """

    def build(
        self,
        *,
        execution_readiness: ExecutionReadinessResult,
    ) -> SubmissionPlan:
        readiness_snapshot = {
            "validation_ok": execution_readiness.validation_ok,
            "auth_bootstrap_ok": execution_readiness.auth_bootstrap_ok,
            "balance_readable": execution_readiness.balance_readable,
            "allowance_readable": execution_readiness.allowance_readable,
            "orderbook_readable": execution_readiness.orderbook_readable,
            "existing_live_order_conflict": execution_readiness.existing_live_order_conflict,
            "funder_address": execution_readiness.funder_address,
            "l2_source": execution_readiness.l2_source,
            "balance_value": execution_readiness.balance_value,
            "allowance_value": execution_readiness.allowance_value,
        }

        return SubmissionPlan(
            intent_id=execution_readiness.intent_id,
            intent_key=execution_readiness.intent_key,
            position_key=execution_readiness.position_key,
            client_order_id=execution_readiness.client_order_id,
            condition_id=execution_readiness.condition_id,
            token_id=execution_readiness.token_id,
            outcome=execution_readiness.outcome,
            side=execution_readiness.side,
            price=execution_readiness.price,
            size=execution_readiness.size,
            notional=execution_readiness.notional,
            submission_allowed=execution_readiness.ready,
            reasons=list(execution_readiness.reasons),
            readiness_snapshot=readiness_snapshot,
        )
