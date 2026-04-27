from __future__ import annotations

from dataclasses import asdict, dataclass

from polywhaler_bot.models import ExecutionReadyIntent


@dataclass(slots=True)
class DryRunExecutionAction:
    """
    Read-only simulated execution action.

    This is NOT persisted and has no side effects.
    """

    action: str
    intent_id: int
    intent_key: str
    position_key: str
    condition_id: str
    token_id: str
    outcome: str
    side: str
    intended_notional: float | None

    def to_dict(self) -> dict:
        return asdict(self)


class ExecutionEngineDryRun:
    """
    Dry-run execution engine.

    Responsibilities:
    - accept an ExecutionReadyIntent
    - if allowed, produce a simulated execution action
    - if not allowed, skip

    This module must NEVER:
    - place real orders
    - call authenticated APIs
    - write to the DB
    - create fills
    - update positions
    """

    ACTION_WOULD_SUBMIT_ORDER = "would_submit_order"

    def simulate(
        self,
        *,
        execution_ready_intent: ExecutionReadyIntent,
    ) -> DryRunExecutionAction | None:
        """
        Return a simulated execution action only for allowed execution-ready intents.
        """
        if not execution_ready_intent.allowed:
            return None

        return DryRunExecutionAction(
            action=self.ACTION_WOULD_SUBMIT_ORDER,
            intent_id=execution_ready_intent.intent_id,
            intent_key=execution_ready_intent.intent_key,
            position_key=execution_ready_intent.position_key,
            condition_id=execution_ready_intent.condition_id,
            token_id=execution_ready_intent.token_id,
            outcome=execution_ready_intent.outcome,
            side=execution_ready_intent.side,
            intended_notional=execution_ready_intent.intended_notional,
        )
