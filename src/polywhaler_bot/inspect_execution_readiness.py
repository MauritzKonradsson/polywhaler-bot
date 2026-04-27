from __future__ import annotations

import argparse
from pprint import pprint

from polywhaler_bot.config import get_settings
from polywhaler_bot.db import StateStore
from polywhaler_bot.execution_preparation import ExecutionPreparer
from polywhaler_bot.execution_readiness import ExecutionReadinessChecker
from polywhaler_bot.execution_ready import ExecutionReadyBuilder


LIVE_OR_SUBMITTED_STATUSES = {
    "submitted",
    "acknowledged",
    "open",
    "partially_filled",
    "filled",
    "live",
}


def _has_existing_live_or_submitted_attempt(
    store: StateStore,
    *,
    intent_key: str,
    client_order_id: str,
) -> bool:
    with store.connect() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM order_attempts
            WHERE
                (intent_key = ? OR client_order_id = ?)
                AND (
                    attempt_status IN ({})
                    OR submitted_at_utc IS NOT NULL
                    OR exchange_order_id IS NOT NULL
                )
            """.format(",".join("?" for _ in LIVE_OR_SUBMITTED_STATUSES)),
            (
                intent_key,
                client_order_id,
                *sorted(LIVE_OR_SUBMITTED_STATUSES),
            ),
        ).fetchone()

    return bool(row["c"]) if row is not None else False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect authenticated execution readiness for pending execution intents."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Number of pending execution intents to inspect (default: 20)",
    )
    parser.add_argument(
        "--available-capital",
        type=float,
        required=True,
        help="Available capital to use for sizing evaluation.",
    )
    args = parser.parse_args()

    settings = get_settings()
    store = StateStore(settings.database_path)
    store.initialize()

    pending_intents = store.get_pending_execution_intents(limit=args.limit)
    ready_builder = ExecutionReadyBuilder()
    preparer = ExecutionPreparer()
    readiness_checker = ExecutionReadinessChecker(settings=settings)

    safe_account_summary = readiness_checker.get_safe_account_summary()

    ready_items: list[dict] = []
    blocked_items: list[dict] = []

    for execution_intent in pending_intents:
        position_key = str(execution_intent.get("position_key") or "")
        exposure_snapshot = store.get_local_position_exposure(position_key)

        try:
            execution_ready_intent = ready_builder.build(
                execution_intent=execution_intent,
                available_capital=args.available_capital,
                exposure_snapshot=exposure_snapshot,
            )
        except Exception as exc:
            blocked_items.append(
                {
                    "intent_id": execution_intent.get("id"),
                    "intent_key": execution_intent.get("intent_key"),
                    "position_key": execution_intent.get("position_key"),
                    "reason": f"execution_ready_build_error: {type(exc).__name__}: {exc}",
                }
            )
            continue

        try:
            pre_execution_order = preparer.build(
                execution_ready_intent=execution_ready_intent
            )
        except Exception as exc:
            blocked_items.append(
                {
                    "intent_id": execution_ready_intent.intent_id,
                    "intent_key": execution_ready_intent.intent_key,
                    "position_key": execution_ready_intent.position_key,
                    "reason": f"execution_preparation_error: {type(exc).__name__}: {exc}",
                    "sizing_reasons": execution_ready_intent.sizing_reasons,
                }
            )
            continue

        existing_conflict = _has_existing_live_or_submitted_attempt(
            store,
            intent_key=pre_execution_order.intent_key,
            client_order_id=pre_execution_order.client_order_id,
        )

        readiness = readiness_checker.evaluate(
            pre_execution_order=pre_execution_order,
            existing_live_order_conflict=existing_conflict,
        )

        row = {
            "intent_id": readiness.intent_id,
            "intent_key": readiness.intent_key,
            "position_key": readiness.position_key,
            "client_order_id": readiness.client_order_id,
            "ready": readiness.ready,
            "reasons": readiness.reasons,
            "price": readiness.price,
            "size": readiness.size,
            "notional": readiness.notional,
            "condition_id": readiness.condition_id,
            "token_id": readiness.token_id,
            "outcome": readiness.outcome,
            "side": readiness.side,
            "validation_ok": readiness.validation_ok,
            "auth_bootstrap_ok": readiness.auth_bootstrap_ok,
            "balance_readable": readiness.balance_readable,
            "allowance_readable": readiness.allowance_readable,
            "orderbook_readable": readiness.orderbook_readable,
            "existing_live_order_conflict": readiness.existing_live_order_conflict,
            "funder_address": readiness.funder_address,
            "l2_source": readiness.l2_source,
            "balance_value": readiness.balance_value,
            "allowance_value": readiness.allowance_value,
        }

        if readiness.ready:
            ready_items.append(row)
        else:
            blocked_items.append(row)

    print("=== Execution readiness ===")
    print(f"limit: {args.limit}")
    print(f"available_capital: {args.available_capital}")
    print(f"pending_intents: {len(pending_intents)}")
    print(f"ready: {len(ready_items)}")
    print(f"blocked: {len(blocked_items)}")
    print()

    print("--- Safe account / balance summary ---")
    pprint(safe_account_summary)
    print()

    print("--- READY ---")
    if ready_items:
        for item in ready_items[:10]:
            pprint(item)
    else:
        print("None")
    print()

    print("--- BLOCKED ---")
    if blocked_items:
        for item in blocked_items[:10]:
            pprint(item)
    else:
        print("None")
    print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
