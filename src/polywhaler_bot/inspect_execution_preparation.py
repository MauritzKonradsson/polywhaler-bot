from __future__ import annotations

import argparse
from pprint import pprint

from polywhaler_bot.config import get_settings
from polywhaler_bot.db import StateStore
from polywhaler_bot.execution_preparation import ExecutionPreparer
from polywhaler_bot.execution_ready import ExecutionReadyBuilder


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect pre-execution orders from pending execution intents."
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

    prepared_items: list[dict] = []
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
                    "reason": f"pre_execution_build_error: {type(exc).__name__}: {exc}",
                    "sizing_reasons": execution_ready_intent.sizing_reasons,
                }
            )
            continue

        prepared_items.append(pre_execution_order.model_dump())

    print("=== Pre-execution orders ===")
    print(f"limit: {args.limit}")
    print(f"available_capital: {args.available_capital}")
    print(f"pending_intents: {len(pending_intents)}")
    print(f"prepared: {len(prepared_items)}")
    print(f"blocked: {len(blocked_items)}")
    print()

    print("--- PREPARED ---")
    if prepared_items:
        for item in prepared_items[:10]:
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
