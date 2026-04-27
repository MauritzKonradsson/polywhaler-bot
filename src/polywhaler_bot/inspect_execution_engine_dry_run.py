from __future__ import annotations

import argparse
from pprint import pprint

from polywhaler_bot.config import get_settings
from polywhaler_bot.db import StateStore
from polywhaler_bot.execution_engine_dry_run import ExecutionEngineDryRun
from polywhaler_bot.execution_ready import ExecutionReadyBuilder


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect dry-run execution actions from pending execution intents."
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
    dry_run_engine = ExecutionEngineDryRun()

    would_execute: list[dict] = []
    skipped: list[dict] = []

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
            skipped.append(
                {
                    "intent_id": execution_intent.get("id"),
                    "intent_key": execution_intent.get("intent_key"),
                    "position_key": execution_intent.get("position_key"),
                    "reason": f"execution_ready_build_error: {type(exc).__name__}: {exc}",
                }
            )
            continue

        action = dry_run_engine.simulate(
            execution_ready_intent=execution_ready_intent
        )

        if action is None:
            skipped.append(
                {
                    "intent_id": execution_ready_intent.intent_id,
                    "intent_key": execution_ready_intent.intent_key,
                    "position_key": execution_ready_intent.position_key,
                    "reason": execution_ready_intent.sizing_reasons
                    if execution_ready_intent.sizing_reasons
                    else ["not_allowed"],
                }
            )
            continue

        would_execute.append(action.to_dict())

    print("=== Dry-run execution ===")
    print(f"limit: {args.limit}")
    print(f"available_capital: {args.available_capital}")
    print(f"pending_intents: {len(pending_intents)}")
    print(f"would_execute: {len(would_execute)}")
    print(f"skipped: {len(skipped)}")
    print()

    print("--- WOULD EXECUTE ---")
    if would_execute:
        for item in would_execute[:10]:
            pprint(item)
    else:
        print("None")
    print()

    print("--- SKIPPED ---")
    if skipped:
        for item in skipped[:10]:
            pprint(item)
    else:
        print("None")
    print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
