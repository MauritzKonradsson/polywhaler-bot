from __future__ import annotations

import argparse
from collections import Counter
from pprint import pprint

from polywhaler_bot.config import get_settings
from polywhaler_bot.db import StateStore
from polywhaler_bot.execution_sizing import ExecutionSizer


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect execution sizing for recent pending execution intents."
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
        help="Available capital to use for safety sizing calculations.",
    )
    args = parser.parse_args()

    settings = get_settings()
    store = StateStore(settings.database_path)
    store.initialize()

    pending_intents = store.get_pending_execution_intents(limit=args.limit)
    sizer = ExecutionSizer()

    allowed_count = 0
    blocked_count = 0
    blocked_reason_counts: Counter[str] = Counter()

    results: list[dict] = []

    for execution_intent in pending_intents:
        position_key = str(execution_intent.get("position_key") or "")
        exposure_snapshot = store.get_local_position_exposure(position_key)

        sizing_result = sizer.evaluate(
            execution_intent=execution_intent,
            available_capital=args.available_capital,
            exposure_snapshot=exposure_snapshot,
        )

        if sizing_result.allowed:
            allowed_count += 1
        else:
            blocked_count += 1
            for reason in sizing_result.reasons:
                blocked_reason_counts[reason] += 1

        results.append(
            {
                "intent_id": sizing_result.intent_id,
                "intent_key": sizing_result.intent_key,
                "position_key": sizing_result.position_key,
                "allowed": sizing_result.allowed,
                "intended_notional": sizing_result.intended_notional,
                "intended_size": sizing_result.intended_size,
                "available_capital": sizing_result.available_capital,
                "ceiling_fraction": sizing_result.ceiling_fraction,
                "ceiling_notional": sizing_result.ceiling_notional,
                "existing_local_exposure": sizing_result.existing_local_exposure,
                "remaining_capacity": sizing_result.remaining_capacity,
                "minimum_order_notional": sizing_result.minimum_order_notional,
                "exposure_snapshot": sizing_result.exposure_snapshot,
                "reasons": sizing_result.reasons,
            }
        )

    print("=== Execution sizing inspection ===")
    print(f"limit: {args.limit}")
    print(f"available_capital: {args.available_capital}")
    print(f"pending_execution_intents_loaded: {len(pending_intents)}")
    print(f"allowed: {allowed_count}")
    print(f"blocked: {blocked_count}")
    print()

    print("--- Sample sizing results ---")
    if results:
        for item in results[:10]:
            pprint(item)
    else:
        print("None")
    print()

    print("--- Blocked reason counts ---")
    if blocked_reason_counts:
        pprint(dict(blocked_reason_counts))
    else:
        print("None")
    print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
