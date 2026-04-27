from __future__ import annotations

import argparse
from pprint import pprint

from polywhaler_bot.config import get_settings
from polywhaler_bot.db import StateStore
from polywhaler_bot.execution_ready import ExecutionReadyBuilder


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect execution-ready intents from pending execution intents."
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
    builder = ExecutionReadyBuilder()

    ready_items: list[dict] = []
    blocked_items: list[dict] = []

    for execution_intent in pending_intents:
        position_key = str(execution_intent.get("position_key") or "")
        exposure_snapshot = store.get_local_position_exposure(position_key)

        try:
            ready_intent = builder.build(
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
                    "allowed": False,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            continue

        row = {
            "intent_id": ready_intent.intent_id,
            "intent_key": ready_intent.intent_key,
            "position_key": ready_intent.position_key,
            "allowed": ready_intent.allowed,
            "intended_notional": ready_intent.intended_notional,
            "intended_size": ready_intent.intended_size,
            "condition_id": ready_intent.condition_id,
            "token_id": ready_intent.token_id,
            "outcome": ready_intent.outcome,
            "side": ready_intent.side,
            "market_slug": ready_intent.market_slug,
            "source_timestamp_utc": ready_intent.source_timestamp_utc,
            "sizing_reasons": ready_intent.sizing_reasons,
        }

        if ready_intent.allowed:
            ready_items.append(row)
        else:
            blocked_items.append(row)

    print("=== Execution-ready inspection ===")
    print(f"limit: {args.limit}")
    print(f"available_capital: {args.available_capital}")
    print(f"pending_intents: {len(pending_intents)}")
    print(f"ready: {len(ready_items)}")
    print(f"blocked: {len(blocked_items)}")
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
