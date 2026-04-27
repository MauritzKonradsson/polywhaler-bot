from __future__ import annotations

import argparse
from collections import Counter
from pprint import pprint

from polywhaler_bot.config import get_settings
from polywhaler_bot.db import StateStore
from polywhaler_bot.order_attempts import OrderAttemptBuilder


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create planned order attempts once from pending execution intents."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Number of pending execution intents to inspect (default: 20)",
    )
    args = parser.parse_args()

    settings = get_settings()
    store = StateStore(settings.database_path)
    store.initialize()

    builder = OrderAttemptBuilder()

    with store.connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM execution_intents
            WHERE intent_status = 'pending'
              AND decision = 'pass'
              AND execution_eligible = 1
            ORDER BY id DESC
            LIMIT ?;
            """,
            (args.limit,),
        ).fetchall()

    execution_intents = [dict(row) for row in rows]

    if not execution_intents:
        print("=== Planned order attempt creation ===")
        print(f"limit: {args.limit}")
        print("pending_execution_intents_loaded: 0")
        print("created: 0")
        print("skipped_existing: 0")
        print("blocked: 0")
        print(f"order_attempts_total: {store.count_order_attempts()}")
        print()
        print("--- Created order attempts ---")
        print("None")
        print()
        print("--- Skipped existing order attempts ---")
        print("None")
        print()
        print("--- Blocked execution intents ---")
        print("None")
        return 0

    created_count = 0
    skipped_count = 0
    blocked_count = 0

    blocked_reason_counts: Counter[str] = Counter()

    created_items: list[dict] = []
    skipped_items: list[dict] = []
    blocked_items: list[dict] = []

    for execution_intent in execution_intents:
        try:
            order_attempt = builder.build(execution_intent=execution_intent)
            order_attempt_id, created = store.upsert_order_attempt(order_attempt)

            item = {
                "order_attempt_id": order_attempt_id,
                "order_attempt_key": order_attempt.order_attempt_key,
                "intent_id": order_attempt.intent_id,
                "intent_key": order_attempt.intent_key,
                "position_key": order_attempt.position_key,
                "condition_id": order_attempt.condition_id,
                "token_id": order_attempt.token_id,
                "outcome": order_attempt.outcome,
                "side": order_attempt.side,
                "attempt_status": order_attempt.attempt_status,
            }

            if created:
                created_count += 1
                created_items.append(item)
            else:
                skipped_count += 1
                skipped_items.append(item)

        except Exception as exc:
            blocked_count += 1
            reason = f"order_attempt_build_error: {type(exc).__name__}: {exc}"
            blocked_reason_counts[reason] += 1
            blocked_items.append(
                {
                    "execution_intent_id": execution_intent.get("id"),
                    "intent_key": execution_intent.get("intent_key"),
                    "position_key": execution_intent.get("position_key"),
                    "reason": reason,
                }
            )

    print("=== Planned order attempt creation ===")
    print(f"limit: {args.limit}")
    print(f"pending_execution_intents_loaded: {len(execution_intents)}")
    print(f"created: {created_count}")
    print(f"skipped_existing: {skipped_count}")
    print(f"blocked: {blocked_count}")
    print(f"order_attempts_total: {store.count_order_attempts()}")
    print()

    print("--- Created order attempts ---")
    if created_items:
        for item in created_items[:10]:
            pprint(item)
    else:
        print("None")
    print()

    print("--- Skipped existing order attempts ---")
    if skipped_items:
        for item in skipped_items[:10]:
            pprint(item)
    else:
        print("None")
    print()

    print("--- Blocked execution intents ---")
    if blocked_items:
        print("blocked_reason_counts:")
        pprint(dict(blocked_reason_counts))
        print()
        for item in blocked_items[:10]:
            pprint(item)
    else:
        print("None")
    print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
