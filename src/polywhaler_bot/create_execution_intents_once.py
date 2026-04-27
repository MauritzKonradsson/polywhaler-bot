from __future__ import annotations

import argparse
from collections import Counter
from pprint import pprint

from polywhaler_bot.config import get_settings
from polywhaler_bot.db import StateStore
from polywhaler_bot.execution_intents import ExecutionIntentBuilder
from polywhaler_bot.insider_visibility import InsiderVisibilityValidator
from polywhaler_bot.market_mapper import MarketMapper
from polywhaler_bot.replication_gates import ReplicationGateEngine


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create execution intents once from pass replication candidates."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Number of recent canonical events to inspect (default: 20)",
    )
    args = parser.parse_args()

    settings = get_settings()
    store = StateStore(settings.database_path)
    store.initialize()

    canonical_events = store.get_recent_canonical_events(limit=args.limit)
    if not canonical_events:
        print("=== Execution intent creation ===")
        print("total_events: 0")
        print("created: 0")
        print("skipped_existing: 0")
        print("blocked: 0")
        print("execution_intents_total: 0")
        return 0

    mapper = MarketMapper(settings=settings)
    visibility_validator = InsiderVisibilityValidator(settings=settings)
    gate_engine = ReplicationGateEngine()
    intent_builder = ExecutionIntentBuilder()

    created_count = 0
    skipped_count = 0
    blocked_count = 0

    gate_decision_counts: Counter[str] = Counter()
    blocked_reason_counts: Counter[str] = Counter()

    created_items: list[dict] = []
    skipped_items: list[dict] = []
    blocked_items: list[dict] = []

    for canonical_event in canonical_events:
        lifecycle_key = str(canonical_event.get("lifecycle_key") or "")
        lifecycle_state = store.get_lifecycle_state_by_key(lifecycle_key)

        resolved_market = mapper.resolve(canonical_event)
        visibility = visibility_validator.evaluate(
            canonical_event=canonical_event,
            lifecycle_state=lifecycle_state,
            resolved_market=resolved_market,
        )
        gate_decision = gate_engine.evaluate(
            canonical_event=canonical_event,
            lifecycle_state=lifecycle_state,
            resolved_market=resolved_market,
            visibility=visibility,
        )

        gate_decision_counts[gate_decision.decision] += 1

        execution_intent = None
        try:
            execution_intent = intent_builder.build(
                canonical_event=canonical_event,
                gate_decision=gate_decision,
            )
        except Exception as exc:
            blocked_count += 1
            reason = f"intent_build_error: {type(exc).__name__}: {exc}"
            blocked_reason_counts[reason] += 1
            blocked_items.append(
                {
                    "canonical_event_id": canonical_event.get("id"),
                    "lifecycle_key": lifecycle_key,
                    "reason": reason,
                    "gate_decision": gate_decision.decision,
                }
            )
            continue

        if execution_intent is None:
            blocked_count += 1
            reason = gate_decision.reasons[0] if gate_decision.reasons else "blocked_by_gate_decision"
            blocked_reason_counts[reason] += 1
            blocked_items.append(
                {
                    "canonical_event_id": canonical_event.get("id"),
                    "lifecycle_key": lifecycle_key,
                    "reason": reason,
                    "gate_decision": gate_decision.decision,
                }
            )
            continue

        intent_id, created = store.upsert_execution_intent(execution_intent)
        item = {
            "intent_id": intent_id,
            "intent_key": execution_intent.intent_key,
            "canonical_event_id": execution_intent.canonical_event_id,
            "lifecycle_key": execution_intent.lifecycle_key,
            "position_key": execution_intent.position_key,
            "condition_id": execution_intent.condition_id,
            "token_id": execution_intent.token_id,
            "outcome": execution_intent.outcome,
            "side": execution_intent.side,
            "market_slug": execution_intent.market_slug,
            "source_timestamp_utc": execution_intent.source_timestamp_utc,
        }

        if created:
            created_count += 1
            created_items.append(item)
        else:
            skipped_count += 1
            skipped_items.append(item)

    print("=== Execution intent creation ===")
    print(f"limit: {args.limit}")
    print(f"total_events: {len(canonical_events)}")
    print(f"created: {created_count}")
    print(f"skipped_existing: {skipped_count}")
    print(f"blocked: {blocked_count}")
    print(f"execution_intents_total: {store.count_execution_intents()}")
    print(f"gate_decision_counts: {dict(gate_decision_counts)}")
    print()

    print("--- Created intents ---")
    if created_items:
        for item in created_items[:10]:
            pprint(item)
    else:
        print("None")
    print()

    print("--- Skipped existing intents ---")
    if skipped_items:
        for item in skipped_items[:10]:
            pprint(item)
    else:
        print("None")
    print()

    print("--- Blocked candidates ---")
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
