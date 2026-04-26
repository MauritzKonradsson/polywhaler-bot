from __future__ import annotations

import argparse
from collections import Counter
from pprint import pprint

from polywhaler_bot.config import get_settings
from polywhaler_bot.db import StateStore
from polywhaler_bot.insider_visibility import InsiderVisibilityValidator
from polywhaler_bot.market_mapper import MarketMapper
from polywhaler_bot.replication_gates import ReplicationGateEngine


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect replication gate decisions for recent canonical events."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of recent canonical events to inspect (default: 10)",
    )
    args = parser.parse_args()

    settings = get_settings()
    store = StateStore(settings.database_path)
    store.initialize()

    canonical_rows = store.get_recent_canonical_events(limit=args.limit)
    if not canonical_rows:
        print("No canonical_events found.")
        return 0

    mapper = MarketMapper(settings=settings)
    visibility_validator = InsiderVisibilityValidator(settings=settings)
    gate_engine = ReplicationGateEngine()

    decision_counts: Counter[str] = Counter()
    mapping_counts: Counter[str] = Counter()
    visibility_counts: Counter[str] = Counter()

    results = []

    for canonical_event in canonical_rows:
        lifecycle_key = str(canonical_event.get("lifecycle_key") or "")
        lifecycle_state = store.get_lifecycle_state_by_key(lifecycle_key)

        resolved_market = mapper.resolve(canonical_event)
        mapping_counts[resolved_market.status] += 1

        visibility = visibility_validator.evaluate(
            canonical_event=canonical_event,
            lifecycle_state=lifecycle_state,
            resolved_market=resolved_market,
        )
        visibility_counts[visibility.status] += 1

        decision = gate_engine.evaluate(
            canonical_event=canonical_event,
            lifecycle_state=lifecycle_state,
            resolved_market=resolved_market,
            visibility=visibility,
        )
        decision_counts[decision.decision] += 1

        results.append(decision)

    print("=== Replication gate inspection ===")
    print(f"limit: {args.limit}")
    print(f"canonical_events_loaded: {len(canonical_rows)}")
    print(f"mapping_status_counts: {dict(mapping_counts)}")
    print(f"visibility_status_counts: {dict(visibility_counts)}")
    print(f"decision_counts: {dict(decision_counts)}")
    print()

    print("Sample decisions:")
    for decision in results[: min(5, len(results))]:
        pprint(
            {
                "canonical_event_id": decision.canonical_event_id,
                "lifecycle_key": decision.lifecycle_key,
                "decision": decision.decision,
                "execution_eligible": decision.execution_eligible,
                "gate_results": decision.gate_results,
                "reasons": decision.reasons,
                "resolved_market": {
                    "status": decision.resolved_market.status,
                    "market_slug": decision.resolved_market.market_slug,
                    "condition_id": decision.resolved_market.condition_id,
                    "token_id": decision.resolved_market.token_id,
                    "outcome": decision.resolved_market.outcome,
                    "replication_side": decision.resolved_market.replication_side,
                    "confidence": decision.resolved_market.confidence,
                },
                "visibility": {
                    "status": decision.visibility.status,
                    "matched_condition_id": decision.visibility.matched_condition_id,
                    "matched_outcome": decision.visibility.matched_outcome,
                    "matched_size": decision.visibility.matched_size,
                    "matched_current_value": decision.visibility.matched_current_value,
                    "one_recheck_performed": decision.visibility.one_recheck_performed,
                },
            }
        )
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
