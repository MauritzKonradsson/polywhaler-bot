from __future__ import annotations

import argparse
from collections import Counter
from pprint import pprint

from polywhaler_bot.config import get_settings
from polywhaler_bot.db import StateStore
from polywhaler_bot.insider_visibility import InsiderVisibilityValidator
from polywhaler_bot.market_mapper import MarketMapper


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect insider visibility for recent canonical events."
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
    validator = InsiderVisibilityValidator(settings=settings)

    mapping_status_counts: Counter[str] = Counter()
    visibility_status_counts: Counter[str] = Counter()

    results = []

    for canonical_event in canonical_rows:
        lifecycle_key = str(canonical_event.get("lifecycle_key") or "")
        lifecycle_state = store.get_lifecycle_state_by_key(lifecycle_key)

        resolved_market = mapper.resolve(canonical_event)
        mapping_status_counts[resolved_market.status] += 1

        visibility = validator.evaluate(
            canonical_event=canonical_event,
            lifecycle_state=lifecycle_state,
            resolved_market=resolved_market,
        )
        visibility_status_counts[visibility.status] += 1

        results.append(
            {
                "canonical_event_id": canonical_event.get("id"),
                "lifecycle_key": lifecycle_key,
                "resolved_market": resolved_market,
                "visibility": visibility,
            }
        )

    print("=== Insider visibility inspection ===")
    print(f"limit: {args.limit}")
    print(f"canonical_events_loaded: {len(canonical_rows)}")
    print(f"mapping_status_counts: {dict(mapping_status_counts)}")
    print(f"visibility_status_counts: {dict(visibility_status_counts)}")
    print()

    print("Sample results:")
    for item in results[: min(5, len(results))]:
        resolved_market = item["resolved_market"]
        visibility = item["visibility"]

        pprint(
            {
                "canonical_event_id": item["canonical_event_id"],
                "lifecycle_key": item["lifecycle_key"],
                "resolved_market_status": resolved_market.status,
                "resolved_condition_id": resolved_market.condition_id,
                "resolved_token_id": resolved_market.token_id,
                "resolved_outcome": resolved_market.outcome,
                "visibility_status": visibility.status,
                "matched_condition_id": visibility.matched_condition_id,
                "matched_asset": visibility.matched_asset,
                "matched_outcome": visibility.matched_outcome,
                "matched_size": visibility.matched_size,
                "matched_current_value": visibility.matched_current_value,
                "reference_last_size": visibility.reference_last_size,
                "reference_last_total_value": visibility.reference_last_total_value,
                "one_recheck_performed": visibility.one_recheck_performed,
                "reasons": visibility.reasons,
            }
        )
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
