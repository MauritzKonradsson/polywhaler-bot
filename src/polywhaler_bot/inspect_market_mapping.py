from __future__ import annotations

import argparse
from collections import Counter
from pprint import pprint

from polywhaler_bot.config import get_settings
from polywhaler_bot.db import StateStore
from polywhaler_bot.market_mapper import MarketMapper


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect market mapping for recent canonical events."
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

    rows = store.get_recent_canonical_events(limit=args.limit)
    if not rows:
        print("No canonical_events found.")
        return 0

    mapper = MarketMapper(settings=settings)

    results = [mapper.resolve(row) for row in rows]
    status_counts = Counter(result.status for result in results)

    print("=== Market mapping inspection ===")
    print(f"limit: {args.limit}")
    print(f"canonical_events_loaded: {len(rows)}")
    print(f"status_counts: {dict(status_counts)}")
    print()

    print("Sample mappings:")
    for result in results[: min(5, len(results))]:
        pprint(
            {
                "canonical_event_id": result.canonical_event_id,
                "lifecycle_key": result.lifecycle_key,
                "status": result.status,
                "market_slug": result.market_slug,
                "condition_id": result.condition_id,
                "token_id": result.token_id,
                "outcome": result.outcome,
                "canonical_side": result.canonical_side,
                "replication_side": result.replication_side,
                "match_method": result.match_method,
                "confidence": result.confidence,
                "market_active": result.market_active,
                "market_readable": result.market_readable,
                "orderbook_available": result.orderbook_available,
                "ambiguity_flags": result.ambiguity_flags,
                "failure_reasons": result.failure_reasons,
            }
        )
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
