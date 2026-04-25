from __future__ import annotations

from collections import Counter

from polywhaler_bot.config import get_settings
from polywhaler_bot.db import StateStore
from polywhaler_bot.lifecycle import (
    LIFECYCLE_LAST_CANONICAL_EVENT_ID_KEY,
    LifecycleEngine,
)
from polywhaler_bot.normalizer import (
    NORMALIZER_LAST_RAW_EVENT_ID_KEY,
    EventNormalizer,
)


def count_table_rows(store: StateStore, table_name: str) -> int:
    with store.connect() as conn:
        row = conn.execute(f"SELECT COUNT(*) AS c FROM {table_name}").fetchone()
        return int(row["c"]) if row is not None else 0


def main() -> int:
    settings = get_settings()
    store = StateStore(settings.database_path)
    store.initialize()

    normalizer = EventNormalizer(store)
    lifecycle_engine = LifecycleEngine(store)

    normalization_results = normalizer.normalize_pending()
    lifecycle_results = lifecycle_engine.process_pending()

    normalization_status_counts = Counter(r.status for r in normalization_results)
    lifecycle_status_counts = Counter(r["status"] for r in lifecycle_results)

    canonical_events_count = count_table_rows(store, "canonical_events")
    lifecycle_state_count = count_table_rows(store, "lifecycle_state")

    last_raw_event_id = store.get_normalizer_state(NORMALIZER_LAST_RAW_EVENT_ID_KEY)
    last_canonical_event_id = store.get_lifecycle_processing_state(
        LIFECYCLE_LAST_CANONICAL_EVENT_ID_KEY
    )

    print("=== Milestone 2 processing summary ===")
    print(f"database_path: {settings.database_path}")
    print()

    print(f"normalization_processed_count: {len(normalization_results)}")
    print(f"normalization_status_counts: {dict(normalization_status_counts)}")
    print()

    print(f"lifecycle_processed_count: {len(lifecycle_results)}")
    print(f"lifecycle_status_counts: {dict(lifecycle_status_counts)}")
    print()

    print(f"canonical_events_count: {canonical_events_count}")
    print(f"lifecycle_state_count: {lifecycle_state_count}")
    print()

    print(
        f"{NORMALIZER_LAST_RAW_EVENT_ID_KEY}: "
        f"{last_raw_event_id if last_raw_event_id is not None else '<unset>'}"
    )
    print(
        f"{LIFECYCLE_LAST_CANONICAL_EVENT_ID_KEY}: "
        f"{last_canonical_event_id if last_canonical_event_id is not None else '<unset>'}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
