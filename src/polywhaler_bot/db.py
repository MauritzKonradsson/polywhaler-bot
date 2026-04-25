from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from polywhaler_bot.constants import (
    TABLE_RAW_EVENTS,
    TABLE_RUNTIME_STATE,
    TABLE_SCHEMA_META,
)
from polywhaler_bot.models import RawFeedEvent, RuntimeStateRecord

# Milestone 2 schema version is intentionally defined here so Step 7.1B can be
# implemented without requiring any other file changes yet.
SCHEMA_VERSION = 2

TABLE_CANONICAL_EVENTS = "canonical_events"
TABLE_LIFECYCLE_STATE = "lifecycle_state"
TABLE_NORMALIZER_STATE = "normalizer_state"


class StateStore:
    """
    SQLite-backed state store.

    Current responsibilities:
    - initialize and migrate the SQLite schema
    - persist raw feed events
    - persist small runtime/session state values
    - provide lightweight read helpers used by the daemon

    Milestone 2 schema additions:
    - canonical_events
    - lifecycle_state
    - normalizer_state
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def initialize(self) -> None:
        """
        Creates and/or migrates the schema to the current version.

        This is idempotent:
        - existing v1 tables/data are preserved
        - missing v2 tables/indexes are added
        - schema_meta singleton row is updated to version 2
        """
        with self.connect() as conn:
            self._create_v1_tables(conn)
            self._create_v1_indexes(conn)

            self._create_v2_tables(conn)
            self._create_v2_indexes(conn)

            self._initialize_or_update_schema_meta(conn)
            conn.commit()

    def _create_v1_tables(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_RAW_EVENTS} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_fingerprint TEXT NOT NULL,
                source_page TEXT NOT NULL,
                source_url TEXT NOT NULL,
                extracted_at_utc TEXT NOT NULL,
                feed_seen_at_utc TEXT,
                market_text TEXT NOT NULL,
                side_text TEXT,
                insider_label_text TEXT,
                insider_address_text TEXT,
                insider_display_name TEXT,
                trade_amount_text TEXT,
                probability_text TEXT,
                impact_text TEXT,
                row_index INTEGER,
                row_html TEXT,
                row_json TEXT NOT NULL,
                created_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            """
        )

        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_RUNTIME_STATE} (
                state_key TEXT PRIMARY KEY,
                state_value TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            """
        )

        # Single-row schema metadata table; singleton_id must always be 1.
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_SCHEMA_META} (
                singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
                schema_version INTEGER NOT NULL,
                applied_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            """
        )

    def _create_v1_indexes(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_raw_events_fingerprint
            ON {TABLE_RAW_EVENTS} (event_fingerprint);
            """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_raw_events_extracted_at
            ON {TABLE_RAW_EVENTS} (extracted_at_utc);
            """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_raw_events_market_text
            ON {TABLE_RAW_EVENTS} (market_text);
            """
        )

    def _create_v2_tables(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_CANONICAL_EVENTS} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_fingerprint TEXT NOT NULL UNIQUE,
                raw_event_id INTEGER NOT NULL,
                canonical_key TEXT NOT NULL,
                lifecycle_key TEXT NOT NULL,
                event_type TEXT NOT NULL,
                market_text TEXT NOT NULL,
                market_slug TEXT,
                condition_id TEXT,
                asset TEXT,
                insider_address TEXT,
                insider_display_name TEXT,
                side TEXT,
                outcome TEXT,
                price REAL,
                size REAL,
                total_value REAL,
                source_timestamp_utc TEXT,
                normalized_at_utc TEXT NOT NULL,
                source_payload_json TEXT,
                normalization_notes TEXT,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            );
            """
        )

        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_LIFECYCLE_STATE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lifecycle_key TEXT NOT NULL UNIQUE,
                market_text TEXT NOT NULL,
                market_slug TEXT,
                condition_id TEXT,
                asset TEXT,
                insider_address TEXT,
                insider_display_name TEXT,
                side TEXT,
                current_state TEXT NOT NULL,
                first_seen_event_id INTEGER,
                last_seen_event_id INTEGER,
                first_seen_at_utc TEXT,
                last_seen_at_utc TEXT,
                last_price REAL,
                last_size REAL,
                last_total_value REAL,
                cumulative_size REAL,
                cumulative_total_value REAL,
                event_count INTEGER NOT NULL DEFAULT 0,
                state_payload_json TEXT,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            );
            """
        )

        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NORMALIZER_STATE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                state_key TEXT NOT NULL UNIQUE,
                state_value TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            );
            """
        )

    def _create_v2_indexes(self, conn: sqlite3.Connection) -> None:
        # canonical_events
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_canonical_events_raw_event_id
            ON {TABLE_CANONICAL_EVENTS} (raw_event_id);
            """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_canonical_events_lifecycle_key
            ON {TABLE_CANONICAL_EVENTS} (lifecycle_key);
            """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_canonical_events_event_type
            ON {TABLE_CANONICAL_EVENTS} (event_type);
            """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_canonical_events_source_timestamp_utc
            ON {TABLE_CANONICAL_EVENTS} (source_timestamp_utc);
            """
        )

        # lifecycle_state
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_lifecycle_state_current_state
            ON {TABLE_LIFECYCLE_STATE} (current_state);
            """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_lifecycle_state_insider_address
            ON {TABLE_LIFECYCLE_STATE} (insider_address);
            """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_lifecycle_state_condition_id
            ON {TABLE_LIFECYCLE_STATE} (condition_id);
            """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_lifecycle_state_last_seen_at_utc
            ON {TABLE_LIFECYCLE_STATE} (last_seen_at_utc);
            """
        )

        # normalizer_state intentionally has only UNIQUE(state_key); no extra indexes needed.

    def _initialize_or_update_schema_meta(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            f"""
            INSERT INTO {TABLE_SCHEMA_META} (
                singleton_id,
                schema_version
            )
            VALUES (1, ?)
            ON CONFLICT(singleton_id)
            DO UPDATE SET
                schema_version = excluded.schema_version,
                applied_at_utc = (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
            """,
            (SCHEMA_VERSION,),
        )

    def insert_raw_event(self, event: RawFeedEvent) -> int:
        """
        Persists one RawFeedEvent and returns the inserted row ID.
        """
        row_json = json.dumps(event.model_dump(mode="json"), ensure_ascii=False)

        with self.connect() as conn:
            cursor = conn.execute(
                f"""
                INSERT INTO {TABLE_RAW_EVENTS} (
                    event_fingerprint,
                    source_page,
                    source_url,
                    extracted_at_utc,
                    feed_seen_at_utc,
                    market_text,
                    side_text,
                    insider_label_text,
                    insider_address_text,
                    insider_display_name,
                    trade_amount_text,
                    probability_text,
                    impact_text,
                    row_index,
                    row_html,
                    row_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    event.event_fingerprint,
                    event.source_page,
                    event.source_url,
                    event.extracted_at_utc,
                    event.feed_seen_at_utc,
                    event.market_text,
                    event.side_text,
                    event.insider_label_text,
                    event.insider_address_text,
                    event.insider_display_name,
                    event.trade_amount_text,
                    event.probability_text,
                    event.impact_text,
                    event.row_index,
                    event.row_html,
                    row_json,
                ),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def set_runtime_state(self, record: RuntimeStateRecord) -> None:
        """
        Upserts one runtime state key/value pair.
        """
        with self.connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {TABLE_RUNTIME_STATE} (
                    state_key,
                    state_value,
                    updated_at_utc
                )
                VALUES (?, ?, ?)
                ON CONFLICT(state_key)
                DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at_utc = excluded.updated_at_utc;
                """,
                (record.state_key, record.state_value, record.updated_at_utc),
            )
            conn.commit()

    def get_runtime_state(self, state_key: str) -> str | None:
        """
        Returns the current value for a runtime state key, or None if missing.
        """
        with self.connect() as conn:
            row = conn.execute(
                f"""
                SELECT state_value
                FROM {TABLE_RUNTIME_STATE}
                WHERE state_key = ?;
                """,
                (state_key,),
            ).fetchone()
            if row is None:
                return None
            return str(row["state_value"])

    def get_schema_version(self) -> int | None:
        """
        Returns the current schema version from the single-row schema_meta table.
        """
        with self.connect() as conn:
            row = conn.execute(
                f"""
                SELECT schema_version
                FROM {TABLE_SCHEMA_META}
                WHERE singleton_id = 1;
                """
            ).fetchone()
            if row is None:
                return None
            return int(row["schema_version"])

    def count_raw_events(self) -> int:
        """
        Convenience helper for milestone verification.
        """
        with self.connect() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS count FROM {TABLE_RAW_EVENTS};"
            ).fetchone()
            return int(row["count"]) if row is not None else 0

    def get_recent_raw_events(self, limit: int = 20) -> list[dict[str, Any]]:
        """
        Convenience helper for milestone verification/debugging.
        Returns recent raw events as dictionaries with row_json parsed back into
        a Python object.
        """
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM {TABLE_RAW_EVENTS}
                ORDER BY id DESC
                LIMIT ?;
                """,
                (limit,),
            ).fetchall()

        results: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            raw_json = item.get("row_json")
            if isinstance(raw_json, str):
                try:
                    item["row_json"] = json.loads(raw_json)
                except json.JSONDecodeError:
                    pass
            results.append(item)

        return results
