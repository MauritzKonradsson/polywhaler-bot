from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from polywhaler_bot.constants import (
    SCHEMA_VERSION,
    TABLE_RAW_EVENTS,
    TABLE_RUNTIME_STATE,
    TABLE_SCHEMA_META,
)
from polywhaler_bot.models import RawFeedEvent, RuntimeStateRecord


class StateStore:
    """
    SQLite-backed state store for milestone 1.

    Responsibilities:
    - initialize the SQLite database and schema
    - persist raw feed events
    - persist small runtime/session state values
    - provide lightweight read helpers used by the daemon
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
        Creates the schema for milestone 1 if it does not already exist.
        """
        with self.connect() as conn:
            self._create_tables(conn)
            self._create_indexes(conn)
            self._initialize_schema_meta(conn)
            conn.commit()

    def _create_tables(self, conn: sqlite3.Connection) -> None:
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

        # Explicit single-row schema metadata table for milestone 1.
        # singleton_id must always be 1.
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_SCHEMA_META} (
                singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
                schema_version INTEGER NOT NULL,
                applied_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            """
        )

    def _create_indexes(self, conn: sqlite3.Connection) -> None:
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

    def _initialize_schema_meta(self, conn: sqlite3.Connection) -> None:
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
                    # Keep the original string if decoding somehow fails.
                    pass
            results.append(item)

        return results
