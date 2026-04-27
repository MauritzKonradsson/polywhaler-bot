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
from polywhaler_bot.models import (
    CanonicalEvent,
    ExecutionIntent,
    LifecycleState,
    NormalizerStateRecord,
    RawFeedEvent,
    RuntimeStateRecord,
)

SCHEMA_VERSION = 3

TABLE_CANONICAL_EVENTS = "canonical_events"
TABLE_LIFECYCLE_STATE = "lifecycle_state"
TABLE_NORMALIZER_STATE = "normalizer_state"

TABLE_EXECUTION_INTENTS = "execution_intents"
TABLE_ORDER_ATTEMPTS = "order_attempts"
TABLE_FILL_RECORDS = "fill_records"
TABLE_POSITION_RECORDS = "position_records"


class StateStore:
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
        with self.connect() as conn:
            self._create_v1_tables(conn)
            self._create_v1_indexes(conn)

            self._create_v2_tables(conn)
            self._create_v2_indexes(conn)

            self._create_v3_tables(conn)
            self._create_v3_indexes(conn)

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

    def _create_v3_tables(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_EXECUTION_INTENTS} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                intent_key TEXT NOT NULL UNIQUE,
                canonical_event_id INTEGER NOT NULL,
                lifecycle_key TEXT NOT NULL,
                position_key TEXT NOT NULL,
                action_type TEXT NOT NULL,
                intent_status TEXT NOT NULL,
                decision TEXT NOT NULL,
                execution_eligible INTEGER NOT NULL DEFAULT 0 CHECK (execution_eligible IN (0,1)),
                market_slug TEXT,
                condition_id TEXT NOT NULL,
                token_id TEXT NOT NULL,
                asset TEXT,
                outcome TEXT NOT NULL,
                side TEXT NOT NULL,
                insider_address TEXT,
                source_timestamp_utc TEXT,
                intended_notional REAL,
                intended_size REAL,
                gate_results_json TEXT,
                gate_reasons_json TEXT,
                resolved_market_json TEXT,
                visibility_json TEXT,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                FOREIGN KEY(canonical_event_id) REFERENCES {TABLE_CANONICAL_EVENTS}(id)
            );
            """
        )

        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_ORDER_ATTEMPTS} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_attempt_key TEXT NOT NULL UNIQUE,
                intent_id INTEGER NOT NULL,
                intent_key TEXT NOT NULL,
                position_key TEXT NOT NULL,
                client_order_id TEXT UNIQUE,
                exchange_order_id TEXT,
                side TEXT NOT NULL,
                token_id TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                outcome TEXT NOT NULL,
                limit_price REAL,
                requested_size REAL,
                requested_notional REAL,
                attempt_status TEXT NOT NULL,
                raw_request_json TEXT,
                raw_response_json TEXT,
                error_text TEXT,
                submitted_at_utc TEXT,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                FOREIGN KEY(intent_id) REFERENCES {TABLE_EXECUTION_INTENTS}(id)
            );
            """
        )

        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_FILL_RECORDS} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fill_key TEXT NOT NULL UNIQUE,
                intent_id INTEGER NOT NULL,
                order_attempt_id INTEGER NOT NULL,
                position_key TEXT NOT NULL,
                exchange_order_id TEXT,
                exchange_trade_id TEXT,
                side TEXT NOT NULL,
                token_id TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                outcome TEXT NOT NULL,
                fill_price REAL NOT NULL,
                fill_size REAL NOT NULL,
                fill_notional REAL,
                fee_amount REAL,
                fill_timestamp_utc TEXT,
                raw_fill_json TEXT,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                FOREIGN KEY(intent_id) REFERENCES {TABLE_EXECUTION_INTENTS}(id),
                FOREIGN KEY(order_attempt_id) REFERENCES {TABLE_ORDER_ATTEMPTS}(id)
            );
            """
        )

        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_POSITION_RECORDS} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                position_key TEXT NOT NULL UNIQUE,
                lifecycle_key TEXT NOT NULL,
                first_canonical_event_id INTEGER NOT NULL,
                last_canonical_event_id INTEGER NOT NULL,
                market_slug TEXT,
                condition_id TEXT NOT NULL,
                token_id TEXT NOT NULL,
                asset TEXT,
                outcome TEXT NOT NULL,
                side TEXT NOT NULL,
                insider_address TEXT,
                position_status TEXT NOT NULL,
                total_filled_size REAL NOT NULL DEFAULT 0,
                total_filled_notional REAL NOT NULL DEFAULT 0,
                avg_entry_price REAL,
                reserved_notional REAL NOT NULL DEFAULT 0,
                order_count INTEGER NOT NULL DEFAULT 0,
                fill_count INTEGER NOT NULL DEFAULT 0,
                opened_at_utc TEXT,
                last_activity_at_utc TEXT,
                closed_at_utc TEXT,
                position_payload_json TEXT,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                FOREIGN KEY(first_canonical_event_id) REFERENCES {TABLE_CANONICAL_EVENTS}(id),
                FOREIGN KEY(last_canonical_event_id) REFERENCES {TABLE_CANONICAL_EVENTS}(id)
            );
            """
        )

    def _create_v3_indexes(self, conn: sqlite3.Connection) -> None:
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_execution_intents_canonical_event_id ON {TABLE_EXECUTION_INTENTS} (canonical_event_id);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_execution_intents_lifecycle_key ON {TABLE_EXECUTION_INTENTS} (lifecycle_key);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_execution_intents_position_key ON {TABLE_EXECUTION_INTENTS} (position_key);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_execution_intents_intent_status ON {TABLE_EXECUTION_INTENTS} (intent_status);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_execution_intents_source_timestamp_utc ON {TABLE_EXECUTION_INTENTS} (source_timestamp_utc);")

        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_order_attempts_intent_id ON {TABLE_ORDER_ATTEMPTS} (intent_id);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_order_attempts_intent_key ON {TABLE_ORDER_ATTEMPTS} (intent_key);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_order_attempts_position_key ON {TABLE_ORDER_ATTEMPTS} (position_key);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_order_attempts_exchange_order_id ON {TABLE_ORDER_ATTEMPTS} (exchange_order_id);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_order_attempts_attempt_status ON {TABLE_ORDER_ATTEMPTS} (attempt_status);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_order_attempts_submitted_at_utc ON {TABLE_ORDER_ATTEMPTS} (submitted_at_utc);")

        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_fill_records_intent_id ON {TABLE_FILL_RECORDS} (intent_id);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_fill_records_order_attempt_id ON {TABLE_FILL_RECORDS} (order_attempt_id);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_fill_records_position_key ON {TABLE_FILL_RECORDS} (position_key);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_fill_records_exchange_order_id ON {TABLE_FILL_RECORDS} (exchange_order_id);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_fill_records_exchange_trade_id ON {TABLE_FILL_RECORDS} (exchange_trade_id);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_fill_records_fill_timestamp_utc ON {TABLE_FILL_RECORDS} (fill_timestamp_utc);")

        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_position_records_lifecycle_key ON {TABLE_POSITION_RECORDS} (lifecycle_key);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_position_records_condition_id ON {TABLE_POSITION_RECORDS} (condition_id);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_position_records_token_id ON {TABLE_POSITION_RECORDS} (token_id);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_position_records_position_status ON {TABLE_POSITION_RECORDS} (position_status);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_position_records_last_activity_at_utc ON {TABLE_POSITION_RECORDS} (last_activity_at_utc);")

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
        with self.connect() as conn:
            row = conn.execute(
                f"""
                SELECT state_value
                FROM {TABLE_RUNTIME_STATE}
                WHERE state_key = ?;
                """,
                (state_key,),
            ).fetchone()
            return None if row is None else str(row["state_value"])

    def get_schema_version(self) -> int | None:
        with self.connect() as conn:
            row = conn.execute(
                f"""
                SELECT schema_version
                FROM {TABLE_SCHEMA_META}
                WHERE singleton_id = 1;
                """
            ).fetchone()
            return None if row is None else int(row["schema_version"])

    def count_raw_events(self) -> int:
        with self.connect() as conn:
            row = conn.execute(f"SELECT COUNT(*) AS count FROM {TABLE_RAW_EVENTS};").fetchone()
            return int(row["count"]) if row is not None else 0

    def get_recent_raw_events(self, limit: int = 20) -> list[dict[str, Any]]:
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

    def get_raw_events_after_id(
        self,
        *,
        last_raw_event_id: int,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        query = f"""
            SELECT *
            FROM {TABLE_RAW_EVENTS}
            WHERE id > ?
            ORDER BY id ASC
        """
        params: list[Any] = [last_raw_event_id]

        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        with self.connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()

        results: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            raw_json = item.get("row_json")
            if isinstance(raw_json, str):
                try:
                    item["row_json"] = json.loads(raw_json)
                except json.JSONDecodeError:
                    item["row_json"] = None
            results.append(item)

        return results

    def insert_canonical_event(self, event: CanonicalEvent) -> tuple[int | None, bool]:
        with self.connect() as conn:
            cursor = conn.execute(
                f"""
                INSERT OR IGNORE INTO {TABLE_CANONICAL_EVENTS} (
                    event_fingerprint,
                    raw_event_id,
                    canonical_key,
                    lifecycle_key,
                    event_type,
                    market_text,
                    market_slug,
                    condition_id,
                    asset,
                    insider_address,
                    insider_display_name,
                    side,
                    outcome,
                    price,
                    size,
                    total_value,
                    source_timestamp_utc,
                    normalized_at_utc,
                    source_payload_json,
                    normalization_notes,
                    created_at_utc,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    event.event_fingerprint,
                    event.raw_event_id,
                    event.canonical_key,
                    event.lifecycle_key,
                    event.event_type,
                    event.market_text,
                    event.market_slug,
                    event.condition_id,
                    event.asset,
                    event.insider_address,
                    event.insider_display_name,
                    event.side,
                    event.outcome,
                    event.price,
                    event.size,
                    event.total_value,
                    event.source_timestamp_utc,
                    event.normalized_at_utc,
                    event.source_payload_json,
                    event.normalization_notes,
                    event.created_at_utc,
                    event.updated_at_utc,
                ),
            )

            if cursor.rowcount == 1:
                conn.commit()
                return int(cursor.lastrowid), True

            existing = conn.execute(
                f"""
                SELECT id
                FROM {TABLE_CANONICAL_EVENTS}
                WHERE event_fingerprint = ?;
                """,
                (event.event_fingerprint,),
            ).fetchone()
            conn.commit()

            existing_id = int(existing["id"]) if existing is not None else None
            return existing_id, False

    def get_normalizer_state(self, state_key: str) -> str | None:
        with self.connect() as conn:
            row = conn.execute(
                f"""
                SELECT state_value
                FROM {TABLE_NORMALIZER_STATE}
                WHERE state_key = ?;
                """,
                (state_key,),
            ).fetchone()
            return None if row is None else str(row["state_value"])

    def set_normalizer_state(self, record: NormalizerStateRecord) -> None:
        with self.connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {TABLE_NORMALIZER_STATE} (
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

    def get_lifecycle_processing_state(self, state_key: str) -> str | None:
        return self.get_normalizer_state(state_key)

    def set_lifecycle_processing_state(self, record: NormalizerStateRecord) -> None:
        self.set_normalizer_state(record)

    def get_canonical_events_after_id(
        self,
        *,
        last_canonical_event_id: int,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        query = f"""
            SELECT *
            FROM {TABLE_CANONICAL_EVENTS}
            WHERE id > ?
            ORDER BY id ASC
        """
        params: list[Any] = [last_canonical_event_id]

        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        with self.connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()

        return [self._parse_json_fields(dict(row), ("source_payload_json",)) for row in rows]

    def get_recent_canonical_events(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM {TABLE_CANONICAL_EVENTS}
                ORDER BY id DESC
                LIMIT ?;
                """,
                (limit,),
            ).fetchall()

        return [self._parse_json_fields(dict(row), ("source_payload_json",)) for row in rows]

    def get_lifecycle_state_by_key(self, lifecycle_key: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                f"""
                SELECT *
                FROM {TABLE_LIFECYCLE_STATE}
                WHERE lifecycle_key = ?;
                """,
                (lifecycle_key,),
            ).fetchone()

        if row is None:
            return None

        return self._parse_json_fields(dict(row), ("state_payload_json",))

    def upsert_lifecycle_state(self, state: LifecycleState) -> int:
        with self.connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {TABLE_LIFECYCLE_STATE} (
                    lifecycle_key,
                    market_text,
                    market_slug,
                    condition_id,
                    asset,
                    insider_address,
                    insider_display_name,
                    side,
                    current_state,
                    first_seen_event_id,
                    last_seen_event_id,
                    first_seen_at_utc,
                    last_seen_at_utc,
                    last_price,
                    last_size,
                    last_total_value,
                    cumulative_size,
                    cumulative_total_value,
                    event_count,
                    state_payload_json,
                    created_at_utc,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(lifecycle_key)
                DO UPDATE SET
                    market_text = excluded.market_text,
                    market_slug = excluded.market_slug,
                    condition_id = excluded.condition_id,
                    asset = excluded.asset,
                    insider_address = excluded.insider_address,
                    insider_display_name = excluded.insider_display_name,
                    side = excluded.side,
                    current_state = excluded.current_state,
                    first_seen_event_id = excluded.first_seen_event_id,
                    last_seen_event_id = excluded.last_seen_event_id,
                    first_seen_at_utc = excluded.first_seen_at_utc,
                    last_seen_at_utc = excluded.last_seen_at_utc,
                    last_price = excluded.last_price,
                    last_size = excluded.last_size,
                    last_total_value = excluded.last_total_value,
                    cumulative_size = excluded.cumulative_size,
                    cumulative_total_value = excluded.cumulative_total_value,
                    event_count = excluded.event_count,
                    state_payload_json = excluded.state_payload_json,
                    created_at_utc = excluded.created_at_utc,
                    updated_at_utc = excluded.updated_at_utc;
                """,
                (
                    state.lifecycle_key,
                    state.market_text,
                    state.market_slug,
                    state.condition_id,
                    state.asset,
                    state.insider_address,
                    state.insider_display_name,
                    state.side,
                    state.current_state,
                    state.first_seen_event_id,
                    state.last_seen_event_id,
                    state.first_seen_at_utc,
                    state.last_seen_at_utc,
                    state.last_price,
                    state.last_size,
                    state.last_total_value,
                    state.cumulative_size,
                    state.cumulative_total_value,
                    state.event_count,
                    state.state_payload_json,
                    state.created_at_utc,
                    state.updated_at_utc,
                ),
            )

            row = conn.execute(
                f"""
                SELECT id
                FROM {TABLE_LIFECYCLE_STATE}
                WHERE lifecycle_key = ?;
                """,
                (state.lifecycle_key,),
            ).fetchone()
            conn.commit()

            if row is None:
                raise RuntimeError(f"Failed to upsert lifecycle_state for lifecycle_key={state.lifecycle_key}")
            return int(row["id"])

    def upsert_execution_intent(self, intent: ExecutionIntent) -> tuple[int, bool]:
        with self.connect() as conn:
            cursor = conn.execute(
                f"""
                INSERT OR IGNORE INTO {TABLE_EXECUTION_INTENTS} (
                    intent_key,
                    canonical_event_id,
                    lifecycle_key,
                    position_key,
                    action_type,
                    intent_status,
                    decision,
                    execution_eligible,
                    market_slug,
                    condition_id,
                    token_id,
                    asset,
                    outcome,
                    side,
                    insider_address,
                    source_timestamp_utc,
                    intended_notional,
                    intended_size,
                    gate_results_json,
                    gate_reasons_json,
                    resolved_market_json,
                    visibility_json,
                    created_at_utc,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    intent.intent_key,
                    intent.canonical_event_id,
                    intent.lifecycle_key,
                    intent.position_key,
                    intent.action_type,
                    intent.intent_status,
                    intent.decision,
                    1 if intent.execution_eligible else 0,
                    intent.market_slug,
                    intent.condition_id,
                    intent.token_id,
                    intent.asset,
                    intent.outcome,
                    intent.side,
                    intent.insider_address,
                    intent.source_timestamp_utc,
                    intent.intended_notional,
                    intent.intended_size,
                    intent.gate_results_json,
                    intent.gate_reasons_json,
                    intent.resolved_market_json,
                    intent.visibility_json,
                    intent.created_at_utc,
                    intent.updated_at_utc,
                ),
            )

            if cursor.rowcount == 1:
                conn.commit()
                return int(cursor.lastrowid), True

            row = conn.execute(
                f"""
                SELECT id
                FROM {TABLE_EXECUTION_INTENTS}
                WHERE intent_key = ?;
                """,
                (intent.intent_key,),
            ).fetchone()
            conn.commit()

            if row is None:
                raise RuntimeError(f"Failed to upsert execution_intent for intent_key={intent.intent_key}")

            return int(row["id"]), False

    def get_execution_intent_by_key(self, intent_key: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                f"""
                SELECT *
                FROM {TABLE_EXECUTION_INTENTS}
                WHERE intent_key = ?;
                """,
                (intent_key,),
            ).fetchone()

        if row is None:
            return None

        return self._parse_json_fields(
            dict(row),
            ("gate_results_json", "gate_reasons_json", "resolved_market_json", "visibility_json"),
        )

    def count_execution_intents(self) -> int:
        with self.connect() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS count FROM {TABLE_EXECUTION_INTENTS};"
            ).fetchone()
            return int(row["count"]) if row is not None else 0

    def get_recent_execution_intents(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM {TABLE_EXECUTION_INTENTS}
                ORDER BY id DESC
                LIMIT ?;
                """,
                (limit,),
            ).fetchall()

        return [
            self._parse_json_fields(
                dict(row),
                ("gate_results_json", "gate_reasons_json", "resolved_market_json", "visibility_json"),
            )
            for row in rows
        ]

    def _parse_json_fields(self, item: dict[str, Any], field_names: tuple[str, ...]) -> dict[str, Any]:
        for field_name in field_names:
            raw_value = item.get(field_name)
            if isinstance(raw_value, str):
                try:
                    item[field_name] = json.loads(raw_value)
                except json.JSONDecodeError:
                    pass
        return item

