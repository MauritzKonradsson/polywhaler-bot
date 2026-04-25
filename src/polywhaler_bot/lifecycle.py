from __future__ import annotations

import json
from typing import Any

from polywhaler_bot.db import StateStore
from polywhaler_bot.models import LifecycleState, NormalizerStateRecord, utc_now_iso

LIFECYCLE_LAST_CANONICAL_EVENT_ID_KEY = "lifecycle.last_canonical_event_id"


class LifecycleEngine:
    """
    Milestone 2 lifecycle engine.

    Responsibilities:
    - read canonical_events incrementally
    - maintain lifecycle_state per lifecycle_key
    - advance lifecycle.last_canonical_event_id checkpoint

    This module does NOT:
    - place orders
    - perform trading/execution logic
    - send notifications
    """

    def __init__(self, state_store: StateStore) -> None:
        self.state_store = state_store

    def process_pending(self, limit: int | None = None) -> list[dict[str, Any]]:
        """
        Processes canonical_events with id > lifecycle.last_canonical_event_id.

        Returns a list of per-event processing summaries.
        """
        last_canonical_event_id = self.get_last_canonical_event_id()
        canonical_rows = self.state_store.get_canonical_events_after_id(
            last_canonical_event_id=last_canonical_event_id,
            limit=limit,
        )

        results: list[dict[str, Any]] = []

        for canonical_row in canonical_rows:
            canonical_event_id = int(canonical_row["id"])
            lifecycle_key = str(canonical_row["lifecycle_key"])

            try:
                existing_state = self.state_store.get_lifecycle_state_by_key(lifecycle_key)
                lifecycle_state, status = self._build_updated_state(
                    canonical_row=canonical_row,
                    existing_state=existing_state,
                )

                lifecycle_state_id = self.state_store.upsert_lifecycle_state(lifecycle_state)
                self.set_last_canonical_event_id(canonical_event_id)

                results.append(
                    {
                        "canonical_event_id": canonical_event_id,
                        "lifecycle_key": lifecycle_key,
                        "current_state": lifecycle_state.current_state,
                        "status": status,
                        "lifecycle_state_id": lifecycle_state_id,
                    }
                )

            except Exception as exc:
                results.append(
                    {
                        "canonical_event_id": canonical_event_id,
                        "lifecycle_key": lifecycle_key,
                        "current_state": None,
                        "status": "error",
                        "notes": f"{type(exc).__name__}: {exc}",
                    }
                )
                break

        return results

    def get_last_canonical_event_id(self) -> int:
        value = self.state_store.get_lifecycle_processing_state(
            LIFECYCLE_LAST_CANONICAL_EVENT_ID_KEY
        )
        if value is None:
            return 0
        try:
            return int(value)
        except ValueError:
            return 0

    def set_last_canonical_event_id(self, canonical_event_id: int) -> None:
        self.state_store.set_lifecycle_processing_state(
            NormalizerStateRecord(
                state_key=LIFECYCLE_LAST_CANONICAL_EVENT_ID_KEY,
                state_value=str(canonical_event_id),
                updated_at_utc=utc_now_iso(),
            )
        )

    def _build_updated_state(
        self,
        *,
        canonical_row: dict[str, Any],
        existing_state: dict[str, Any] | None,
    ) -> tuple[LifecycleState, str]:
        source_timestamp = (
            self._string_or_none(canonical_row.get("source_timestamp_utc"))
            or self._string_or_none(canonical_row.get("normalized_at_utc"))
            or self._string_or_none(canonical_row.get("created_at_utc"))
            or utc_now_iso()
        )

        price = self._float_or_none(canonical_row.get("price"))
        size = self._float_or_none(canonical_row.get("size"))
        total_value = self._float_or_none(canonical_row.get("total_value"))
        side = self._string_or_none(canonical_row.get("side"))

        current_state = self._determine_current_state(
            side=side,
            size=size,
            total_value=total_value,
        )

        if existing_state is None:
            created_at = utc_now_iso()
            lifecycle_state = LifecycleState(
                lifecycle_key=str(canonical_row["lifecycle_key"]),
                market_text=str(canonical_row["market_text"]),
                market_slug=self._string_or_none(canonical_row.get("market_slug")),
                condition_id=self._string_or_none(canonical_row.get("condition_id")),
                asset=self._string_or_none(canonical_row.get("asset")),
                insider_address=self._string_or_none(canonical_row.get("insider_address")),
                insider_display_name=self._string_or_none(
                    canonical_row.get("insider_display_name")
                ),
                side=side,
                current_state=current_state,
                first_seen_event_id=int(canonical_row["id"]),
                last_seen_event_id=int(canonical_row["id"]),
                first_seen_at_utc=source_timestamp,
                last_seen_at_utc=source_timestamp,
                last_price=price,
                last_size=size,
                last_total_value=total_value,
                cumulative_size=size,
                cumulative_total_value=total_value,
                event_count=1,
                state_payload_json=self._build_state_payload_json(canonical_row),
                created_at_utc=created_at,
                updated_at_utc=created_at,
            )
            return lifecycle_state, "created"

        cumulative_size = self._sum_optional_numbers(
            self._float_or_none(existing_state.get("cumulative_size")),
            size,
        )
        cumulative_total_value = self._sum_optional_numbers(
            self._float_or_none(existing_state.get("cumulative_total_value")),
            total_value,
        )

        lifecycle_state = LifecycleState(
            id=self._int_or_none(existing_state.get("id")),
            lifecycle_key=str(canonical_row["lifecycle_key"]),
            market_text=str(canonical_row["market_text"]),
            market_slug=self._string_or_none(canonical_row.get("market_slug")),
            condition_id=self._string_or_none(canonical_row.get("condition_id")),
            asset=self._string_or_none(canonical_row.get("asset")),
            insider_address=self._string_or_none(canonical_row.get("insider_address")),
            insider_display_name=self._string_or_none(
                canonical_row.get("insider_display_name")
            ),
            side=side,
            current_state=current_state,
            first_seen_event_id=self._int_or_none(existing_state.get("first_seen_event_id"))
            or int(canonical_row["id"]),
            last_seen_event_id=int(canonical_row["id"]),
            first_seen_at_utc=self._string_or_none(existing_state.get("first_seen_at_utc"))
            or source_timestamp,
            last_seen_at_utc=source_timestamp,
            last_price=price if price is not None else self._float_or_none(existing_state.get("last_price")),
            last_size=size if size is not None else self._float_or_none(existing_state.get("last_size")),
            last_total_value=(
                total_value
                if total_value is not None
                else self._float_or_none(existing_state.get("last_total_value"))
            ),
            cumulative_size=cumulative_size,
            cumulative_total_value=cumulative_total_value,
            event_count=int(existing_state.get("event_count") or 0) + 1,
            state_payload_json=self._build_state_payload_json(canonical_row),
            created_at_utc=self._string_or_none(existing_state.get("created_at_utc"))
            or utc_now_iso(),
            updated_at_utc=utc_now_iso(),
        )
        return lifecycle_state, "updated"

    def _determine_current_state(
        self,
        *,
        side: str | None,
        size: float | None,
        total_value: float | None,
    ) -> str:
        if side and ((size is not None and size > 0) or (total_value is not None and total_value > 0)):
            return "in_position"
        return "observed"

    def _build_state_payload_json(self, canonical_row: dict[str, Any]) -> str:
        payload = {
            "last_canonical_event_id": self._int_or_none(canonical_row.get("id")),
            "event_fingerprint": self._string_or_none(canonical_row.get("event_fingerprint")),
            "event_type": self._string_or_none(canonical_row.get("event_type")),
            "side": self._string_or_none(canonical_row.get("side")),
            "outcome": self._string_or_none(canonical_row.get("outcome")),
            "market_slug": self._string_or_none(canonical_row.get("market_slug")),
            "condition_id": self._string_or_none(canonical_row.get("condition_id")),
            "asset": self._string_or_none(canonical_row.get("asset")),
            "source_timestamp_utc": self._string_or_none(
                canonical_row.get("source_timestamp_utc")
            ),
        }
        return json.dumps(payload, ensure_ascii=False)

    def _sum_optional_numbers(self, left: float | None, right: float | None) -> float | None:
        if left is None and right is None:
            return None
        return (left or 0.0) + (right or 0.0)

    def _float_or_none(self, value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _int_or_none(self, value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _string_or_none(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None
