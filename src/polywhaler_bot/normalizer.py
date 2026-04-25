from __future__ import annotations

import hashlib
import json
from typing import Any

from polywhaler_bot.db import StateStore
from polywhaler_bot.models import (
    CanonicalEvent,
    NormalizationResult,
    NormalizerStateRecord,
    utc_now_iso,
)

NORMALIZER_LAST_RAW_EVENT_ID_KEY = "normalizer.last_raw_event_id"
DEFAULT_EVENT_TYPE = "raw_trade"


class EventNormalizer:
    """
    Milestone 2 event normalizer.

    Responsibilities:
    - read raw_events incrementally from SQLite
    - parse row_json
    - map raw API-native events into CanonicalEvent
    - insert canonical_events idempotently
    - advance normalizer.last_raw_event_id only after successful processing
    - return NormalizationResult objects

    This module does NOT:
    - update lifecycle_state
    - perform execution/trading logic
    - do notification or strategy logic
    """

    def __init__(self, state_store: StateStore) -> None:
        self.state_store = state_store

    def normalize_pending(self, limit: int | None = None) -> list[NormalizationResult]:
        """
        Processes raw_events with id > normalizer.last_raw_event_id.

        Safety behavior:
        - process rows in ascending id order
        - update checkpoint only after each successful insert/skip
        - stop on the first normalization error so we do not advance past bad data
        """
        last_raw_event_id = self.get_last_raw_event_id()
        raw_rows = self.state_store.get_raw_events_after_id(
            last_raw_event_id=last_raw_event_id,
            limit=limit,
        )

        results: list[NormalizationResult] = []

        for raw_row in raw_rows:
            raw_event_id = int(raw_row["id"])

            try:
                canonical_event = self._normalize_raw_row(raw_row)
                canonical_event_id, inserted = self.state_store.insert_canonical_event(
                    canonical_event
                )

                status = "inserted" if inserted else "skipped_existing"

                result = NormalizationResult(
                    raw_event_id=raw_event_id,
                    canonical_event_id=canonical_event_id,
                    event_fingerprint=canonical_event.event_fingerprint,
                    lifecycle_key=canonical_event.lifecycle_key,
                    event_type=canonical_event.event_type,
                    status=status,
                    notes=canonical_event.normalization_notes,
                )
                results.append(result)

                self.set_last_raw_event_id(raw_event_id)

            except Exception as exc:
                results.append(
                    NormalizationResult(
                        raw_event_id=raw_event_id,
                        canonical_event_id=None,
                        event_fingerprint=self._string_or_none(
                            raw_row.get("event_fingerprint")
                        )
                        or f"raw-event-{raw_event_id}",
                        lifecycle_key="",
                        event_type=DEFAULT_EVENT_TYPE,
                        status="error",
                        notes=f"{type(exc).__name__}: {exc}",
                    )
                )
                break

        return results

    def get_last_raw_event_id(self) -> int:
        value = self.state_store.get_normalizer_state(NORMALIZER_LAST_RAW_EVENT_ID_KEY)
        if value is None:
            return 0
        try:
            return int(value)
        except ValueError:
            return 0

    def set_last_raw_event_id(self, raw_event_id: int) -> None:
        self.state_store.set_normalizer_state(
            NormalizerStateRecord(
                state_key=NORMALIZER_LAST_RAW_EVENT_ID_KEY,
                state_value=str(raw_event_id),
                updated_at_utc=utc_now_iso(),
            )
        )

    def _normalize_raw_row(self, raw_row: dict[str, Any]) -> CanonicalEvent:
        row_json = raw_row.get("row_json")
        if not isinstance(row_json, dict):
            raise ValueError("raw_event row_json is missing or not parsed as a dict")

        source_payload = row_json.get("source_payload")
        if source_payload is None:
            source_payload = {}
        if not isinstance(source_payload, dict):
            raise ValueError("raw_event source_payload is not a dict")

        notes: list[str] = []

        raw_event_id = int(raw_row["id"])
        raw_event_fingerprint = self._string_or_none(raw_row.get("event_fingerprint"))

        transaction_hash = self._string_or_none(source_payload.get("transactionHash"))
        condition_id = self._string_or_none(source_payload.get("conditionId"))
        proxy_wallet = self._string_or_none(source_payload.get("proxyWallet"))
        side = (
            self._string_or_none(source_payload.get("side"))
            or self._string_or_none(row_json.get("side_text"))
        )
        timestamp = (
            self._string_or_none(source_payload.get("timestamp"))
            or self._string_or_none(row_json.get("feed_seen_at_utc"))
        )

        event_fingerprint = raw_event_fingerprint or self._build_fallback_event_fingerprint(
            transaction_hash=transaction_hash,
            condition_id=condition_id,
            proxy_wallet=proxy_wallet,
            side=side,
            timestamp=timestamp,
            price=source_payload.get("price"),
            size=source_payload.get("size"),
            raw_event_id=raw_event_id,
        )
        if raw_event_fingerprint is None:
            notes.append("used fallback event_fingerprint")

        market_text = (
            self._string_or_none(raw_row.get("market_text"))
            or self._string_or_none(row_json.get("market_text"))
            or self._string_or_none(source_payload.get("title"))
            or "<missing-market>"
        )
        if market_text == "<missing-market>":
            notes.append("missing market_text")

        market_slug = self._string_or_none(source_payload.get("slug"))
        asset = self._string_or_none(source_payload.get("asset"))
        insider_address = proxy_wallet or self._string_or_none(
            row_json.get("insider_address_text")
        )
        insider_display_name = (
            self._string_or_none(source_payload.get("pseudonym"))
            or self._string_or_none(source_payload.get("name"))
            or self._string_or_none(row_json.get("insider_display_name"))
        )
        outcome = self._string_or_none(source_payload.get("outcome"))

        price = self._float_or_none(source_payload.get("price"))
        size = self._float_or_none(source_payload.get("size"))
        total_value = self._float_or_none(source_payload.get("totalValue"))

        lifecycle_market_part = (
            condition_id or market_slug or market_text or f"raw-market-{raw_event_id}"
        )
        lifecycle_wallet_part = insider_address or "unknown_wallet"
        lifecycle_side_part = side or "unknown_side"

        if condition_id is None:
            notes.append("lifecycle_key used market fallback instead of condition_id")
        if insider_address is None:
            notes.append("lifecycle_key missing insider_address")
        if side is None:
            notes.append("lifecycle_key missing side")

        lifecycle_key = (
            f"{lifecycle_market_part}|{lifecycle_wallet_part}|{lifecycle_side_part}"
        )

        canonical_terminal_part = transaction_hash or timestamp or f"raw_{raw_event_id}"
        if transaction_hash is None and timestamp is None:
            notes.append("canonical_key used raw_event_id fallback terminal")

        canonical_key = f"{lifecycle_key}|{canonical_terminal_part}"

        source_payload_json = (
            json.dumps(source_payload, ensure_ascii=False) if source_payload else None
        )

        normalization_notes = "; ".join(notes) if notes else None

        return CanonicalEvent(
            event_fingerprint=event_fingerprint,
            raw_event_id=raw_event_id,
            canonical_key=canonical_key,
            lifecycle_key=lifecycle_key,
            event_type=DEFAULT_EVENT_TYPE,
            market_text=market_text,
            market_slug=market_slug,
            condition_id=condition_id,
            asset=asset,
            insider_address=insider_address,
            insider_display_name=insider_display_name,
            side=side,
            outcome=outcome,
            price=price,
            size=size,
            total_value=total_value,
            source_timestamp_utc=timestamp,
            normalized_at_utc=utc_now_iso(),
            source_payload_json=source_payload_json,
            normalization_notes=normalization_notes,
            created_at_utc=utc_now_iso(),
            updated_at_utc=utc_now_iso(),
        )

    def _build_fallback_event_fingerprint(
        self,
        *,
        transaction_hash: str | None,
        condition_id: str | None,
        proxy_wallet: str | None,
        side: str | None,
        timestamp: str | None,
        price: Any,
        size: Any,
        raw_event_id: int,
    ) -> str:
        if transaction_hash:
            parts = [
                transaction_hash,
                condition_id or "",
                proxy_wallet or "",
                side or "",
            ]
        else:
            parts = [
                condition_id or "",
                proxy_wallet or "",
                side or "",
                timestamp or "",
                self._string_or_none(price) or "",
                self._string_or_none(size) or "",
                str(raw_event_id),
            ]

        joined = "||".join(parts)
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    def _float_or_none(self, value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _string_or_none(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None
