from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ConfigDict


def utc_now_iso() -> str:
    """
    Returns a UTC timestamp in ISO-8601 format with a trailing 'Z'.
    Example: 2026-04-21T12:00:05.123456Z
    """
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class RawFeedEvent(BaseModel):
    """
    Structured representation of one visible Polywhaler trade row / API trade item
    as extracted during milestone 1.

    This is intentionally raw and close to the source:
    - no lifecycle interpretation
    - no strategy logic
    - no normalization beyond field shaping
    """

    model_config = ConfigDict(extra="ignore")

    event_fingerprint: str
    source_page: str
    source_url: str
    source_kind: str | None = None
    source_payload: dict[str, Any] | None = None

    extracted_at_utc: str = Field(default_factory=utc_now_iso)
    feed_seen_at_utc: str | None = None

    market_text: str
    side_text: str | None = None
    insider_label_text: str | None = None
    insider_address_text: str | None = None
    insider_display_name: str | None = None
    trade_amount_text: str | None = None
    probability_text: str | None = None
    impact_text: str | None = None

    row_index: int | None = None
    row_html: str | None = None


class CanonicalEvent(BaseModel):
    """
    One normalized logical event derived from a raw_event row.

    Aligned to the schema_version=2 canonical_events table.
    """

    model_config = ConfigDict(extra="ignore")

    id: int | None = None
    event_fingerprint: str
    raw_event_id: int
    canonical_key: str
    lifecycle_key: str
    event_type: str

    market_text: str
    market_slug: str | None = None
    condition_id: str | None = None
    asset: str | None = None

    insider_address: str | None = None
    insider_display_name: str | None = None

    side: str | None = None
    outcome: str | None = None

    price: float | None = None
    size: float | None = None
    total_value: float | None = None

    source_timestamp_utc: str | None = None
    normalized_at_utc: str = Field(default_factory=utc_now_iso)

    source_payload_json: str | None = None
    normalization_notes: str | None = None

    created_at_utc: str = Field(default_factory=utc_now_iso)
    updated_at_utc: str = Field(default_factory=utc_now_iso)


class LifecycleState(BaseModel):
    """
    Current lifecycle state per market/insider/side.

    Aligned to the schema_version=2 lifecycle_state table.
    """

    model_config = ConfigDict(extra="ignore")

    id: int | None = None
    lifecycle_key: str

    market_text: str
    market_slug: str | None = None
    condition_id: str | None = None
    asset: str | None = None

    insider_address: str | None = None
    insider_display_name: str | None = None

    side: str | None = None
    current_state: str

    first_seen_event_id: int | None = None
    last_seen_event_id: int | None = None

    first_seen_at_utc: str | None = None
    last_seen_at_utc: str | None = None

    last_price: float | None = None
    last_size: float | None = None
    last_total_value: float | None = None

    cumulative_size: float | None = None
    cumulative_total_value: float | None = None

    event_count: int = 0
    state_payload_json: str | None = None

    created_at_utc: str = Field(default_factory=utc_now_iso)
    updated_at_utc: str = Field(default_factory=utc_now_iso)

class ResolvedMarket(BaseModel):
    """
    Market-mapping result for one canonical event.

    This is a read-only Milestone 4 model and is not persisted in the DB.
    """

    model_config = ConfigDict(extra="ignore")

    canonical_event_id: int
    lifecycle_key: str

    status: str  # resolved | ambiguous | failed

    market_slug: str | None = None
    market_text: str
    condition_id: str | None = None

    token_id: str | None = None
    asset: str | None = None
    outcome: str | None = None

    canonical_side: str | None = None
    replication_side: str | None = None

    match_method: str | None = None
    confidence: float = 0.0

    market_active: bool | None = None
    market_readable: bool | None = None
    orderbook_available: bool | None = None

    ambiguity_flags: list[str] = Field(default_factory=list)
    failure_reasons: list[str] = Field(default_factory=list)

class NormalizerStateRecord(BaseModel):
    """
    Key/value checkpoint state for idempotent normalization runs.

    Aligned to the schema_version=2 normalizer_state table.
    """

    model_config = ConfigDict(extra="ignore")

    id: int | None = None
    state_key: str
    state_value: str
    updated_at_utc: str = Field(default_factory=utc_now_iso)


class NormalizationResult(BaseModel):
    """
    Lightweight non-table model for reporting one normalization outcome.

    Useful later for run-once normalizer output and testing, but does not map
    directly to a DB table.
    """

    model_config = ConfigDict(extra="ignore")

    raw_event_id: int
    canonical_event_id: int | None = None
    event_fingerprint: str
    lifecycle_key: str
    event_type: str
    status: str
    notes: str | None = None
    processed_at_utc: str = Field(default_factory=utc_now_iso)


class RuntimeStateRecord(BaseModel):
    """
    Key/value record for the runtime_state SQLite table.
    """

    model_config = ConfigDict(extra="ignore")

    state_key: str
    state_value: str
    updated_at_utc: str = Field(default_factory=utc_now_iso)


class AuditLogEntry(BaseModel):
    """
    Standard JSONL audit log envelope for milestone 1.
    """

    model_config = ConfigDict(extra="ignore")

    ts_utc: str = Field(default_factory=utc_now_iso)
    level: str
    event_type: str
    run_id: str
    component: str
    message: str
    data: dict[str, Any] = Field(default_factory=dict)


class SessionHealth(BaseModel):
    """
    Lightweight session health snapshot for milestone 1.
    """

    model_config = ConfigDict(extra="ignore")

    status: str
    url: str | None = None
    reason: str | None = None
    checked_at_utc: str = Field(default_factory=utc_now_iso)


class FeedExtractionResult(BaseModel):
    """
    Result of one extraction cycle.

    Used internally by the feed extractor and daemon to summarize:
    - source page/url
    - row count
    - extracted raw events
    - whether the cycle appears healthy
    """

    model_config = ConfigDict(extra="ignore")

    source_page: str
    source_url: str
    extracted_at_utc: str = Field(default_factory=utc_now_iso)
    row_count: int = 0
    events: list[RawFeedEvent] = Field(default_factory=list)
    session_healthy: bool = True
    login_required: bool = False
    error_message: str | None = None


@dataclass(slots=True)
class ParsedRow:
    """
    Lightweight internal container for a single parsed DOM row before it is
    turned into a Pydantic RawFeedEvent.

    This is only for milestone-1 extraction flow and avoids overusing Pydantic
    during DOM parsing.
    """

    market_text: str
    side_text: str | None
    insider_label_text: str | None
    insider_address_text: str | None
    insider_display_name: str | None
    trade_amount_text: str | None
    probability_text: str | None
    impact_text: str | None
    row_index: int | None
    row_html: str | None
