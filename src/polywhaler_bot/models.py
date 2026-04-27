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

class InsiderVisibilityResult(BaseModel):
    """
    Read-only insider visibility classification result for one canonical event.

    This is a Milestone 4 model and is not persisted to the DB.
    """

    model_config = ConfigDict(extra="ignore")

    canonical_event_id: int
    lifecycle_key: str
    insider_address: str | None = None

    status: str  # still_in | partial_reduce | full_exit | flip | ambiguous | unavailable

    matched_condition_id: str | None = None
    matched_asset: str | None = None
    matched_outcome: str | None = None
    matched_size: float | None = None
    matched_current_value: float | None = None

    reference_last_size: float | None = None
    reference_last_total_value: float | None = None

    one_recheck_performed: bool = False

    reasons: list[str] = Field(default_factory=list)

class GateDecision(BaseModel):
    """
    Read-only replication gate decision for one canonical event.

    This is a Milestone 4 model and is not persisted to the DB.
    """

    model_config = ConfigDict(extra="ignore")

    canonical_event_id: int
    lifecycle_key: str

    decision: str  # pass | fail | ambiguous
    execution_eligible: bool = False

    reasons: list[str] = Field(default_factory=list)
    gate_results: dict[str, str] = Field(default_factory=dict)

    resolved_market: ResolvedMarket
    visibility: InsiderVisibilityResult

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

class ExecutionIntent(BaseModel):
    """
    Persistent execution intent derived from one execution-eligible gate decision.
    """

    model_config = ConfigDict(extra="ignore")

    id: int | None = None
    intent_key: str
    canonical_event_id: int
    lifecycle_key: str
    position_key: str

    action_type: str
    intent_status: str

    decision: str
    execution_eligible: bool = False

    market_slug: str | None = None
    condition_id: str
    token_id: str
    asset: str | None = None
    outcome: str
    side: str
    insider_address: str | None = None
    source_timestamp_utc: str | None = None

    intended_notional: float | None = None
    intended_size: float | None = None

    gate_results_json: str | None = None
    gate_reasons_json: str | None = None
    resolved_market_json: str | None = None
    visibility_json: str | None = None

    created_at_utc: str = Field(default_factory=utc_now_iso)
    updated_at_utc: str = Field(default_factory=utc_now_iso)


class OrderAttempt(BaseModel):
    """
    Persistent exchange submission attempt linked to one execution intent.
    """

    model_config = ConfigDict(extra="ignore")

    id: int | None = None
    order_attempt_key: str
    intent_id: int
    intent_key: str
    position_key: str

    client_order_id: str | None = None
    exchange_order_id: str | None = None

    side: str
    token_id: str
    condition_id: str
    outcome: str

    limit_price: float | None = None
    requested_size: float | None = None
    requested_notional: float | None = None

    attempt_status: str
    raw_request_json: str | None = None
    raw_response_json: str | None = None
    error_text: str | None = None

    submitted_at_utc: str | None = None
    created_at_utc: str = Field(default_factory=utc_now_iso)
    updated_at_utc: str = Field(default_factory=utc_now_iso)


class FillRecord(BaseModel):
    """
    Persistent fill record linked to one order attempt and one execution intent.
    """

    model_config = ConfigDict(extra="ignore")

    id: int | None = None
    fill_key: str
    intent_id: int
    order_attempt_id: int
    position_key: str

    exchange_order_id: str | None = None
    exchange_trade_id: str | None = None

    side: str
    token_id: str
    condition_id: str
    outcome: str

    fill_price: float
    fill_size: float
    fill_notional: float | None = None
    fee_amount: float | None = None
    fill_timestamp_utc: str | None = None

    raw_fill_json: str | None = None
    created_at_utc: str = Field(default_factory=utc_now_iso)
    updated_at_utc: str = Field(default_factory=utc_now_iso)


class PositionRecord(BaseModel):
    """
    Persistent local accounting row for one executable position lane.
    """

    model_config = ConfigDict(extra="ignore")

    id: int | None = None
    position_key: str
    lifecycle_key: str

    first_canonical_event_id: int
    last_canonical_event_id: int

    market_slug: str | None = None
    condition_id: str
    token_id: str
    asset: str | None = None
    outcome: str
    side: str
    insider_address: str | None = None

    position_status: str

    total_filled_size: float = 0.0
    total_filled_notional: float = 0.0
    avg_entry_price: float | None = None
    reserved_notional: float = 0.0

    order_count: int = 0
    fill_count: int = 0

    opened_at_utc: str | None = None
    last_activity_at_utc: str | None = None
    closed_at_utc: str | None = None

    position_payload_json: str | None = None

    created_at_utc: str = Field(default_factory=utc_now_iso)
    updated_at_utc: str = Field(default_factory=utc_now_iso)

class ExecutionSizingResult(BaseModel):
    """
    Read-only sizing / safety evaluation for one execution intent.

    This is a Milestone 5 model and is not persisted to the DB.
    """

    model_config = ConfigDict(extra="ignore")

    intent_id: int
    intent_key: str
    position_key: str

    allowed: bool
    intended_notional: float | None = None
    intended_size: float | None = None

    available_capital: float
    ceiling_fraction: float = 0.25
    ceiling_notional: float
    existing_local_exposure: float
    remaining_capacity: float
    minimum_order_notional: float = 2.0

    exposure_snapshot: dict[str, float] = Field(default_factory=dict)
    reasons: list[str] = Field(default_factory=list)

class ExecutionReadyIntent(BaseModel):
    """
    Read-only execution-ready view derived from an execution intent plus sizing.

    This model is NOT persisted to the DB.
    """

    model_config = ConfigDict(extra="ignore")

    intent_id: int
    intent_key: str
    position_key: str

    allowed: bool
    intended_notional: float | None = None
    intended_size: float | None = None

    condition_id: str
    token_id: str
    outcome: str
    side: str

    market_slug: str | None = None
    source_timestamp_utc: str | None = None

    sizing_reasons: list[str] = Field(default_factory=list)

class PreExecutionOrder(BaseModel):
    """
    Read-only pre-execution order representation.

    This model is NOT persisted to the DB and has no side effects.
    """

    model_config = ConfigDict(extra="ignore")

    intent_id: int
    intent_key: str
    position_key: str
    client_order_id: str

    condition_id: str
    token_id: str
    outcome: str
    side: str

    price: float
    size: float
    notional: float

    pricing_source: str = "mock_static"
    sizing_source: str = "notional_to_size_estimate"

class ExecutionValidationResult(BaseModel):
    """
    Read-only validation result for a pre-execution order.

    This model is NOT persisted to the DB.
    """

    model_config = ConfigDict(extra="ignore")

    intent_id: int
    intent_key: str
    position_key: str
    client_order_id: str

    valid: bool
    reasons: list[str] = Field(default_factory=list)

    price: float
    size: float
    notional: float

    condition_id: str
    token_id: str
    outcome: str
    side: str

class ExecutionReadinessResult(BaseModel):
    """
    Read-only authenticated pre-submit readiness result.

    This model is NOT persisted to the DB.
    """

    model_config = ConfigDict(extra="ignore")

    intent_id: int
    intent_key: str
    position_key: str
    client_order_id: str

    ready: bool
    reasons: list[str] = Field(default_factory=list)

    price: float
    size: float
    notional: float

    condition_id: str
    token_id: str
    outcome: str
    side: str

    validation_ok: bool
    auth_bootstrap_ok: bool
    balance_readable: bool
    allowance_readable: bool
    orderbook_readable: bool
    existing_live_order_conflict: bool

    funder_address: str | None = None
    l2_source: str | None = None
    balance_value: str | None = None
    allowance_value: str | None = None

class SubmissionPlan(BaseModel):
    """
    Read-only submission plan derived from an ExecutionReadinessResult.

    This model is NOT persisted to the DB.
    """

    model_config = ConfigDict(extra="ignore")

    intent_id: int
    intent_key: str
    position_key: str
    client_order_id: str

    condition_id: str
    token_id: str
    outcome: str
    side: str

    price: float
    size: float
    notional: float

    submission_allowed: bool
    reasons: list[str] = Field(default_factory=list)
    readiness_snapshot: dict[str, Any] = Field(default_factory=dict)
