from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


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