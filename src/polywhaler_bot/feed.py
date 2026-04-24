from __future__ import annotations

import hashlib
import json
from typing import Any
from urllib.parse import urlencode, urljoin

from playwright.sync_api import Error, Page

from polywhaler_bot.audit import AuditLogger
from polywhaler_bot.config import Settings
from polywhaler_bot.constants import (
    COMPONENT_FEED_EXTRACTOR,
    EVENT_DB_ERROR,
    EVENT_DB_WRITE_RAW_EVENT,
    EVENT_FEED_EXTRACT_CYCLE_STARTED,
    EVENT_FEED_EXTRACT_EMPTY,
    EVENT_FEED_EXTRACT_ERROR,
    EVENT_FEED_EXTRACT_SUCCESS,
    EVENT_RAW_EVENT_CREATED,
    EVENT_RAW_EVENT_PERSISTED,
    EVENT_RAW_EVENT_PERSISTENCE_ERROR,
    SESSION_STATUS_HEALTHY,
    SESSION_STATUS_LOGIN_REQUIRED,
    STATE_FEED_LAST_EXTRACT_ATTEMPT_UTC,
    STATE_FEED_LAST_ROW_COUNT,
    STATE_FEED_LAST_SOURCE_URL,
    STATE_FEED_LAST_SUCCESSFUL_EXTRACT_UTC,
)
from polywhaler_bot.db import StateStore
from polywhaler_bot.models import (
    FeedExtractionResult,
    RawFeedEvent,
    RuntimeStateRecord,
    utc_now_iso,
)
from polywhaler_bot.session import PolywhalerSessionManager


class PolywhalerFeedExtractor:
    """
    Milestone 1 network-first feed extractor.

    Responsibilities:
    - ensure the authenticated /deep page/session is usable
    - call /api/trades from inside the authenticated browser/page context
    - parse trades[] items into RawFeedEvent objects
    - persist raw events to SQLite
    - write JSONL audit logs

    This module does NOT:
    - deduplicate or normalize events across cycles
    - interpret lifecycle meaning
    - perform trading logic
    """

    SOURCE_PAGE_NAME = "deep_trades_api"
    SOURCE_KIND = "api_trades"

    def __init__(
        self,
        *,
        settings: Settings,
        state_store: StateStore,
        audit_logger: AuditLogger,
        session_manager: PolywhalerSessionManager,
    ) -> None:
        self.settings = settings
        self.state_store = state_store
        self.audit_logger = audit_logger
        self.session_manager = session_manager

    def extract_once(self) -> FeedExtractionResult:
        """
        Executes one full session-check / API-fetch / persist cycle.

        Returns a FeedExtractionResult summarizing the cycle outcome.
        """
        extracted_at_utc = utc_now_iso()

        self._set_runtime_state(
            STATE_FEED_LAST_EXTRACT_ATTEMPT_UTC,
            extracted_at_utc,
        )

        page = self.session_manager.open_feed_page()

        self.audit_logger.info(
            event_type=EVENT_FEED_EXTRACT_CYCLE_STARTED,
            component=COMPONENT_FEED_EXTRACTOR,
            message="Starting Polywhaler /api/trades extraction cycle",
            data={
                "source_page": self.SOURCE_PAGE_NAME,
                "page_url": page.url,
            },
        )

        try:
            health = self.session_manager.check_health(page)
            if health.status != SESSION_STATUS_HEALTHY:
                return FeedExtractionResult(
                    source_page=self.SOURCE_PAGE_NAME,
                    source_url=health.url or page.url,
                    extracted_at_utc=extracted_at_utc,
                    row_count=0,
                    events=[],
                    session_healthy=False,
                    login_required=(health.status == SESSION_STATUS_LOGIN_REQUIRED),
                    error_message=health.reason,
                )

            api_result = self._fetch_trades_api(page)
            api_url = str(api_result["url"])
            trades = api_result["trades"]

            if not trades:
                self.audit_logger.info(
                    event_type=EVENT_FEED_EXTRACT_EMPTY,
                    component=COMPONENT_FEED_EXTRACTOR,
                    message="API extraction completed with zero trades",
                    data={
                        "row_count": 0,
                        "source_page": self.SOURCE_PAGE_NAME,
                        "source_url": api_url,
                    },
                )
                self._set_runtime_state(STATE_FEED_LAST_ROW_COUNT, "0")
                self._set_runtime_state(STATE_FEED_LAST_SOURCE_URL, api_url)
                self._set_runtime_state(
                    STATE_FEED_LAST_SUCCESSFUL_EXTRACT_UTC,
                    extracted_at_utc,
                )
                return FeedExtractionResult(
                    source_page=self.SOURCE_PAGE_NAME,
                    source_url=api_url,
                    extracted_at_utc=extracted_at_utc,
                    row_count=0,
                    events=[],
                    session_healthy=True,
                    login_required=False,
                    error_message=None,
                )

            events: list[RawFeedEvent] = []
            for row_index, trade in enumerate(trades):
                try:
                    raw_event = self._build_raw_event_from_trade_item(
                        trade=trade,
                        source_url=api_url,
                        extracted_at_utc=extracted_at_utc,
                        row_index=row_index,
                    )

                    if self.settings.verbose_row_logging:
                        self.audit_logger.info(
                            event_type=EVENT_RAW_EVENT_CREATED,
                            component=COMPONENT_FEED_EXTRACTOR,
                            message="Created raw feed event from /api/trades item",
                            data={
                                "row_index": row_index,
                                "market_text": raw_event.market_text,
                                "event_fingerprint": raw_event.event_fingerprint,
                                "transaction_hash": trade.get("transactionHash"),
                            },
                        )

                    try:
                        inserted_id = self.state_store.insert_raw_event(raw_event)
                        self.audit_logger.info(
                            event_type=EVENT_DB_WRITE_RAW_EVENT,
                            component=COMPONENT_FEED_EXTRACTOR,
                            message="Stored raw API trade event in SQLite",
                            data={
                                "db_row_id": inserted_id,
                                "event_fingerprint": raw_event.event_fingerprint,
                                "market_text": raw_event.market_text,
                                "row_index": raw_event.row_index,
                                "source_url": api_url,
                            },
                        )
                        self.audit_logger.info(
                            event_type=EVENT_RAW_EVENT_PERSISTED,
                            component=COMPONENT_FEED_EXTRACTOR,
                            message="Persisted raw API trade event",
                            data={
                                "db_row_id": inserted_id,
                                "event_fingerprint": raw_event.event_fingerprint,
                                "market_text": raw_event.market_text,
                                "row_index": raw_event.row_index,
                                "source_url": api_url,
                            },
                        )
                    except Exception as exc:
                        self.audit_logger.exception(
                            event_type=EVENT_DB_ERROR,
                            component=COMPONENT_FEED_EXTRACTOR,
                            message="Database write failed for API trade event",
                            error=exc,
                            data={
                                "event_fingerprint": raw_event.event_fingerprint,
                                "market_text": raw_event.market_text,
                                "row_index": raw_event.row_index,
                                "source_url": api_url,
                            },
                        )
                        self.audit_logger.exception(
                            event_type=EVENT_RAW_EVENT_PERSISTENCE_ERROR,
                            component=COMPONENT_FEED_EXTRACTOR,
                            message="Failed to persist raw API trade event",
                            error=exc,
                            data={
                                "event_fingerprint": raw_event.event_fingerprint,
                                "market_text": raw_event.market_text,
                                "row_index": raw_event.row_index,
                                "source_url": api_url,
                            },
                        )
                        continue

                    events.append(raw_event)

                except Exception as exc:
                    self.audit_logger.exception(
                        event_type=EVENT_FEED_EXTRACT_ERROR,
                        component=COMPONENT_FEED_EXTRACTOR,
                        message="Failed to map one /api/trades item into RawFeedEvent",
                        error=exc,
                        data={
                            "row_index": row_index,
                            "source_url": api_url,
                            "transaction_hash": trade.get("transactionHash"),
                            "condition_id": trade.get("conditionId"),
                        },
                    )
                    continue

            row_count = len(events)
            self._set_runtime_state(STATE_FEED_LAST_ROW_COUNT, str(row_count))
            self._set_runtime_state(STATE_FEED_LAST_SOURCE_URL, api_url)
            self._set_runtime_state(
                STATE_FEED_LAST_SUCCESSFUL_EXTRACT_UTC,
                extracted_at_utc,
            )

            if row_count == 0:
                self.audit_logger.info(
                    event_type=EVENT_FEED_EXTRACT_EMPTY,
                    component=COMPONENT_FEED_EXTRACTOR,
                    message="API extraction completed with zero persisted rows",
                    data={
                        "row_count": 0,
                        "source_page": self.SOURCE_PAGE_NAME,
                        "source_url": api_url,
                    },
                )
            else:
                self.audit_logger.info(
                    event_type=EVENT_FEED_EXTRACT_SUCCESS,
                    component=COMPONENT_FEED_EXTRACTOR,
                    message="Extracted /api/trades items successfully",
                    data={
                        "row_count": row_count,
                        "source_page": self.SOURCE_PAGE_NAME,
                        "source_url": api_url,
                    },
                )

            return FeedExtractionResult(
                source_page=self.SOURCE_PAGE_NAME,
                source_url=api_url,
                extracted_at_utc=extracted_at_utc,
                row_count=row_count,
                events=events,
                session_healthy=True,
                login_required=False,
                error_message=None,
            )

        except Exception as exc:
            self.audit_logger.exception(
                event_type=EVENT_FEED_EXTRACT_ERROR,
                component=COMPONENT_FEED_EXTRACTOR,
                message="API extraction cycle failed",
                error=exc,
                data={
                    "source_page": self.SOURCE_PAGE_NAME,
                    "page_url": getattr(page, "url", self.settings.polywhaler_feed_url),
                },
            )
            return FeedExtractionResult(
                source_page=self.SOURCE_PAGE_NAME,
                source_url=getattr(page, "url", self.settings.polywhaler_feed_url),
                extracted_at_utc=extracted_at_utc,
                row_count=0,
                events=[],
                session_healthy=False,
                login_required=False,
                error_message=str(exc),
            )

    def _fetch_trades_api(self, page: Page) -> dict[str, Any]:
        """
        Calls /api/trades from inside the current authenticated page/session context.

        No hidden keys are extracted and no separate auth is built.
        The browser session remains the auth holder.
        """
        query = urlencode({"time": "24h"})
        relative_path = f"/api/trades?{query}"

        script = """
        async ({ relativePath }) => {
          const response = await fetch(relativePath, {
            method: "GET",
            credentials: "include",
            headers: {
              "Accept": "application/json"
            }
          });

          const contentType = response.headers.get("content-type") || "";
          const text = await response.text();

          return {
            ok: response.ok,
            status: response.status,
            url: response.url,
            contentType,
            text
          };
        }
        """

        result = page.evaluate(script, {"relativePath": relative_path})

        if not isinstance(result, dict):
            raise RuntimeError("Unexpected /api/trades fetch result shape")

        status = int(result.get("status", 0))
        ok = bool(result.get("ok", False))
        content_type = str(result.get("contentType", ""))
        source_url = str(result.get("url") or urljoin(page.url, relative_path))
        text = str(result.get("text", ""))

        if not ok:
            raise RuntimeError(f"/api/trades request failed with status={status}")

        if "json" not in content_type.lower():
            raise RuntimeError(
                f"/api/trades returned unexpected content-type={content_type!r}"
            )

        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"/api/trades JSON parse failed: {exc}") from exc

        if not isinstance(payload, dict):
            raise RuntimeError("/api/trades payload is not a JSON object")

        trades = payload.get("trades")
        if not isinstance(trades, list):
            raise RuntimeError("/api/trades payload does not contain a trades[] list")

        return {
            "url": source_url,
            "status": status,
            "content_type": content_type,
            "trades": trades,
            "payload": payload,
        }

    def _build_raw_event_from_trade_item(
        self,
        *,
        trade: dict[str, Any],
        source_url: str,
        extracted_at_utc: str,
        row_index: int,
    ) -> RawFeedEvent:
        """
        Maps one /api/trades item into the milestone-1 RawFeedEvent shape.
        """
        market_text = self._string_or_none(trade.get("title")) or "<missing-title>"
        side_text = self._string_or_none(trade.get("side"))
        insider_address_text = self._string_or_none(trade.get("proxyWallet"))
        insider_display_name = (
            self._string_or_none(trade.get("pseudonym"))
            or self._string_or_none(trade.get("name"))
        )

        trade_amount_text = self._format_trade_amount_text(trade)
        probability_text = self._format_probability_text(trade.get("price"))
        feed_seen_at_utc = self._string_or_none(trade.get("timestamp"))

        fingerprint = self._compute_trade_fingerprint(trade)

        return RawFeedEvent(
            event_fingerprint=fingerprint,
            source_page=self.SOURCE_PAGE_NAME,
            source_url=source_url,
            source_kind=self.SOURCE_KIND,
            source_payload=trade,
            extracted_at_utc=extracted_at_utc,
            feed_seen_at_utc=feed_seen_at_utc,
            market_text=market_text,
            side_text=side_text,
            insider_label_text=None,
            insider_address_text=insider_address_text,
            insider_display_name=insider_display_name,
            trade_amount_text=trade_amount_text,
            probability_text=probability_text,
            impact_text=None,
            row_index=row_index,
            row_html=None,
        )

    def _compute_trade_fingerprint(self, trade: dict[str, Any]) -> str:
        transaction_hash = self._string_or_none(trade.get("transactionHash"))
        condition_id = self._string_or_none(trade.get("conditionId"))
        proxy_wallet = self._string_or_none(trade.get("proxyWallet"))
        side = self._string_or_none(trade.get("side"))
        timestamp = self._string_or_none(trade.get("timestamp"))
        price = self._string_or_none(trade.get("price"))
        size = self._string_or_none(trade.get("size"))

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
                price or "",
                size or "",
            ]

        joined = "||".join(parts)
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    def _format_trade_amount_text(self, trade: dict[str, Any]) -> str | None:
        total_value = trade.get("totalValue")
        if total_value is not None:
            numeric = self._float_or_none(total_value)
            if numeric is not None:
                return self._format_money(numeric)
            return self._string_or_none(total_value)

        size = trade.get("size")
        if size is not None:
            numeric = self._float_or_none(size)
            if numeric is not None:
                return self._format_number(numeric)
            return self._string_or_none(size)

        return None

    def _format_probability_text(self, price_value: Any) -> str | None:
        numeric = self._float_or_none(price_value)
        if numeric is None:
            return self._string_or_none(price_value)

        percentage = numeric * 100.0
        formatted = f"{percentage:.2f}".rstrip("0").rstrip(".")
        return f"{formatted}%"

    def _format_money(self, value: float) -> str:
        if value.is_integer():
            return f"${int(value):,}"
        return f"${value:,.2f}"

    def _format_number(self, value: float) -> str:
        if value.is_integer():
            return str(int(value))
        return f"{value:.6f}".rstrip("0").rstrip(".")

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

    def _set_runtime_state(self, key: str, value: str) -> None:
        self.state_store.set_runtime_state(
            RuntimeStateRecord(
                state_key=key,
                state_value=value,
            )
        )