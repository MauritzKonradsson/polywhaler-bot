from __future__ import annotations

import hashlib
import re
from urllib.parse import urlparse

from playwright.sync_api import Error, Locator, Page, TimeoutError

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
    EVENT_FEED_REFRESH_COMPLETED,
    EVENT_FEED_ROW_PARSED,
    EVENT_FEED_SELECTOR_MISS,
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
    ParsedRow,
    RawFeedEvent,
    RuntimeStateRecord,
    utc_now_iso,
)
from polywhaler_bot.session import PolywhalerSessionManager


class PolywhalerFeedExtractor:
    """
    Milestone 1 deterministic feed extractor.

    Responsibilities:
    - trigger refresh/extract cycles against the current Polywhaler feed page
    - verify session health before extracting
    - parse visible feed rows into structured raw events
    - persist raw events to SQLite
    - write JSONL audit logs

    This module does NOT:
    - deduplicate or normalize events across cycles
    - interpret lifecycle meaning
    - perform trading logic
    """

    SIDE_PATTERN = re.compile(r"\b(YES|NO|BUY|SELL|UP|DOWN)\b", re.IGNORECASE)
    ADDRESS_PATTERN = re.compile(r"\b0x[a-fA-F0-9]{6,40}\b")
    IMPACT_PATTERN = re.compile(
        r"\b(?:low|medium|high)\s+impact\b(?:[:\s-]+[^\n\r]+)?",
        re.IGNORECASE,
    )
    INSIDER_LABEL_LINE_PATTERN = re.compile(
        r"\b(?:insider|risk|low|moderate|medium|high|yellow|red|blue)\b",
        re.IGNORECASE,
    )
    PROBABILITY_PATTERN = re.compile(r"\b\d{1,3}(?:\.\d+)?%\b")
    MONEY_PATTERN = re.compile(
        r"(?i)(\$ ?[\d,]+(?:\.\d+)?[kmb]?|\b[\d,]+(?:\.\d+)? ?(?:USDC|USD)\b)"
    )

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
        Executes one full refresh/extract/persist cycle.

        Returns a FeedExtractionResult summarizing the cycle outcome.
        """
        extracted_at_utc = utc_now_iso()

        self._set_runtime_state(
            STATE_FEED_LAST_EXTRACT_ATTEMPT_UTC,
            extracted_at_utc,
        )

        page = self._ensure_feed_page_for_cycle()

        self.audit_logger.info(
            event_type=EVENT_FEED_EXTRACT_CYCLE_STARTED,
            component=COMPONENT_FEED_EXTRACTOR,
            message="Starting Polywhaler feed extraction cycle",
            data={
                "source_page": self.settings.feed_source_page_name,
                "source_url": page.url,
            },
        )

        try:
            self._refresh_page(page)

            health = self.session_manager.check_health(page)
            if health.status != SESSION_STATUS_HEALTHY:
                return FeedExtractionResult(
                    source_page=self.settings.feed_source_page_name,
                    source_url=health.url or page.url,
                    extracted_at_utc=extracted_at_utc,
                    row_count=0,
                    events=[],
                    session_healthy=False,
                    login_required=(health.status == SESSION_STATUS_LOGIN_REQUIRED),
                    error_message=health.reason,
                )

            rows = self._get_row_locators(page)
            if not rows:
                self.audit_logger.warning(
                    event_type=EVENT_FEED_SELECTOR_MISS,
                    component=COMPONENT_FEED_EXTRACTOR,
                    message="No feed rows matched the configured selector",
                    data={
                        "selector": self.settings.feed_row_selector,
                        "source_page": self.settings.feed_source_page_name,
                        "source_url": page.url,
                    },
                )
                self.audit_logger.info(
                    event_type=EVENT_FEED_EXTRACT_EMPTY,
                    component=COMPONENT_FEED_EXTRACTOR,
                    message="Feed extraction completed with zero visible rows",
                    data={
                        "row_count": 0,
                        "source_page": self.settings.feed_source_page_name,
                        "source_url": page.url,
                    },
                )
                self._set_runtime_state(STATE_FEED_LAST_ROW_COUNT, "0")
                self._set_runtime_state(STATE_FEED_LAST_SOURCE_URL, page.url)
                self._set_runtime_state(
                    STATE_FEED_LAST_SUCCESSFUL_EXTRACT_UTC,
                    extracted_at_utc,
                )
                return FeedExtractionResult(
                    source_page=self.settings.feed_source_page_name,
                    source_url=page.url,
                    extracted_at_utc=extracted_at_utc,
                    row_count=0,
                    events=[],
                    session_healthy=True,
                    login_required=False,
                    error_message=None,
                )

            events: list[RawFeedEvent] = []
            for row_index, row in enumerate(rows):
                try:
                    parsed = self._parse_row(row=row, row_index=row_index)
                    raw_event = self._build_raw_event(
                        parsed=parsed,
                        source_url=page.url,
                        extracted_at_utc=extracted_at_utc,
                    )

                    if self.settings.verbose_row_logging:
                        self.audit_logger.info(
                            event_type=EVENT_FEED_ROW_PARSED,
                            component=COMPONENT_FEED_EXTRACTOR,
                            message="Parsed feed row",
                            data={
                                "row_index": row_index,
                                "market_text": raw_event.market_text,
                                "event_fingerprint": raw_event.event_fingerprint,
                            },
                        )
                        self.audit_logger.info(
                            event_type=EVENT_RAW_EVENT_CREATED,
                            component=COMPONENT_FEED_EXTRACTOR,
                            message="Created raw feed event",
                            data={
                                "row_index": row_index,
                                "market_text": raw_event.market_text,
                                "side_text": raw_event.side_text,
                                "event_fingerprint": raw_event.event_fingerprint,
                            },
                        )

                    try:
                        inserted_id = self.state_store.insert_raw_event(raw_event)
                        self.audit_logger.info(
                            event_type=EVENT_DB_WRITE_RAW_EVENT,
                            component=COMPONENT_FEED_EXTRACTOR,
                            message="Stored raw event in SQLite",
                            data={
                                "db_row_id": inserted_id,
                                "event_fingerprint": raw_event.event_fingerprint,
                                "market_text": raw_event.market_text,
                                "row_index": raw_event.row_index,
                            },
                        )
                        self.audit_logger.info(
                            event_type=EVENT_RAW_EVENT_PERSISTED,
                            component=COMPONENT_FEED_EXTRACTOR,
                            message="Persisted raw feed event",
                            data={
                                "db_row_id": inserted_id,
                                "event_fingerprint": raw_event.event_fingerprint,
                                "market_text": raw_event.market_text,
                                "row_index": raw_event.row_index,
                            },
                        )
                    except Exception as exc:
                        self.audit_logger.exception(
                            event_type=EVENT_DB_ERROR,
                            component=COMPONENT_FEED_EXTRACTOR,
                            message="Database write failed for raw event",
                            error=exc,
                            data={
                                "event_fingerprint": raw_event.event_fingerprint,
                                "market_text": raw_event.market_text,
                                "row_index": raw_event.row_index,
                            },
                        )
                        self.audit_logger.exception(
                            event_type=EVENT_RAW_EVENT_PERSISTENCE_ERROR,
                            component=COMPONENT_FEED_EXTRACTOR,
                            message="Failed to persist raw feed event",
                            error=exc,
                            data={
                                "event_fingerprint": raw_event.event_fingerprint,
                                "market_text": raw_event.market_text,
                                "row_index": raw_event.row_index,
                            },
                        )
                        continue

                    events.append(raw_event)

                except Exception as exc:
                    self.audit_logger.exception(
                        event_type=EVENT_FEED_EXTRACT_ERROR,
                        component=COMPONENT_FEED_EXTRACTOR,
                        message="Failed to parse one feed row",
                        error=exc,
                        data={"row_index": row_index, "source_url": page.url},
                    )
                    continue

            row_count = len(events)
            self._set_runtime_state(STATE_FEED_LAST_ROW_COUNT, str(row_count))
            self._set_runtime_state(STATE_FEED_LAST_SOURCE_URL, page.url)
            self._set_runtime_state(
                STATE_FEED_LAST_SUCCESSFUL_EXTRACT_UTC,
                extracted_at_utc,
            )

            if row_count == 0:
                self.audit_logger.info(
                    event_type=EVENT_FEED_EXTRACT_EMPTY,
                    component=COMPONENT_FEED_EXTRACTOR,
                    message="Feed extraction completed with zero persisted rows",
                    data={
                        "row_count": 0,
                        "source_page": self.settings.feed_source_page_name,
                        "source_url": page.url,
                    },
                )
            else:
                self.audit_logger.info(
                    event_type=EVENT_FEED_EXTRACT_SUCCESS,
                    component=COMPONENT_FEED_EXTRACTOR,
                    message="Extracted visible feed rows successfully",
                    data={
                        "row_count": row_count,
                        "source_page": self.settings.feed_source_page_name,
                        "source_url": page.url,
                    },
                )

            return FeedExtractionResult(
                source_page=self.settings.feed_source_page_name,
                source_url=page.url,
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
                message="Feed extraction cycle failed",
                error=exc,
                data={
                    "source_page": self.settings.feed_source_page_name,
                    "source_url": getattr(page, "url", self.settings.polywhaler_feed_url),
                },
            )
            return FeedExtractionResult(
                source_page=self.settings.feed_source_page_name,
                source_url=getattr(page, "url", self.settings.polywhaler_feed_url),
                extracted_at_utc=extracted_at_utc,
                row_count=0,
                events=[],
                session_healthy=False,
                login_required=False,
                error_message=str(exc),
            )

    def _ensure_feed_page_for_cycle(self) -> Page:
        """
        Reuses the current page if it already exists and looks like a Polywhaler page.
        Navigates only if needed.

        This prevents unnecessary navigate-then-reload behavior on every cycle.
        """
        try:
            page = self.session_manager.page
        except RuntimeError:
            return self.session_manager.open_feed_page()

        current_url = getattr(page, "url", "") or ""
        target_host = urlparse(self.settings.polywhaler_feed_url).netloc

        if not current_url or current_url == "about:blank":
            return self.session_manager.open_feed_page()

        if target_host and target_host not in current_url:
            return self.session_manager.open_feed_page()

        return page

    def _refresh_page(self, page: Page) -> None:
        """
        Refreshes the current page and waits for DOM readiness.
        """
        try:
            page.reload(wait_until="domcontentloaded")
        except TimeoutError:
            page.wait_for_load_state("domcontentloaded", timeout=5_000)

        self.audit_logger.info(
            event_type=EVENT_FEED_REFRESH_COMPLETED,
            component=COMPONENT_FEED_EXTRACTOR,
            message="Polywhaler feed page refreshed",
            data={
                "source_page": self.settings.feed_source_page_name,
                "source_url": page.url,
            },
        )

    def _get_row_locators(self, page: Page) -> list[Locator]:
        """
        Returns a list of row locators matching the configured feed row selector.

        If selector lookup fails unexpectedly, returns an empty list so the
        caller falls into selector-miss / empty-extraction handling instead of
        crashing harder than necessary.
        """
        selector = self.settings.feed_row_selector
        try:
            locator = page.locator(selector)
            count = locator.count()
            return [locator.nth(i) for i in range(count)]
        except Exception as exc:
            self.audit_logger.exception(
                event_type=EVENT_FEED_SELECTOR_MISS,
                component=COMPONENT_FEED_EXTRACTOR,
                message="Feed row selector lookup failed unexpectedly",
                error=exc,
                data={
                    "selector": selector,
                    "source_page": self.settings.feed_source_page_name,
                    "source_url": getattr(page, "url", self.settings.polywhaler_feed_url),
                },
            )
            return []

    def _parse_row(self, *, row: Locator, row_index: int) -> ParsedRow:
        """
        Parses one visible DOM row into a lightweight ParsedRow using deterministic
        text-based heuristics.

        This is intentionally raw for milestone 1 and should not perform any
        lifecycle interpretation.
        """
        row_text = self._safe_inner_text(row).strip()
        row_html = self._safe_inner_html(row)

        lines = [line.strip() for line in row_text.splitlines() if line.strip()]
        if not lines:
            raise ValueError("row contained no visible text")

        links = self._safe_extract_links(row)

        market_text = self._extract_market_text(lines)
        side_text = self._extract_first_match(lines, self.SIDE_PATTERN)
        insider_label_text = self._extract_insider_label(lines)
        trade_amount_text = self._extract_first_match(lines, self.MONEY_PATTERN)
        probability_text = self._extract_first_match(lines, self.PROBABILITY_PATTERN)
        impact_text = self._extract_impact_text(lines)
        insider_address_text = self._extract_address(lines, links)
        insider_display_name = self._extract_display_name(
            lines=lines,
            market_text=market_text,
            side_text=side_text,
            insider_label_text=insider_label_text,
            trade_amount_text=trade_amount_text,
            probability_text=probability_text,
            impact_text=impact_text,
            insider_address_text=insider_address_text,
        )

        return ParsedRow(
            market_text=market_text,
            side_text=side_text,
            insider_label_text=insider_label_text,
            insider_address_text=insider_address_text,
            insider_display_name=insider_display_name,
            trade_amount_text=trade_amount_text,
            probability_text=probability_text,
            impact_text=impact_text,
            row_index=row_index,
            row_html=row_html,
        )

    def _build_raw_event(
        self,
        *,
        parsed: ParsedRow,
        source_url: str,
        extracted_at_utc: str,
    ) -> RawFeedEvent:
        """
        Converts a ParsedRow into a persisted RawFeedEvent with a stable fingerprint.
        """
        fingerprint = self._compute_fingerprint(
            source_page=self.settings.feed_source_page_name,
            source_url=source_url,
            parsed=parsed,
        )

        return RawFeedEvent(
            event_fingerprint=fingerprint,
            source_page=self.settings.feed_source_page_name,
            source_url=source_url,
            extracted_at_utc=extracted_at_utc,
            feed_seen_at_utc=None,
            market_text=parsed.market_text,
            side_text=parsed.side_text,
            insider_label_text=parsed.insider_label_text,
            insider_address_text=parsed.insider_address_text,
            insider_display_name=parsed.insider_display_name,
            trade_amount_text=parsed.trade_amount_text,
            probability_text=parsed.probability_text,
            impact_text=parsed.impact_text,
            row_index=parsed.row_index,
            row_html=parsed.row_html,
        )

    def _compute_fingerprint(
        self,
        *,
        source_page: str,
        source_url: str,
        parsed: ParsedRow,
    ) -> str:
        parts = [
            source_page,
            source_url,
            parsed.market_text or "",
            parsed.side_text or "",
            parsed.insider_label_text or "",
            parsed.insider_address_text or "",
            parsed.insider_display_name or "",
            parsed.trade_amount_text or "",
            parsed.probability_text or "",
            parsed.impact_text or "",
            str(parsed.row_index if parsed.row_index is not None else ""),
        ]
        joined = "||".join(parts)
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    def _extract_market_text(self, lines: list[str]) -> str:
        """
        Picks the most likely market line using simple deterministic rules:
        1. first line containing a question mark
        2. otherwise first line that does not look like side/probability/impact/amount
        3. fallback to the first non-empty line
        """
        for line in lines:
            if "?" in line:
                return line

        for line in lines:
            if self.SIDE_PATTERN.fullmatch(line):
                continue
            if self.PROBABILITY_PATTERN.search(line):
                continue
            if self.MONEY_PATTERN.search(line):
                continue
            if self.IMPACT_PATTERN.search(line):
                continue
            if self.INSIDER_LABEL_LINE_PATTERN.search(line):
                continue
            if self.ADDRESS_PATTERN.search(line):
                continue
            return line

        return lines[0]

    def _extract_insider_label(self, lines: list[str]) -> str | None:
        """
        Tries to preserve a useful visible insider/risk label line rather than
        only returning a single matched word.
        """
        for line in lines:
            if self.INSIDER_LABEL_LINE_PATTERN.search(line):
                return line
        return None

    def _extract_impact_text(self, lines: list[str]) -> str | None:
        """
        Preserves a useful visible impact label/value such as:
        - High Impact
        - Medium Impact
        - High Impact: 14%
        """
        for line in lines:
            if self.IMPACT_PATTERN.search(line):
                return line
        return None

    def _extract_display_name(
        self,
        *,
        lines: list[str],
        market_text: str,
        side_text: str | None,
        insider_label_text: str | None,
        trade_amount_text: str | None,
        probability_text: str | None,
        impact_text: str | None,
        insider_address_text: str | None,
    ) -> str | None:
        ignored = {
            market_text,
            side_text,
            insider_label_text,
            trade_amount_text,
            probability_text,
            impact_text,
            insider_address_text,
        }

        for line in lines:
            if not line or line in ignored:
                continue
            if self.PROBABILITY_PATTERN.search(line):
                continue
            if self.MONEY_PATTERN.search(line):
                continue
            if self.IMPACT_PATTERN.search(line):
                continue
            if self.ADDRESS_PATTERN.search(line):
                continue
            if self.SIDE_PATTERN.fullmatch(line):
                continue
            if self.INSIDER_LABEL_LINE_PATTERN.search(line):
                continue
            return line

        return None

    def _extract_address(self, lines: list[str], links: list[str]) -> str | None:
        for line in lines:
            match = self.ADDRESS_PATTERN.search(line)
            if match:
                return match.group(0)

        for href in links:
            match = self.ADDRESS_PATTERN.search(href)
            if match:
                return match.group(0)

        return None

    def _extract_first_match(
        self,
        lines: list[str],
        pattern: re.Pattern[str],
    ) -> str | None:
        for line in lines:
            match = pattern.search(line)
            if match:
                return match.group(0)
        return None

    def _safe_inner_text(self, row: Locator) -> str:
        try:
            value = row.inner_text(timeout=self.settings.browser_timeout_ms)
            return value or ""
        except Error:
            return ""

    def _safe_inner_html(self, row: Locator) -> str | None:
        try:
            return row.inner_html(timeout=self.settings.browser_timeout_ms)
        except Error:
            return None

    def _safe_extract_links(self, row: Locator) -> list[str]:
        try:
            return row.locator("a").evaluate_all(
                """elements => elements
                    .map(el => el.getAttribute('href'))
                    .filter(Boolean)
                """
            )
        except Error:
            return []

    def _set_runtime_state(self, key: str, value: str) -> None:
        self.state_store.set_runtime_state(
            RuntimeStateRecord(
                state_key=key,
                state_value=value,
            )
        )
