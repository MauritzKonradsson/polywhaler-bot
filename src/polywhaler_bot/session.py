from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    TimeoutError,
    sync_playwright,
)

from polywhaler_bot.audit import AuditLogger
from polywhaler_bot.config import Settings
from polywhaler_bot.constants import (
    COMPONENT_SESSION_MANAGER,
    EVENT_SESSION_ERROR,
    EVENT_SESSION_HEALTHY,
    EVENT_SESSION_LAUNCH_COMPLETED,
    EVENT_SESSION_LAUNCH_STARTED,
    EVENT_SESSION_LOGIN_REQUIRED,
    EVENT_SESSION_PAGE_OPENED,
    SESSION_STATUS_ERROR,
    SESSION_STATUS_HEALTHY,
    SESSION_STATUS_LAUNCHING,
    SESSION_STATUS_LOGIN_REQUIRED,
    SESSION_STATUS_UNKNOWN,
    STATE_SESSION_LAST_FAILURE_REASON,
    STATE_SESSION_LAST_LOGIN_REQUIRED_UTC,
    STATE_SESSION_LAST_OK_UTC,
    STATE_SESSION_LAST_URL,
    STATE_SESSION_STATUS,
)
from polywhaler_bot.db import StateStore
from polywhaler_bot.models import RuntimeStateRecord, SessionHealth, utc_now_iso


def _env_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


class PolywhalerSessionManager:
    """
    Session manager supporting two modes:

    1. launch mode:
       - launches a persistent Chromium profile via Playwright
    2. CDP mode:
       - attaches to an already-running Chromium-based browser via CDP

    In CDP mode, the external browser is the real session/auth holder.
    """

    def __init__(
        self,
        *,
        settings: Settings,
        state_store: StateStore,
        audit_logger: AuditLogger,
    ) -> None:
        self.settings = settings
        self.state_store = state_store
        self.audit_logger = audit_logger

        # Minimal reversible CDP support without forcing config.py changes.
        self.use_cdp_browser = _env_bool(
            os.getenv("USE_CDP_BROWSER"),
            default=bool(getattr(settings, "use_cdp_browser", False)),
        )
        self.cdp_endpoint = os.getenv(
            "CDP_ENDPOINT",
            str(getattr(settings, "cdp_endpoint", "http://127.0.0.1:9222")),
        )

        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None

    @property
    def context(self) -> BrowserContext:
        if self._context is None:
            raise RuntimeError("Browser context is not started")
        return self._context

    @property
    def page(self) -> Page:
        if self._page is None:
            raise RuntimeError("Session page is not initialized")
        return self._page

    def start(self) -> None:
        """
        Starts the session manager in either:
        - launch mode
        - CDP attach mode
        """
        if self._context is not None:
            return

        self.audit_logger.info(
            event_type=EVENT_SESSION_LAUNCH_STARTED,
            component=COMPONENT_SESSION_MANAGER,
            message="Starting Polywhaler session manager",
            data={
                "mode": "cdp" if self.use_cdp_browser else "launch",
                "profile_dir": str(self.settings.playwright_profile_dir),
                "headless": self.settings.browser_headless,
                "cdp_endpoint": self.cdp_endpoint if self.use_cdp_browser else None,
            },
        )

        self._set_session_status(
            status=SESSION_STATUS_LAUNCHING,
            reason=None,
            current_url=None,
        )

        self._playwright = sync_playwright().start()

        try:
            if self.use_cdp_browser:
                self._start_cdp_mode()
            else:
                self._start_launch_mode()

            self.audit_logger.info(
                event_type=EVENT_SESSION_LAUNCH_COMPLETED,
                component=COMPONENT_SESSION_MANAGER,
                message="Polywhaler session manager started",
                data={
                    "mode": "cdp" if self.use_cdp_browser else "launch",
                    "profile_dir": str(self.settings.playwright_profile_dir),
                    "headless": self.settings.browser_headless,
                    "cdp_endpoint": self.cdp_endpoint if self.use_cdp_browser else None,
                    "current_url": getattr(self._page, "url", None),
                },
            )
        except Exception as exc:
            self._set_session_status(
                status=SESSION_STATUS_ERROR,
                reason=str(exc),
                current_url=None,
            )
            self.audit_logger.exception(
                event_type=EVENT_SESSION_ERROR,
                component=COMPONENT_SESSION_MANAGER,
                message="Failed to start Polywhaler session manager",
                error=exc,
                data={
                    "mode": "cdp" if self.use_cdp_browser else "launch",
                    "profile_dir": str(self.settings.playwright_profile_dir),
                    "cdp_endpoint": self.cdp_endpoint if self.use_cdp_browser else None,
                },
            )
            self.stop()
            raise

    def _start_launch_mode(self) -> None:
        if self._playwright is None:
            raise RuntimeError("Playwright is not started")

        self._context = self._playwright.chromium.launch_persistent_context(
            user_data_dir=str(self.settings.playwright_profile_dir),
            headless=self.settings.browser_headless,
            slow_mo=self.settings.browser_slow_mo_ms,
            viewport={"width": 1440, "height": 1000},
        )
        self._context.set_default_timeout(self.settings.browser_timeout_ms)
        self._context.set_default_navigation_timeout(self.settings.browser_timeout_ms)
        self._page = self._ensure_page_launch_mode()

    def _start_cdp_mode(self) -> None:
        if self._playwright is None:
            raise RuntimeError("Playwright is not started")

        self._browser = self._playwright.chromium.connect_over_cdp(
            self.cdp_endpoint,
            timeout=self.settings.browser_timeout_ms,
        )

        if not self._browser.contexts:
            raise RuntimeError(
                "CDP browser exposes no contexts. Open the external browser normally "
                "with remote debugging enabled and at least one window."
            )

        self._context = self._browser.contexts[0]
        self._context.set_default_timeout(self.settings.browser_timeout_ms)
        self._context.set_default_navigation_timeout(self.settings.browser_timeout_ms)
        self._page = self._select_or_create_page_cdp()

    def stop(self) -> None:
        """
        Cleans up resources.

        Launch mode:
        - closes the launched Playwright browser context

        CDP mode:
        - does NOT close the external browser
        - only tears down the Playwright client connection
        """
        if self.use_cdp_browser:
            self._page = None
            self._context = None
            self._browser = None
            try:
                if self._playwright is not None:
                    self._playwright.stop()
            except Exception:
                pass
            finally:
                self._playwright = None
            return

        try:
            if self._page is not None:
                self._page.close()
        except Exception:
            pass
        finally:
            self._page = None

        try:
            if self._context is not None:
                self._context.close()
        except Exception:
            pass
        finally:
            self._context = None

        try:
            if self._playwright is not None:
                self._playwright.stop()
        except Exception:
            pass
        finally:
            self._playwright = None

    def open_feed_page(self) -> Page:
        """
        Ensures a usable page exists and navigates to the configured feed URL
        only if needed.

        In CDP mode:
        - reuse an existing /deep page if present
        - otherwise create a new page in the attached browser context
        - avoid hijacking unrelated tabs if possible
        """
        if self._context is None:
            raise RuntimeError("Session manager has not been started")

        page = self._ensure_page()

        try:
            if not self._is_target_feed_page(page):
                page.goto(self.settings.polywhaler_feed_url, wait_until="domcontentloaded")

            self.audit_logger.info(
                event_type=EVENT_SESSION_PAGE_OPENED,
                component=COMPONENT_SESSION_MANAGER,
                message="Prepared Polywhaler feed page",
                data={"url": page.url, "mode": "cdp" if self.use_cdp_browser else "launch"},
            )
            self._set_session_last_url(page.url)
            return page
        except Exception as exc:
            self._set_session_status(
                status=SESSION_STATUS_ERROR,
                reason=f"navigation_failed: {exc}",
                current_url=getattr(page, "url", None),
            )
            self.audit_logger.exception(
                event_type=EVENT_SESSION_ERROR,
                component=COMPONENT_SESSION_MANAGER,
                message="Failed to prepare Polywhaler feed page",
                error=exc,
                data={
                    "target_url": self.settings.polywhaler_feed_url,
                    "mode": "cdp" if self.use_cdp_browser else "launch",
                },
            )
            raise

    def check_health(self, page: Page | None = None) -> SessionHealth:
        """
        Determines whether the current Polywhaler session appears healthy.
        """
        if self._context is None:
            health = SessionHealth(
                status=SESSION_STATUS_UNKNOWN,
                url=None,
                reason="browser_not_started",
            )
            self._set_session_status(
                status=health.status,
                reason=health.reason,
                current_url=health.url,
            )
            return health

        current_page = page or self._ensure_page()

        try:
            current_page.wait_for_load_state("domcontentloaded", timeout=5_000)
            current_url = current_page.url
            self._set_session_last_url(current_url)

            if self._is_login_required(current_page):
                health = SessionHealth(
                    status=SESSION_STATUS_LOGIN_REQUIRED,
                    url=current_url,
                    reason="login_prompt_detected",
                )
                self._set_session_status(
                    status=health.status,
                    reason=health.reason,
                    current_url=current_url,
                )
                self.audit_logger.warning(
                    event_type=EVENT_SESSION_LOGIN_REQUIRED,
                    component=COMPONENT_SESSION_MANAGER,
                    message="Polywhaler login required",
                    data={
                        "reason": health.reason,
                        "url": current_url,
                        "mode": "cdp" if self.use_cdp_browser else "launch",
                    },
                )
                return health

            health = SessionHealth(
                status=SESSION_STATUS_HEALTHY,
                url=current_url,
                reason=None,
            )
            self._set_session_status(
                status=health.status,
                reason=None,
                current_url=current_url,
            )
            self.audit_logger.info(
                event_type=EVENT_SESSION_HEALTHY,
                component=COMPONENT_SESSION_MANAGER,
                message="Polywhaler session appears healthy",
                data={
                    "url": current_url,
                    "mode": "cdp" if self.use_cdp_browser else "launch",
                },
            )
            return health

        except TimeoutError as exc:
            current_url = getattr(current_page, "url", None)
            health = SessionHealth(
                status=SESSION_STATUS_ERROR,
                url=current_url,
                reason=f"load_timeout: {exc}",
            )
            self._set_session_status(
                status=health.status,
                reason=health.reason,
                current_url=current_url,
            )
            self.audit_logger.exception(
                event_type=EVENT_SESSION_ERROR,
                component=COMPONENT_SESSION_MANAGER,
                message="Session health check timed out",
                error=exc,
                data={
                    "url": current_url,
                    "mode": "cdp" if self.use_cdp_browser else "launch",
                },
            )
            return health

        except Exception as exc:
            current_url = getattr(current_page, "url", None)
            health = SessionHealth(
                status=SESSION_STATUS_ERROR,
                url=current_url,
                reason=f"health_check_failed: {exc}",
            )
            self._set_session_status(
                status=health.status,
                reason=health.reason,
                current_url=current_url,
            )
            self.audit_logger.exception(
                event_type=EVENT_SESSION_ERROR,
                component=COMPONENT_SESSION_MANAGER,
                message="Session health check failed",
                error=exc,
                data={
                    "url": current_url,
                    "mode": "cdp" if self.use_cdp_browser else "launch",
                },
            )
            return health

    def _ensure_page(self) -> Page:
        if self.use_cdp_browser:
            return self._ensure_page_cdp()
        return self._ensure_page_launch_mode()

    def _ensure_page_launch_mode(self) -> Page:
        if self._context is None:
            raise RuntimeError("Browser context is not started")

        if self._page is not None and not self._page.is_closed():
            return self._page

        existing_pages = [p for p in self._context.pages if not p.is_closed()]
        if existing_pages:
            self._page = existing_pages[0]
            return self._page

        self._page = self._context.new_page()
        return self._page

    def _ensure_page_cdp(self) -> Page:
        if self._context is None:
            raise RuntimeError("CDP browser context is not started")

        if self._page is not None and not self._page.is_closed():
            return self._page

        self._page = self._select_or_create_page_cdp()
        return self._page

    def _select_or_create_page_cdp(self) -> Page:
        """
        CDP page selection strategy:
        1. Reuse an existing /deep target page if present
        2. Otherwise create a new page in the default context

        This minimizes interference with unrelated manual browsing.
        """
        if self._context is None:
            raise RuntimeError("CDP browser context is not started")

        for page in self._context.pages:
            if page.is_closed():
                continue
            if self._is_target_feed_page(page):
                return page

        return self._context.new_page()

    def _is_target_feed_page(self, page: Page) -> bool:
        try:
            current_url = page.url or ""
        except Exception:
            return False

        target = self.settings.polywhaler_feed_url.rstrip("/")
        current = current_url.rstrip("/")
        return current == target

    def _is_login_required(self, page: Page) -> bool:
        selector = self.settings.login_required_selector.strip()
        if not selector:
            return False

        try:
            locator = page.locator(selector).first
            return locator.is_visible(timeout=2_000)
        except Exception:
            return False

    def _set_session_status(
        self,
        *,
        status: str,
        reason: str | None,
        current_url: str | None,
    ) -> None:
        self.state_store.set_runtime_state(
            RuntimeStateRecord(
                state_key=STATE_SESSION_STATUS,
                state_value=status,
            )
        )

        if current_url:
            self._set_session_last_url(current_url)

        now_utc = utc_now_iso()

        if status == SESSION_STATUS_HEALTHY:
            self.state_store.set_runtime_state(
                RuntimeStateRecord(
                    state_key=STATE_SESSION_LAST_OK_UTC,
                    state_value=now_utc,
                )
            )

        if status == SESSION_STATUS_LOGIN_REQUIRED:
            self.state_store.set_runtime_state(
                RuntimeStateRecord(
                    state_key=STATE_SESSION_LAST_LOGIN_REQUIRED_UTC,
                    state_value=now_utc,
                )
            )

        if reason is not None:
            self.state_store.set_runtime_state(
                RuntimeStateRecord(
                    state_key=STATE_SESSION_LAST_FAILURE_REASON,
                    state_value=reason,
                )
            )

    def _set_session_last_url(self, current_url: str) -> None:
        self.state_store.set_runtime_state(
            RuntimeStateRecord(
                state_key=STATE_SESSION_LAST_URL,
                state_value=current_url,
            )
        )
