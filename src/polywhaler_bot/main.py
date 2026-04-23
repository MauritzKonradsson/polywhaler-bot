from __future__ import annotations

import os
import signal
import sys
import time
from secrets import token_hex

from polywhaler_bot.audit import AuditLogger
from polywhaler_bot.config import get_settings
from polywhaler_bot.constants import (
    COMPONENT_MAIN,
    EVENT_DAEMON_ERROR,
    EVENT_DAEMON_START,
    EVENT_DAEMON_STOP,
    EVENT_DB_INIT_COMPLETED,
    EVENT_DB_INIT_STARTED,
    STATE_DAEMON_LAST_START_UTC,
    STATE_DAEMON_LAST_STOP_UTC,
)
from polywhaler_bot.db import StateStore
from polywhaler_bot.feed import PolywhalerFeedExtractor
from polywhaler_bot.models import RuntimeStateRecord, utc_now_iso
from polywhaler_bot.session import PolywhalerSessionManager


def build_run_id() -> str:
    ts = utc_now_iso().replace(":", "").replace("-", "").replace(".", "")
    return f"{ts}-{token_hex(3)}"


def main() -> int:
    settings = get_settings()
    run_id = build_run_id()

    state_store = StateStore(settings.database_path)
    audit_logger = AuditLogger(settings.logs_dir, run_id)

    audit_logger.info(
        event_type=EVENT_DAEMON_START,
        component=COMPONENT_MAIN,
        message="Daemon starting",
        data={
            "pid": os.getpid(),
            "python_executable": sys.executable,
            "environment": settings.environment,
            "database_path": str(settings.database_path),
            "playwright_profile_dir": str(settings.playwright_profile_dir),
            "feed_url": settings.polywhaler_feed_url,
            "refresh_interval_seconds": settings.feed_refresh_interval_seconds,
        },
    )

    state_store.set_runtime_state(
        RuntimeStateRecord(
            state_key=STATE_DAEMON_LAST_START_UTC,
            state_value=utc_now_iso(),
        )
    )

    audit_logger.info(
        event_type=EVENT_DB_INIT_STARTED,
        component=COMPONENT_MAIN,
        message="Initializing SQLite state store",
        data={"database_path": str(settings.database_path)},
    )
    state_store.initialize()
    audit_logger.info(
        event_type=EVENT_DB_INIT_COMPLETED,
        component=COMPONENT_MAIN,
        message="SQLite state store initialized",
        data={
            "database_path": str(settings.database_path),
            "schema_version": state_store.get_schema_version(),
        },
    )

    session_manager = PolywhalerSessionManager(
        settings=settings,
        state_store=state_store,
        audit_logger=audit_logger,
    )
    feed_extractor = PolywhalerFeedExtractor(
        settings=settings,
        state_store=state_store,
        audit_logger=audit_logger,
        session_manager=session_manager,
    )

    stop_requested = False

    def _request_stop(signum: int, _frame: object) -> None:
        nonlocal stop_requested
        stop_requested = True
        audit_logger.info(
            event_type=EVENT_DAEMON_STOP,
            component=COMPONENT_MAIN,
            message="Stop requested by signal",
            data={"signal": signum},
        )

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    try:
        session_manager.start()
        session_manager.open_feed_page()

        while not stop_requested:
            result = feed_extractor.extract_once()

            if result.error_message:
                # We log the cycle-level error here once more at daemon level so
                # it is visible without scanning every component log line.
                audit_logger.error(
                    event_type=EVENT_DAEMON_ERROR,
                    component=COMPONENT_MAIN,
                    message="Extraction cycle returned an error state",
                    data={
                        "source_page": result.source_page,
                        "source_url": result.source_url,
                        "session_healthy": result.session_healthy,
                        "login_required": result.login_required,
                        "error_message": result.error_message,
                    },
                )

            # Sleep in small increments so stop signals can be honored promptly.
            remaining = settings.feed_refresh_interval_seconds
            while remaining > 0 and not stop_requested:
                time.sleep(1)
                remaining -= 1

    except KeyboardInterrupt:
        stop_requested = True

    except Exception as exc:
        audit_logger.exception(
            event_type=EVENT_DAEMON_ERROR,
            component=COMPONENT_MAIN,
            message="Fatal daemon error",
            error=exc,
            data={},
        )
        return 1

    finally:
        state_store.set_runtime_state(
            RuntimeStateRecord(
                state_key=STATE_DAEMON_LAST_STOP_UTC,
                state_value=utc_now_iso(),
            )
        )
        session_manager.stop()
        audit_logger.info(
            event_type=EVENT_DAEMON_STOP,
            component=COMPONENT_MAIN,
            message="Daemon stopped",
            data={"run_id": run_id},
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
