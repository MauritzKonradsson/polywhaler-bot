from __future__ import annotations

# =========================
# Schema / database constants
# =========================

SCHEMA_VERSION = 1

TABLE_RAW_EVENTS = "raw_events"
TABLE_RUNTIME_STATE = "runtime_state"
TABLE_SCHEMA_META = "schema_meta"


# =========================
# Runtime state keys
# =========================

STATE_DAEMON_LAST_START_UTC = "daemon.last_start_utc"
STATE_DAEMON_LAST_STOP_UTC = "daemon.last_stop_utc"

STATE_FEED_LAST_EXTRACT_ATTEMPT_UTC = "feed.last_extract_attempt_utc"
STATE_FEED_LAST_SUCCESSFUL_EXTRACT_UTC = "feed.last_successful_extract_utc"
STATE_FEED_LAST_ROW_COUNT = "feed.last_row_count"
STATE_FEED_LAST_SOURCE_URL = "feed.last_source_url"

STATE_SESSION_STATUS = "session.status"
STATE_SESSION_LAST_OK_UTC = "session.last_ok_utc"
STATE_SESSION_LAST_LOGIN_REQUIRED_UTC = "session.last_login_required_utc"
STATE_SESSION_LAST_FAILURE_REASON = "session.last_failure_reason"
STATE_SESSION_LAST_URL = "session.last_url"


# =========================
# Session status labels
# =========================

SESSION_STATUS_UNKNOWN = "unknown"
SESSION_STATUS_LAUNCHING = "launching"
SESSION_STATUS_HEALTHY = "healthy"
SESSION_STATUS_LOGIN_REQUIRED = "login_required"
SESSION_STATUS_ERROR = "error"


# =========================
# Log levels
# =========================

LOG_LEVEL_DEBUG = "DEBUG"
LOG_LEVEL_INFO = "INFO"
LOG_LEVEL_WARNING = "WARNING"
LOG_LEVEL_ERROR = "ERROR"


# =========================
# Audit log event types
# =========================

# Daemon/runtime lifecycle
EVENT_DAEMON_START = "daemon.start"
EVENT_DAEMON_STOP = "daemon.stop"
EVENT_DAEMON_ERROR = "daemon.error"

# Database/state
EVENT_DB_INIT_STARTED = "db.init_started"
EVENT_DB_INIT_COMPLETED = "db.init_completed"
EVENT_DB_WRITE_RAW_EVENT = "db.write_raw_event"
EVENT_DB_WRITE_RUNTIME_STATE = "db.write_runtime_state"
EVENT_DB_ERROR = "db.error"

# Browser/session
EVENT_SESSION_LAUNCH_STARTED = "session.launch_started"
EVENT_SESSION_LAUNCH_COMPLETED = "session.launch_completed"
EVENT_SESSION_PAGE_OPENED = "session.page_opened"
EVENT_SESSION_HEALTHY = "session.healthy"
EVENT_SESSION_LOGIN_REQUIRED = "session.login_required"
EVENT_SESSION_ERROR = "session.error"

# Feed extraction
EVENT_FEED_EXTRACT_CYCLE_STARTED = "feed.extract_cycle_started"
EVENT_FEED_REFRESH_COMPLETED = "feed.refresh_completed"
EVENT_FEED_EXTRACT_SUCCESS = "feed.extract_success"
EVENT_FEED_EXTRACT_EMPTY = "feed.extract_empty"
EVENT_FEED_ROW_PARSED = "feed.row_parsed"
EVENT_FEED_EXTRACT_ERROR = "feed.extract_error"
EVENT_FEED_SELECTOR_MISS = "feed.selector_miss"

# Raw event lifecycle
EVENT_RAW_EVENT_CREATED = "raw_event.created"
EVENT_RAW_EVENT_PERSISTED = "raw_event.persisted"
EVENT_RAW_EVENT_PERSISTENCE_ERROR = "raw_event.persistence_error"


# =========================
# Component names
# =========================

COMPONENT_MAIN = "main"
COMPONENT_STATE_STORE = "state_store"
COMPONENT_AUDIT_LOGGER = "audit_logger"
COMPONENT_SESSION_MANAGER = "polywhaler_session_manager"
COMPONENT_FEED_EXTRACTOR = "polywhaler_feed_extractor"


# =========================
# Source page names
# =========================

SOURCE_PAGE_DEEP_TRADES_FEED = "deep_trades_feed"
