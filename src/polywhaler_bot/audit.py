from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from polywhaler_bot.models import AuditLogEntry, utc_now_iso


class AuditLogger:
    """
    Append-only JSONL audit logger for milestone 1.

    Behavior:
    - writes one JSON object per line
    - rotates by UTC calendar day
    - keeps the envelope structure defined by AuditLogEntry
    """

    def __init__(self, logs_dir: Path, run_id: str) -> None:
        self.logs_dir = Path(logs_dir).expanduser().resolve()
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.run_id = run_id

    def _current_log_path(self) -> Path:
        """
        Returns the current UTC daily log file path.
        Example: data/logs/2026-04-21.jsonl
        """
        day = utc_now_iso()[:10]
        return self.logs_dir / f"{day}.jsonl"

    def log(
        self,
        *,
        level: str,
        event_type: str,
        component: str,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        """
        Writes one structured JSONL log line.
        """
        entry = AuditLogEntry(
            level=level,
            event_type=event_type,
            run_id=self.run_id,
            component=component,
            message=message,
            data=data or {},
        )

        log_path = self._current_log_path()
        with log_path.open("a", encoding="utf-8") as f:
            f.write(entry.model_dump_json())
            f.write("\n")

    def debug(
        self,
        *,
        event_type: str,
        component: str,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        self.log(
            level="DEBUG",
            event_type=event_type,
            component=component,
            message=message,
            data=data,
        )

    def info(
        self,
        *,
        event_type: str,
        component: str,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        self.log(
            level="INFO",
            event_type=event_type,
            component=component,
            message=message,
            data=data,
        )

    def warning(
        self,
        *,
        event_type: str,
        component: str,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        self.log(
            level="WARNING",
            event_type=event_type,
            component=component,
            message=message,
            data=data,
        )

    def error(
        self,
        *,
        event_type: str,
        component: str,
        message: str,
        data: dict[str, Any] | None = None,
    ) -> None:
        self.log(
            level="ERROR",
            event_type=event_type,
            component=component,
            message=message,
            data=data,
        )

    def exception(
        self,
        *,
        event_type: str,
        component: str,
        message: str,
        error: Exception,
        data: dict[str, Any] | None = None,
    ) -> None:
        payload = dict(data or {})
        payload.update(
            {
                "error_type": type(error).__name__,
                "error": str(error),
            }
        )
        self.log(
            level="ERROR",
            event_type=event_type,
            component=component,
            message=message,
            data=payload,
        )
