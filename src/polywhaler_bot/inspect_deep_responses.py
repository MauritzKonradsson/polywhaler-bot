from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from pathlib import Path
from secrets import token_hex
from typing import Any
from urllib.parse import urlparse

from playwright.sync_api import Response

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
from polywhaler_bot.models import RuntimeStateRecord, utc_now_iso
from polywhaler_bot.session import PolywhalerSessionManager


DEEP_URL = "https://www.polywhaler.com/deep"

STATIC_RESOURCE_TYPES = {
    "image",
    "media",
    "font",
    "stylesheet",
    "other",
}

STATIC_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".svg",
    ".ico",
    ".css",
    ".woff",
    ".woff2",
    ".ttf",
    ".otf",
    ".map",
}

JSON_CONTENT_HINTS = (
    "application/json",
    "text/json",
    "application/problem+json",
)

URL_HINTS = (
    "/api/",
    "graphql",
    "_next/data",
    "json",
)

CANDIDATE_RESOURCE_TYPES = {"xhr", "fetch"}


def build_run_id() -> str:
    ts = utc_now_iso().replace(":", "").replace("-", "").replace(".", "")
    return f"{ts}-{token_hex(3)}"


def safe_content_type(response: Response) -> str:
    try:
        headers = response.headers
        return headers.get("content-type", "")
    except Exception:
        return ""


def strip_query_and_fragment(url: str) -> str:
    parsed = urlparse(url)
    return parsed.path or url


def is_static_asset(url: str, resource_type: str, content_type: str) -> bool:
    if resource_type in STATIC_RESOURCE_TYPES:
        return True

    lowered_path = strip_query_and_fragment(url).lower()
    if any(lowered_path.endswith(ext) for ext in STATIC_EXTENSIONS):
        return True

    lowered_ct = content_type.lower()
    if lowered_ct.startswith("image/"):
        return True
    if "javascript" in lowered_ct:
        return True
    if "text/css" in lowered_ct:
        return True
    if "font/" in lowered_ct:
        return True

    return False


def looks_like_candidate(url: str, resource_type: str, content_type: str) -> bool:
    if is_static_asset(url, resource_type, content_type):
        return False

    lowered_url = url.lower()
    lowered_ct = content_type.lower()

    if resource_type in CANDIDATE_RESOURCE_TYPES:
        return True

    if any(hint in lowered_ct for hint in JSON_CONTENT_HINTS):
        return True

    if any(hint in lowered_url for hint in URL_HINTS):
        return True

    return False


def short_url(url: str, max_len: int = 120) -> str:
    if len(url) <= max_len:
        return url
    return url[: max_len - 3] + "..."


def build_payload_preview(value: Any, *, depth: int = 0, max_depth: int = 2) -> Any:
    """
    Small safe preview of the payload shape.
    Keeps the output useful without dumping full payloads.
    """
    if depth >= max_depth:
        return type(value).__name__

    if isinstance(value, dict):
        preview: dict[str, Any] = {}
        for key in list(value.keys())[:10]:
            preview[str(key)] = build_payload_preview(value[key], depth=depth + 1, max_depth=max_depth)
        return preview

    if isinstance(value, list):
        return {
            "__type__": "list",
            "count": len(value),
            "sample": [build_payload_preview(item, depth=depth + 1, max_depth=max_depth) for item in value[:2]],
        }

    if isinstance(value, (str, int, float, bool)) or value is None:
        if isinstance(value, str) and len(value) > 160:
            return value[:157] + "..."
        return value

    return type(value).__name__


def summarize_json_payload(payload: Any) -> tuple[str, list[str] | None, int | None, Any]:
    if isinstance(payload, dict):
        keys = list(payload.keys())[:30]
        preview = build_payload_preview(payload)
        return "dict", keys, None, preview

    if isinstance(payload, list):
        preview = build_payload_preview(payload)
        return "list", None, len(payload), preview

    preview = build_payload_preview(payload)
    return type(payload).__name__, None, None, preview


def ensure_debug_dir(base_data_dir: Path) -> Path:
    debug_dir = base_data_dir / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    return debug_dir


def write_jsonl_line(path: Path, payload: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False))
        f.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Temporary /deep response inspection tool for milestone 1."
    )
    parser.add_argument(
        "--url",
        default=DEEP_URL,
        help="Target Polywhaler page to inspect (default: /deep).",
    )
    parser.add_argument(
        "--observe-seconds",
        type=int,
        default=8,
        help="How long to continue observing responses after reload (default: 8).",
    )
    parser.add_argument(
        "--wait-for-manual-login",
        action="store_true",
        help="Pause after opening /deep so you can complete login manually before capture starts.",
    )
    args = parser.parse_args()

    settings = get_settings()
    run_id = build_run_id()

    state_store = StateStore(settings.database_path)
    audit_logger = AuditLogger(settings.logs_dir, run_id)

    debug_dir = ensure_debug_dir(settings.data_dir)
    jsonl_path = debug_dir / f"deep_response_candidates_{run_id}.jsonl"
    summary_path = debug_dir / f"deep_response_summary_{run_id}.json"

    audit_logger.info(
        event_type=EVENT_DAEMON_START,
        component=COMPONENT_MAIN,
        message="Deep response inspection tool starting",
        data={
            "pid": os.getpid(),
            "target_url": args.url,
            "observe_seconds": args.observe_seconds,
            "wait_for_manual_login": args.wait_for_manual_login,
            "jsonl_output": str(jsonl_path),
            "summary_output": str(summary_path),
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
        message="Initializing SQLite state store for inspection tool",
        data={"database_path": str(settings.database_path)},
    )
    state_store.initialize()
    audit_logger.info(
        event_type=EVENT_DB_INIT_COMPLETED,
        component=COMPONENT_MAIN,
        message="SQLite state store initialized for inspection tool",
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

    records: list[dict[str, Any]] = []
    summary_counts_by_url: Counter[str] = Counter()
    summary_counts_by_resource_type: Counter[str] = Counter()
    summary_counts_by_content_type: Counter[str] = Counter()
    summary_url_to_keys: dict[str, list[str]] = {}
    sequence = 0

    def handle_response(response: Response) -> None:
        nonlocal sequence

        try:
            request = response.request
            url = response.url
            status = response.status
            resource_type = request.resource_type
            content_type = safe_content_type(response)
            candidate = looks_like_candidate(url, resource_type, content_type)

            if not candidate:
                return

            sequence += 1

            record: dict[str, Any] = {
                "ts_utc": utc_now_iso(),
                "sequence": sequence,
                "url": url,
                "status": status,
                "method": request.method,
                "resource_type": resource_type,
                "content_type": content_type,
                "looks_like_candidate": candidate,
                "json_parse_success": False,
                "json_top_level_type": None,
                "json_top_level_keys": None,
                "json_list_item_count": None,
                "payload_preview": None,
            }

            try:
                payload = response.json()
                (
                    payload_type,
                    payload_keys,
                    payload_count,
                    payload_preview,
                ) = summarize_json_payload(payload)

                record["json_parse_success"] = True
                record["json_top_level_type"] = payload_type
                record["json_top_level_keys"] = payload_keys
                record["json_list_item_count"] = payload_count
                record["payload_preview"] = payload_preview

                if payload_keys is not None and url not in summary_url_to_keys:
                    summary_url_to_keys[url] = payload_keys

            except Exception as exc:
                record["json_parse_success"] = False
                record["json_error"] = f"{type(exc).__name__}: {exc}"

            records.append(record)
            write_jsonl_line(jsonl_path, record)

            summary_counts_by_url[url] += 1
            summary_counts_by_resource_type[resource_type] += 1
            summary_counts_by_content_type[content_type or "<missing>"] += 1

            summary_tail = ""
            if record["json_parse_success"]:
                if record["json_top_level_type"] == "dict":
                    summary_tail = f"keys={record['json_top_level_keys'][:6]}"
                elif record["json_top_level_type"] == "list":
                    summary_tail = f"items={record['json_list_item_count']}"
                else:
                    summary_tail = f"type={record['json_top_level_type']}"
            else:
                summary_tail = "json=NO"

            print(
                f"[{sequence:03d}] {status} {resource_type:<5} "
                f"{content_type or '<no-content-type>'} | {summary_tail} | "
                f"{short_url(url)}"
            )

        except Exception as exc:
            print(f"[response-handler-error] {type(exc).__name__}: {exc}")

    try:
        session_manager.start()
        page = session_manager.page
        page.on("response", handle_response)

        print(f"Opening: {args.url}")
        page.goto(args.url, wait_until="domcontentloaded")

        health = session_manager.check_health(page)
        print(f"Session status after goto: {health.status} | url={health.url}")

        if args.wait_for_manual_login:
            print("\n=== MANUAL LOGIN HOLD MODE ===")
            print("1. Complete Google login in the Playwright browser if needed.")
            print("2. Confirm the real /deep feed is visible (actual repeated feed/cards).")
            print("3. Optionally refresh once manually to verify the session persists.")
            print("4. Return to this terminal and press Enter to begin the capture phase.")
            input("\nPress Enter to continue with reload/capture... ")

            health = session_manager.check_health(page)
            print(f"Session status before capture: {health.status} | url={health.url}")

        else:
            if health.status == "login_required":
                print("Login is required. Log in manually in the browser, then re-run this tool.")
                return 1

        page.wait_for_timeout(3000)

        print("Reloading page to capture load responses...")
        page.reload(wait_until="domcontentloaded")
        page.wait_for_timeout(args.observe_seconds * 1000)

        summary = {
            "run_id": run_id,
            "target_url": args.url,
            "captured_at_utc": utc_now_iso(),
            "candidate_count": len(records),
            "jsonl_output": str(jsonl_path),
            "top_urls": [
                {"url": url, "count": count, "top_level_keys": summary_url_to_keys.get(url)}
                for url, count in summary_counts_by_url.most_common(20)
            ],
            "resource_type_counts": dict(summary_counts_by_resource_type),
            "content_type_counts": dict(summary_counts_by_content_type),
        }

        with summary_path.open("w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        print("\nDone.")
        print(f"Candidate JSONL:  {jsonl_path}")
        print(f"Summary JSON:     {summary_path}")
        print(f"Captured records: {len(records)}")

    except Exception as exc:
        audit_logger.exception(
            event_type=EVENT_DAEMON_ERROR,
            component=COMPONENT_MAIN,
            message="Deep response inspection tool failed",
            error=exc,
            data={"target_url": args.url},
        )
        raise

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
            message="Deep response inspection tool stopped",
            data={"run_id": run_id},
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
