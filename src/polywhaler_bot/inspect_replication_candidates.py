from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

from polywhaler_bot.config import get_settings
from polywhaler_bot.db import StateStore
from polywhaler_bot.insider_visibility import InsiderVisibilityValidator
from polywhaler_bot.market_mapper import MarketMapper
from polywhaler_bot.models import GateDecision, InsiderVisibilityResult, ResolvedMarket
from polywhaler_bot.replication_gates import ReplicationGateEngine


def _parse_iso_utc(value: str | None) -> datetime:
    if not value:
        return datetime.min
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min


def _safe_str(value: Any) -> str:
    if value is None:
        return "-"
    text = str(value).strip()
    return text if text else "-"


def _short_reasons(reasons: list[str], limit: int = 2) -> list[str]:
    cleaned = [r for r in reasons if r]
    return cleaned[:limit]


def _primary_reason(decision: GateDecision) -> str:
    if decision.reasons:
        return decision.reasons[0]
    return "unknown_reason"


def _source_timestamp(canonical_event: dict[str, Any]) -> str | None:
    value = canonical_event.get("source_timestamp_utc")
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _summarize_gate_results(gate_results: dict[str, str]) -> str:
    ordered_keys = [
        "signal_exists",
        "market_resolved",
        "outcome_token_resolved",
        "market_active_tradable_readable",
        "insider_still_visibly_in",
        "no_duplicate_conflict",
        "no_obvious_stale_broken_signal",
    ]
    parts: list[str] = []
    for key in ordered_keys:
        value = gate_results.get(key)
        if value:
            parts.append(f"{key}={value}")
    for key, value in gate_results.items():
        if key not in ordered_keys:
            parts.append(f"{key}={value}")
    return ", ".join(parts)


def _candidate_row(
    canonical_event: dict[str, Any],
    decision: GateDecision,
) -> dict[str, Any]:
    return {
        "canonical_event_id": decision.canonical_event_id,
        "lifecycle_key": decision.lifecycle_key,
        "market_slug": decision.resolved_market.market_slug,
        "condition_id": decision.resolved_market.condition_id,
        "token_id": decision.resolved_market.token_id,
        "outcome": decision.resolved_market.outcome,
        "replication_side": decision.resolved_market.replication_side,
        "insider_address": decision.visibility.insider_address,
        "visibility_status": decision.visibility.status,
        "matched_size": decision.visibility.matched_size,
        "source_timestamp_utc": _source_timestamp(canonical_event),
    }


def _print_pass_section(pass_items: list[dict[str, Any]]) -> None:
    print("--- PASS (execution-eligible) ---")
    if not pass_items:
        print("None")
        print()
        return

    for item in pass_items:
        row = item["row"]
        print(
            f"[PASS] "
            f"canonical_event_id={row['canonical_event_id']} | "
            f"lifecycle_key={_safe_str(row['lifecycle_key'])} | "
            f"market_slug={_safe_str(row['market_slug'])} | "
            f"condition_id={_safe_str(row['condition_id'])} | "
            f"token_id={_safe_str(row['token_id'])} | "
            f"outcome={_safe_str(row['outcome'])} | "
            f"replication_side={_safe_str(row['replication_side'])} | "
            f"insider_address={_safe_str(row['insider_address'])} | "
            f"visibility_status={_safe_str(row['visibility_status'])} | "
            f"matched_size={_safe_str(row['matched_size'])} | "
            f"source_timestamp_utc={_safe_str(row['source_timestamp_utc'])}"
        )
    print()


def _print_blocked_section(blocked_items: list[dict[str, Any]]) -> None:
    print("--- BLOCKED ---")
    if not blocked_items:
        print("None")
        print()
        return

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in blocked_items:
        grouped[item["primary_reason"]].append(item)

    for primary_reason, items in sorted(
        grouped.items(),
        key=lambda kv: (-len(kv[1]), kv[0]),
    ):
        print(f"[GROUP] primary_failure_reason={primary_reason} | count={len(items)}")
        for item in items:
            decision: GateDecision = item["decision"]
            canonical_event: dict[str, Any] = item["canonical_event"]
            top_reasons = _short_reasons(decision.reasons, limit=2)
            print(
                f"  canonical_event_id={decision.canonical_event_id} | "
                f"market_slug={_safe_str(decision.resolved_market.market_slug)} | "
                f"condition_id={_safe_str(decision.resolved_market.condition_id)} | "
                f"visibility_status={_safe_str(decision.visibility.status)} | "
                f"source_timestamp_utc={_safe_str(_source_timestamp(canonical_event))}"
            )
            print(f"    reasons={top_reasons if top_reasons else ['-']}")
            print(f"    gate_results={_summarize_gate_results(decision.gate_results)}")
        print()
    print()


def _print_ambiguous_section(ambiguous_items: list[dict[str, Any]]) -> None:
    print("--- AMBIGUOUS ---")
    if not ambiguous_items:
        print("None")
        print()
        return

    for item in ambiguous_items:
        decision: GateDecision = item["decision"]
        canonical_event: dict[str, Any] = item["canonical_event"]
        ambiguity_reasons = decision.reasons or ["ambiguous_without_reason"]
        print(
            f"[AMBIGUOUS] "
            f"canonical_event_id={decision.canonical_event_id} | "
            f"lifecycle_key={_safe_str(decision.lifecycle_key)} | "
            f"market_slug={_safe_str(decision.resolved_market.market_slug)} | "
            f"condition_id={_safe_str(decision.resolved_market.condition_id)} | "
            f"token_id={_safe_str(decision.resolved_market.token_id)} | "
            f"visibility_status={_safe_str(decision.visibility.status)} | "
            f"source_timestamp_utc={_safe_str(_source_timestamp(canonical_event))}"
        )
        print(f"  ambiguity_reasons={ambiguity_reasons}")
        print(f"  gate_results={_summarize_gate_results(decision.gate_results)}")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run-once replication candidate inspection dashboard."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Number of recent canonical events to inspect (default: 20)",
    )
    args = parser.parse_args()

    settings = get_settings()
    store = StateStore(settings.database_path)
    store.initialize()

    canonical_events = store.get_recent_canonical_events(limit=args.limit)
    if not canonical_events:
        print("=== Replication candidate inspection ===")
        print("total_events: 0")
        print("pass: 0")
        print("fail: 0")
        print("ambiguous: 0")
        print()
        print("--- PASS (execution-eligible) ---")
        print("None")
        print()
        print("--- BLOCKED ---")
        print("None")
        print()
        print("--- AMBIGUOUS ---")
        print("None")
        return 0

    mapper = MarketMapper(settings=settings)
    visibility_validator = InsiderVisibilityValidator(settings=settings)
    gate_engine = ReplicationGateEngine()

    inspected: list[dict[str, Any]] = []
    decision_counts: Counter[str] = Counter()

    for canonical_event in canonical_events:
        lifecycle_key = str(canonical_event.get("lifecycle_key") or "")
        lifecycle_state = store.get_lifecycle_state_by_key(lifecycle_key)

        try:
            resolved_market: ResolvedMarket = mapper.resolve(canonical_event)
            visibility: InsiderVisibilityResult = visibility_validator.evaluate(
                canonical_event=canonical_event,
                lifecycle_state=lifecycle_state,
                resolved_market=resolved_market,
            )
            decision: GateDecision = gate_engine.evaluate(
                canonical_event=canonical_event,
                lifecycle_state=lifecycle_state,
                resolved_market=resolved_market,
                visibility=visibility,
            )
        except Exception as exc:
            # Defensive fallback for the operator dashboard only.
            source_ts = _source_timestamp(canonical_event)
            resolved_market = ResolvedMarket(
                canonical_event_id=int(canonical_event.get("id") or 0),
                lifecycle_key=lifecycle_key,
                status="failed",
                market_slug=canonical_event.get("market_slug"),
                market_text=str(canonical_event.get("market_text") or "<missing-market>"),
                condition_id=canonical_event.get("condition_id"),
                token_id=None,
                asset=canonical_event.get("asset"),
                outcome=canonical_event.get("outcome"),
                canonical_side=canonical_event.get("side"),
                replication_side=None,
                match_method=None,
                confidence=0.0,
                market_active=None,
                market_readable=None,
                orderbook_available=None,
                ambiguity_flags=[],
                failure_reasons=[f"inspection_internal_error: {type(exc).__name__}: {exc}"],
            )
            visibility = InsiderVisibilityResult(
                canonical_event_id=int(canonical_event.get("id") or 0),
                lifecycle_key=lifecycle_key,
                insider_address=canonical_event.get("insider_address"),
                status="unavailable",
                matched_condition_id=None,
                matched_asset=None,
                matched_outcome=None,
                matched_size=None,
                matched_current_value=None,
                reference_last_size=(lifecycle_state or {}).get("last_size"),
                reference_last_total_value=(lifecycle_state or {}).get("last_total_value"),
                one_recheck_performed=False,
                reasons=[f"inspection_internal_error: {type(exc).__name__}: {exc}"],
            )
            decision = GateDecision(
                canonical_event_id=int(canonical_event.get("id") or 0),
                lifecycle_key=lifecycle_key,
                decision="ambiguous",
                execution_eligible=False,
                reasons=[f"inspection_internal_error: {type(exc).__name__}: {exc}"],
                gate_results={
                    "signal_exists": "ambiguous",
                    "market_resolved": "ambiguous",
                    "outcome_token_resolved": "ambiguous",
                    "market_active_tradable_readable": "ambiguous",
                    "insider_still_visibly_in": "ambiguous",
                    "no_duplicate_conflict": "ambiguous",
                    "no_obvious_stale_broken_signal": "ambiguous",
                },
                resolved_market=resolved_market,
                visibility=visibility,
            )

        decision_counts[decision.decision] += 1
        inspected.append(
            {
                "canonical_event": canonical_event,
                "lifecycle_state": lifecycle_state,
                "resolved_market": resolved_market,
                "visibility": visibility,
                "decision": decision,
                "row": _candidate_row(canonical_event, decision),
                "primary_reason": _primary_reason(decision),
                "source_ts": _parse_iso_utc(source_ts if (source_ts := _source_timestamp(canonical_event)) else None),
            }
        )

    pass_items = [item for item in inspected if item["decision"].decision == "pass"]
    fail_items = [item for item in inspected if item["decision"].decision == "fail"]
    ambiguous_items = [item for item in inspected if item["decision"].decision == "ambiguous"]

    pass_items.sort(key=lambda item: item["source_ts"], reverse=True)
    fail_items.sort(key=lambda item: item["source_ts"], reverse=True)
    ambiguous_items.sort(key=lambda item: item["source_ts"], reverse=True)

    print("=== Replication candidate inspection ===")
    print(f"total_events: {len(inspected)}")
    print(f"pass: {decision_counts.get('pass', 0)}")
    print(f"fail: {decision_counts.get('fail', 0)}")
    print(f"ambiguous: {decision_counts.get('ambiguous', 0)}")
    print()

    _print_pass_section(pass_items)
    _print_blocked_section(fail_items)
    _print_ambiguous_section(ambiguous_items)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
