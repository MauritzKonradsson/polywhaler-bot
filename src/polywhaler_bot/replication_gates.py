from __future__ import annotations

from datetime import datetime
from typing import Any

from polywhaler_bot.models import GateDecision, InsiderVisibilityResult, ResolvedMarket


class ReplicationGateEngine:
    """
    Milestone 4 replication gate engine.

    Read-only only:
    - no DB writes
    - no execution logic
    - no order placement
    - no persistence of gate results
    """

    GATE_SIGNAL_EXISTS = "signal_exists"
    GATE_MARKET_RESOLVED = "market_resolved"
    GATE_OUTCOME_TOKEN_RESOLVED = "outcome_token_resolved"
    GATE_MARKET_ACTIVE_TRADABLE_READABLE = "market_active_tradable_readable"
    GATE_INSIDER_STILL_VISIBLY_IN = "insider_still_visibly_in"
    GATE_NO_DUPLICATE_CONFLICT = "no_duplicate_conflict"
    GATE_NO_OBVIOUS_STALE_BROKEN_SIGNAL = "no_obvious_stale_broken_signal"

    def evaluate(
        self,
        canonical_event: dict[str, Any],
        lifecycle_state: dict[str, Any] | None,
        resolved_market: ResolvedMarket,
        visibility: InsiderVisibilityResult,
    ) -> GateDecision:
        try:
            canonical_event_id = self._int_or_zero(canonical_event.get("id"))
            lifecycle_key = self._string_or_none(canonical_event.get("lifecycle_key")) or ""

            gate_results: dict[str, str] = {}
            reasons: list[str] = []

            gate_name, result, gate_reasons = self._gate_signal_exists(canonical_event)
            gate_results[gate_name] = result
            reasons.extend(self._prefix_reasons(gate_name, gate_reasons))

            gate_name, result, gate_reasons = self._gate_market_resolved(resolved_market)
            gate_results[gate_name] = result
            reasons.extend(self._prefix_reasons(gate_name, gate_reasons))

            gate_name, result, gate_reasons = self._gate_outcome_token_resolved(resolved_market)
            gate_results[gate_name] = result
            reasons.extend(self._prefix_reasons(gate_name, gate_reasons))

            gate_name, result, gate_reasons = self._gate_market_active_tradable_readable(
                resolved_market
            )
            gate_results[gate_name] = result
            reasons.extend(self._prefix_reasons(gate_name, gate_reasons))

            gate_name, result, gate_reasons = self._gate_insider_still_visibly_in(visibility)
            gate_results[gate_name] = result
            reasons.extend(self._prefix_reasons(gate_name, gate_reasons))

            gate_name, result, gate_reasons = self._gate_no_duplicate_conflict(
                canonical_event, lifecycle_state
            )
            gate_results[gate_name] = result
            reasons.extend(self._prefix_reasons(gate_name, gate_reasons))

            gate_name, result, gate_reasons = self._gate_no_obvious_stale_broken_signal(
                canonical_event, resolved_market
            )
            gate_results[gate_name] = result
            reasons.extend(self._prefix_reasons(gate_name, gate_reasons))

            if any(value == "fail" for value in gate_results.values()):
                decision = "fail"
            elif any(value == "ambiguous" for value in gate_results.values()):
                decision = "ambiguous"
            else:
                decision = "pass"

            return GateDecision(
                canonical_event_id=canonical_event_id,
                lifecycle_key=lifecycle_key,
                decision=decision,
                execution_eligible=(decision == "pass"),
                reasons=reasons,
                gate_results=gate_results,
                resolved_market=resolved_market,
                visibility=visibility,
            )

        except Exception as exc:
            canonical_event_id = self._int_or_zero(canonical_event.get("id"))
            lifecycle_key = self._string_or_none(canonical_event.get("lifecycle_key")) or ""

            return GateDecision(
                canonical_event_id=canonical_event_id,
                lifecycle_key=lifecycle_key,
                decision="ambiguous",
                execution_eligible=False,
                reasons=[f"internal_error: {type(exc).__name__}: {exc}"],
                gate_results={
                    self.GATE_SIGNAL_EXISTS: "ambiguous",
                    self.GATE_MARKET_RESOLVED: "ambiguous",
                    self.GATE_OUTCOME_TOKEN_RESOLVED: "ambiguous",
                    self.GATE_MARKET_ACTIVE_TRADABLE_READABLE: "ambiguous",
                    self.GATE_INSIDER_STILL_VISIBLY_IN: "ambiguous",
                    self.GATE_NO_DUPLICATE_CONFLICT: "ambiguous",
                    self.GATE_NO_OBVIOUS_STALE_BROKEN_SIGNAL: "ambiguous",
                },
                resolved_market=resolved_market,
                visibility=visibility,
            )

    def _gate_signal_exists(
        self,
        canonical_event: dict[str, Any],
    ) -> tuple[str, str, list[str]]:
        reasons: list[str] = []

        canonical_event_id = self._int_or_zero(canonical_event.get("id"))
        lifecycle_key = self._string_or_none(canonical_event.get("lifecycle_key"))
        event_type = self._string_or_none(canonical_event.get("event_type"))
        market_text = self._string_or_none(canonical_event.get("market_text"))

        if canonical_event_id <= 0:
            reasons.append("missing_or_invalid_canonical_event_id")
        if not lifecycle_key:
            reasons.append("missing_lifecycle_key")
        if event_type != "raw_trade":
            reasons.append(f"unsupported_event_type={event_type!r}")
        if not market_text:
            reasons.append("missing_market_text")

        if reasons:
            return self.GATE_SIGNAL_EXISTS, "fail", reasons

        return self.GATE_SIGNAL_EXISTS, "pass", []

    def _gate_market_resolved(
        self,
        resolved_market: ResolvedMarket,
    ) -> tuple[str, str, list[str]]:
        reasons: list[str] = []

        if resolved_market.status == "resolved" and resolved_market.condition_id:
            return self.GATE_MARKET_RESOLVED, "pass", []

        if resolved_market.status == "ambiguous":
            reasons.extend(resolved_market.ambiguity_flags or ["market_mapping_ambiguous"])
            if not resolved_market.condition_id:
                reasons.append("missing_condition_id")
            return self.GATE_MARKET_RESOLVED, "ambiguous", reasons

        reasons.extend(resolved_market.failure_reasons or ["market_mapping_failed"])
        if not resolved_market.condition_id:
            reasons.append("missing_condition_id")
        return self.GATE_MARKET_RESOLVED, "fail", reasons

    def _gate_outcome_token_resolved(
        self,
        resolved_market: ResolvedMarket,
    ) -> tuple[str, str, list[str]]:
        reasons: list[str] = []

        missing = []
        if not resolved_market.token_id:
            missing.append("token_id")
        if not resolved_market.outcome:
            missing.append("outcome")
        if not resolved_market.replication_side:
            missing.append("replication_side")

        if not missing and resolved_market.status == "resolved":
            return self.GATE_OUTCOME_TOKEN_RESOLVED, "pass", []

        reasons.append(f"missing_fields={','.join(missing)}" if missing else "unresolved_fields")

        if resolved_market.status == "ambiguous":
            reasons.extend(resolved_market.ambiguity_flags or [])
            return self.GATE_OUTCOME_TOKEN_RESOLVED, "ambiguous", reasons

        reasons.extend(resolved_market.failure_reasons or [])
        return self.GATE_OUTCOME_TOKEN_RESOLVED, "fail", reasons

    def _gate_market_active_tradable_readable(
        self,
        resolved_market: ResolvedMarket,
    ) -> tuple[str, str, list[str]]:
        reasons: list[str] = []

        if resolved_market.market_active is False:
            reasons.append("market_inactive")
        if resolved_market.market_readable is False:
            reasons.append("market_not_readable")
        if resolved_market.orderbook_available is False:
            reasons.append("orderbook_unavailable")

        if reasons:
            return self.GATE_MARKET_ACTIVE_TRADABLE_READABLE, "fail", reasons

        unknowns = []
        if resolved_market.market_active is None:
            unknowns.append("market_active_unknown")
        if resolved_market.market_readable is None:
            unknowns.append("market_readable_unknown")
        if resolved_market.orderbook_available is None:
            unknowns.append("orderbook_availability_unknown")

        if unknowns:
            return self.GATE_MARKET_ACTIVE_TRADABLE_READABLE, "ambiguous", unknowns

        return self.GATE_MARKET_ACTIVE_TRADABLE_READABLE, "pass", []

    def _gate_insider_still_visibly_in(
        self,
        visibility: InsiderVisibilityResult,
    ) -> tuple[str, str, list[str]]:
        if visibility.status == "still_in":
            return self.GATE_INSIDER_STILL_VISIBLY_IN, "pass", []

        if visibility.status in {"ambiguous", "unavailable"}:
            return (
                self.GATE_INSIDER_STILL_VISIBLY_IN,
                "ambiguous",
                visibility.reasons or [visibility.status],
            )

        return (
            self.GATE_INSIDER_STILL_VISIBLY_IN,
            "fail",
            visibility.reasons or [visibility.status],
        )

    def _gate_no_duplicate_conflict(
        self,
        canonical_event: dict[str, Any],
        lifecycle_state: dict[str, Any] | None,
    ) -> tuple[str, str, list[str]]:
        reasons: list[str] = []

        if lifecycle_state is None:
            return self.GATE_NO_DUPLICATE_CONFLICT, "ambiguous", ["missing_lifecycle_state"]

        canonical_event_id = self._int_or_zero(canonical_event.get("id"))
        lifecycle_key = self._string_or_none(canonical_event.get("lifecycle_key"))
        state_lifecycle_key = self._string_or_none(lifecycle_state.get("lifecycle_key"))
        last_seen_event_id = self._int_or_none(lifecycle_state.get("last_seen_event_id"))

        if not lifecycle_key or not state_lifecycle_key:
            reasons.append("missing_lifecycle_key")
            return self.GATE_NO_DUPLICATE_CONFLICT, "ambiguous", reasons

        if self._normalize_text(lifecycle_key) != self._normalize_text(state_lifecycle_key):
            reasons.append("lifecycle_key_mismatch")
            return self.GATE_NO_DUPLICATE_CONFLICT, "fail", reasons

        if last_seen_event_id is None:
            reasons.append("missing_last_seen_event_id")
            return self.GATE_NO_DUPLICATE_CONFLICT, "ambiguous", reasons

        if canonical_event_id != last_seen_event_id:
            reasons.append(
                f"superseded_by_newer_lifecycle_event(last_seen_event_id={last_seen_event_id})"
            )
            return self.GATE_NO_DUPLICATE_CONFLICT, "fail", reasons

        return self.GATE_NO_DUPLICATE_CONFLICT, "pass", []

    def _gate_no_obvious_stale_broken_signal(
        self,
        canonical_event: dict[str, Any],
        resolved_market: ResolvedMarket,
    ) -> tuple[str, str, list[str]]:
        reasons: list[str] = []

        source_timestamp_utc = self._string_or_none(canonical_event.get("source_timestamp_utc"))
        canonical_condition_id = self._string_or_none(canonical_event.get("condition_id"))
        canonical_outcome = self._string_or_none(canonical_event.get("outcome"))

        if not source_timestamp_utc:
            reasons.append("missing_source_timestamp_utc")
            return self.GATE_NO_OBVIOUS_STALE_BROKEN_SIGNAL, "fail", reasons

        if not self._is_valid_iso_timestamp(source_timestamp_utc):
            reasons.append("invalid_source_timestamp_utc")
            return self.GATE_NO_OBVIOUS_STALE_BROKEN_SIGNAL, "fail", reasons

        if canonical_condition_id and resolved_market.condition_id:
            if self._normalize_text(canonical_condition_id) != self._normalize_text(
                resolved_market.condition_id
            ):
                reasons.append("canonical_condition_id_conflicts_with_resolved_market")
                return self.GATE_NO_OBVIOUS_STALE_BROKEN_SIGNAL, "fail", reasons

        if canonical_outcome and resolved_market.outcome:
            if self._normalize_text(canonical_outcome) != self._normalize_text(
                resolved_market.outcome
            ):
                reasons.append("canonical_outcome_conflicts_with_resolved_market")
                return self.GATE_NO_OBVIOUS_STALE_BROKEN_SIGNAL, "fail", reasons

        if resolved_market.market_active is False:
            reasons.append("resolved_market_inactive")
            return self.GATE_NO_OBVIOUS_STALE_BROKEN_SIGNAL, "fail", reasons

        return self.GATE_NO_OBVIOUS_STALE_BROKEN_SIGNAL, "pass", []

    def _prefix_reasons(self, gate_name: str, reasons: list[str]) -> list[str]:
        return [f"{gate_name}: {reason}" for reason in reasons]

    def _is_valid_iso_timestamp(self, value: str) -> bool:
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            return True
        except ValueError:
            return False

    def _normalize_text(self, value: Any) -> str:
        text = self._string_or_none(value) or ""
        return text.lower().strip()

    def _string_or_none(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None

    def _int_or_zero(self, value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def _int_or_none(self, value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
