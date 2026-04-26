from __future__ import annotations

from typing import Any

from polywhaler_bot.config import Settings
from polywhaler_bot.models import InsiderVisibilityResult, ResolvedMarket
from polywhaler_bot.polymarket_public import (
    PolymarketPublicAPIError,
    PolymarketPublicClient,
)


class InsiderVisibilityValidator:
    """
    Milestone 4 insider visibility validator.

    Uses ONLY public Polymarket positions data to determine whether the insider
    tied to a canonical event is still visibly in the resolved market position.

    This module is strictly read-only:
    - no DB writes
    - no execution logic
    - no websockets
    - no authenticated APIs
    """

    THRESHOLD = 0.01

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.public_client = PolymarketPublicClient(settings=settings)

    def evaluate(
        self,
        canonical_event: dict[str, Any],
        lifecycle_state: dict[str, Any] | None,
        resolved_market: ResolvedMarket,
    ) -> InsiderVisibilityResult:
        canonical_event_id = self._int_or_zero(canonical_event.get("id"))
        lifecycle_key = self._string_or_none(canonical_event.get("lifecycle_key")) or ""
        insider_address = self._string_or_none(canonical_event.get("insider_address"))

        reference_last_size = self._float_or_none(
            (lifecycle_state or {}).get("last_size")
        )
        reference_last_total_value = self._float_or_none(
            (lifecycle_state or {}).get("last_total_value")
        )

        reasons: list[str] = []

        if not insider_address:
            return self._result(
                canonical_event_id=canonical_event_id,
                lifecycle_key=lifecycle_key,
                insider_address=None,
                status="unavailable",
                reference_last_size=reference_last_size,
                reference_last_total_value=reference_last_total_value,
                reasons=["missing_insider_address"],
            )

        if resolved_market.status != "resolved":
            return self._result(
                canonical_event_id=canonical_event_id,
                lifecycle_key=lifecycle_key,
                insider_address=insider_address,
                status="ambiguous",
                reference_last_size=reference_last_size,
                reference_last_total_value=reference_last_total_value,
                reasons=["resolved_market_not_resolved"],
            )

        if not resolved_market.condition_id or not resolved_market.outcome:
            return self._result(
                canonical_event_id=canonical_event_id,
                lifecycle_key=lifecycle_key,
                insider_address=insider_address,
                status="ambiguous",
                reference_last_size=reference_last_size,
                reference_last_total_value=reference_last_total_value,
                reasons=["resolved_market_missing_condition_or_outcome"],
            )

        try:
            positions = self._fetch_positions(insider_address)
        except Exception as exc:
            return self._result(
                canonical_event_id=canonical_event_id,
                lifecycle_key=lifecycle_key,
                insider_address=insider_address,
                status="unavailable",
                reference_last_size=reference_last_size,
                reference_last_total_value=reference_last_total_value,
                reasons=[f"positions_fetch_failed: {type(exc).__name__}: {exc}"],
            )

        classified = self._classify_positions(
            canonical_event_id=canonical_event_id,
            lifecycle_key=lifecycle_key,
            insider_address=insider_address,
            positions=positions,
            resolved_market=resolved_market,
            reference_last_size=reference_last_size,
            reference_last_total_value=reference_last_total_value,
            one_recheck_performed=False,
        )

        # Only recheck when no matching position was found and full_exit is plausible.
        if classified.status == "full_exit":
            try:
                recheck_positions = self._fetch_positions(insider_address)
            except Exception as exc:
                return self._result(
                    canonical_event_id=canonical_event_id,
                    lifecycle_key=lifecycle_key,
                    insider_address=insider_address,
                    status="unavailable",
                    reference_last_size=reference_last_size,
                    reference_last_total_value=reference_last_total_value,
                    one_recheck_performed=True,
                    reasons=[
                        "initial_no_match",
                        f"recheck_positions_fetch_failed: {type(exc).__name__}: {exc}",
                    ],
                )

            return self._classify_positions(
                canonical_event_id=canonical_event_id,
                lifecycle_key=lifecycle_key,
                insider_address=insider_address,
                positions=recheck_positions,
                resolved_market=resolved_market,
                reference_last_size=reference_last_size,
                reference_last_total_value=reference_last_total_value,
                one_recheck_performed=True,
            )

        return classified

    def _fetch_positions(self, insider_address: str) -> list[dict[str, Any]]:
        response = self.public_client.get_current_positions(user=insider_address)
        payload = response.data

        if not isinstance(payload, list):
            raise PolymarketPublicAPIError(
                f"Expected positions payload list, got {type(payload).__name__}"
            )

        return [item for item in payload if isinstance(item, dict)]

    def _classify_positions(
        self,
        *,
        canonical_event_id: int,
        lifecycle_key: str,
        insider_address: str,
        positions: list[dict[str, Any]],
        resolved_market: ResolvedMarket,
        reference_last_size: float | None,
        reference_last_total_value: float | None,
        one_recheck_performed: bool,
    ) -> InsiderVisibilityResult:
        reasons: list[str] = []

        same_condition_positions = [
            p
            for p in positions
            if self._normalize_text(self._extract_condition_id(p))
            == self._normalize_text(resolved_market.condition_id)
        ]

        exact_matches = [
            p
            for p in same_condition_positions
            if self._normalize_text(self._extract_outcome(p))
            == self._normalize_text(resolved_market.outcome)
        ]

        opposite_matches = [
            p
            for p in same_condition_positions
            if self._normalize_text(self._extract_outcome(p))
            != self._normalize_text(resolved_market.outcome)
        ]

        if len(exact_matches) > 1:
            return self._result(
                canonical_event_id=canonical_event_id,
                lifecycle_key=lifecycle_key,
                insider_address=insider_address,
                status="ambiguous",
                reference_last_size=reference_last_size,
                reference_last_total_value=reference_last_total_value,
                one_recheck_performed=one_recheck_performed,
                reasons=["multiple_conflicting_exact_matches"],
            )

        if exact_matches:
            pos = exact_matches[0]
            visible_size = self._float_or_none(pos.get("size"))
            current_value = self._float_or_none(
                pos.get("currentValue") or pos.get("current_value")
            )

            if visible_size is None:
                return self._result(
                    canonical_event_id=canonical_event_id,
                    lifecycle_key=lifecycle_key,
                    insider_address=insider_address,
                    status="ambiguous",
                    matched_condition_id=self._extract_condition_id(pos),
                    matched_asset=self._string_or_none(pos.get("asset")),
                    matched_outcome=self._extract_outcome(pos),
                    matched_size=None,
                    matched_current_value=current_value,
                    reference_last_size=reference_last_size,
                    reference_last_total_value=reference_last_total_value,
                    one_recheck_performed=one_recheck_performed,
                    reasons=["matched_position_missing_size"],
                )

            if visible_size <= self.THRESHOLD:
                # Treat effectively zero visible size as absent after recheck path.
                if one_recheck_performed:
                    return self._result(
                        canonical_event_id=canonical_event_id,
                        lifecycle_key=lifecycle_key,
                        insider_address=insider_address,
                        status="full_exit",
                        matched_condition_id=self._extract_condition_id(pos),
                        matched_asset=self._string_or_none(pos.get("asset")),
                        matched_outcome=self._extract_outcome(pos),
                        matched_size=visible_size,
                        matched_current_value=current_value,
                        reference_last_size=reference_last_size,
                        reference_last_total_value=reference_last_total_value,
                        one_recheck_performed=one_recheck_performed,
                        reasons=["matched_position_below_visibility_threshold"],
                    )
                return self._result(
                    canonical_event_id=canonical_event_id,
                    lifecycle_key=lifecycle_key,
                    insider_address=insider_address,
                    status="full_exit",
                    matched_condition_id=self._extract_condition_id(pos),
                    matched_asset=self._string_or_none(pos.get("asset")),
                    matched_outcome=self._extract_outcome(pos),
                    matched_size=visible_size,
                    matched_current_value=current_value,
                    reference_last_size=reference_last_size,
                    reference_last_total_value=reference_last_total_value,
                    one_recheck_performed=one_recheck_performed,
                    reasons=["matched_position_below_visibility_threshold"],
                )

            if reference_last_size is not None and visible_size < reference_last_size:
                return self._result(
                    canonical_event_id=canonical_event_id,
                    lifecycle_key=lifecycle_key,
                    insider_address=insider_address,
                    status="partial_reduce",
                    matched_condition_id=self._extract_condition_id(pos),
                    matched_asset=self._string_or_none(pos.get("asset")),
                    matched_outcome=self._extract_outcome(pos),
                    matched_size=visible_size,
                    matched_current_value=current_value,
                    reference_last_size=reference_last_size,
                    reference_last_total_value=reference_last_total_value,
                    one_recheck_performed=one_recheck_performed,
                    reasons=["visible_size_less_than_reference_last_size"],
                )

            return self._result(
                canonical_event_id=canonical_event_id,
                lifecycle_key=lifecycle_key,
                insider_address=insider_address,
                status="still_in",
                matched_condition_id=self._extract_condition_id(pos),
                matched_asset=self._string_or_none(pos.get("asset")),
                matched_outcome=self._extract_outcome(pos),
                matched_size=visible_size,
                matched_current_value=current_value,
                reference_last_size=reference_last_size,
                reference_last_total_value=reference_last_total_value,
                one_recheck_performed=one_recheck_performed,
                reasons=["matching_position_visible"],
            )

        if opposite_matches:
            # If more than one opposite-outcome match exists, treat as ambiguous
            # rather than confidently classifying a flip.
            if len(opposite_matches) > 1:
                return self._result(
                    canonical_event_id=canonical_event_id,
                    lifecycle_key=lifecycle_key,
                    insider_address=insider_address,
                    status="ambiguous",
                    reference_last_size=reference_last_size,
                    reference_last_total_value=reference_last_total_value,
                    one_recheck_performed=one_recheck_performed,
                    reasons=["multiple_opposite_outcome_positions"],
                )

            pos = opposite_matches[0]
            return self._result(
                canonical_event_id=canonical_event_id,
                lifecycle_key=lifecycle_key,
                insider_address=insider_address,
                status="flip",
                matched_condition_id=self._extract_condition_id(pos),
                matched_asset=self._string_or_none(pos.get("asset")),
                matched_outcome=self._extract_outcome(pos),
                matched_size=self._float_or_none(pos.get("size")),
                matched_current_value=self._float_or_none(
                    pos.get("currentValue") or pos.get("current_value")
                ),
                reference_last_size=reference_last_size,
                reference_last_total_value=reference_last_total_value,
                one_recheck_performed=one_recheck_performed,
                reasons=["opposite_outcome_position_visible"],
            )

        if one_recheck_performed:
            reasons.append("no_matching_position_after_recheck")
        else:
            reasons.append("no_matching_position_initial_check")

        return self._result(
            canonical_event_id=canonical_event_id,
            lifecycle_key=lifecycle_key,
            insider_address=insider_address,
            status="full_exit",
            reference_last_size=reference_last_size,
            reference_last_total_value=reference_last_total_value,
            one_recheck_performed=one_recheck_performed,
            reasons=reasons,
        )

    def _extract_condition_id(self, position: dict[str, Any]) -> str | None:
        return self._string_or_none(
            position.get("conditionId") or position.get("condition_id")
        )

    def _extract_outcome(self, position: dict[str, Any]) -> str | None:
        return self._string_or_none(position.get("outcome"))

    def _result(
        self,
        *,
        canonical_event_id: int,
        lifecycle_key: str,
        insider_address: str | None,
        status: str,
        matched_condition_id: str | None = None,
        matched_asset: str | None = None,
        matched_outcome: str | None = None,
        matched_size: float | None = None,
        matched_current_value: float | None = None,
        reference_last_size: float | None = None,
        reference_last_total_value: float | None = None,
        one_recheck_performed: bool = False,
        reasons: list[str] | None = None,
    ) -> InsiderVisibilityResult:
        return InsiderVisibilityResult(
            canonical_event_id=canonical_event_id,
            lifecycle_key=lifecycle_key,
            insider_address=insider_address,
            status=status,
            matched_condition_id=matched_condition_id,
            matched_asset=matched_asset,
            matched_outcome=matched_outcome,
            matched_size=matched_size,
            matched_current_value=matched_current_value,
            reference_last_size=reference_last_size,
            reference_last_total_value=reference_last_total_value,
            one_recheck_performed=one_recheck_performed,
            reasons=reasons or [],
        )

    def _normalize_text(self, value: Any) -> str:
        text = self._string_or_none(value) or ""
        return text.lower().strip()

    def _string_or_none(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None

    def _float_or_none(self, value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _int_or_zero(self, value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0
