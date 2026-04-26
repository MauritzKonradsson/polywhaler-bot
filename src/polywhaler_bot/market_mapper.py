from __future__ import annotations

import json
import re
from typing import Any

from polywhaler_bot.config import Settings
from polywhaler_bot.models import ResolvedMarket
from polywhaler_bot.polymarket_public import (
    PolymarketPublicAPIError,
    PolymarketPublicClient,
)


class MarketMapper:
    """
    Milestone 4 market resolver.

    Responsibilities:
    - accept a canonical_event row/dict
    - parse source_payload_json when present
    - resolve market identity in precedence order:
        1) condition_id
        2) slug
        3) title/text fallback
    - hydrate market data when initial payload lacks token/outcome structures
    - resolve token_id/outcome from realistic Polymarket market payload shapes
    - confirm orderbook availability for resolved token
    - report confidence / ambiguity / failure reasons

    This module does NOT:
    - perform insider visibility checks
    - run gate evaluation
    - place orders
    - persist anything to the DB
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.public_client = PolymarketPublicClient(settings=settings)

    def resolve(self, canonical_event: dict[str, Any]) -> ResolvedMarket:
        canonical_event_id = self._int_or_zero(canonical_event.get("id"))
        lifecycle_key = self._string_or_none(canonical_event.get("lifecycle_key")) or ""

        market_text = (
            self._string_or_none(canonical_event.get("market_text"))
            or "<missing-market>"
        )

        payload = self._coerce_payload(canonical_event.get("source_payload_json"))

        condition_id = (
            self._string_or_none(canonical_event.get("condition_id"))
            or self._string_or_none(payload.get("conditionId"))
        )
        canonical_slug = (
            self._string_or_none(canonical_event.get("market_slug"))
            or self._string_or_none(payload.get("slug"))
            or self._string_or_none(payload.get("eventSlug"))
        )
        canonical_asset = (
            self._string_or_none(canonical_event.get("asset"))
            or self._string_or_none(payload.get("asset"))
        )
        canonical_outcome = (
            self._string_or_none(canonical_event.get("outcome"))
            or self._string_or_none(payload.get("outcome"))
        )
        canonical_side = self._normalize_side(
            self._string_or_none(canonical_event.get("side"))
            or self._string_or_none(payload.get("side"))
        )

        ambiguity_flags: list[str] = []
        failure_reasons: list[str] = []

        market: dict[str, Any] | None = None
        match_method: str | None = None
        confidence = 0.0

        try:
            # -----------------------------------------------------------------
            # 1) Resolve by condition_id first
            # -----------------------------------------------------------------
            if condition_id:
                candidates = self._find_market_candidates_by_condition_id(condition_id)
                market, result_status = self._select_single_market(candidates)
                if result_status == "resolved":
                    match_method = "condition_id_direct"
                    confidence = 0.95
                elif result_status == "ambiguous":
                    ambiguity_flags.append("multiple_markets_for_condition_id")
                else:
                    failure_reasons.append("condition_id_not_found")

            # -----------------------------------------------------------------
            # 2) Resolve by slug second
            # -----------------------------------------------------------------
            if market is None and canonical_slug:
                candidates = self._find_market_candidates_by_slug(canonical_slug)
                market, result_status = self._select_single_market(candidates)
                if result_status == "resolved":
                    match_method = "slug_direct"
                    confidence = 0.85
                elif result_status == "ambiguous":
                    ambiguity_flags.append("multiple_markets_for_slug")
                elif not condition_id:
                    failure_reasons.append("slug_not_found")

            # -----------------------------------------------------------------
            # 3) Resolve by title/text fallback last
            # -----------------------------------------------------------------
            if market is None:
                candidates = self._find_market_candidates_by_title(market_text)
                market, result_status = self._select_single_market(candidates)
                if result_status == "resolved":
                    match_method = "title_fallback"
                    confidence = 0.65
                elif result_status == "ambiguous":
                    ambiguity_flags.append("multiple_markets_for_title")
                else:
                    failure_reasons.append("title_not_found")

            if market is None:
                return ResolvedMarket(
                    canonical_event_id=canonical_event_id,
                    lifecycle_key=lifecycle_key,
                    status="ambiguous" if ambiguity_flags else "failed",
                    market_slug=canonical_slug,
                    market_text=market_text,
                    condition_id=condition_id,
                    token_id=None,
                    asset=canonical_asset,
                    outcome=canonical_outcome,
                    canonical_side=canonical_side,
                    replication_side=canonical_side,
                    match_method=match_method,
                    confidence=0.0,
                    market_active=None,
                    market_readable=False,
                    orderbook_available=False,
                    ambiguity_flags=sorted(set(ambiguity_flags)),
                    failure_reasons=sorted(set(failure_reasons)),
                )

            # -----------------------------------------------------------------
            # Hydrate token data if initial market payload is incomplete
            # -----------------------------------------------------------------
            hydrated_market, hydration_notes = self._hydrate_market(
                market=market,
                condition_id=condition_id or self._extract_condition_id(market),
                slug=canonical_slug or self._extract_market_slug(market),
                market_text=market_text,
            )
            market = hydrated_market
            ambiguity_flags.extend(hydration_notes["ambiguity_flags"])
            failure_reasons.extend(hydration_notes["failure_reasons"])

            # -----------------------------------------------------------------
            # Resolve token/outcome from realistic market shapes
            # -----------------------------------------------------------------
            token_resolution = self._resolve_token_and_outcome(
                market=market,
                canonical_asset=canonical_asset,
                canonical_outcome=canonical_outcome,
            )

            ambiguity_flags.extend(token_resolution["ambiguity_flags"])
            failure_reasons.extend(token_resolution["failure_reasons"])

            token_id = token_resolution["token_id"]
            resolved_outcome = token_resolution["outcome"]
            resolved_asset = token_resolution["asset"]

            # -----------------------------------------------------------------
            # Confirm orderbook availability for resolved token
            # -----------------------------------------------------------------
            orderbook_available = False
            if token_id:
                try:
                    order_book = self.public_client.get_order_book(token_id=token_id)
                    orderbook_available = isinstance(order_book.data, dict)
                except PolymarketPublicAPIError:
                    orderbook_available = False

            market_active = self._infer_market_active(market)
            market_readable = self._market_is_readable(market)

            if token_id and resolved_outcome:
                confidence = min(confidence + 0.05, 1.0)

            status = self._final_status(
                ambiguity_flags=ambiguity_flags,
                failure_reasons=failure_reasons,
                token_id=token_id,
                outcome=resolved_outcome,
            )

            return ResolvedMarket(
                canonical_event_id=canonical_event_id,
                lifecycle_key=lifecycle_key,
                status=status,
                market_slug=self._extract_market_slug(market) or canonical_slug,
                market_text=self._extract_market_text(market) or market_text,
                condition_id=self._extract_condition_id(market) or condition_id,
                token_id=token_id,
                asset=resolved_asset or canonical_asset,
                outcome=resolved_outcome or canonical_outcome,
                canonical_side=canonical_side,
                replication_side=canonical_side,
                match_method=match_method,
                confidence=confidence if status == "resolved" else max(confidence - 0.15, 0.0),
                market_active=market_active,
                market_readable=market_readable,
                orderbook_available=orderbook_available,
                ambiguity_flags=sorted(set(ambiguity_flags)),
                failure_reasons=sorted(set(failure_reasons)),
            )

        except PolymarketPublicAPIError as exc:
            return ResolvedMarket(
                canonical_event_id=canonical_event_id,
                lifecycle_key=lifecycle_key,
                status="failed",
                market_slug=canonical_slug,
                market_text=market_text,
                condition_id=condition_id,
                token_id=None,
                asset=canonical_asset,
                outcome=canonical_outcome,
                canonical_side=canonical_side,
                replication_side=canonical_side,
                match_method=match_method,
                confidence=0.0,
                market_active=None,
                market_readable=False,
                orderbook_available=False,
                ambiguity_flags=sorted(set(ambiguity_flags)),
                failure_reasons=sorted(set(failure_reasons + [f"public_api_error: {exc}"])),
            )

    # -------------------------------------------------------------------------
    # Candidate lookup
    # -------------------------------------------------------------------------
    def _find_market_candidates_by_condition_id(self, condition_id: str) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []

        payload = self.public_client.get_simplified_markets(
            params={"condition_id": condition_id, "limit": 25}
        ).data
        candidates.extend(
            [
                item
                for item in self._coerce_market_list(payload)
                if self._normalize_text(self._extract_condition_id(item))
                == self._normalize_text(condition_id)
            ]
        )

        if not candidates:
            gamma_payload = self.public_client.get_gamma_markets(
                params={"condition_id": condition_id, "limit": 25}
            ).data
            candidates.extend(
                [
                    item
                    for item in self._coerce_market_list(gamma_payload)
                    if self._normalize_text(self._extract_condition_id(item))
                    == self._normalize_text(condition_id)
                ]
            )

        return candidates

    def _find_market_candidates_by_slug(self, slug: str) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []

        payload = self.public_client.get_simplified_markets(
            params={"slug": slug, "limit": 25}
        ).data
        candidates.extend(
            [
                item
                for item in self._coerce_market_list(payload)
                if self._normalize_text(self._extract_market_slug(item))
                == self._normalize_text(slug)
            ]
        )

        gamma_payload = self.public_client.get_gamma_markets(
            params={"slug": slug, "limit": 25}
        ).data
        candidates.extend(
            [
                item
                for item in self._coerce_market_list(gamma_payload)
                if self._normalize_text(self._extract_market_slug(item))
                == self._normalize_text(slug)
            ]
        )

        return candidates

    def _find_market_candidates_by_title(self, market_text: str) -> list[dict[str, Any]]:
        normalized_target = self._normalize_text(market_text)
        if not normalized_target:
            return []

        gamma_payload = self.public_client.get_gamma_markets(
            params={"limit": 50, "active": True}
        ).data

        candidates: list[dict[str, Any]] = []
        for item in self._coerce_market_list(gamma_payload):
            title = self._extract_market_text(item)
            if self._normalize_text(title) == normalized_target:
                candidates.append(item)

        return candidates

    def _select_single_market(
        self,
        candidates: list[dict[str, Any]],
    ) -> tuple[dict[str, Any] | None, str]:
        if not candidates:
            return None, "failed"

        unique_by_key: dict[str, dict[str, Any]] = {}
        for item in candidates:
            key = (
                self._extract_condition_id(item)
                or self._extract_market_slug(item)
                or self._extract_market_text(item)
                or repr(item)
            )
            unique_by_key[key] = item

        deduped = list(unique_by_key.values())

        if len(deduped) == 1:
            return deduped[0], "resolved"

        return None, "ambiguous"

    # -------------------------------------------------------------------------
    # Hydration
    # -------------------------------------------------------------------------
    def _hydrate_market(
        self,
        *,
        market: dict[str, Any],
        condition_id: str | None,
        slug: str | None,
        market_text: str | None,
    ) -> tuple[dict[str, Any], dict[str, list[str]]]:
        """
        Ensure the matched market carries enough token/outcome data for resolution.

        If the initial market lacks tokens, try public hydration sources in order:
        1) simplified-markets by condition_id
        2) gamma markets by condition_id
        3) simplified-markets by slug
        4) gamma markets by slug
        5) gamma title fallback

        Returns:
        - merged market dict
        - notes: {"ambiguity_flags": [...], "failure_reasons": [...]}
        """
        notes = {"ambiguity_flags": [], "failure_reasons": []}

        if self._extract_token_candidates(market):
            return market, notes

        hydration_candidates: list[dict[str, Any]] = []

        if condition_id:
            hydration_candidates.extend(self._find_market_candidates_by_condition_id(condition_id))

        if slug:
            hydration_candidates.extend(self._find_market_candidates_by_slug(slug))

        if not hydration_candidates and market_text:
            hydration_candidates.extend(self._find_market_candidates_by_title(market_text))

        hydrated = self._pick_hydrated_candidate(hydration_candidates)

        if hydrated is None:
            notes["failure_reasons"].append("market_tokens_missing")
            return market, notes

        merged = dict(market)
        merged.update(hydrated)

        if not self._extract_token_candidates(merged):
            notes["failure_reasons"].append("market_tokens_missing")
            return merged, notes

        return merged, notes

    def _pick_hydrated_candidate(
        self,
        candidates: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """
        Prefer a candidate that already exposes token candidates.
        Fallback to the first candidate if available.
        """
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()

        for item in candidates:
            key = (
                self._extract_condition_id(item)
                or self._extract_market_slug(item)
                or self._extract_market_text(item)
                or repr(item)
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)

        for item in deduped:
            if self._extract_token_candidates(item):
                return item

        return deduped[0] if deduped else None

    # -------------------------------------------------------------------------
    # Token / outcome resolution
    # -------------------------------------------------------------------------
    def _resolve_token_and_outcome(
        self,
        *,
        market: dict[str, Any],
        canonical_asset: str | None,
        canonical_outcome: str | None,
    ) -> dict[str, Any]:
        ambiguity_flags: list[str] = []
        failure_reasons: list[str] = []

        token_candidates = self._extract_token_candidates(market)
        if not token_candidates:
            return {
                "token_id": None,
                "asset": canonical_asset,
                "outcome": canonical_outcome,
                "ambiguity_flags": ambiguity_flags,
                "failure_reasons": ["market_tokens_missing"],
            }

        normalized_asset = self._normalize_text(canonical_asset)
        normalized_outcome = self._normalize_text(canonical_outcome)

        if normalized_asset:
            asset_matches = [
                token
                for token in token_candidates
                if self._normalize_text(token.get("token_id")) == normalized_asset
                or self._normalize_text(token.get("asset")) == normalized_asset
                or self._normalize_text(token.get("asset_id")) == normalized_asset
            ]
            if len(asset_matches) == 1:
                token = asset_matches[0]
                token_outcome = self._string_or_none(token.get("outcome"))
                if normalized_outcome and self._normalize_text(token_outcome) != normalized_outcome:
                    ambiguity_flags.append("asset_outcome_mismatch")
                return {
                    "token_id": self._string_or_none(token.get("token_id")),
                    "asset": self._string_or_none(token.get("asset"))
                    or self._string_or_none(token.get("token_id"))
                    or canonical_asset,
                    "outcome": token_outcome or canonical_outcome,
                    "ambiguity_flags": ambiguity_flags,
                    "failure_reasons": failure_reasons,
                }
            if len(asset_matches) > 1:
                ambiguity_flags.append("multiple_tokens_for_asset")

        if normalized_outcome:
            outcome_matches = [
                token
                for token in token_candidates
                if self._normalize_text(token.get("outcome")) == normalized_outcome
            ]
            if len(outcome_matches) == 1:
                token = outcome_matches[0]
                return {
                    "token_id": self._string_or_none(token.get("token_id")),
                    "asset": self._string_or_none(token.get("asset"))
                    or self._string_or_none(token.get("token_id"))
                    or canonical_asset,
                    "outcome": self._string_or_none(token.get("outcome")) or canonical_outcome,
                    "ambiguity_flags": ambiguity_flags,
                    "failure_reasons": failure_reasons,
                }
            if len(outcome_matches) > 1:
                ambiguity_flags.append("multiple_tokens_for_outcome")

        if len(token_candidates) == 1:
            token = token_candidates[0]
            return {
                "token_id": self._string_or_none(token.get("token_id")),
                "asset": self._string_or_none(token.get("asset"))
                or self._string_or_none(token.get("token_id"))
                or canonical_asset,
                "outcome": self._string_or_none(token.get("outcome")) or canonical_outcome,
                "ambiguity_flags": ambiguity_flags,
                "failure_reasons": failure_reasons,
            }

        failure_reasons.append("token_or_outcome_not_resolved")
        return {
            "token_id": None,
            "asset": canonical_asset,
            "outcome": canonical_outcome,
            "ambiguity_flags": ambiguity_flags,
            "failure_reasons": failure_reasons,
        }

    def _extract_token_candidates(self, market: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Support realistic public Polymarket market payload shapes, including:
        - tokens: [{token_id, outcome}, ...]
        - tokens: [{tokenId, outcome}, ...]
        - clobTokenIds: ["...", "..."] or JSON string list
        - outcomes: ["YES", "NO"] or JSON string list
        - outcomePrices / prices ignored for now
        """
        candidates: list[dict[str, Any]] = []

        raw_tokens = market.get("tokens")
        if isinstance(raw_tokens, list):
            for token in raw_tokens:
                if isinstance(token, dict):
                    token_id = (
                        self._string_or_none(token.get("token_id"))
                        or self._string_or_none(token.get("tokenId"))
                        or self._string_or_none(token.get("asset"))
                        or self._string_or_none(token.get("asset_id"))
                    )
                    outcome = self._string_or_none(token.get("outcome"))
                    asset = (
                        self._string_or_none(token.get("asset"))
                        or self._string_or_none(token.get("asset_id"))
                        or token_id
                    )
                    if token_id or outcome:
                        candidates.append(
                            {
                                "token_id": token_id,
                                "asset": asset,
                                "asset_id": self._string_or_none(token.get("asset_id")),
                                "outcome": outcome,
                                "raw": token,
                            }
                        )
                elif isinstance(token, str):
                    candidates.append(
                        {
                            "token_id": token.strip() or None,
                            "asset": token.strip() or None,
                            "asset_id": None,
                            "outcome": None,
                            "raw": token,
                        }
                    )

        if candidates:
            return self._dedupe_token_candidates(candidates)

        clob_token_ids = self._coerce_string_list(
            market.get("clobTokenIds")
            or market.get("clob_token_ids")
            or market.get("tokenIds")
            or market.get("token_ids")
        )
        outcomes = self._coerce_string_list(
            market.get("outcomes")
            or market.get("marketOutcomes")
            or market.get("market_outcomes")
        )

        if clob_token_ids:
            for idx, token_id in enumerate(clob_token_ids):
                outcome = outcomes[idx] if idx < len(outcomes) else None
                candidates.append(
                    {
                        "token_id": token_id,
                        "asset": token_id,
                        "asset_id": None,
                        "outcome": outcome,
                        "raw": {
                            "token_id": token_id,
                            "outcome": outcome,
                        },
                    }
                )

        return self._dedupe_token_candidates(candidates)

    def _dedupe_token_candidates(self, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped: dict[str, dict[str, Any]] = {}
        for item in candidates:
            key = (
                self._string_or_none(item.get("token_id"))
                or self._string_or_none(item.get("asset"))
                or self._string_or_none(item.get("outcome"))
                or repr(item)
            )
            deduped[key] = item
        return list(deduped.values())

    def _coerce_string_list(self, value: Any) -> list[str]:
        if value is None:
            return []

        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]

        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            try:
                decoded = json.loads(text)
                if isinstance(decoded, list):
                    return [str(item).strip() for item in decoded if str(item).strip()]
            except json.JSONDecodeError:
                pass
            return [part.strip() for part in text.split(",") if part.strip()]

        return []

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------
    def _final_status(
        self,
        *,
        ambiguity_flags: list[str],
        failure_reasons: list[str],
        token_id: str | None,
        outcome: str | None,
    ) -> str:
        if failure_reasons or not token_id or not outcome:
            return "failed"
        if ambiguity_flags:
            return "ambiguous"
        return "resolved"

    def _coerce_payload(self, value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str) and value.strip():
            try:
                decoded = json.loads(value)
                if isinstance(decoded, dict):
                    return decoded
            except json.JSONDecodeError:
                return {}
        return {}

    def _coerce_market_list(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            if isinstance(payload.get("data"), list):
                return [item for item in payload["data"] if isinstance(item, dict)]
            if isinstance(payload.get("markets"), list):
                return [item for item in payload["markets"] if isinstance(item, dict)]
        return []

    def _extract_condition_id(self, market: dict[str, Any]) -> str | None:
        return (
            self._string_or_none(market.get("condition_id"))
            or self._string_or_none(market.get("conditionId"))
            or self._string_or_none(market.get("market"))
        )

    def _extract_market_slug(self, market: dict[str, Any]) -> str | None:
        return (
            self._string_or_none(market.get("slug"))
            or self._string_or_none(market.get("market_slug"))
        )

    def _extract_market_text(self, market: dict[str, Any]) -> str | None:
        return (
            self._string_or_none(market.get("question"))
            or self._string_or_none(market.get("title"))
            or self._string_or_none(market.get("description"))
        )

    def _infer_market_active(self, market: dict[str, Any]) -> bool | None:
        if "active" in market and isinstance(market["active"], bool):
            return market["active"]
        if "closed" in market and isinstance(market["closed"], bool):
            return not market["closed"]
        if "archived" in market and isinstance(market["archived"], bool):
            return not market["archived"]
        if "resolved" in market and isinstance(market["resolved"], bool):
            return not market["resolved"]
        return None

    def _market_is_readable(self, market: dict[str, Any]) -> bool:
        if self._extract_market_text(market) is None:
            return False
        if not self._extract_condition_id(market):
            return False
        if not self._extract_token_candidates(market):
            return False
        return True

    def _normalize_side(self, value: str | None) -> str | None:
        if value is None:
            return None
        upper = value.strip().upper()
        return upper if upper in {"BUY", "SELL", "YES", "NO"} else upper

    def _normalize_text(self, value: Any) -> str:
        text = self._string_or_none(value) or ""
        text = text.lower()
        text = re.sub(r"\s+", " ", text).strip()
        return text

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
