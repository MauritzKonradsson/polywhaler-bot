from __future__ import annotations

from typing import Any

from polywhaler_bot.config import Settings
from polywhaler_bot.execution_validation import ExecutionValidator
from polywhaler_bot.models import ExecutionReadinessResult, PreExecutionOrder
from polywhaler_bot.polymarket_auth import (
    PolymarketAuthBootstrapError,
    PolymarketAuthClient,
)
from polywhaler_bot.polymarket_public import (
    PolymarketPublicAPIError,
    PolymarketPublicClient,
)


class ExecutionReadinessChecker:
    """
    Authenticated pre-submit readiness checker.

    Responsibilities:
    - validate PreExecutionOrder using existing ExecutionValidator
    - bootstrap authenticated Polymarket client
    - read safe balance / allowance summary
    - confirm public orderbook is readable for token_id
    - fail closed if a live/submitted order conflict already exists

    Hard constraints:
    - NO order submission
    - NO create_order
    - NO post_order
    - NO fills
    - NO position updates
    - NO DB writes
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.validator = ExecutionValidator()
        self.auth_client = PolymarketAuthClient(settings)
        self.public_client = PolymarketPublicClient(settings)

        self._account_context_loaded = False
        self._auth_bootstrap_ok = False
        self._auth_error: str | None = None
        self._safe_account_summary: dict[str, Any] = {}

    def get_safe_account_summary(self) -> dict[str, Any]:
        self._ensure_account_context()
        return dict(self._safe_account_summary)

    def evaluate(
        self,
        *,
        pre_execution_order: PreExecutionOrder,
        existing_live_order_conflict: bool,
    ) -> ExecutionReadinessResult:
        reasons: list[str] = []

        validation = self.validator.validate(pre_execution_order=pre_execution_order)
        if not validation.valid:
            reasons.extend(validation.reasons)

        self._ensure_account_context()

        if not self._auth_bootstrap_ok:
            reasons.append(self._auth_error or "auth_bootstrap_failed")

        balance_readable = bool(self._safe_account_summary.get("balance_readable"))
        allowance_readable = bool(self._safe_account_summary.get("allowance_readable"))
        if not balance_readable:
            reasons.append("balance_not_readable")
        if not allowance_readable:
            reasons.append("allowance_not_readable")

        orderbook_readable = self._check_orderbook_readable(pre_execution_order.token_id)
        if not orderbook_readable:
            reasons.append("orderbook_not_readable")

        if existing_live_order_conflict:
            reasons.append("existing_submitted_or_live_order_attempt_conflict")

        ready = (
            validation.valid
            and self._auth_bootstrap_ok
            and balance_readable
            and allowance_readable
            and orderbook_readable
            and not existing_live_order_conflict
        )

        return ExecutionReadinessResult(
            intent_id=pre_execution_order.intent_id,
            intent_key=pre_execution_order.intent_key,
            position_key=pre_execution_order.position_key,
            client_order_id=pre_execution_order.client_order_id,
            ready=ready,
            reasons=reasons,
            price=pre_execution_order.price,
            size=pre_execution_order.size,
            notional=pre_execution_order.notional,
            condition_id=pre_execution_order.condition_id,
            token_id=pre_execution_order.token_id,
            outcome=pre_execution_order.outcome,
            side=pre_execution_order.side,
            validation_ok=validation.valid,
            auth_bootstrap_ok=self._auth_bootstrap_ok,
            balance_readable=balance_readable,
            allowance_readable=allowance_readable,
            orderbook_readable=orderbook_readable,
            existing_live_order_conflict=existing_live_order_conflict,
            funder_address=self._safe_account_summary.get("funder_address"),
            l2_source=self._safe_account_summary.get("l2_source"),
            balance_value=self._safe_account_summary.get("balance_value"),
            allowance_value=self._safe_account_summary.get("allowance_value"),
        )

    def _ensure_account_context(self) -> None:
        if self._account_context_loaded:
            return

        self._account_context_loaded = True

        try:
            bootstrap_summary = self.auth_client.bootstrap()
            balance_payload = self.auth_client.get_collateral_balance_allowance()

            balance_value = self._extract_named_value(balance_payload, "balance")
            allowance_value = self._extract_named_value(balance_payload, "allowance")

            self._auth_bootstrap_ok = True
            self._safe_account_summary = {
                "auth_bootstrap_ok": True,
                "host": bootstrap_summary.host,
                "chain_id": bootstrap_summary.chain_id,
                "signature_type": bootstrap_summary.signature_type,
                "funder_address": bootstrap_summary.funder_address,
                "l2_source": bootstrap_summary.l2_source,
                "balance_readable": balance_value is not None,
                "allowance_readable": allowance_value is not None,
                "balance_value": balance_value,
                "allowance_value": allowance_value,
                "balance_payload_keys": list(balance_payload.keys())
                if isinstance(balance_payload, dict)
                else [],
            }
        except PolymarketAuthBootstrapError as exc:
            self._auth_bootstrap_ok = False
            self._auth_error = f"auth_bootstrap_failed: {exc}"
            self._safe_account_summary = {
                "auth_bootstrap_ok": False,
                "balance_readable": False,
                "allowance_readable": False,
                "balance_value": None,
                "allowance_value": None,
                "error": self._auth_error,
            }
        except Exception as exc:
            self._auth_bootstrap_ok = False
            self._auth_error = f"auth_bootstrap_failed: {type(exc).__name__}: {exc}"
            self._safe_account_summary = {
                "auth_bootstrap_ok": False,
                "balance_readable": False,
                "allowance_readable": False,
                "balance_value": None,
                "allowance_value": None,
                "error": self._auth_error,
            }

    def _check_orderbook_readable(self, token_id: str) -> bool:
        if not token_id or not token_id.strip():
            return False

        try:
            result = self.public_client.get_order_book(token_id=token_id)
        except PolymarketPublicAPIError:
            return False
        except Exception:
            return False

        if not isinstance(result.data, dict):
            return False

        # Keep this mechanical: if a dict came back, public orderbook is readable enough.
        return True

    def _extract_named_value(self, payload: Any, name_fragment: str) -> str | None:
        if not isinstance(payload, dict):
            return None

        lowered_fragment = name_fragment.lower()
        for key, value in payload.items():
            if lowered_fragment in str(key).lower():
                text = self._string_or_none(value)
                if text is not None:
                    return text
        return None

    def _string_or_none(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None
