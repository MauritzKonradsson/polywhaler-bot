from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from polywhaler_bot.config import Settings


class PolymarketAuthBootstrapError(RuntimeError):
    """Raised when authenticated Polymarket client bootstrap fails."""


@dataclass(slots=True)
class AuthBootstrapSummary:
    host: str
    chain_id: int
    signature_type: int
    funder_address: str
    l2_source: str  # "provided" | "derived"
    has_l1_auth: bool
    has_l2_creds: bool


class PolymarketAuthClient:
    """
    Authenticated Polymarket client bootstrap wrapper.

    Scope:
    - authenticated client bootstrap only
    - read-only authenticated checks only
    - no order placement
    - no cancellation
    - no execution logic
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._client: Any | None = None
        self._bootstrap_summary: AuthBootstrapSummary | None = None

    @property
    def client(self) -> Any:
        if self._client is None:
            raise PolymarketAuthBootstrapError(
                "Authenticated client is not initialized. Call bootstrap() first."
            )
        return self._client

    @property
    def bootstrap_summary(self) -> AuthBootstrapSummary:
        if self._bootstrap_summary is None:
            raise PolymarketAuthBootstrapError(
                "Authenticated client is not initialized. Call bootstrap() first."
            )
        return self._bootstrap_summary

    def bootstrap(self) -> AuthBootstrapSummary:
        """
        Build an authenticated CLOB client.

        Rules:
        - L1 auth is always required for this milestone's authenticated bootstrap.
        - If L2 creds are present in env/config, use them.
        - Otherwise, derive/create L2 creds from L1 auth.
        - Keep secrets in memory only.
        """
        self._require_l1_auth()

        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds
        except ImportError as exc:
            raise PolymarketAuthBootstrapError(
                "py-clob-client is not installed. Install it with: pip install py-clob-client"
            ) from exc

        private_key = self.settings.polymarket_private_key
        if private_key is None:
            raise PolymarketAuthBootstrapError(
                "POLYMARKET_PRIVATE_KEY is required for authenticated bootstrap."
            )

        signature_type = self.settings.polymarket_signature_type
        funder_address = self.settings.polymarket_funder_address

        if signature_type is None:
            raise PolymarketAuthBootstrapError(
                "POLYMARKET_SIGNATURE_TYPE is required for authenticated bootstrap."
            )
        if not funder_address:
            raise PolymarketAuthBootstrapError(
                "POLYMARKET_FUNDER_ADDRESS is required for authenticated bootstrap."
            )

        client = ClobClient(
            self.settings.polymarket_clob_host,
            key=private_key.get_secret_value(),
            chain_id=self.settings.polymarket_chain_id,
            signature_type=signature_type,
            funder=funder_address,
        )

        if self.settings.has_polymarket_l2_creds:
            api_secret = self.settings.polymarket_api_secret
            api_passphrase = self.settings.polymarket_api_passphrase
            if api_secret is None or api_passphrase is None:
                raise PolymarketAuthBootstrapError(
                    "L2 credential fields are partially configured. "
                    "POLYMARKET_API_KEY, POLYMARKET_API_SECRET, and "
                    "POLYMARKET_API_PASSPHRASE must all be set together."
                )

            creds = ApiCreds(
                api_key=self.settings.polymarket_api_key,
                api_secret=api_secret.get_secret_value(),
                api_passphrase=api_passphrase.get_secret_value(),
            )
            client.set_api_creds(creds)
            l2_source = "provided"
        else:
            try:
                creds = client.create_or_derive_api_creds()
                client.set_api_creds(creds)
            except Exception as exc:
                raise PolymarketAuthBootstrapError(
                    f"Failed to create/derive L2 credentials from L1 auth: {exc}"
                ) from exc
            l2_source = "derived"

        self._client = client
        self._bootstrap_summary = AuthBootstrapSummary(
            host=self.settings.polymarket_clob_host,
            chain_id=self.settings.polymarket_chain_id,
            signature_type=signature_type,
            funder_address=funder_address,
            l2_source=l2_source,
            has_l1_auth=True,
            has_l2_creds=True,
        )
        return self._bootstrap_summary

    def get_collateral_balance_allowance(self) -> dict[str, Any]:
        """
        Read-only authenticated check for collateral balance/allowance.
        """
        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
        except ImportError as exc:
            raise PolymarketAuthBootstrapError(
                "py-clob-client is not installed. Install it with: pip install py-clob-client"
            ) from exc

        signature_type = self.settings.polymarket_signature_type
        if signature_type is None:
            raise PolymarketAuthBootstrapError(
                "POLYMARKET_SIGNATURE_TYPE is required for balance/allowance reads."
            )

        params = BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL,
            signature_type=signature_type,
        )

        try:
            result = self.client.get_balance_allowance(params=params)
        except Exception as exc:
            raise PolymarketAuthBootstrapError(
                f"Authenticated get_balance_allowance failed: {exc}"
            ) from exc

        if not isinstance(result, dict):
            raise PolymarketAuthBootstrapError(
                "Authenticated get_balance_allowance returned a non-dict response."
            )
        return result

    def get_open_orders(self) -> list[Any]:
        """
        Read-only authenticated check for open orders.
        """
        try:
            from py_clob_client.clob_types import OpenOrderParams
        except ImportError as exc:
            raise PolymarketAuthBootstrapError(
                "py-clob-client is not installed. Install it with: pip install py-clob-client"
            ) from exc

        try:
            result = self.client.get_orders(OpenOrderParams())
        except Exception as exc:
            raise PolymarketAuthBootstrapError(
                f"Authenticated get_orders failed: {exc}"
            ) from exc

        if isinstance(result, list):
            return result
        return [result]

    def get_trades(self) -> list[Any]:
        """
        Read-only authenticated check for user trades.
        """
        try:
            result = self.client.get_trades()
        except Exception as exc:
            raise PolymarketAuthBootstrapError(
                f"Authenticated get_trades failed: {exc}"
            ) from exc

        if isinstance(result, list):
            return result
        return [result]

    def safe_summary(self) -> dict[str, Any]:
        """
        Safe-to-print authenticated bootstrap summary with no secrets.
        """
        summary = self.bootstrap_summary
        return {
            "host": summary.host,
            "chain_id": summary.chain_id,
            "signature_type": summary.signature_type,
            "funder_address": summary.funder_address,
            "l2_source": summary.l2_source,
            "has_l1_auth": summary.has_l1_auth,
            "has_l2_creds": summary.has_l2_creds,
        }

    def _require_l1_auth(self) -> None:
        if not self.settings.has_polymarket_l1_auth:
            raise PolymarketAuthBootstrapError(
                "Authenticated bootstrap requires L1 auth config. "
                "Set POLYMARKET_PRIVATE_KEY, POLYMARKET_SIGNATURE_TYPE, "
                "and POLYMARKET_FUNDER_ADDRESS."
            )
