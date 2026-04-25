from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from polywhaler_bot.config import Settings


DEFAULT_TIMEOUT_SECONDS = 15


class PolymarketPublicAPIError(RuntimeError):
    """
    Raised when a public Polymarket API request fails due to HTTP/network/JSON issues.
    """


@dataclass(slots=True)
class PublicResponse:
    """
    Lightweight response wrapper for debugging and inspection.
    """
    url: str
    status: int
    data: Any


class PolymarketPublicClient:
    """
    Public, read-only Polymarket REST client.

    Scope:
    - Gamma API market discovery
    - CLOB simplified markets lookup
    - CLOB orderbook snapshot lookup
    - Data API public profile / positions lookup

    Out of scope:
    - authentication
    - private keys / L2 credentials
    - trading / execution
    - websockets
    """

    def __init__(
        self,
        settings: Settings,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self.settings = settings
        self.timeout_seconds = timeout_seconds

    # -------------------------------------------------------------------------
    # Generic GET helper
    # -------------------------------------------------------------------------
    def _get_json(
        self,
        *,
        base_url: str,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> PublicResponse:
        query = urlencode(
            {k: v for k, v in (params or {}).items() if v is not None},
            doseq=True,
        )
        url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
        if query:
            url = f"{url}?{query}"

        req = Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "polywhaler-bot-public-client/0.1",
            },
            method="GET",
        )

        try:
            with urlopen(req, timeout=self.timeout_seconds) as resp:
                status = int(resp.status)
                body = resp.read().decode("utf-8")
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise PolymarketPublicAPIError(
                f"HTTP {exc.code} for GET {url}: {body[:300]}"
            ) from exc
        except URLError as exc:
            raise PolymarketPublicAPIError(
                f"Network error for GET {url}: {exc}"
            ) from exc
        except Exception as exc:
            raise PolymarketPublicAPIError(
                f"Unexpected error for GET {url}: {exc}"
            ) from exc

        if status != 200:
            raise PolymarketPublicAPIError(
                f"Unexpected status {status} for GET {url}"
            )

        try:
            data = json.loads(body)
        except json.JSONDecodeError as exc:
            raise PolymarketPublicAPIError(
                f"Invalid JSON from GET {url}: {exc}"
            ) from exc

        return PublicResponse(url=url, status=status, data=data)

    # -------------------------------------------------------------------------
    # Gamma API
    # -------------------------------------------------------------------------
    def get_gamma_markets(
        self,
        *,
        params: dict[str, Any] | None = None,
    ) -> PublicResponse:
        """
        Gamma API market discovery.
        """
        return self._get_json(
            base_url=self.settings.polymarket_gamma_host,
            path="/markets",
            params=params,
        )

    # -------------------------------------------------------------------------
    # CLOB public REST
    # -------------------------------------------------------------------------
    def get_simplified_markets(
        self,
        *,
        params: dict[str, Any] | None = None,
    ) -> PublicResponse:
        """
        Public CLOB simplified markets lookup.
        """
        return self._get_json(
            base_url=self.settings.polymarket_clob_host,
            path="/simplified-markets",
            params=params,
        )

    def get_order_book(
        self,
        *,
        token_id: str,
    ) -> PublicResponse:
        """
        Public CLOB order book snapshot for a token ID.
        """
        if not token_id:
            raise ValueError("token_id is required")

        return self._get_json(
            base_url=self.settings.polymarket_clob_host,
            path="/book",
            params={"token_id": token_id},
        )

    # -------------------------------------------------------------------------
    # Data / public profile endpoints
    # -------------------------------------------------------------------------
    def get_public_profile(
        self,
        *,
        address: str,
    ) -> PublicResponse:
        """
        Public profile lookup by wallet address.
        """
        if not address:
            raise ValueError("address is required")

        return self._get_json(
            base_url=self.settings.polymarket_gamma_host,
            path="/public-profile",
            params={"address": address},
        )

    def get_current_positions(
        self,
        *,
        user: str,
        market: str | None = None,
        event_id: str | None = None,
        size_threshold: float | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> PublicResponse:
        """
        Public current positions lookup by user/profile address.
        """
        if not user:
            raise ValueError("user is required")

        params: dict[str, Any] = {
            "user": user,
            "market": market,
            "eventId": event_id,
            "sizeThreshold": size_threshold,
            "limit": limit,
            "offset": offset,
        }

        return self._get_json(
            base_url=self.settings.polymarket_data_host,
            path="/positions",
            params=params,
        )
