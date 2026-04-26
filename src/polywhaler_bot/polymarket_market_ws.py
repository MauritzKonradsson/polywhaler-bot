from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

import websockets

from polywhaler_bot.config import Settings
from polywhaler_bot.polymarket_public import (
    PolymarketPublicAPIError,
    PolymarketPublicClient,
)

MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class PolymarketMarketWSError(RuntimeError):
    """Raised when the public market WebSocket inspection flow fails."""


@dataclass(slots=True)
class ResolvedToken:
    token_id: str
    source: str
    details: dict[str, Any]


class PolymarketMarketWSClient:
    """
    Public market WebSocket validation helper.

    Scope:
    - public market channel only
    - no auth
    - no trading
    - no DB writes
    """

    def __init__(
        self,
        settings: Settings,
        *,
        timeout_seconds: int = 15,
    ) -> None:
        self.settings = settings
        self.timeout_seconds = timeout_seconds
        self.public_client = PolymarketPublicClient(settings=settings)

    def resolve_token_id(self) -> ResolvedToken:
        """
        Resolve a token ID for market-channel validation.

        Priority:
        1. POLYMARKET_TEST_TOKEN_ID if set
        2. First token_id from public simplified-markets lookup
        """
        if self.settings.polymarket_test_token_id:
            return ResolvedToken(
                token_id=self.settings.polymarket_test_token_id,
                source="env",
                details={"token_id": self.settings.polymarket_test_token_id},
            )

        simplified = self.public_client.get_simplified_markets(params={"limit": 1})
        payload = simplified.data

        if not isinstance(payload, dict):
            raise PolymarketMarketWSError(
                f"Expected simplified-markets payload to be dict, got {type(payload).__name__}"
            )

        data = payload.get("data")
        if not isinstance(data, list) or not data:
            raise PolymarketMarketWSError(
                "simplified-markets returned no market data; cannot derive a token_id"
            )

        market = data[0]
        if not isinstance(market, dict):
            raise PolymarketMarketWSError(
                "simplified-markets returned an unexpected market item shape"
            )

        tokens = market.get("tokens")
        if not isinstance(tokens, list) or not tokens:
            raise PolymarketMarketWSError(
                "simplified-markets returned a market with no tokens[]"
            )

        token = tokens[0]
        if not isinstance(token, dict):
            raise PolymarketMarketWSError(
                "simplified-markets returned an unexpected token item shape"
            )

        token_id = token.get("token_id")
        if not isinstance(token_id, str) or not token_id.strip():
            raise PolymarketMarketWSError(
                "simplified-markets token entry did not contain a usable token_id"
            )

        return ResolvedToken(
            token_id=token_id.strip(),
            source="simplified_markets",
            details={
                "market": market,
                "token": token,
                "lookup_url": simplified.url,
            },
        )

    async def receive_first_event(self) -> dict[str, Any]:
        """
        Connect to the public market channel, subscribe to one token ID, and
        return the first payload that contains an event_type.
        """
        resolved = self.resolve_token_id()
        subscription = {
            "assets_ids": [resolved.token_id],
            "type": "market",
            "custom_feature_enabled": True,
        }

        try:
            async with websockets.connect(
                MARKET_WS_URL,
                open_timeout=self.timeout_seconds,
                close_timeout=5,
                ping_interval=20,
                ping_timeout=20,
                max_size=2**20,
            ) as ws:
                await ws.send(json.dumps(subscription))

                while True:
                    raw_message = await asyncio.wait_for(
                        ws.recv(),
                        timeout=self.timeout_seconds,
                    )

                    try:
                        payload = json.loads(raw_message)
                    except json.JSONDecodeError:
                        continue

                    print("WS_MESSAGE:", payload)

if isinstance(payload, list) and payload:
    first = payload[0]
    if isinstance(first, dict):
        return {
            "token_resolution": {
                "token_id": resolved.token_id,
                "source": resolved.source,
                "details": resolved.details,
            },
            "subscription": subscription,
            "event": first,
        }

if isinstance(payload, dict):
    if "event_type" in payload or "asset_id" in payload or "market" in payload:
        return {
            "token_resolution": {
                "token_id": resolved.token_id,
                "source": resolved.source,
                "details": resolved.details,
            },
            "subscription": subscription,
            "event": payload,
        }
                            "token_resolution": {
                                "token_id": resolved.token_id,
                                "source": resolved.source,
                                "details": resolved.details,
                            },
                            "subscription": subscription,
                            "event": payload,
                        }

        except asyncio.TimeoutError as exc:
            raise PolymarketMarketWSError(
                f"Timed out waiting for a market event on token_id={resolved.token_id}"
            ) from exc
        except PolymarketPublicAPIError as exc:
            raise PolymarketMarketWSError(
                f"Token resolution failed via public market lookup: {exc}"
            ) from exc
        except websockets.WebSocketException as exc:
            raise PolymarketMarketWSError(
                f"Public market WebSocket failed: {exc}"
            ) from exc
        except Exception as exc:
            raise PolymarketMarketWSError(
                f"Unexpected public market WebSocket failure: {exc}"
            ) from exc

    def receive_first_event_sync(self) -> dict[str, Any]:
        return asyncio.run(self.receive_first_event())
