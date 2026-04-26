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
    pass


@dataclass(slots=True)
class ResolvedToken:
    token_id: str
    source: str
    details: dict[str, Any]


class PolymarketMarketWSClient:
    def __init__(self, settings: Settings, *, timeout_seconds: int = 15) -> None:
        self.settings = settings
        self.timeout_seconds = timeout_seconds
        self.public_client = PolymarketPublicClient(settings=settings)

    def resolve_token_id(self) -> ResolvedToken:
        if self.settings.polymarket_test_token_id:
            return ResolvedToken(
                token_id=self.settings.polymarket_test_token_id,
                source="env",
                details={"token_id": self.settings.polymarket_test_token_id},
            )

        simplified = self.public_client.get_simplified_markets(params={"limit": 1})
        payload = simplified.data

        if isinstance(payload, dict):
            data = payload.get("data")
        elif isinstance(payload, list):
            data = payload
        else:
            raise PolymarketMarketWSError("Invalid simplified-markets payload")

        if not data:
            raise PolymarketMarketWSError("No markets returned")

        market = data[0]
        token = market["tokens"][0]

        return ResolvedToken(
            token_id=token["token_id"],
            source="simplified_markets",
            details={"market": market, "token": token},
        )

    async def receive_first_event(self) -> dict[str, Any]:
        resolved = self.resolve_token_id()

        subscription = {
            "assets_ids": [resolved.token_id],
            "type": "market",
            "custom_feature_enabled": True,
        }

        try:
            async with websockets.connect(MARKET_WS_URL) as ws:
                await ws.send(json.dumps(subscription))

                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=self.timeout_seconds)

                    try:
                        payload = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    print("WS_MESSAGE:", payload)

                    # 🔥 HANDLE LIST PAYLOADS (MAIN FIX)
                    if isinstance(payload, list):
                        if not payload:
                            continue

                        for item in payload:
                            if isinstance(item, dict):
                                return {
                                    "token_resolution": {
                                        "token_id": resolved.token_id,
                                        "source": resolved.source,
                                        "details": resolved.details,
                                    },
                                    "subscription": subscription,
                                    "event": item,
                                }

                    # 🔥 HANDLE DICT PAYLOADS
                    if isinstance(payload, dict):
                        if (
                            "event_type" in payload
                            or "asset_id" in payload
                            or "market" in payload
                            or "bids" in payload
                            or "asks" in payload
                        ):
                            return {
                                "token_resolution": {
                                    "token_id": resolved.token_id,
                                    "source": resolved.source,
                                    "details": resolved.details,
                                },
                                "subscription": subscription,
                                "event": payload,
                            }

        except asyncio.TimeoutError:
            raise PolymarketMarketWSError(
                f"Timed out waiting for event on token {resolved.token_id}"
            )
        except PolymarketPublicAPIError as exc:
            raise PolymarketMarketWSError(f"Public API error: {exc}")
        except Exception as exc:
            raise PolymarketMarketWSError(f"Unexpected WS error: {exc}")

    def receive_first_event_sync(self) -> dict[str, Any]:
        return asyncio.run(self.receive_first_event())
