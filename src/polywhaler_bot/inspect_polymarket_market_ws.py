from __future__ import annotations

from pprint import pprint

from polywhaler_bot.config import get_settings
from polywhaler_bot.polymarket_market_ws import (
    MARKET_WS_URL,
    PolymarketMarketWSClient,
    PolymarketMarketWSError,
)


def main() -> int:
    settings = get_settings()
    client = PolymarketMarketWSClient(settings=settings, timeout_seconds=15)

    print("=== Polymarket public market WebSocket validation ===")
    print(f"market_ws_url: {MARKET_WS_URL}")
    print(
        f"configured_test_token_id: "
        f"{settings.polymarket_test_token_id or '<not set>'}"
    )
    print(
        f"configured_test_market_slug: "
        f"{settings.polymarket_test_market_slug or '<not set>'}"
    )
    print()

    result = client.receive_first_event_sync()

    resolution = result["token_resolution"]
    event = result["event"]

    print("[1] Token resolution")
    print(f"token_id: {resolution['token_id']}")
    print(f"source: {resolution['source']}")
    print()

    print("[2] Subscription payload")
    pprint(result["subscription"])
    print()

    print("[3] First valid market event received")
    print(f"event_type: {event.get('event_type')}")
    pprint(event)
    print()

    print("Public market WebSocket validation completed successfully.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PolymarketMarketWSError as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(1)
