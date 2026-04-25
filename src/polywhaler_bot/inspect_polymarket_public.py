from __future__ import annotations

from pprint import pprint

from polywhaler_bot.config import get_settings
from polywhaler_bot.polymarket_public import (
    PolymarketPublicAPIError,
    PolymarketPublicClient,
)


def summarize_payload(name: str, payload):
    if isinstance(payload, list):
        print(f"{name}: list(len={len(payload)})")
        if payload:
            first = payload[0]
            if isinstance(first, dict):
                print(f"{name} first item keys: {list(first.keys())[:20]}")
            else:
                print(f"{name} first item type: {type(first).__name__}")
    elif isinstance(payload, dict):
        print(f"{name}: dict(keys={list(payload.keys())[:20]})")
    else:
        print(f"{name}: {type(payload).__name__}")


def main() -> int:
    settings = get_settings()
    client = PolymarketPublicClient(settings=settings)

    print("=== Polymarket public connectivity inspection ===")
    print(f"gamma_host: {settings.polymarket_gamma_host}")
    print(f"clob_host: {settings.polymarket_clob_host}")
    print(f"data_host: {settings.polymarket_data_host}")
    print()

    # 1) Gamma markets
    print("[1] Gamma markets")
    gamma = client.get_gamma_markets(params={"limit": 5})
    print(f"status=200 url={gamma.url}")
    summarize_payload("gamma_markets", gamma.data)
    print()

    # 2) CLOB simplified markets
    print("[2] CLOB simplified markets")
    params = {"limit": 5}
    if settings.polymarket_test_market_slug:
        params["slug"] = settings.polymarket_test_market_slug
    simplified = client.get_simplified_markets(params=params)
    print(f"status=200 url={simplified.url}")
    summarize_payload("simplified_markets", simplified.data)
    print()

    # 3) Optional orderbook snapshot
    if settings.polymarket_test_token_id:
        print("[3] CLOB order book")
        order_book = client.get_order_book(token_id=settings.polymarket_test_token_id)
        print(f"status=200 url={order_book.url}")
        summarize_payload("order_book", order_book.data)
        print()
    else:
        print("[3] CLOB order book skipped (POLYMARKET_TEST_TOKEN_ID not set)")
        print()

    # 4) Optional public profile + positions
    if settings.polymarket_profile_address:
        print("[4] Public profile")
        profile = client.get_public_profile(address=settings.polymarket_profile_address)
        print(f"status=200 url={profile.url}")
        summarize_payload("public_profile", profile.data)
        print()

        print("[5] Current positions")
        positions = client.get_current_positions(user=settings.polymarket_profile_address)
        print(f"status=200 url={positions.url}")
        summarize_payload("current_positions", positions.data)
        print()
    else:
        print("[4] Public profile skipped (POLYMARKET_PROFILE_ADDRESS not set)")
        print("[5] Current positions skipped (POLYMARKET_PROFILE_ADDRESS not set)")
        print()

    print("Public connectivity inspection completed successfully.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PolymarketPublicAPIError as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(1)
