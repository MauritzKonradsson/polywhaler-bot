from __future__ import annotations

from pprint import pprint
from typing import Any

from polywhaler_bot.config import get_settings
from polywhaler_bot.polymarket_auth import (
    PolymarketAuthBootstrapError,
    PolymarketAuthClient,
)
from polywhaler_bot.polymarket_public import (
    PolymarketPublicAPIError,
    PolymarketPublicClient,
)


def normalize_address(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text.lower() if text else None


def summarize_profile(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "payload_type": type(payload).__name__,
            "keys": None,
            "address": None,
        }

    summary = {
        "keys": list(payload.keys())[:20],
        "name": payload.get("name"),
        "pseudonym": payload.get("pseudonym"),
        "proxyWallet": payload.get("proxyWallet"),
        "walletAddress": payload.get("walletAddress"),
        "profileImage": payload.get("profileImage"),
    }
    return summary


def summarize_position(position: dict[str, Any]) -> dict[str, Any]:
    return {
        "proxyWallet": position.get("proxyWallet"),
        "asset": position.get("asset"),
        "conditionId": position.get("conditionId"),
        "size": position.get("size"),
        "avgPrice": position.get("avgPrice"),
        "initialValue": position.get("initialValue"),
        "currentValue": position.get("currentValue"),
        "cashPnl": position.get("cashPnl"),
        "percentPnl": position.get("percentPnl"),
        "redeemable": position.get("redeemable"),
        "outcome": position.get("outcome"),
        "title": position.get("title"),
    }


def main() -> int:
    settings = get_settings()

    if not settings.polymarket_profile_address:
        print(
            "ERROR: POLYMARKET_PROFILE_ADDRESS is required for Step 8.5 "
            "account visibility validation."
        )
        return 1

    public_client = PolymarketPublicClient(settings=settings)
    auth_client = PolymarketAuthClient(settings=settings)

    print("=== Polymarket account visibility + positions validation ===")
    print(f"profile_address: {settings.polymarket_profile_address}")
    print(f"clob_host: {settings.polymarket_clob_host}")
    print(f"gamma_host: {settings.polymarket_gamma_host}")
    print(f"data_host: {settings.polymarket_data_host}")
    print()

    # ---------------------------------------------------------------------
    # 1) Authenticated bootstrap summary
    # ---------------------------------------------------------------------
    bootstrap = auth_client.bootstrap()
    funder_address = bootstrap.funder_address

    print("[1] Authenticated account bootstrap summary")
    pprint(auth_client.safe_summary())
    print()

    # ---------------------------------------------------------------------
    # 2) Public profile lookup
    # ---------------------------------------------------------------------
    profile_resp = public_client.get_public_profile(
        address=settings.polymarket_profile_address
    )

    print("[2] Public profile lookup")
    print(f"status=200 url={profile_resp.url}")
    profile_summary = summarize_profile(profile_resp.data)
    pprint(profile_summary)
    print()

    # ---------------------------------------------------------------------
    # 3) Current positions lookup
    # ---------------------------------------------------------------------
    positions_resp = public_client.get_current_positions(
        user=settings.polymarket_profile_address
    )

    positions_payload = positions_resp.data
    if not isinstance(positions_payload, list):
        raise PolymarketPublicAPIError(
            f"Expected positions payload to be a list, got {type(positions_payload).__name__}"
        )

    print("[3] Current positions lookup")
    print(f"status=200 url={positions_resp.url}")
    print(f"positions_count: {len(positions_payload)}")
    print("positions_sample:")
    for item in positions_payload[:3]:
        if isinstance(item, dict):
            pprint(summarize_position(item))
        else:
            pprint({"payload_type": type(item).__name__, "value": item})
    print()

    # ---------------------------------------------------------------------
    # 4) Identity consistency check
    # ---------------------------------------------------------------------
    normalized_profile = normalize_address(settings.polymarket_profile_address)
    normalized_funder = normalize_address(funder_address)

    profile_wallet_candidates = []
    if isinstance(profile_resp.data, dict):
        for key in ("proxyWallet", "walletAddress", "wallet", "address"):
            value = profile_resp.data.get(key)
            if isinstance(value, str) and value.strip():
                profile_wallet_candidates.append(value)

    normalized_profile_candidates = [
        normalize_address(v) for v in profile_wallet_candidates if normalize_address(v)
    ]

    profile_matches_funder = normalized_profile == normalized_funder
    public_profile_matches_funder = normalized_funder in normalized_profile_candidates

    position_wallets = sorted(
        {
            normalize_address(p.get("proxyWallet"))
            for p in positions_payload
            if isinstance(p, dict) and p.get("proxyWallet")
        }
    )
    position_wallets = [w for w in position_wallets if w]
    positions_match_funder = (not position_wallets) or (normalized_funder in position_wallets)

    print("[4] Identity consistency check")
    print(f"configured_profile_address: {settings.polymarket_profile_address}")
    print(f"authenticated_funder_address: {funder_address}")
    print(f"profile_address_matches_funder: {profile_matches_funder}")
    print(f"public_profile_matches_funder: {public_profile_matches_funder}")
    print(f"positions_match_funder: {positions_match_funder}")
    print(f"position_wallets_seen: {position_wallets[:10]}")
    print()

    # Strict milestone conclusion:
    # For this milestone we want the configured public profile address and the
    # authenticated funder address to resolve to the same logical account.
    if not profile_matches_funder:
        print(
            "WARNING: POLYMARKET_PROFILE_ADDRESS does not exactly match the "
            "authenticated funder address."
        )

    if not public_profile_matches_funder and isinstance(profile_resp.data, dict):
        print(
            "WARNING: Public profile payload does not clearly expose the same "
            "wallet address as the authenticated funder."
        )

    if not positions_match_funder:
        print(
            "WARNING: Returned positions do not appear to belong to the "
            "authenticated funder address."
        )

    # Hard fail only if the configured address and authenticated funder differ,
    # because Step 8.5 is meant to validate same-account visibility.
    if not profile_matches_funder:
        print(
            "\nRESULT: FAIL — configured public profile address does not match "
            "authenticated funder address."
        )
        return 1

    print("RESULT: PASS — public account visibility and positions validation succeeded.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (PolymarketPublicAPIError, PolymarketAuthBootstrapError) as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(1)
