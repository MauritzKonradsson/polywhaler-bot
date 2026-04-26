from __future__ import annotations

from pprint import pprint

from polywhaler_bot.config import get_settings
from polywhaler_bot.polymarket_auth import (
    PolymarketAuthBootstrapError,
    PolymarketAuthClient,
)


def main() -> int:
    settings = get_settings()
    client = PolymarketAuthClient(settings)

    print("=== Polymarket authenticated bootstrap inspection ===")
    print(f"clob_host: {settings.polymarket_clob_host}")
    print(f"chain_id: {settings.polymarket_chain_id}")
    print(
        f"has_l1_auth: {settings.has_polymarket_l1_auth} | "
        f"has_l2_creds: {settings.has_polymarket_l2_creds}"
    )
    print()

    summary = client.bootstrap()
    print("[1] Authenticated client bootstrap")
    pprint(client.safe_summary())
    print()

    print("[2] Collateral balance / allowance")
    balance = client.get_collateral_balance_allowance()
    pprint(balance)
    print()

    print("[3] Open orders (read-only)")
    try:
        open_orders = client.get_open_orders()
        print(f"open_orders_count: {len(open_orders)}")
        if open_orders:
            pprint(open_orders[:1])
    except PolymarketAuthBootstrapError as exc:
        print(f"open_orders_check_error: {exc}")
    print()

    print("[4] Trades (read-only)")
    try:
        trades = client.get_trades()
        print(f"trades_count: {len(trades)}")
        if trades:
            pprint(trades[:1])
    except PolymarketAuthBootstrapError as exc:
        print(f"trades_check_error: {exc}")
    print()

    print("Authenticated inspection completed successfully.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PolymarketAuthBootstrapError as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(1)
