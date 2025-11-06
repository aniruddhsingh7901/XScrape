#!/usr/bin/env python3
"""
tws_pool_load_accounts.py

Bulk-load refreshed X accounts (with auth_token + ct0) into a twscrape pool DB,
assigning proxies per account (round-robin).

Input files:
- accounts_with_ct0.refreshed.txt lines (format):
  username:password:email:email_password:auth_token:ct0[:extra...]
- proxy.txt lines (format):
  host:port:user:pass

What it does:
- For each account, constructs cookies "auth_token=...; ct0=..."
- Adds account to the pool with its assigned proxy
- Reports totals (added/failed) and prints a few sample assignments

Usage:
  python scripts/tws_pool_load_accounts.py \\
    --accounts twitter/X_scrapping/accounts_with_ct0.refreshed.txt \\
    --proxies twitter/X_scrapping/proxy.txt \\
    --pool scripts/cache/twscrape_refresh.db \\
    --limit 100

Notes:
- This script only adds accounts; it does not attempt login.
- After loading, you can run:
    python scripts/tws_pool_status.py --pool scripts/cache/twscrape_refresh.db
    python scripts/tws_pool_reset_locks.py --pool scripts/cache/twscrape_refresh.db
    python scripts/twitter_one_shot.py --pool scripts/cache/twscrape_refresh.db --query "#bitcoin -filter:retweets" --limit 3
"""

import sys
from pathlib import Path
import argparse

# Ensure vendored twscrape import
BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR / "twitter" / "twscrape"))

from twscrape import API  # type: ignore


def read_accounts(path: Path):
    items = []
    if not path.exists():
        print(f"Accounts file not found: {path}")
        return items
    for ln in path.read_text(encoding="utf-8").splitlines():
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        parts = ln.split(":")
        if len(parts) < 6:
            continue
        username, password, email, email_password, auth_token, ct0, *rest = parts
        items.append({
            "username": username,
            "password": password,
            "email": email,
            "email_password": email_password,
            "auth_token": auth_token,
            "ct0": ct0,
        })
    return items


def read_proxies(path: Path):
    # host:port:user:pass -> http://user:pass@host:port
    proxies = []
    if not path.exists():
        return proxies
    for ln in path.read_text(encoding="utf-8").splitlines():
        ln = ln.strip()
        if not ln:
            continue
        parts = ln.split(":")
        if len(parts) != 4:
            continue
        host, port, user, pwd = parts
        proxies.append(f"http://{user}:{pwd}@{host}:{port}")
    return proxies


async def main_async():
    ap = argparse.ArgumentParser(description="Bulk-load accounts with ct0 into twscrape pool with proxies")
    ap.add_argument("--accounts", default="twitter/X_scrapping/accounts_with_ct0.refreshed.txt", help="Accounts file (with ct0)")
    ap.add_argument("--proxies", default="twitter/X_scrapping/proxy.txt", help="Proxy list file")
    ap.add_argument("--pool", default="scripts/cache/twscrape_refresh.db", help="Path to twscrape accounts DB")
    ap.add_argument("--limit", type=int, default=0, help="Max accounts to load (0 = all)")
    args = ap.parse_args()

    accounts_path = Path(args.accounts)
    proxies_path = Path(args.proxies)

    accounts = read_accounts(accounts_path)
    proxies = read_proxies(proxies_path)
    if not accounts:
        print("No accounts to load.")
        return

    if not proxies:
        print("Warning: no proxies found. Proceeding without proxies.")

    api = API(pool=args.pool)  # create pool DB if not exists

    added = 0
    failed = 0
    assignments = []

    p_idx = 0
    max_load = args.limit if args.limit > 0 else len(accounts)

    for i, acc in enumerate(accounts[:max_load], 1):
        cookies = ""
        if acc["auth_token"]:
            cookies += f"auth_token={acc['auth_token']}; "
        if acc["ct0"]:
            cookies += f"ct0={acc['ct0']}; "
        cookies = cookies.strip().strip(";")

        proxy = None
        if proxies:
            proxy = proxies[p_idx % len(proxies)]
            p_idx += 1

        try:
            await api.pool.add_account(
                username=acc["username"],
                password=acc["password"] or "x",
                email=acc["email"] or "x@example.invalid",
                email_password=acc["email_password"] or "x",
                cookies=cookies or None,
                proxy=proxy,
            )
            added += 1
            if i <= 10:  # keep a few samples to print
                assignments.append((acc["username"], proxy))
            print(f"[{i}/{max_load}] Added {acc['username']} (proxy={proxy})")
        except Exception as e:
            failed += 1
            print(f"[{i}/{max_load}] Failed to add {acc['username']}: {e}")

    print("\nSummary:")
    print(f"  Added: {added}")
    print(f"  Failed: {failed}")
    print(f"  Pool DB: {args.pool}")
    if assignments:
        print("\nSample assignments:")
        for u, px in assignments:
            print(f"  {u:>24} -> {px}")


def main():
    import asyncio
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
