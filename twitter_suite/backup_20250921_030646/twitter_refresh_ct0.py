#!/usr/bin/env python3
"""
twitter_refresh_ct0.py

Goal:
- Check which accounts are active (can make a simple X GraphQL call)
- If inactive, try to refresh (re-login) to generate a new ct0 cookie (if email/IMAP works)
- Save a refreshed accounts file with updated ct0 values for all accounts that pass

Supported input formats:
1) twitter/X_scrapping/accounts_with_ct0.txt lines (7 fields):
   username:password:email:email_password:auth_token:ct0:extra_hash
   - We use username,password,email,email_password,auth_token,ct0
   - "extra_hash" at the end is preserved if present (opaque carry-through)

2) twitter/X_scrapping/twitteracc.txt lines (5 fields):
   username:password:email:email_password:auth_token
   - No ct0 initially; script will try to login and generate ct0 if IMAP is supported

Notes:
- Uses vendored twscrape (twitter/twscrape) to add accounts, attach cookies, and run login/requests
- A simple "user_by_login('x')" or "search('e', limit=1')" is used as a healthcheck
- If a ct0+auth_token cookies combo fails, we attempt to relogin (if credentials permit)
- Updated ct0 values are written to an output file (default: twitter/X_scrapping/accounts_with_ct0.refreshed.txt)
- Optional proxy pool from twitter/X_scrapping/proxy.txt (host:port:user:pass), assigned round-robin per account

Usage examples:
  python scripts/twitter_refresh_ct0.py \\
    --accounts-in twitter/X_scrapping/accounts_with_ct0.txt \\
    --proxies twitter/X_scrapping/proxy.txt \\
    --accounts-out twitter/X_scrapping/accounts_with_ct0.refreshed.txt \\
    --limit 1 --verbose

  python scripts/twitter_refresh_ct0.py \\
    --accounts-in twitter/X_scrapping/twitteracc.txt \\
    --accounts-out twitter/X_scrapping/accounts_with_ct0.refreshed.txt \\
    --limit 1 --verbose
"""

import os
import sys
import asyncio
import json
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any

# Ensure vendored twscrape is importable
BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR / "twitter" / "twscrape"))
sys.path.insert(0, str(BASE_DIR))

VERBOSE = False

try:
    from twscrape import API  # type: ignore
    from twscrape.accounts_pool import AccountsPool  # type: ignore
    TWSCRAPE_AVAILABLE = True
except Exception as e:
    TWSCRAPE_AVAILABLE = False


def vprint(*args):
    if VERBOSE:
        print(*args)


def parse_accounts_line(line: str) -> Optional[Dict[str, Any]]:
    """
    Parse a line from either accounts_with_ct0.txt (7 fields) or twitteracc.txt (5 fields)
    Returns a dict with keys: username, password, email, email_password, auth_token, ct0, extra (optional)
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    parts = line.split(":")
    # accounts_with_ct0.txt: 7 fields minimum (some sources show 7th as a long hash)
    if len(parts) >= 6:
        # username:password:email:email_password:auth_token:ct0[:extra...]
        username, password, email, email_password, auth_token, ct0, *rest = parts
        extra = ":".join(rest) if rest else ""
        return {
            "username": username,
            "password": password,
            "email": email,
            "email_password": email_password,
            "auth_token": auth_token,
            "ct0": ct0,
            "extra": extra,
        }
    # twitteracc.txt: 5 fields
    if len(parts) == 5:
        username, password, email, email_password, auth_token = parts
        return {
            "username": username,
            "password": password,
            "email": email,
            "email_password": email_password,
            "auth_token": auth_token,
            "ct0": "",  # none yet
            "extra": "",
        }
    # unsupported
    return None


def read_accounts_file(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not path.exists():
        print(f"Accounts file not found: {path}")
        return out
    with path.open("r", encoding="utf-8") as f:
        for ln in f:
            acc = parse_accounts_line(ln)
            if acc:
                out.append(acc)
    return out


def read_proxies_file(path: Path) -> List[str]:
    """
    proxy.txt format: host:port:user:pass
    Returns proxy URLs like http://user:pass@host:port
    """
    proxies: List[str] = []
    if not path.exists():
        return proxies
    with path.open("r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            parts = ln.split(":")
            if len(parts) == 4:
                host, port, user, pwd = parts
                proxies.append(f"http://{user}:{pwd}@{host}:{port}")
    return proxies


async def simple_healthcheck(api: API, limit: int = 1) -> bool:
    """
    Try a very cheap request to confirm the account works:
      - user_by_login("x") OR a 1-tweet search ("e")
    """
    try:
        # user_by_login hits a very light endpoint
        user = await api.user_by_login("x")
        if user:
            return True
    except Exception:
        pass

    try:
        # fallback: tiny search
        async for _ in api.search("e", limit=limit):
            return True
    except Exception:
        pass

    return False


async def try_add_with_cookies(api: API, account: Dict[str, Any], proxy: Optional[str]) -> bool:
    """
    Add account to pool with cookies (auth_token + ct0).
    Returns True if added and healthcheck passed.
    """
    cookies = ""
    if account.get("auth_token"):
        cookies += f"auth_token={account['auth_token']}; "
    if account.get("ct0"):
        cookies += f"ct0={account['ct0']}; "
    cookies = cookies.strip().strip(";")

    try:
        await api.pool.add_account(
            username=account["username"],
            password=account.get("password") or "x",
            email=account.get("email") or "x@example.invalid",
            email_password=account.get("email_password") or "x",
            cookies=cookies or None,
            proxy=proxy or None,
        )
        vprint(f"  add_account with cookies: {account['username']}")
    except Exception as e:
        vprint(f"  add_account note: {e}")

    ok = await simple_healthcheck(api, limit=1)
    return ok


async def try_relogin(api: API, account: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Attempt to relogin via twscrape (IMAP flow). Returns (ok, new_ct0) where new_ct0 may be None.
    """
    try:
        # relogin resets cookies then login_all() for that user
        await api.pool.relogin(account["username"])
    except Exception as e:
        vprint(f"  relogin error: {e}")

    ok = await simple_healthcheck(api, limit=1)
    if not ok:
        return False, None

    # Extract ct0 from pool-stored cookies for the user
    try:
        acc = await api.pool.get(account["username"])
        ct0 = acc.cookies.get("ct0") if acc and hasattr(acc, "cookies") else None
        return True, ct0
    except Exception:
        return True, None


def build_output_line(account: Dict[str, Any], new_ct0: Optional[str]) -> str:
    """
    Write back to 'accounts_with_ct0' shape if possible.
    Preserve 'extra' if we parsed it from accounts_with_ct0.txt.
    """
    ct0_out = new_ct0 if (new_ct0 is not None and new_ct0 != "") else (account.get("ct0") or "")
    fields = [
        account.get("username", ""),
        account.get("password", ""),
        account.get("email", ""),
        account.get("email_password", ""),
        account.get("auth_token", ""),
        ct0_out,
    ]
    line = ":".join(fields)
    extra = account.get("extra", "")
    if extra:
        line += f":{extra}"
    return line


async def main_async():
    import argparse

    parser = argparse.ArgumentParser(description="Check active X accounts and refresh ct0 where possible.")
    parser.add_argument("--accounts-in", type=str, default="twitter/X_scrapping/accounts_with_ct0.txt", help="Input accounts file")
    parser.add_argument("--accounts-out", type=str, default="twitter/X_scrapping/accounts_with_ct0.refreshed.txt", help="Output refreshed accounts file")
    parser.add_argument("--proxies", type=str, default="twitter/X_scrapping/proxy.txt", help="Optional proxy list file")
    parser.add_argument("--pool-db", type=str, default="scripts/cache/twscrape_refresh.db", help="Path to twscrape accounts db")
    parser.add_argument("--limit", type=int, default=1, help="Healthcheck tweet limit")
    parser.add_argument("--verbose", action="store_true", help="Verbose logs")
    args = parser.parse_args()

    global VERBOSE
    VERBOSE = bool(args.verbose)

    if not TWSCRAPE_AVAILABLE:
        print("❌ twscrape not available. Ensure vendor is present (twitter/twscrape).")
        sys.exit(1)

    acc_path = Path(args.accounts_in)
    accounts = read_accounts_file(acc_path)
    if not accounts:
        print(f"❌ No accounts loaded from {acc_path}")
        sys.exit(1)

    proxies_list = read_proxies_file(Path(args.proxies))
    p_idx = 0

    # Make sure pool db dir exists
    pool_db_path = Path(args.pool_db)
    pool_db_path.parent.mkdir(parents=True, exist_ok=True)

    refreshed_lines: List[str] = []
    total = len(accounts)
    ok_count = 0
    refreshed_count = 0
    failed_count = 0

    for idx, account in enumerate(accounts, 1):
        print(f"[{idx}/{total}] Checking {account['username']} ...")
        proxy = proxies_list[p_idx % len(proxies_list)] if proxies_list else None
        if proxies_list:
            p_idx += 1

        # Fresh API instance per account (isolated pool DB)
        api = API(pool=str(pool_db_path))

        # Try cookies first if present
        ok = False
        new_ct0: Optional[str] = None
        if account.get("auth_token") and account.get("ct0"):
            ok = await try_add_with_cookies(api, account, proxy)
            if ok:
                ok_count += 1
                refreshed_lines.append(build_output_line(account, None))  # keep ct0 as-is
                print(f"  ✅ Active with cookies")
                continue
            else:
                print(f"  ⚠️ Cookies failed; attempting relogin...")

        # Try relogin (only if we have creds)
        if account.get("username") and account.get("password") and account.get("email") and account.get("email_password"):
            ok, new_ct0 = await try_relogin(api, account)
            if ok:
                ok_count += 1
                # Save updated ct0 if any
                if new_ct0:
                    refreshed_count += 1
                refreshed_lines.append(build_output_line(account, new_ct0))
                print(f"  ✅ Active after relogin (ct0 {'updated' if new_ct0 else 'unchanged'})")
                continue

        # Could not validate
        failed_count += 1
        refreshed_lines.append(build_output_line(account, None))
        print(f"  ❌ Inactive (could not authenticate or refresh)")

    out_path = Path(args.accounts_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        f.write("\n".join(refreshed_lines) + "\n")

    print("\nSummary:")
    print(f"  Total accounts:      {total}")
    print(f"  Active (cookies):    {ok_count - refreshed_count}")
    print(f"  Active (refreshed):  {refreshed_count}")
    print(f"  Inactive/failed:     {failed_count}")
    print(f"  Output written to:   {out_path}")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
