#!/usr/bin/env python3
"""
tws_pool_status.py

Inspect a twscrape pool DB:
- Show total/active/inactive counts
- Show how many accounts are currently locked for a given queue (default: SearchTimeline)
- Show next available time for that queue
- List a few example accounts with their lock timestamp and last_used

Usage:
  python scripts/tws_pool_status.py --pool scripts/cache/twscrape_refresh.db --queue SearchTimeline
"""

import sys
from pathlib import Path
import argparse
from datetime import datetime, timezone

# Ensure vendored twscrape import
BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR / "twitter" / "twscrape"))

from twscrape.accounts_pool import AccountsPool  # type: ignore

def _ts(d: datetime | None) -> float:
    """
    Safe timestamp for sorting:
    - None -> 0
    - Naive -> assume UTC
    - Aware -> use as-is
    """
    if d is None:
        return 0.0
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.timestamp()


def main():
    ap = argparse.ArgumentParser(description="Show twscrape pool status")
    ap.add_argument("--pool", required=True, help="Path to twscrape accounts DB")
    ap.add_argument("--queue", default="SearchTimeline", help="Queue name to inspect, e.g., SearchTimeline")
    ap.add_argument("--limit", type=int, default=8, help="How many example accounts to print")
    args = ap.parse_args()

    pool = AccountsPool(db_file=args.pool, raise_when_no_account=False)
    stats = {}
    try:
        import asyncio
        stats = asyncio.run(pool.stats())
    except Exception as e:
        print(f"Failed to read stats: {e}")

    print(f"Pool: {args.pool}")
    if stats:
        print("Global stats:")
        for k, v in stats.items():
            print(f"  {k:>16}: {v}")

    # Next available time for queue
    try:
        import asyncio
        nxt = asyncio.run(pool.next_available_at(args.queue))
        print(f"\nQueue: {args.queue}")
        print(f"Next available at: {nxt}")
    except Exception as e:
        print(f"Failed to compute next_available_at: {e}")

    # Inspect accounts for per-account lock timestamps
    try:
        import asyncio
        accounts = asyncio.run(pool.get_all())
        # Filter and sort
        locked = []
        unlocked = []
        for acc in accounts:
            # acc.locks is dict[str, datetime] (from Account.from_rs)
            when = None
            if acc.locks and args.queue in acc.locks:
                when = acc.locks.get(args.queue)
            item = (
                acc.username,
                acc.active,
                acc.last_used,
                when,
            )
            if when:
                locked.append(item)
            else:
                unlocked.append(item)

        print(f"\nLocked accounts for {args.queue}: {len(locked)}")
        for u, act, last_used, when in sorted(locked, key=lambda x: _ts(x[3]))[: args.limit]:
            print(f"  {u:>24} | active={act} | last_used={last_used} | unlock={when}")

        print(f"\nUnlocked accounts for {args.queue}: {len(unlocked)}")
        for u, act, last_used, when in sorted(unlocked, key=lambda x: _ts(x[2]), reverse=True)[: args.limit]:
            print(f"  {u:>24} | active={act} | last_used={last_used} | unlock=None")

    except Exception as e:
        print(f"Failed to enumerate accounts: {e}")


if __name__ == "__main__":
    main()
