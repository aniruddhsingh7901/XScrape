#!/usr/bin/env python3
"""
twitter_acceptance_runner.py

End-to-end acceptance test for a 20-account sample:
- Refresh/validate ct0 cookies for the sample accounts
- Load accounts into a twscrape pool with per-account proxies
- Reset locks and show pool status
- Sanity scrape (one_shot) to print validator-compatible XContent JSON
- Small batch scrape to SQLite (DataEntity schema), then print DB counts

Usage:
  python scripts/twitter_acceptance_runner.py \\
    --accounts-in /path/to/accounts_with_ct0.sample.txt \\
    --proxies twitter/X_scrapping/proxy.txt \\
    --pool scripts/cache/twscrape_refresh.db \\
    --db scripts/twitter_miner_data.sqlite \\
    --one-shot-query "#bitcoin -filter:retweets" \\
    --batch-queries "#bitcoin -filter:retweets,#crypto -filter:retweets" \\
    --batch-limit 3 \\
    --limit 20 \\
    --verbose

Notes:
- Input line format must be:
    username:password:email:email_password:auth_token:ct0
- This script orchestrates existing tools we created:
    twitter_refresh_ct0.py
    tws_pool_load_accounts.py
    tws_pool_reset_locks.py
    tws_pool_status.py
    twitter_one_shot.py
    twitter_scrape_batch.py
"""

import argparse
import sqlite3
import subprocess
from pathlib import Path
import sys


def run(cmd: list[str], title: str, stop_on_error: bool = True):
    print(f"\n=== {title} ===")
    print("$ " + " ".join(cmd))
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Step failed: {title} (exit {e.returncode})")
        if stop_on_error:
            sys.exit(e.returncode)


def print_db_summary(db_path: Path, limit: int = 5):
    if not db_path.exists():
        print(f"[WARN] DB not found: {db_path}")
        return
    print(f"\n=== DB Summary: {db_path} ===")
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM DataEntity")
        total = cur.fetchone()[0]
        print("Total DataEntity rows:", total)
        print("Latest rows:")
        for row in cur.execute("SELECT uri, datetime, label FROM DataEntity ORDER BY datetime DESC LIMIT ?", (limit,)):
            print("  ", row)
    except Exception as e:
        print(f"[WARN] Could not summarize DB: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser(description="Run acceptance test for 20-account X sample")
    ap.add_argument("--accounts-in", required=True, help="Path to sample accounts file (username:password:email:email_password:auth_token:ct0)")
    ap.add_argument("--proxies", default="twitter/X_scrapping/proxy.txt", help="Proxy list file (host:port:user:pass)")
    ap.add_argument("--pool", default="scripts/cache/twscrape_refresh.db", help="Path to twscrape accounts DB")
    ap.add_argument("--db", default="scripts/twitter_miner_data.sqlite", help="SQLite DB output path")
    ap.add_argument("--one-shot-query", default="#bitcoin -filter:retweets", help="Query for one_shot sanity scrape")
    ap.add_argument("--batch-queries", default="#bitcoin -filter:retweets,#crypto -filter:retweets", help="Comma-separated queries for batch scrape")
    ap.add_argument("--batch-limit", type=int, default=3, help="Tweets per query in batch")
    ap.add_argument("--limit", type=int, default=20, help="Max accounts to load into pool")
    ap.add_argument("--verbose", action="store_true", help="Verbose mode")
    args = ap.parse_args()

    accounts_in = Path(args.accounts_in).resolve()
    accounts_out = accounts_in.with_suffix(".refreshed.txt")
    proxies = Path(args.proxies).resolve()
    pool = Path(args.pool).resolve()
    db = Path(args.db).resolve()

    # 1) Refresh/validate ct0
    run([
        sys.executable, "scripts/twitter_refresh_ct0.py",
        "--accounts-in", str(accounts_in),
        "--accounts-out", str(accounts_out),
        "--proxies", str(proxies),
        "--limit", "1" if not args.verbose else "1",
        "--verbose" if args.verbose else ""
    ], "Refresh/validate ct0", stop_on_error=False)

    # 2) Load into pool with per-account proxies
    run([
        sys.executable, "scripts/tws_pool_load_accounts.py",
        "--accounts", str(accounts_out if accounts_out.exists() else accounts_in),
        "--proxies", str(proxies),
        "--pool", str(pool),
        "--limit", str(args.limit)
    ], "Load accounts into pool with proxies")

    # 3) Reset locks and show status
    run([
        sys.executable, "scripts/tws_pool_reset_locks.py",
        "--pool", str(pool)
    ], "Reset pool locks", stop_on_error=False)

    run([
        sys.executable, "scripts/tws_pool_status.py",
        "--pool", str(pool),
        "--queue", "SearchTimeline",
        "--limit", "12"
    ], "Pool status", stop_on_error=False)

    # 4) Sanity scrape (one_shot)
    run([
        sys.executable, "scripts/twitter_one_shot.py",
        "--pool", str(pool),
        "--query", args.one_shot_query,
        "--limit", "3",
        "--verbose"
    ], "One-shot sanity scrape (XContent JSON)", stop_on_error=False)

    # 5) Batch scrape to SQLite
    run([
        sys.executable, "scripts/twitter_scrape_batch.py",
        "--pool", str(pool),
        "--queries", args.batch_queries,
        "--limit-per-query", str(args.batch_limit),
        "--db", str(db),
        "--verbose" if args.verbose else ""
    ], "Batch scrape to SQLite (DataEntity schema)", stop_on_error=False)

    # 6) DB summary
    print_db_summary(db, limit=5)

    print("\n=== Acceptance run completed ===")
    print("If results look good (valid JSON, some DB inserts, low locks), we can scale to 120–150 accounts and ramp req/min.")


if __name__ == "__main__":
    main()
