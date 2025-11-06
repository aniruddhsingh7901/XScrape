#!/usr/bin/env python3
"""
cleanup_db.py

Delete DataEntity rows older than a given age (default: 30 days) from the miner SQLite DB.
Uses the timeBucketId (hour bucket) for fast range deletes.

Usage:
  python scripts/cleanup_db.py --db SqliteMinerStorage.sqlite --max-age-days 30 --verbose
Optional:
  --vacuum     Run VACUUM after deletion to reclaim space (offline, may lock DB)
  --dry-run    Print how many rows would be deleted without actually deleting

Notes:
- timeBucketId is number of hours since epoch (UTC). We delete rows where timeBucketId < cutoffBucket.
- cutoffBucket = floor((now_utc - max_age_days)/3600).
"""

import sys
import sqlite3
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta


def compute_cutoff_bucket(max_age_days: int) -> int:
    now = datetime.now(timezone.utc)
    cutoff_dt = now - timedelta(days=max(0, max_age_days))
    cutoff_bucket = int(cutoff_dt.timestamp() // 3600)
    return cutoff_bucket


def count_rows(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM DataEntity")
    (n,) = cur.fetchone()
    return int(n or 0)


def count_older(conn: sqlite3.Connection, cutoff_bucket: int) -> int:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM DataEntity WHERE timeBucketId < ?", (cutoff_bucket,))
    (n,) = cur.fetchone()
    return int(n or 0)


def delete_older(conn: sqlite3.Connection, cutoff_bucket: int) -> int:
    cur = conn.cursor()
    cur.execute("DELETE FROM DataEntity WHERE timeBucketId < ?", (cutoff_bucket,))
    return cur.rowcount if cur.rowcount is not None else 0


def vacuum(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("VACUUM")


def main():
    ap = argparse.ArgumentParser(description="Cleanup DataEntity rows older than N days")
    ap.add_argument("--db", type=str, default="SqliteMinerStorage.sqlite", help="Path to miner SQLite DB")
    ap.add_argument("--max-age-days", type=int, default=30, help="Delete rows older than this many days (default 30)")
    ap.add_argument("--vacuum", action="store_true", help="Run VACUUM after deletion")
    ap.add_argument("--dry-run", action="store_true", help="Only print counts; do not delete")
    ap.add_argument("--verbose", action="store_true", help="Verbose logs")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"[ERROR] DB not found: {db_path}")
        sys.exit(1)

    cutoff_bucket = compute_cutoff_bucket(int(args.max_age_days))
    if args.verbose:
        now = datetime.now(timezone.utc)
        cutoff_dt = datetime.fromtimestamp(cutoff_bucket * 3600, tz=timezone.utc)
        print(f"[INFO] Now (UTC):      {now.isoformat()}")
        print(f"[INFO] Max age (days): {args.max_age_days}")
        print(f"[INFO] Cutoff bucket:  {cutoff_bucket} -> {cutoff_dt.isoformat()} (rows older than this will be deleted)")

    conn = sqlite3.connect(str(db_path))
    try:
        total_before = count_rows(conn)
        older = count_older(conn, cutoff_bucket)
        print(f"[INFO] Total rows: {total_before}")
        print(f"[INFO] Rows older than {args.max_age_days}d: {older}")

        if args.dry_run:
            print("[DRY-RUN] No deletion performed.")
            return

        deleted = delete_older(conn, cutoff_bucket)
        conn.commit()
        total_after = count_rows(conn)
        print(f"[OK] Deleted rows: {deleted}")
        print(f"[OK] Rows remaining: {total_after}")

        if args.vacuum:
            if args.verbose:
                print("[INFO] Running VACUUM ...")
            vacuum(conn)
            print("[OK] VACUUM complete.")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
