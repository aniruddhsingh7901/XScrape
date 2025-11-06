#!/usr/bin/env python3
"""
twitter_suite/report.py

Pool reporting and CSV/JSON export for production monitoring:
- Summarize twscrape pool (total, active, inactive, locked per queue)
- Export per-account status CSV (username, active, last_used, proxy, locked_queues, cookie presence)
- Optional: include scrape DB (DataEntity) summary

Outputs:
- <out_dir>/accounts_status.csv
- <out_dir>/pool_metrics.json
- Console summary

Usage:
  python twitter_suite/report.py \
    --pool scripts/cache/twscrape_refresh.db \
    --out-dir twitter_suite/reports \
    --queues SearchTimeline \
    --db scripts/twitter_miner_data.sqlite \
    --verbose
"""

import csv
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import sys
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "twitter" / "twscrape"))

try:
    from twscrape.accounts_pool import AccountsPool  # type: ignore
    TWSCRAPE_AVAILABLE = True
except Exception:
    TWSCRAPE_AVAILABLE = False

def to_iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # Normalize to Z suffix
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def has_cookie(acc: Any, key: str) -> bool:
    try:
        return bool(getattr(acc, "cookies", {}).get(key))
    except Exception:
        return False

async def gather_pool_data(pool_path: str, queues: List[str], verbose: bool) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    pool = AccountsPool(db_file=pool_path, raise_when_no_account=False)
    metrics: Dict[str, Any] = {"pool_path": pool_path, "queues": queues}
    try:
        stats = await pool.stats()
    except Exception as e:
        stats = {}
        if verbose:
            print(f"[WARN] pool.stats() failed: {e}")
    metrics["stats"] = stats

    # Next-available per queue
    next_available: Dict[str, Optional[str]] = {}
    for q in queues:
        try:
            nxt = await pool.next_available_at(q)
            next_available[q] = to_iso(nxt) if nxt else None
        except Exception as e:
            next_available[q] = None
            if verbose:
                print(f"[WARN] next_available_at({q}) failed: {e}")
    metrics["next_available_at"] = next_available

    # Per-account rows
    rows: List[Dict[str, Any]] = []
    try:
        accounts = await pool.get_all()
    except Exception as e:
        if verbose:
            print(f"[ERROR] pool.get_all() failed: {e}")
        accounts = []

    # Aggregate locked counts per queue
    locked_counts = {q: 0 for q in queues}

    for acc in accounts:
        # Basic fields
        username = getattr(acc, "username", "")
        active = bool(getattr(acc, "active", False))
        last_used = getattr(acc, "last_used", None)
        proxy = getattr(acc, "proxy", None)
        cookies = getattr(acc, "cookies", {}) or {}
        locks = getattr(acc, "locks", {}) or {}

        # Locked queues and unlock times (ISO)
        locked_queues: List[str] = []
        unlock_times: Dict[str, Optional[str]] = {}
        for q in queues:
            unlock_dt = locks.get(q)
            if unlock_dt:
                locked_queues.append(q)
                unlock_times[q] = to_iso(unlock_dt)
                locked_counts[q] = locked_counts.get(q, 0) + 1
            else:
                unlock_times[q] = None

        row = {
            "username": username,
            "active": active,
            "last_used": to_iso(last_used),
            "proxy": proxy,
            "has_auth_token": bool(cookies.get("auth_token")),
            "has_ct0": bool(cookies.get("ct0")),
            "locked_queues": ",".join(locked_queues) if locked_queues else "",
        }
        # Include per-queue unlock columns
        for q in queues:
            col = f"unlock_at__{q}"
            row[col] = unlock_times.get(q)

        rows.append(row)

    metrics["locked_counts"] = locked_counts
    metrics["total_accounts"] = len(rows)
    metrics["active_accounts"] = sum(1 for r in rows if r["active"])
    metrics["inactive_accounts"] = metrics["total_accounts"] - metrics["active_accounts"]

    return metrics, rows

def summarize_db(db_path: Path, verbose: bool) -> Dict[str, Any]:
    out: Dict[str, Any] = {"db_path": str(db_path)}
    if not db_path.exists():
        out["present"] = False
        return out
    out["present"] = True
    try:
        import sqlite3
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM DataEntity")
        total = cur.fetchone()[0]
        out["dataentity_count"] = total
        # latest few
        latest: List[Tuple[str, str, Optional[str]]] = []
        for row in cur.execute("SELECT uri, datetime, label FROM DataEntity ORDER BY datetime DESC LIMIT 5"):
            latest.append(tuple(row))
        out["latest"] = latest
        conn.close()
    except Exception as e:
        if verbose:
            print(f"[WARN] DB summarize failed: {e}")
    return out

def write_csv(rows: List[Dict[str, Any]], out_csv: Path):
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    # Collect fieldnames from keys union to include queue-specific columns
    fieldnames: List[str] = []
    for r in rows:
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def write_json(obj: Dict[str, Any], out_json: Path):
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def main():
    ap = argparse.ArgumentParser(description="Export twscrape pool status and account CSV + metrics JSON")
    ap.add_argument("--pool", required=True, help="Path to twscrape pool DB (e.g., scripts/cache/twscrape_refresh.db)")
    ap.add_argument("--out-dir", default="twitter_suite/reports", help="Output directory for CSV/JSON")
    ap.add_argument("--queues", default="SearchTimeline", help="Comma-separated queue names (default: SearchTimeline)")
    ap.add_argument("--db", default="", help="Optional path to SQLite (DataEntity) to summarize")
    ap.add_argument("--verbose", action="store_true", help="Verbose logs")
    args = ap.parse_args()

    if not TWSCRAPE_AVAILABLE:
        print("❌ twscrape not importable. Ensure vendor is present at twitter/twscrape.")
        sys.exit(1)

    queues = [q.strip() for q in args.queues.split(",") if q.strip()]
    out_dir = Path(args.out_dir)

    import asyncio
    metrics, rows = asyncio.run(gather_pool_data(args.pool, queues, args.verbose))

    # Optional DB summary
    if args.db:
        db_summary = summarize_db(Path(args.db), args.verbose)
        metrics["db_summary"] = db_summary

    # Write outputs
    out_csv = out_dir / "accounts_status.csv"
    out_json = out_dir / "pool_metrics.json"
    write_csv(rows, out_csv)
    write_json(metrics, out_json)

    # Console summary
    print("Pool Metrics Summary")
    print("--------------------")
    print(json.dumps(metrics, indent=2))
    print(f"\nCSV written:   {out_csv}")
    print(f"Metrics JSON:  {out_json}")

if __name__ == "__main__":
    import sys
    main()
