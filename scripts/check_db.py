#!/usr/bin/env python3
"""
check_db.py

Quick checks to verify that scrapers are injecting rows into the miner SQLite DB.

Usage examples:
  # Basic summary (all sources)
  python scripts/check_db.py --db SqliteMinerStorage.sqlite

  # Focus on Twitter (source=2 assumed) and last 15 minutes
  python scripts/check_db.py --db SqliteMinerStorage.sqlite --source 2 --minutes 15 --limit 10

  # Show latest 20 rows (URIs, datetime, label)
  python scripts/check_db.py --db SqliteMinerStorage.sqlite --limit 20

What it prints:
- Total rows in DataEntity
- Row count for the selected source (if provided)
- Row count within the last N minutes (UTC)
- Top labels by recent insert
- Latest rows with URI, datetime, label
"""

import argparse
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Verify DB ingestion for DataEntity table")
    ap.add_argument("--db", type=str, default="SqliteMinerStorage.sqlite", help="Path to miner SQLite DB")
    ap.add_argument("--table", type=str, default="DataEntity", help="Table name (default: DataEntity)")
