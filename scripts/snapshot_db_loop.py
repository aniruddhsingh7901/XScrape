#!/usr/bin/env python3
"""
Continuously create public snapshots of a live SQLite DB at a fixed interval.

Usage:
  python scripts/snapshot_db_loop.py \
    --db SqliteMinerStorage.sqlite \
    --out-dir scripts/public_db \
    --gzip \
    --interval-seconds 600

- Uses scripts/snapshot_db.py under the hood.
- Safe with WAL: uses the sqlite backup API for consistent snapshots.
- Keeps running until terminated (CTRL+C or PM2 stop).

Tip:
- Serve the out-dir via HTTP to make the DB publicly downloadable.
  Example: python -m http.server 8080 --directory scripts/public_db --bind 0.0.0.0
"""

import argparse
import signal
import sys
import time
from pathlib import Path

# Import snapshot logic
from snapshot_db import make_snapshot


_stop = False


def _handle_sigterm(signum, frame):
    global _stop
    _stop = True


def parse_args():
    p = argparse.ArgumentParser(description="Continuous DB snapshot loop.")
    p.add_argument("--db", type=str, default="SqliteMinerStorage.sqlite", help="Source SQLite DB path")
    p.add_argument("--out-dir", type=str, default="scripts/public_db", help="Output directory for snapshots")
    p.add_argument("--gzip", action="store_true", help="Also write .gz compressed copies")
    p.add_argument("--interval-seconds", type=int, default=600, help="Seconds to wait between snapshots")
    return p.parse_args()


def main():
    args = parse_args()
    src_db = Path(args.db).resolve()
    out_dir = Path(args.out_dir).resolve()

    if not src_db.exists():
        print(f"[FATAL] Source DB does not exist: {src_db}", file=sys.stderr)
        sys.exit(1)

    # Signal handlers for graceful stop
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        try:
            signal.signal(sig, _handle_sigterm)
        except Exception:
            pass

    print(f"[INFO] Starting snapshot loop: db={src_db} out_dir={out_dir} interval={args.interval_seconds}s gzip={args.gzip}")
    while not _stop:
        try:
            snap_path = make_snapshot(src_db, out_dir, args.gzip)
            print(f"[OK] Snapshot created: {snap_path}")
        except Exception as e:
            print(f"[ERROR] Snapshot failed: {e}", file=sys.stderr)
        # Sleep in small chunks to react faster to stop signal
        remaining = args.interval_seconds
        while remaining > 0 and not _stop:
            time.sleep(min(5, remaining))
            remaining -= 5

    print("[INFO] Snapshot loop stopping.")


if __name__ == "__main__":
    main()
