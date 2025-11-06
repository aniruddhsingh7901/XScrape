#!/usr/bin/env python3
"""
Create a consistent, read-only snapshot of a live SQLite database (WAL-safe) for public download.

Usage:
  python scripts/snapshot_db.py \
    --db SqliteMinerStorage.sqlite \
    --out-dir scripts/public_db \
    [--gzip]

- Produces a timestamped snapshot and updates a "latest" copy:
    scripts/public_db/SqliteMinerStorage_YYYYmmdd_HHMMSS.sqlite
    scripts/public_db/SqliteMinerStorage_latest.sqlite

- If --gzip is provided, also writes .gz compressed copies next to them.

Notes:
- Uses sqlite3 backup API for consistency even while the source DB is being written to.
- Sets file permissions to 0644 so they can be served by a static HTTP server.
"""

import argparse
import datetime as dt
import gzip
import os
import shutil
import sqlite3
import sys
from pathlib import Path


def make_snapshot(src_db: Path, out_dir: Path, do_gzip: bool) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    snap_name = f"SqliteMinerStorage_{ts}.sqlite"
    snap_path = out_dir / snap_name
    latest_path = out_dir / "SqliteMinerStorage_latest.sqlite"

    # Create consistent snapshot using sqlite backup API
    try:
        with sqlite3.connect(str(src_db), timeout=60.0) as src_conn:
            with sqlite3.connect(str(snap_path)) as dst_conn:
                src_conn.backup(dst_conn)
    except Exception as e:
        print(f"[ERROR] Snapshot failed: {e}", file=sys.stderr)
        raise

    # Permissions: world-readable
    try:
        os.chmod(snap_path, 0o644)
    except Exception:
        pass

    # Replace latest copy atomically
    try:
        # Copy rather than symlink to maximize client compatibility
        tmp_latest = latest_path.with_suffix(".sqlite.tmp")
        shutil.copyfile(snap_path, tmp_latest)
        os.chmod(tmp_latest, 0o644)
        tmp_latest.replace(latest_path)
    except Exception as e:
        print(f"[WARN] Failed to update latest copy: {e}", file=sys.stderr)

    if do_gzip:
        for p in (snap_path, latest_path):
            gz_path = p.with_suffix(p.suffix + ".gz")
            try:
                with open(p, "rb") as f_in, gzip.open(gz_path, "wb", compresslevel=6) as f_out:
                    shutil.copyfileobj(f_in, f_out)
                os.chmod(gz_path, 0o644)
            except Exception as e:
                print(f"[WARN] Failed to gzip {p.name}: {e}", file=sys.stderr)

    return snap_path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Create a consistent snapshot of a live SQLite DB (WAL-safe).")
    p.add_argument("--db", type=str, default="SqliteMinerStorage.sqlite", help="Source SQLite DB path")
    p.add_argument("--out-dir", type=str, default="scripts/public_db", help="Output directory for snapshots")
    p.add_argument("--gzip", action="store_true", help="Also write .gz compressed copies")
    return p.parse_args()


def main():
    args = parse_args()
    src_db = Path(args.db).resolve()
    out_dir = Path(args.out_dir).resolve()

    if not src_db.exists():
        print(f"[FATAL] Source DB does not exist: {src_db}", file=sys.stderr)
        sys.exit(1)

    try:
        snap_path = make_snapshot(src_db, out_dir, args.gzip)
    except Exception:
        sys.exit(1)

    print(f"[OK] Snapshot written: {snap_path}")
    print(f"[OK] Latest copy: {out_dir / 'SqliteMinerStorage_latest.sqlite'}")
    if args.gzip:
        print(f"[OK] Gzip copies also written.")


if __name__ == "__main__":
    main()
