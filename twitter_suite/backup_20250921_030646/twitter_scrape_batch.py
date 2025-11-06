#!/usr/bin/env python3
"""
twitter_scrape_batch.py

Batch-scrape tweets using twscrape sessions, convert to validator-compatible XContent/DataEntity,
and store into a local SQLite database in the DataEntity schema.

What this does:
- Reuses an existing twscrape pool DB (from scripts/twitter_refresh_ct0.py).
- Runs a list of queries (hashtags/keywords) sequentially with per-query limits.
- Converts each Tweet -> XContent -> DataEntity bytes (minute-obfuscated timestamp in JSON).
- Inserts into SQLite table DataEntity with columns:
    uri TEXT PK,
    datetime TEXT (ISO UTC),
    timeBucketId INTEGER,
    source INTEGER (2 for X),
    label TEXT NULLABLE,
    content BLOB,
    contentSizeBytes INTEGER
  and index on (timeBucketId, source, label, contentSizeBytes).

Usage examples:
  python scripts/twitter_scrape_batch.py \\
    --pool scripts/cache/twscrape_refresh.db \\
    --queries "#bittensor,#bitcoin,#crypto" \\
    --limit-per-query 10 \\
    --db scripts/twitter_miner_data.sqlite \\
    --verbose

  python scripts/twitter_scrape_batch.py \\
    --pool scripts/cache/twscrape_refresh.db \\
    --query-file my_queries.txt \\
    --limit-per-query 5 \\
    --db scripts/twitter_miner_data.sqlite
"""

import os
import sys
import sqlite3
import argparse
from pathlib import Path
from typing import List, Optional, Any
from datetime import datetime, timezone
import json

# Ensure project imports
BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR / "twitter" / "twscrape"))
sys.path.insert(0, str(BASE_DIR))

VERBOSE = False

# twscrape
try:
    from twscrape import API  # type: ignore
    TWSCRAPE_AVAILABLE = True
except Exception:
    TWSCRAPE_AVAILABLE = False

# Project models
from scraping.x.model import XContent
from common.data import DataEntity, DataSource


def vprint(*args):
    if VERBOSE:
        print(*args)


def ensure_db(db_path: Path):
    """Create the DataEntity table and index if not exists."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS DataEntity (
            uri TEXT PRIMARY KEY,
            datetime TEXT NOT NULL,
            timeBucketId INTEGER NOT NULL,
            source INTEGER NOT NULL,
            label TEXT,
            content BLOB NOT NULL,
            contentSizeBytes INTEGER NOT NULL
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_data_entity_bucket
        ON DataEntity (timeBucketId, source, label, contentSizeBytes)
    """)
    conn.commit()
    conn.close()


def time_bucket_id(dt_utc: datetime) -> int:
    return int(dt_utc.timestamp() // 3600)


def insert_data_entity(db_path: Path, de: DataEntity):
    """Insert (or upsert) a DataEntity row."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    label_val = de.label.value if de.label else None
    # datetime is aware UTC; store as ISO string
    dt_iso = de.datetime.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    tbi = time_bucket_id(de.datetime.astimezone(timezone.utc))
    cur.execute("""
        INSERT INTO DataEntity (uri, datetime, timeBucketId, source, label, content, contentSizeBytes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(uri) DO UPDATE SET
            datetime=excluded.datetime,
            timeBucketId=excluded.timeBucketId,
            content=excluded.content,
            contentSizeBytes=excluded.contentSizeBytes
    """, (
        de.uri,
        dt_iso,
        tbi,
        int(DataSource.X),
        label_val,
        de.content,
        de.content_size_bytes
    ))
    conn.commit()
    conn.close()


def tweet_to_xcontent(tweet: Any) -> XContent:
    """Map twscrape Tweet -> XContent (static fields only)."""
    # username with leading @
    username = f"@{getattr(tweet.user, 'username', '')}".strip()

    # text
    text = getattr(tweet, "rawContent", "") or ""

    # url -> normalize to x.com
    url = getattr(tweet, "url", "") or f"https://x.com/{getattr(tweet.user, 'username', '')}/status/{getattr(tweet, 'id', '')}"
    url = url.replace("twitter.com/", "x.com/")

    # timestamp aware UTC
    ts = getattr(tweet, "date", None)
    if ts is None:
        ts = datetime.now(timezone.utc)
    else:
        if getattr(ts, "tzinfo", None) is None:
            ts = ts.replace(tzinfo=timezone.utc)

    # hashtags ordered
    tags = []
    hs = getattr(tweet, "hashtags", []) or []
    for t in hs:
        t = str(t)
        if not t.startswith("#"):
            t = f"#{t}"
        if t not in tags:
            tags.append(t)

    # reply/quote, ids
    is_reply = getattr(tweet, "inReplyToTweetId", None) is not None
    is_quote = getattr(tweet, "isQuoted", False) if hasattr(tweet, "isQuoted") else False
    tweet_id = str(getattr(tweet, "id", "")) or None
    conv_id = str(getattr(tweet, "conversationId", "") or getattr(tweet, "id", "")) or None

    # user profile basic
    user_verified = bool(getattr(tweet.user, "verified", False)) if getattr(tweet, "user", None) else None
    profile_image_url = getattr(tweet.user, "profileImageUrl", None) if getattr(tweet, "user", None) else None

    # language
    language = getattr(tweet, "lang", None)

    return XContent(
        username=username,
        text=text,
        url=url,
        timestamp=ts,
        tweet_hashtags=tags,
        media=None,  # safe default; we can add media mapping later
        user_verified=user_verified,
        tweet_id=tweet_id,
        is_reply=is_reply,
        is_quote=is_quote,
        conversation_id=conv_id,
        language=language,
        profile_image_url=profile_image_url,
    )


async def scrape_queries(pool: str, proxy: Optional[str], queries: List[str], limit_per_query: int, db_path: Path):
    if not TWSCRAPE_AVAILABLE:
        print("❌ twscrape not available.")
        sys.exit(1)

    ensure_db(db_path)
    api = API(pool=pool, proxy=proxy) if (pool or proxy) else API()

    total_inserted = 0
    total_seen = 0
    for qi, q in enumerate(queries, 1):
        print(f"[{qi}/{len(queries)}] Query: {q} (limit {limit_per_query})")
        got = 0
        try:
            async for t in api.search(q, limit=limit_per_query):
                total_seen += 1
                got += 1
                # Skip retweets by checking raw text prefix
                txt = getattr(t, "rawContent", "") or ""
                if txt.lstrip().startswith("RT @"):
                    continue

                xc = tweet_to_xcontent(t)
                de: DataEntity = XContent.to_data_entity(xc)
                try:
                    insert_data_entity(db_path, de)
                    total_inserted += 1
                except Exception as e:
                    vprint(f"  Insert error ({de.uri}): {e}")
                if got >= limit_per_query:
                    break
        except Exception as e:
            print(f"  ❌ Search error for '{q}': {e}")

    print("\nSummary:")
    print(f"  Queries run:       {len(queries)}")
    print(f"  Tweets seen:       {total_seen}")
    print(f"  DataEntities kept: {total_inserted}")
    print(f"  DB file:           {db_path}")


def main():
    parser = argparse.ArgumentParser(description="Batch-scrape X with twscrape pool into SQLite DataEntity schema")
    parser.add_argument("--pool", type=str, required=True, help="Path to twscrape accounts DB (e.g., scripts/cache/twscrape_refresh.db)")
    parser.add_argument("--proxy", type=str, default=None, help="Proxy URL, e.g., http://user:pass@host:port")
    parser.add_argument("--queries", type=str, default="", help="Comma-separated queries (e.g., \"#bittensor,#bitcoin\")")
    parser.add_argument("--query-file", type=str, default="", help="File with one query per line")
    parser.add_argument("--limit-per-query", type=int, default=5, help="Max tweets to pull per query")
    parser.add_argument("--db", type=str, default="scripts/twitter_miner_data.sqlite", help="SQLite DB path")
    parser.add_argument("--verbose", action="store_true", help="Verbose logs")
    args = parser.parse_args()

    global VERBOSE
    VERBOSE = bool(args.verbose)

    # Build query list
    qset: List[str] = []
    if args.queries:
        qset.extend([q.strip() for q in args.queries.split(",") if q.strip()])
    if args.query_file:
        p = Path(args.query_file)
        if p.exists():
            qset.extend([ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()])

    if not qset:
        # fallback basic queries
        qset = ["#bittensor -filter:retweets", "#bitcoin -filter:retweets", "#crypto -filter:retweets"]

    import asyncio
    asyncio.run(scrape_queries(args.pool, args.proxy, qset, args.limit_per_query, Path(args.db)))


if __name__ == "__main__":
    main()
