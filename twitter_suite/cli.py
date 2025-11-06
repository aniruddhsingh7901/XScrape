#!/usr/bin/env python3
"""
twitter_suite/cli.py

All-in-one, end-to-end Twitter/X scraping CLI that bundles:
- Cookie/session validation and ct0 refresh (via IMAP re-login)
- Pool loading with per-account proxies
- Pool status and lock reset
- One-shot sanity scrape (prints validator-ready JSON)
- Batch scrape to SQLite (DataEntity schema)
- Acceptance runner to orchestrate a full sample test

Subcommands:
  refresh        Validate cookies; refresh ct0 via IMAP re-login if needed
  load           Load accounts + per-account proxies into twscrape pool
  reset-locks    Clear 15-minute queue locks in the pool
  status         Print pool status (active/locked accounts, next available time)
  one-shot       Fetch N tweets for a query and print validator-ready JSON
  batch          Run queries and store DataEntities into SQLite
  accept         Orchestrate refresh -> load -> reset/status -> one-shot -> batch -> DB summary

Defaults assume:
- Vendor accounts file under twitter/X_scrapping
- Proxies file under twitter/X_scrapping
- twscrape pool DB under scripts/cache
- Output SQLite under scripts/

You can override paths via CLI flags on each subcommand.

Examples:
  # 1. Validate/refresh a new delivery
  python twitter_suite/cli.py refresh \
    --accounts-in twitter/X_scrapping/twitteracc.txt \
    --accounts-out twitter/X_scrapping/twitteracc.refreshed.txt \
    --proxies twitter/X_scrapping/proxy.txt \
    --verbose

  # 2. Load 150 accounts into the pool with proxies
  python twitter_suite/cli.py load \
    --accounts twitter/X_scrapping/twitteracc.refreshed.txt \
    --proxies twitter/X_scrapping/proxy.txt \
    --pool scripts/cache/twscrape_refresh.db \
    --limit 150

  # 3. Status/locks
  python twitter_suite/cli.py reset-locks --pool scripts/cache/twscrape_refresh.db
  python twitter_suite/cli.py status --pool scripts/cache/twscrape_refresh.db --queue SearchTimeline

  # 4. One-shot sanity
  python twitter_suite/cli.py one-shot \
    --pool scripts/cache/twscrape_refresh.db \
    --query "#bitcoin -filter:retweets" \
    --limit 3 --verbose

  # 5. Batch to SQLite
  python twitter_suite/cli.py batch \
    --pool scripts/cache/twscrape_refresh.db \
    --queries "#bitcoin -filter:retweets,#crypto -filter:retweets" \
    --limit-per-query 3 \
    --db scripts/twitter_miner_data.sqlite \
    --verbose

  # 6. Acceptance orchestration (20-account sample)
  python twitter_suite/cli.py accept \
    --accounts-in twitter/X_scrapping/twitteracc.txt \
    --proxies twitter/X_scrapping/proxy.txt \
    --pool scripts/cache/twscrape_refresh.db \
    --db scripts/twitter_miner_data.sqlite \
    --one-shot-query "#bitcoin -filter:retweets" \
    --batch-queries "#bitcoin -filter:retweets,#crypto -filter:retweets" \
    --batch-limit 2 \
    --limit 20 \
    --verbose
"""

import os
import sys
import re
import json
import sqlite3
import argparse
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
from pathlib import Path

# Project root (this file is in <repo>/twitter_suite/cli.py)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "twitter" / "twscrape"))  # vendored twscrape
sys.path.insert(0, str(PROJECT_ROOT))  # project modules

# twscrape
try:
    from twscrape import API  # type: ignore
    from twscrape.accounts_pool import AccountsPool  # type: ignore
    TWSCRAPE_AVAILABLE = True
except Exception as e:
    TWSCRAPE_AVAILABLE = False

# Project models
try:
    from scraping.x.model import XContent
    from common.data import DataEntity, DataSource
    PROJECT_MODELS_AVAILABLE = True
except Exception:
    PROJECT_MODELS_AVAILABLE = False

# Reporting module (CSV/JSON exports)
try:
    from twitter_suite import report as suite_report  # type: ignore
    REPORT_AVAILABLE = True
except Exception:
    REPORT_AVAILABLE = False

# Targets/trends provider (for twitter targets/trending files)
try:
    import scripts.target_provider as suite_targets  # type: ignore
    TARGETS_AVAILABLE = True
except Exception:
    TARGETS_AVAILABLE = False

VERBOSE = False


def vprint(*args):
    if VERBOSE:
        print(*args)


# --------------------------
# Shared helpers (files)
# --------------------------
def parse_accounts_line(line: str) -> Optional[Dict[str, str]]:
    """
    Accept the following line formats:

    1) 6 fields (preferred):
       username:password:email:email_password:auth_token:ct0

    2) 5 fields (legacy cookies-only; ct0 missing):
       username:password:email:email_password:auth_token
       -> ct0 set to ""

    Returns dict keys: username, password, email, email_password, auth_token, ct0
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    parts = line.split(":")
    if len(parts) >= 6:
        u, p, e, ep, at, ct0, *_ = parts
        return {
            "username": u,
            "password": p,
            "email": e,
            "email_password": ep,
            "auth_token": at,
            "ct0": ct0,
        }
    if len(parts) == 5:
        u, p, e, ep, at = parts
        return {
            "username": u,
            "password": p,
            "email": e,
            "email_password": ep,
            "auth_token": at,
            "ct0": "",
        }
    return None


def read_accounts_file(path: Path) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    if not path.exists():
        print(f"Accounts file not found: {path}")
        return out
    for ln in path.read_text(encoding="utf-8").splitlines():
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
    for ln in path.read_text(encoding="utf-8").splitlines():
        ln = ln.strip()
        if not ln:
            continue
        parts = ln.split(":")
        if len(parts) == 4:
            host, port, user, pwd = parts
            proxies.append(f"http://{user}:{pwd}@{host}:{port}")
    return proxies


def mask_value(v: str, keep: int = 6) -> str:
    if not isinstance(v, str):
        return str(v)
    if len(v) <= keep:
        return "*" * len(v)
    return v[:keep] + "..." + ("*" * max(0, len(v) - keep - 3))


# --------------------------
# twscrape helpers
# --------------------------
async def simple_healthcheck(api: API, limit: int = 1) -> bool:
    """
    Try a very cheap request to confirm the account works:
    - user_by_login("x") OR a 1-tweet search ("e")
    """
    try:
        user = await api.user_by_login("x")
        if user:
            return True
    except Exception:
        pass

    try:
        async for _ in api.search("e", limit=limit):
            return True
    except Exception:
        pass

    return False


async def try_add_with_cookies(api: API, account: Dict[str, str], proxy: Optional[str]) -> bool:
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


async def try_relogin(api: API, account: Dict[str, str]) -> Tuple[bool, Optional[str]]:
    """
    Attempt to relogin via twscrape (IMAP flow). Returns (ok, new_ct0) where new_ct0 may be None.
    """
    try:
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


def build_output_line(account: Dict[str, str], new_ct0: Optional[str]) -> str:
    ct0_out = new_ct0 if (new_ct0 is not None and new_ct0 != "") else (account.get("ct0") or "")
    fields = [
        account.get("username", ""),
        account.get("password", ""),
        account.get("email", ""),
        account.get("email_password", ""),
        account.get("auth_token", ""),
        ct0_out,
    ]
    return ":".join(fields)


# --------------------------
# XContent mapping + storage
# --------------------------
def extract_hashtags(text: str, fallback: List[str]) -> List[str]:
    tags = []
    if fallback:
        tags = [f"#{t}" if not str(t).startswith("#") else str(t) for t in fallback]
    else:
        tags = re.findall(r"#\w+", text or "")
    # Deduplicate preserving order
    seen = set()
    out: List[str] = []
    for t in tags:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def normalize_x_url(url: str) -> str:
    return (url or "").replace("twitter.com/", "x.com/")


def tweet_to_xcontent(tweet: Any) -> XContent:
    username = f"@{getattr(tweet.user, 'username', '')}".strip()
    text = getattr(tweet, "rawContent", "") or ""
    url = normalize_x_url(getattr(tweet, "url", "") or f"https://x.com/{getattr(tweet.user, 'username', '')}/status/{getattr(tweet, 'id', '')}")

    ts = getattr(tweet, "date", None)
    if ts is None:
        ts = datetime.now(timezone.utc)
    else:
        if getattr(ts, "tzinfo", None) is None:
            ts = ts.replace(tzinfo=timezone.utc)

    fallback_tags = getattr(tweet, "hashtags", []) or []
    tags = extract_hashtags(text, fallback_tags)

    is_reply = getattr(tweet, "inReplyToTweetId", None) is not None
    is_quote = getattr(tweet, "isQuoted", False) if hasattr(tweet, "isQuoted") else False
    tweet_id = str(getattr(tweet, "id", "")) or None
    conversation_id = str(getattr(tweet, "conversationId", "") or getattr(tweet, "id", "")) or None

    user_verified = bool(getattr(tweet.user, "verified", False)) if getattr(tweet, "user", None) else None
    profile_image_url = getattr(tweet.user, "profileImageUrl", None) if getattr(tweet, "user", None) else None
    cover_picture_url = None
    language = getattr(tweet, "lang", None)

    return XContent(
        username=username,
        text=text,
        url=url,
        timestamp=ts,
        tweet_hashtags=tags,
        media=None,
        user_verified=user_verified,
        tweet_id=tweet_id,
        is_reply=is_reply,
        is_quote=is_quote,
        conversation_id=conversation_id,
        language=language,
        profile_image_url=profile_image_url,
        cover_picture_url=cover_picture_url,
    )


def ensure_db(db_path: Path):
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

from datetime import timedelta

def _compute_window(since_str: str, until_str: str, max_age_days: int) -> Tuple[str, str]:
    """
    Return (since, until) as YYYY-MM-DD strings in UTC.
    - If since/until provided, use them.
    - Else compute since = today - max_age_days, until = today (UTC date).
    """
    today = datetime.now(timezone.utc).date()
    if until_str:
        try:
            u = datetime.strptime(until_str, "%Y-%m-%d").date()
        except Exception:
            u = today
    else:
        u = today
    if since_str:
        try:
            s = datetime.strptime(since_str, "%Y-%m-%d").date()
        except Exception:
            s = u - timedelta(days=max(1, int(max_age_days or 30)))
    else:
        s = u - timedelta(days=max(1, int(max_age_days or 30)))
    return (s.isoformat(), u.isoformat())

def _append_window_to_query(q: str, since_str: str, until_str: str) -> str:
    """
    Append since:/until: if not already present in q.
    """
    q2 = q or ""
    if " since:" not in q2 and "since:" not in q2:
        q2 += f" since:{since_str}"
    if " until:" not in q2 and "until:" not in q2:
        q2 += f" until:{until_str}"
    return q2


def time_bucket_id(dt_utc: datetime) -> int:
    return int(dt_utc.timestamp() // 3600)


def insert_data_entity(db_path: Path, de: DataEntity):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    label_val = de.label.value if de.label else None
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


# --------------------------
# Subcommand implementations
# --------------------------
async def cmd_refresh(args: argparse.Namespace):
    if not TWSCRAPE_AVAILABLE:
        print("❌ twscrape not available.")
        sys.exit(1)
    accounts = read_accounts_file(Path(args.accounts_in))
    if not accounts:
        print(f"❌ No accounts loaded from {args.accounts_in}")
        sys.exit(1)
    proxies = read_proxies_file(Path(args.proxies))
    p_idx = 0

    out_lines: List[str] = []
    total = len(accounts)
    ok_count = 0
    refreshed_count = 0
    failed_count = 0

    pool_db_path = Path(args.pool) if args.pool else (PROJECT_ROOT / "scripts" / "cache" / "twscrape_refresh.db")
    pool_db_path.parent.mkdir(parents=True, exist_ok=True)

    for idx, account in enumerate(accounts, 1):
        print(f"[{idx}/{total}] Checking {account['username']} ...")
        proxy = proxies[p_idx % len(proxies)] if proxies else None
        if proxies:
            p_idx += 1

        api = API(pool=str(pool_db_path))

        ok = False
        new_ct0: Optional[str] = None
        if account.get("auth_token") and account.get("ct0"):
            ok = await try_add_with_cookies(api, account, proxy)
            if ok:
                ok_count += 1
                out_lines.append(build_output_line(account, None))
                print(f"  ✅ Active with cookies")
                continue
            else:
                print(f"  ⚠️ Cookies failed; attempting relogin...")

        if account.get("username") and account.get("password") and account.get("email") and account.get("email_password"):
            ok, new_ct0 = await try_relogin(api, account)
            if ok:
                ok_count += 1
                if new_ct0:
                    refreshed_count += 1
                out_lines.append(build_output_line(account, new_ct0))
                print(f"  ✅ Active after relogin (ct0 {'updated' if new_ct0 else 'unchanged'})")
                continue

        failed_count += 1
        out_lines.append(build_output_line(account, None))
        print(f"  ❌ Inactive (could not authenticate or refresh)")

    out_path = Path(args.accounts_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(out_lines) + "\n", encoding="utf-8")

    print("\nSummary:")
    print(f"  Total accounts:      {total}")
    print(f"  Active (cookies):    {ok_count - refreshed_count}")
    print(f"  Active (refreshed):  {refreshed_count}")
    print(f"  Inactive/failed:     {failed_count}")
    print(f"  Output written to:   {out_path}")


async def cmd_load(args: argparse.Namespace):
    if not TWSCRAPE_AVAILABLE:
        print("❌ twscrape not available.")
        sys.exit(1)
    accounts = read_accounts_file(Path(args.accounts))
    if not accounts:
        print("No accounts to load.")
        return
    proxies = read_proxies_file(Path(args.proxies))
    if not proxies:
        print("Warning: no proxies found. Proceeding without proxies.")
    api = API(pool=args.pool)

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
        proxy = proxies[p_idx % len(proxies)] if proxies else None
        if proxies:
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
            if i <= 10:
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


async def cmd_reset_locks(args: argparse.Namespace):
    if not TWSCRAPE_AVAILABLE:
        print("❌ twscrape not available.")
        sys.exit(1)
    pool = AccountsPool(db_file=args.pool, raise_when_no_account=False)
    await pool.reset_locks()
    print("Done. All locks cleared.")


def _ts(d: Optional[datetime]) -> float:
    if d is None:
        return 0.0
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.timestamp()


async def cmd_status(args: argparse.Namespace):
    if not TWSCRAPE_AVAILABLE:
        print("❌ twscrape not available.")
        sys.exit(1)
    pool = AccountsPool(db_file=args.pool, raise_when_no_account=False)
    stats = {}
    try:
        stats = await pool.stats()
    except Exception as e:
        print(f"Failed to read stats: {e}")
    print(f"Pool: {args.pool}")
    if stats:
        print("Global stats:")
        for k, v in stats.items():
            print(f"  {k:>16}: {v}")

    try:
        nxt = await pool.next_available_at(args.queue)
        print(f"\nQueue: {args.queue}")
        print(f"Next available at: {nxt}")
    except Exception as e:
        print(f"Failed to compute next_available_at: {e}")

    try:
        accounts = await pool.get_all()
        locked: List[Tuple[str, bool, Optional[datetime], Optional[datetime]]] = []
        unlocked: List[Tuple[str, bool, Optional[datetime], Optional[datetime]]] = []
        for acc in accounts:
            when = None
            if acc.locks and args.queue in acc.locks:
                when = acc.locks.get(args.queue)
            item = (acc.username, acc.active, acc.last_used, when)
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


async def cmd_one_shot(args: argparse.Namespace):
    if not TWSCRAPE_AVAILABLE or not PROJECT_MODELS_AVAILABLE:
        print("❌ twscrape or project models not available.")
        sys.exit(1)
    api = API(pool=args.pool, proxy=args.proxy) if (args.pool or args.proxy) else API()
    if args.proxy and VERBOSE:
        print(f"Using proxy: {mask_value(args.proxy, keep=12)}")

    print("Step 1: Initialize twscrape API ...")
    print("  ✅ twscrape API ready.")
    print("Step 2: Using pool sessions (if provided) ...")
    print("Step 3: Search query and paginate ...")
    if VERBOSE:
        print(f"  Query: {args.query}")
        print(f"  Limit: {args.limit}")

    tweets: List[Any] = []
    try:
        async for t in api.search(args.query, limit=args.limit):
            tweets.append(t)
            if len(tweets) >= args.limit:
                break
    except Exception as e:
        print(f"  ❌ Search failed: {e}")
        sys.exit(1)

    if not tweets:
        print("  ⚠️ No tweets returned for query.")
        sys.exit(0)

    print("\n" + "=" * 80)
    print("Step 4: Transform each to XContent JSON (validator-ready)")
    print("=" * 80)

    for idx, t in enumerate(tweets, 1):
        try:
            xcontent = tweet_to_xcontent(t)
            de: DataEntity = XContent.to_data_entity(xcontent)
            json_str = de.content.decode("utf-8")
            obj = json.loads(json_str)

            print(f"\n--- [{idx}/{len(tweets)}] ---")
            print(json.dumps(obj, indent=2))

            # basic validation
            ok = True
            if "x.com" not in obj.get("url", ""):
                print("  ❌ URL must be x.com")
                ok = False
            ts = obj.get("timestamp", "")
            if len(ts) >= 19 and ts[17:19] != "00":
                print("  ❌ timestamp must be minute-obfuscated (seconds == 00)")
                ok = False
            if ok:
                print("  ✅ XContent JSON looks valid for validator expectations.")
        except Exception as e:
            print(f"  ❌ Transform/print failed: {e}")

    print("\nDone.")


async def cmd_batch(args: argparse.Namespace):
    if not TWSCRAPE_AVAILABLE or not PROJECT_MODELS_AVAILABLE:
        print("❌ twscrape or project models not available.")
        sys.exit(1)
    queries: List[str] = []
    if args.queries:
        queries.extend([q.strip() for q in args.queries.split(",") if q.strip()])
    if args.query_file:
        p = Path(args.query_file)
        if p.exists():
            queries.extend([ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()])
    if not queries:
        queries = ["#bittensor -filter:retweets", "#bitcoin -filter:retweets", "#crypto -filter:retweets"]

    # Apply date window (default last 30 days) to each query
    s_since, s_until = _compute_window(getattr(args, "since", ""), getattr(args, "until", ""), getattr(args, "max_age_days", 30))
    queries = [_append_window_to_query(q, s_since, s_until) for q in queries]

    api = API(pool=args.pool, proxy=args.proxy) if (args.pool or args.proxy) else API()
    ensure_db(Path(args.db))
    total_inserted = 0
    total_seen = 0
    for qi, q in enumerate(queries, 1):
        print(f"[{qi}/{len(queries)}] Query: {q} (limit {args.limit_per_query})")
        got = 0
        try:
            async for t in api.search(q, limit=args.limit_per_query):
                total_seen += 1
                got += 1
                txt = getattr(t, "rawContent", "") or ""
                if txt.lstrip().startswith("RT @"):
                    continue
                xc = tweet_to_xcontent(t)
                de: DataEntity = XContent.to_data_entity(xc)
                try:
                    insert_data_entity(Path(args.db), de)
                    total_inserted += 1
                except Exception as e:
                    vprint(f"  Insert error ({de.uri}): {e}")
                if got >= args.limit_per_query:
                    break
        except Exception as e:
            print(f"  ❌ Search error for '{q}': {e}")

    print("\nSummary:")
    print(f"  Queries run:       {len(queries)}")
    print(f"  Tweets seen:       {total_seen}")
    print(f"  DataEntities kept: {total_inserted}")
    print(f"  DB file:           {args.db}")

async def cmd_batch_targets(args: argparse.Namespace):
    """
    Read queries from a targets JSON file (e.g., scripts/cache/twitter_targets.json) and run batch.
    JSON shape:
      { "labels": ["#bitcoin -filter:retweets", "..."] }
    """
    if not TWSCRAPE_AVAILABLE or not PROJECT_MODELS_AVAILABLE:
        print("❌ twscrape or project models not available.")
        sys.exit(1)
    # Load targets JSON
    try:
        with Path(args.targets_json).open("r", encoding="utf-8") as f:
            data = json.load(f)
        labels = data.get("labels", [])
        if not isinstance(labels, list) or not labels:
            print(f"❌ No 'labels' in {args.targets_json}")
            sys.exit(1)
        queries = [str(x).strip() for x in labels if str(x).strip()]
    except Exception as e:
        print(f"❌ Failed to read targets JSON {args.targets_json}: {e}")
        sys.exit(1)

    # Apply date window (default last 30 days) to each query read from targets file
    s_since, s_until = _compute_window(getattr(args, "since", ""), getattr(args, "until", ""), getattr(args, "max_age_days", 30))
    queries = [_append_window_to_query(q, s_since, s_until) for q in queries]

    api = API(pool=args.pool, proxy=args.proxy) if (args.pool or args.proxy) else API()
    ensure_db(Path(args.db))
    total_inserted = 0
    total_seen = 0
    for qi, q in enumerate(queries, 1):
        print(f"[{qi}/{len(queries)}] Query: {q} (limit {args.limit_per_query})")
        got = 0
        try:
            async for t in api.search(q, limit=args.limit_per_query):
                total_seen += 1
                got += 1
                txt = getattr(t, "rawContent", "") or ""
                if txt.lstrip().startswith("RT @"):
                    continue
                xc = tweet_to_xcontent(t)
                de: DataEntity = XContent.to_data_entity(xc)
                try:
                    insert_data_entity(Path(args.db), de)
                    total_inserted += 1
                except Exception as e:
                    vprint(f"  Insert error ({de.uri}): {e}")
                if got >= args.limit_per_query:
                    break
        except Exception as e:
            print(f"  ❌ Search error for '{q}': {e}")

    print("\nSummary:")
    print(f"  Queries run:       {len(queries)}")
    print(f"  Tweets seen:       {total_seen}")
    print(f"  DataEntities kept: {total_inserted}")
    print(f"  DB file:           {args.db}")

async def cmd_trends(args: argparse.Namespace):
    """
    Update twitter targets/trending files via suite target provider.
    Runs the update in a thread to avoid nested event loop conflicts.
    """
    if not TARGETS_AVAILABLE:
        print("❌ targets module not available.")
        sys.exit(1)
    try:
        import asyncio
        loop = asyncio.get_running_loop()
        # Run synchronous update (which may use its own loop internally) in a thread
        await loop.run_in_executor(
            None,
            suite_targets.update_twitter_targets,
            args.pool,
            True,
            args.max_labels,
            0.0,
            args.per_query_limit,
            args.trending_max_out,
        )
    except Exception as e:
        print(f"❌ trends update failed: {e}")
        sys.exit(1)
    print("✅ Twitter targets/trending updated.")

async def cmd_report(args: argparse.Namespace):
    if not REPORT_AVAILABLE:
        print("❌ Reporting module not available.")
        sys.exit(1)
    queues = [q.strip() for q in args.queues.split(",") if q.strip()]
    # Gather pool metrics and per-account rows
    metrics, rows = await suite_report.gather_pool_data(args.pool, queues, args.verbose)
    # Optional DB summary
    if args.db:
        metrics["db_summary"] = suite_report.summarize_db(Path(args.db), args.verbose)
    # Write outputs
    out_dir = Path(args.out_dir)
    out_csv = out_dir / "accounts_status.csv"
    out_json = out_dir / "pool_metrics.json"
    suite_report.write_csv(rows, out_csv)
    suite_report.write_json(metrics, out_json)

    # Console summary
    print("Pool Metrics Summary")
    print("--------------------")
    print(json.dumps(metrics, indent=2))
    print(f"\nCSV written:   {out_csv}")
    print(f"Metrics JSON:  {out_json}")


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


async def cmd_accept(args: argparse.Namespace):
    # 1) Refresh
    await cmd_refresh(argparse.Namespace(
        accounts_in=args.accounts_in,
        accounts_out=(Path(args.accounts_in).with_suffix(".refreshed.txt")),
        proxies=args.proxies,
        pool=args.pool,
        verbose=args.verbose
    ))
    accounts_refreshed = str(Path(args.accounts_in).with_suffix(".refreshed.txt"))
    # 2) Load
    await cmd_load(argparse.Namespace(
        accounts=accounts_refreshed,
        proxies=args.proxies,
        pool=args.pool,
        limit=args.limit,
        verbose=args.verbose
    ))
    # 3) Reset locks + Status
    await cmd_reset_locks(argparse.Namespace(pool=args.pool))
    await cmd_status(argparse.Namespace(pool=args.pool, queue="SearchTimeline", limit=12))
    # 4) One-shot
    await cmd_one_shot(argparse.Namespace(pool=args.pool, proxy=None, query=args.one_shot_query, limit=3, verbose=args.verbose))
    # 5) Batch
    await cmd_batch(argparse.Namespace(pool=args.pool, proxy=None, queries=args.batch_queries, query_file="", limit_per_query=args.batch_limit, db=args.db, verbose=args.verbose))
    # 6) DB summary
    print_db_summary(Path(args.db), limit=5)
    print("\n=== Acceptance run completed ===")


# --------------------------
# Argparse (subcommands)
# --------------------------
def main():
    global VERBOSE
    ap = argparse.ArgumentParser(description="All-in-one Twitter/X scraping suite")
    sub = ap.add_subparsers(dest="command")

    # refresh
    ap_refresh = sub.add_parser("refresh", help="Validate cookies; refresh ct0 via IMAP re-login if needed")
    ap_refresh.add_argument("--accounts-in", type=str, default="twitter/X_scrapping/twitteracc.txt")
    ap_refresh.add_argument("--accounts-out", type=str, default="twitter/X_scrapping/twitteracc.refreshed.txt")
    ap_refresh.add_argument("--proxies", type=str, default="twitter/X_scrapping/proxy.txt")
    ap_refresh.add_argument("--pool", type=str, default=str(PROJECT_ROOT / "scripts" / "cache" / "twscrape_refresh.db"))
    ap_refresh.add_argument("--verbose", action="store_true")
    ap_refresh.set_defaults(func=cmd_refresh)

    # load
    ap_load = sub.add_parser("load", help="Load accounts + per-account proxies into twscrape pool")
    ap_load.add_argument("--accounts", type=str, default="twitter/X_scrapping/twitteracc.refreshed.txt")
    ap_load.add_argument("--proxies", type=str, default="twitter/X_scrapping/proxy.txt")
    ap_load.add_argument("--pool", type=str, default=str(PROJECT_ROOT / "scripts" / "cache" / "twscrape_refresh.db"))
    ap_load.add_argument("--limit", type=int, default=0, help="0 = all")
    ap_load.add_argument("--verbose", action="store_true")
    ap_load.set_defaults(func=cmd_load)

    # reset-locks
    ap_reset = sub.add_parser("reset-locks", help="Clear 15-minute queue locks in the pool")
    ap_reset.add_argument("--pool", type=str, default=str(PROJECT_ROOT / "scripts" / "cache" / "twscrape_refresh.db"))
    ap_reset.set_defaults(func=cmd_reset_locks)

    # status
    ap_status = sub.add_parser("status", help="Print pool status (active/locked; next available time)")
    ap_status.add_argument("--pool", type=str, default=str(PROJECT_ROOT / "scripts" / "cache" / "twscrape_refresh.db"))
    ap_status.add_argument("--queue", type=str, default="SearchTimeline")
    ap_status.add_argument("--limit", type=int, default=12)
    ap_status.set_defaults(func=cmd_status)

    # one-shot
    ap_one = sub.add_parser("one-shot", help="Fetch N tweets and print validator-ready JSON")
    ap_one.add_argument("--pool", type=str, default=str(PROJECT_ROOT / "scripts" / "cache" / "twscrape_refresh.db"))
    ap_one.add_argument("--proxy", type=str, default=None)
    ap_one.add_argument("--query", type=str, default="#bittensor -filter:retweets")
    ap_one.add_argument("--limit", type=int, default=3)
    ap_one.add_argument("--verbose", action="store_true")
    ap_one.set_defaults(func=cmd_one_shot)

    # batch
    ap_batch = sub.add_parser("batch", help="Run queries and store DataEntities into SQLite")
    ap_batch.add_argument("--pool", type=str, default=str(PROJECT_ROOT / "scripts" / "cache" / "twscrape_refresh.db"))
    ap_batch.add_argument("--proxy", type=str, default=None)
    ap_batch.add_argument("--queries", type=str, default="")
    ap_batch.add_argument("--query-file", type=str, default="")
    ap_batch.add_argument("--limit-per-query", type=int, default=100)
    ap_batch.add_argument("--db", type=str, default=str(PROJECT_ROOT / "scripts" / "twitter_miner_data.sqlite"))
    # Date window controls
    ap_batch.add_argument("--since", type=str, default="", help="YYYY-MM-DD (UTC) lower bound; defaults to today-30d")
    ap_batch.add_argument("--until", type=str, default="", help="YYYY-MM-DD (UTC) upper bound; defaults to today (exclusive at 00:00Z by X semantics)")
    ap_batch.add_argument("--max-age-days", type=int, default=30, help="Default rolling window if since/until not provided")
    ap_batch.add_argument("--verbose", action="store_true")
    ap_batch.set_defaults(func=cmd_batch)

    # report
    ap_report = sub.add_parser("report", help="Export pool status to CSV/JSON for prod monitoring")
    ap_report.add_argument("--pool", type=str, default=str(PROJECT_ROOT / "scripts" / "cache" / "twscrape_refresh.db"))
    ap_report.add_argument("--out-dir", type=str, default="twitter_suite/reports")
    ap_report.add_argument("--queues", type=str, default="SearchTimeline")
    ap_report.add_argument("--db", type=str, default=str(PROJECT_ROOT / "scripts" / "twitter_miner_data.sqlite"))
    ap_report.add_argument("--verbose", action="store_true")
    ap_report.set_defaults(func=cmd_report)

    # batch-targets
    ap_batch_targets = sub.add_parser("batch-targets", help="Run batch using queries from targets JSON")
    ap_batch_targets.add_argument("--pool", type=str, default=str(PROJECT_ROOT / "scripts" / "cache" / "twscrape_refresh.db"))
    ap_batch_targets.add_argument("--proxy", type=str, default=None)
    ap_batch_targets.add_argument("--targets-json", type=str, default=str(PROJECT_ROOT / "scripts" / "cache" / "twitter_targets.json"))
    ap_batch_targets.add_argument("--limit-per-query", type=int, default=100)
    ap_batch_targets.add_argument("--db", type=str, default=str(PROJECT_ROOT / "SqliteMinerStorage.sqlite"))
    # Date window controls for targets
    ap_batch_targets.add_argument("--since", type=str, default="", help="YYYY-MM-DD (UTC) lower bound; defaults to today-30d")
    ap_batch_targets.add_argument("--until", type=str, default="", help="YYYY-MM-DD (UTC) upper bound; defaults to today")
    ap_batch_targets.add_argument("--max-age-days", type=int, default=30, help="Default rolling window if since/until not provided")
    ap_batch_targets.add_argument("--verbose", action="store_true")
    ap_batch_targets.set_defaults(func=cmd_batch_targets)

    # trends (update targets)
    ap_trends = sub.add_parser("trends", help="Update twitter targets/trending via pool sampling")
    ap_trends.add_argument("--pool", type=str, default=str(PROJECT_ROOT / "scripts" / "cache" / "twscrape_refresh.db"))
    ap_trends.add_argument("--max-labels", type=int, default=60)
    ap_trends.add_argument("--per-query-limit", type=int, default=100)
    ap_trends.add_argument("--trending-max-out", type=int, default=100)
    ap_trends.add_argument("--verbose", action="store_true")
    ap_trends.set_defaults(func=cmd_trends)


    # accept
    ap_accept = sub.add_parser("accept", help="Orchestrate end-to-end sample test")
    ap_accept.add_argument("--accounts-in", type=str, default="twitter/X_scrapping/twitteracc.txt")
    ap_accept.add_argument("--proxies", type=str, default="twitter/X_scrapping/proxy.txt")
    ap_accept.add_argument("--pool", type=str, default=str(PROJECT_ROOT / "scripts" / "cache" / "twscrape_refresh.db"))
    ap_accept.add_argument("--db", type=str, default=str(PROJECT_ROOT / "scripts" / "twitter_miner_data.sqlite"))
    ap_accept.add_argument("--one-shot-query", type=str, default="#bitcoin -filter:retweets")
    ap_accept.add_argument("--batch-queries", type=str, default="#bitcoin -filter:retweets,#crypto -filter:retweets")
    ap_accept.add_argument("--batch-limit", type=int, default=2)
    ap_accept.add_argument("--limit", type=int, default=20)
    ap_accept.add_argument("--verbose", action="store_true")
    ap_accept.set_defaults(func=cmd_accept)

    args = ap.parse_args()
    if not getattr(args, "command", None):
        ap.print_help()
        sys.exit(0)

    VERBOSE = bool(getattr(args, "verbose", False))

    if not TWSCRAPE_AVAILABLE:
        print("❌ twscrape not importable (vendor missing?). Ensure twitter/twscrape exists.")
        sys.exit(1)
    if not PROJECT_MODELS_AVAILABLE:
        print("❌ Project models not importable. Ensure scraping.x.model and common.data exist.")
        sys.exit(1)

    import asyncio
    asyncio.run(args.func(args))


if __name__ == "__main__":
    main()
