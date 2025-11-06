#!/usr/bin/env python3
"""
Reddit multi-account runner:
- 1:1 account -> residential proxy -> unique UA
- Per-account OAuth token cache (persisted to disk) + refresh near expiry
- Per-account adaptive pacing using rate-limit headers (rpm_safe = 0.8*(R/T*60))
- Fetches listings (new and/or comments) with limit=100, converts to RedditContent, stores into miner's SQLite
- Shards target subreddits across accounts (from scripts/target_provider.py cache)

Notes:
- Uses raw HTTP (aiohttp) to read rate-limit headers and to pace correctly
- Uses RedditContent.to_data_entity to ensure validator-compliant serialization (minute obfuscation, r/... label)
- Default to single in-flight per account; you can raise to 2 for more throughput (still safe)

Usage (from repo root):
  python scripts/reddit_multi_runner.py \
    --accounts-json reddit.json \
    --proxies-file reddit-verifier/proxies.txt \
    --max-accounts 15 \
    --db SqliteMinerStorage.sqlite \
    --endpoint both \
    --per-account-inflight 1 \
    --run-seconds 600 \
    --log-csv scripts/cache/reddit_multi.csv

Options:
  --accounts-json: JSON with a list of accounts; supported keys: username, password, client_id, client_secret, user_agent
                   (If keys differ, see --user-key, --pass-key, etc.)
  --proxies-file:  text file with one proxy per line: http://user:pass@host:port (or https://...)
  --max-accounts:  how many accounts to use (will map first N accounts to first N proxies)
  --db:            SQLite path used by miner (REPLACE INTO by uri)
  --endpoint:      new | comments | both (listing endpoints per account loop)
  --per-account-inflight: 1 or 2 (requests in flight per account)
  --run-seconds:   duration to run before exit (0 = run once per label shard, >0 = run for that many seconds)
  --refresh-targets / --include-trending: refresh target labels before run
  --max-labels:    how many labels to read from target_provider cache before sharding
  --csv logging:   --log-csv path to CSV (timestamp, account, subreddit, endpoint, limit, status, used, remaining, reset, items, latency_ms, rpm_safe)

Security:
- Token caches saved to scripts/cache/tokens/{username}.json by default; override with --tokens-dir
"""

import os
import sys
import json
import time
import math
import asyncio
import csv
import random
import traceback
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import aiohttp

def log_event(level: str, msg: str):
    print(f"[{level}] {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {msg}", flush=True)

# Repo root
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(REPO_ROOT))

# Storage + content models
from storage.miner.sqlite_miner_storage import SqliteMinerStorage  # type: ignore
from scraping.reddit.model import RedditContent  # type: ignore
from common.data import DataEntity, DataLabel  # type: ignore

# Target provider
from scripts.target_provider import get_reddit_labels_from_cache, update_reddit_targets, load_static_reddit_labels  # type: ignore

# OAuth helper from one_shot (with retries)
from scripts.reddit_one_shot import get_token_with_retry  # type: ignore


DEFAULT_TOKENS_DIR = REPO_ROOT / "scripts" / "cache" / "tokens"

# Listing endpoints
NEW_URL = "https://oauth.reddit.com/r/{sub}/new?limit={limit}&raw_json=1"
COMMENTS_URL = "https://oauth.reddit.com/r/{sub}/comments?limit={limit}&raw_json=1"


@dataclass
class Account:
    username: str
    password: str
    client_id: str
    client_secret: str
    user_agent: str
    proxy: Optional[str]  # http://user:pass@host:port


def load_accounts(accounts_json: Path,
                  user_key="username", pass_key="password",
                  cid_key="client_id", csec_key="client_secret",
                  ua_key="user_agent") -> List[Dict[str, Any]]:
    data = json.loads(accounts_json.read_text(encoding="utf-8"))
    if isinstance(data, dict) and "accounts" in data:
        data = data["accounts"]
    if not isinstance(data, list):
        raise ValueError("accounts json must be a list or contain 'accounts' list")

    out = []
    for idx, a in enumerate(data):
        try:
            out.append({
                "username": a[user_key],
                "password": a[pass_key],
                "client_id": a[cid_key],
                "client_secret": a[csec_key],
                "user_agent": a.get(ua_key) or f"MultiRunner/1.0 by u/{a[user_key]}",
            })
        except KeyError as e:
            raise ValueError(f"Missing key in account {idx}: {e}")
    return out


def load_proxies(proxies_file: Path) -> List[str]:
    def normalize(line: str) -> Optional[str]:
        s = line.strip()
        if not s or s.startswith("#"):
            return None
        # Already a full URL
        if s.startswith("http://") or s.startswith("https://"):
            return s
        # user:pass@host:port
        if "@" in s and ":" in s:
            return "http://" + s if not s.startswith("http") else s
        parts = s.split(":")
        # host:port:user:pass (or with username:password)
        if len(parts) == 4:
            host, port, user, pwd = parts
            return f"http://{user}:{pwd}@{host}:{port}"
        # host:port
        if len(parts) == 2 and all(parts):
            host, port = parts
            return f"http://{host}:{port}"
        return None

    lines = proxies_file.read_text(encoding="utf-8").splitlines()
    proxies: List[str] = []
    for ln in lines:
        try:
            p = normalize(ln)
            if p:
                proxies.append(p)
        except Exception:
            # Skip invalid lines
            continue
    return proxies


def map_accounts_to_proxies(accts: List[Dict[str, Any]], proxies: List[str], max_accounts: int) -> List[Account]:
    N = min(max_accounts, len(accts), len(proxies))
    out: List[Account] = []
    for i in range(N):
        a = accts[i]
        out.append(Account(
            username=a["username"],
            password=a["password"],
            client_id=a["client_id"],
            client_secret=a["client_secret"],
            user_agent=a["user_agent"],
            proxy=proxies[i]
        ))
    return out


async def fetch_listing(session: aiohttp.ClientSession, url: str, headers: Dict[str, str]) -> Tuple[int, Dict[str, Any], Dict[str, str]]:
    t0 = time.time()
    try:
        async with session.get(url, headers=headers, timeout=60) as resp:
            status = resp.status
            h = {k.lower(): v for k, v in resp.headers.items()}
            try:
                data = await resp.json(content_type=None)
            except Exception:
                data = {}
            return status, data, h
    except Exception:
        return 0, {}, {}


def reddit_content_from_post(item: Dict[str, Any]) -> Dict[str, Any]:
    name = item.get("name")
    permalink = item.get("permalink") or ""
    url = f"https://www.reddit.com{permalink}"
    username = item.get("author") or "[deleted]"
    community = item.get("subreddit_name_prefixed") or (f"r/{item.get('subreddit')}" if item.get('subreddit') else None)
    # Ensure body is a string; empty string if missing
    body = item.get("selftext")
    if body is None:
        body = ""
    title = item.get("title") or None
    created_utc = float(item.get("created_utc") or 0)
    created_at = dt.datetime.fromtimestamp(created_utc, tz=dt.timezone.utc) if created_utc > 0 else dt.datetime.now(dt.timezone.utc)
    if not name:
        name = url
    if not community:
        community = "r/unknown"
    is_nsfw = bool(item.get("over_18") or False)

    # Enrich optional fields to increase bytes while staying validator-compliant
    score = item.get("score")
    try:
        upvote_ratio = float(item.get("upvote_ratio")) if item.get("upvote_ratio") is not None else None
    except Exception:
        upvote_ratio = None
    try:
        num_comments = int(item.get("num_comments")) if item.get("num_comments") is not None else None
    except Exception:
        num_comments = None

    # Extract media URLs (preview, gallery, direct url) - capped to avoid bloat
    media_urls: List[str] = []
    def _add(u: Optional[str]):
        if isinstance(u, str) and u:
            media_urls.append(u)
    _add(item.get("url_overridden_by_dest"))

    preview = (item.get("preview") or {})
    for im in (preview.get("images") or []):
        src = ((im.get("source") or {}).get("url"))
        _add(src)
        for r in (im.get("resolutions") or []):
            _add(r.get("url"))

    md = item.get("media_metadata")
    gd = item.get("gallery_data")
    if isinstance(md, dict) and isinstance(gd, dict):
        for it in (gd.get("items") or []):
            mid = it.get("media_id")
            m = md.get(mid) if mid else None
            if isinstance(m, dict):
                s = m.get("s")
                if isinstance(s, dict):
                    _add(s.get("u"))
                for p in (m.get("p") or []):
                    if isinstance(p, dict):
                        _add(p.get("u"))

    # Clean & dedupe
    cleaned = []
    seen = set()
    for u in media_urls:
        cu = u.replace("&", "&")
        if cu not in seen:
            seen.add(cu)
            cleaned.append(cu)
    media = cleaned[:20] if cleaned else None

    return {
        "id": name,
        "url": url,
        "username": username,
        "communityName": community,
        "body": body,
        "createdAt": created_at,
        "dataType": "post",
        "title": title,
        "parentId": None,
        "media": media,
        "is_nsfw": is_nsfw,
        "score": score,
        "upvote_ratio": upvote_ratio,
        "num_comments": num_comments
    }


def reddit_content_from_comment(item: Dict[str, Any]) -> Dict[str, Any]:
    name = item.get("name")
    permalink = item.get("permalink") or ""
    url = f"https://www.reddit.com{permalink}"
    username = item.get("author") or "[deleted]"
    community = item.get("subreddit_name_prefixed") or (f"r/{item.get('subreddit')}" if item.get('subreddit') else None)
    body = item.get("body")
    if body is None:
        body = ""
    parent_id = item.get("parent_id") or None
    created_utc = float(item.get("created_utc") or 0)
    created_at = dt.datetime.fromtimestamp(created_utc, tz=dt.timezone.utc) if created_utc > 0 else dt.datetime.now(dt.timezone.utc)
    if not name:
        name = url
    if not community:
        community = "r/unknown"
    is_nsfw = bool(item.get("over_18") or item.get("subreddit_over18") or False)

    # Enrich with score if present
    score = item.get("score")

    return {
        "id": name,
        "url": url,
        "username": username,
        "communityName": community,
        "body": body,
        "createdAt": created_at,
        "dataType": "comment",
        "title": None,
        "parentId": parent_id,
        "media": None,
        "is_nsfw": is_nsfw,
        "score": score
    }


def to_data_entities(items: List[Dict[str, Any]], typ: str) -> List[DataEntity]:
    entities: List[DataEntity] = []
    for it in items:
        try:
            rc = reddit_content_from_post(it) if typ == "new" else reddit_content_from_comment(it)
            # RedditContent will minute-obfuscate createdAt, normalize label, etc.
            content = RedditContent(**rc)
            entities.append(RedditContent.to_data_entity(content))
        except Exception:
            continue
    return entities


def adaptive_sleep_seconds(ratelimit_remaining: Optional[float], ratelimit_reset: Optional[float],
                           safety: float = 0.8, min_sleep: float = 0.3) -> float:
    """
    Compute sleep between requests from headers:
      rpm_safe = 0.8 * (remaining/reset * 60)
      sleep = 60 / rpm_safe
    """
    try:
        R = float(ratelimit_remaining) if ratelimit_remaining is not None else None
        T = float(ratelimit_reset) if ratelimit_reset is not None else None
        if R is None or T is None or T <= 0:
            return 1.0  # default
        rpm_theoretical = (R / T) * 60.0
        rpm_safe = max(1.0, safety * rpm_theoretical)
        sleep = max(min_sleep, 60.0 / rpm_safe)
        return sleep
    except Exception:
        return 1.0


async def account_worker(
    account: Account,
    subreddits: List[str],
    db_path: Path,
    tokens_dir: Path,
    endpoint: str,
    per_account_inflight: int,
    run_seconds: int,
    max_age_days: int,
    csv_path: Optional[Path],
):
    """
    A single account loop:
      - Get/refresh token (cache {username}.json with expiry)
      - Loop labels, call listing endpoint with limit=100
      - Store DataEntities into SQLite
      - Pace based on rate-limit headers
    """
    tokens_dir.mkdir(parents=True, exist_ok=True)
    token_cache = tokens_dir / f"{account.username}.json"

    # Load from cache
    token: Optional[str] = None
    expiry_at: float = 0.0
    try:
        cache = json.loads(token_cache.read_text(encoding="utf-8"))
        token = cache.get("token")
        expiry_at = float(cache.get("expiry_unix", 0))
    except Exception:
        pass

    # helper to refresh token
    async def ensure_token():
        nonlocal token, expiry_at
        now = time.time()
        if (not token) or (expiry_at - now < 180):  # refresh if <3 minutes remaining
            # blocking call; wrap in thread executor if needed
            try:
                t, expires_in, scope = get_token_with_retry(account.client_id, account.client_secret,
                                                            account.username, account.password,
                                                            account.user_agent, retries=3, backoff_seconds=2.0)
                token = t
                expiry_at = time.time() + max(60, int(expires_in))  # at least 60s
                token_cache.write_text(json.dumps({
                    "token": token,
                    "expiry_unix": expiry_at,
                    "scope": scope
                }, indent=2), encoding="utf-8")
                log_event("INFO", f"token refreshed account={account.username} expires_in={int(expires_in)}s")
            except Exception as e:
                log_event("ERROR", f"token refresh failed account={account.username} err={e}")
                raise

    await ensure_token()

    # aiohttp session with proxy
    timeout = aiohttp.ClientTimeout(total=70)
    connector = aiohttp.TCPConnector(limit=per_account_inflight, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        storage = SqliteMinerStorage(str(db_path))
        start_time = time.time()
        label_index = 0
        # CSV logger
        def log_csv_row(sub: str, endpoint: str, limit: int, status: int, used: str, rem: str, reset: str, items: int, latency_ms: int):
            if not csv_path:
                return
            csv_path.parent.mkdir(parents=True, exist_ok=True)
            write_header = not csv_path.exists()
            with csv_path.open("a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if write_header:
                    w.writerow(["timestamp_utc", "account", "proxy", "endpoint", "subreddit", "limit",
                                "status", "ratelimit_used", "ratelimit_remaining", "ratelimit_reset",
                                "items", "latency_ms"])
                w.writerow([
                    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    account.username, account.proxy, endpoint, sub, limit,
                    status, used, rem, reset, items, latency_ms
                ])

        while True:
            if run_seconds > 0 and (time.time() - start_time) >= run_seconds:
                break

            # rotate through labels
            sub = subreddits[label_index % len(subreddits)]
            sub_name = sub[2:] if sub.lower().startswith("r/") else sub
            label_index += 1

            await ensure_token()
            headers = {"Authorization": f"Bearer {token}", "User-Agent": account.user_agent}
            limit = 100

            # choose endpoint
            chosen_endpoints = []
            if endpoint == "both":
                # alternate between new and comments
                chosen_endpoints = ["new", "comments"]
            else:
                chosen_endpoints = [endpoint]

            for ep in chosen_endpoints:
                url = (NEW_URL if ep == "new" else COMMENTS_URL).format(sub=sub_name, limit=limit)
                t0 = time.time()
                used = rem = reset = ""
                items = 0
                status = 0
                try:
                    # attach proxy per account
                    async with session.get(url, headers=headers, timeout=60, proxy=account.proxy) as resp:
                        status = resp.status
                        h = {k.lower(): v for k, v in resp.headers.items()}
                        used = h.get("x-ratelimit-used", "")
                        rem = h.get("x-ratelimit-remaining", "")
                        reset = h.get("x-ratelimit-reset", "")
                        data = await resp.json(content_type=None)
                        children = ((data.get("data") or {}).get("children") or [])
                        items = len(children)
                        if status != 200:
                            reason = ""
                            try:
                                if isinstance(data, dict):
                                    reason = str((data.get("message") or data.get("error") or data.get("reason") or ""))
                            except Exception:
                                reason = ""
                            text_snippet = ""
                            try:
                                text_snippet = json.dumps(data)[:200] if isinstance(data, dict) else str(data)[:200]
                            except Exception:
                                text_snippet = ""
                            banned = ("ban" in text_snippet.lower() or "suspend" in text_snippet.lower())
                            level = "ALERT" if (banned or status in (401, 403)) else "WARN"
                            log_event(level, f"account={account.username} sub={sub} ep={ep} status={status} reason={reason}")
                        elif items == 0:
                            log_event("INFO", f"account={account.username} sub={sub} ep={ep} status=200 items=0")
                        # convert to DataEntities
                        if items > 0:
                            typ = "new" if ep == "new" else "comments"
                            list_items = [c.get("data", {}) for c in children]
                            # Enforce last-N-days window (default 30d). Compute cutoff in UTC.
                            if max_age_days and max_age_days > 0:
                                try:
                                    cutoff_ts = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=max_age_days)).timestamp()
                                except Exception:
                                    cutoff_ts = None
                                if cutoff_ts is not None:
                                    before_len = len(list_items)
                                    list_items = [it for it in list_items if float(it.get("created_utc") or 0) >= cutoff_ts]
                                    dropped = before_len - len(list_items)
                                    if dropped > 0:
                                        log_event("INFO", f"filtered_by_age account={account.username} sub={sub} ep={ep} dropped={dropped}")
                            entities = to_data_entities(list_items, typ="new" if ep == "new" else "comments")
                            converted = len(entities)
                            log_event("INFO", f"converted account={account.username} sub={sub} ep={ep} typ={typ} items={items} entities={converted}")
                            if converted == 0:
                                log_event("WARN", f"conversion yielded zero entities account={account.username} sub={sub} ep={ep} typ={typ}")
                            # store entities in chunks
                            CHUNK = 2000
                            for i in range(0, len(entities), CHUNK):
                                subset = entities[i:i + CHUNK]
                                try:
                                    storage.store_data_entities(subset)
                                except Exception as e:
                                    log_event("ERROR", f"store failed account={account.username} sub={sub} ep={ep} count={len(subset)} err={e}")
                                    traceback.print_exc()
                            if converted > 0:
                                log_event("INFO", f"stored account={account.username} sub={sub} ep={ep} typ={typ} stored={converted}")
                except Exception as e:
                    # keep defaults
                    log_event("ERROR", f"request failed account={account.username} sub={sub} ep={ep} err={e}")
                    traceback.print_exc()
                latency_ms = int((time.time() - t0) * 1000)
                log_csv_row(sub, ep, limit, status, used, rem, reset, items, latency_ms)

                # compute adaptive sleep from headers
                rem_f = None
                reset_f = None
                try:
                    rem_f = float(rem) if rem != "" else None
                    reset_f = float(reset) if reset != "" else None
                except Exception:
                    pass
                sleep_s = adaptive_sleep_seconds(rem_f, reset_f, safety=0.8, min_sleep=0.3)
                # add small jitter to avoid sync across workers
                sleep_s += random.uniform(0, 0.2)
                await asyncio.sleep(sleep_s)


def shard_labels(labels: List[str], n: int) -> List[List[str]]:
    """Round-robin shard labels across n buckets."""
    buckets = [[] for _ in range(max(1, n))]
    for i, lab in enumerate(labels):
        buckets[i % len(buckets)].append(lab)
    return buckets


def parse_args():
    import argparse
    p = argparse.ArgumentParser(description="Multi-account Reddit runner with 1:1 proxy mapping, token cache, and adaptive pacing.")
    p.add_argument("--accounts-json", type=str, default=str(REPO_ROOT / "reddit.json"))
    p.add_argument("--proxies-file", type=str, default=str(REPO_ROOT / "reddit-verifier" / "proxies.txt"))
    p.add_argument("--max-accounts", type=int, default=15)
    p.add_argument("--db", type=str, default="SqliteMinerStorage.sqlite")
    p.add_argument("--endpoint", choices=["new", "comments", "both"], default="both")
    p.add_argument("--per-account-inflight", type=int, default=1)
    p.add_argument("--run-seconds", type=int, default=0)
    p.add_argument("--tokens-dir", type=str, default=str(DEFAULT_TOKENS_DIR))
    # Date window for Reddit (default last 30 days)
    p.add_argument("--max-age-days", type=int, default=30, help="Keep only items created within the last N days (default: 30). Set 0 to disable.")

    # account keys in JSON (customizable)
    p.add_argument("--user-key", type=str, default="username")
    p.add_argument("--pass-key", type=str, default="password")
    p.add_argument("--cid-key", type=str, default="client_id")
    p.add_argument("--csec-key", type=str, default="client_secret")
    p.add_argument("--ua-key", type=str, default="user_agent")

    # targets
    p.add_argument("--refresh-targets", action="store_true")
    p.add_argument("--include-trending", action="store_true")
    p.add_argument("--max-labels", type=int, default=60)
    p.add_argument("--include-static-all", action="store_true", help="Include ALL subreddits from scripts/static/reddit_labels.json in addition to DD targets before sharding")

    # logging
    p.add_argument("--log-csv", type=str, default=str(REPO_ROOT / "scripts" / "cache" / "reddit_multi.csv"))
    return p.parse_args()


def main():
    args = parse_args()
    try:
        accounts_data = load_accounts(Path(args.accounts_json),
                                      user_key=args.user_key, pass_key=args.pass_key,
                                      cid_key=args.cid_key, csec_key=args.csec_key, ua_key=args.ua_key)
        proxies = load_proxies(Path(args.proxies_file))
        accounts = map_accounts_to_proxies(accounts_data, proxies, args.max_accounts)
        if not accounts:
            print("[ERROR] No accounts or proxies available.")
            sys.exit(1)

        if args.refresh_targets:
            try:
                update_reddit_targets(include_trending=args.include_trending, max_labels=args.max_labels, longtail_ratio=0.2)
            except Exception as e:
                print(f"[WARN] Failed to refresh targets: {e}")

        labels = get_reddit_labels_from_cache()
        if not labels:
            print("[ERROR] No target subreddits available (cache + static failed).")
            sys.exit(1)

        # Optionally merge ALL static subreddits in addition to DD/trending targets
        if args.include_static_all:
            try:
                static_labels = load_static_reddit_labels()
            except Exception:
                static_labels = []
            # De-duplicate while preserving order: DD labels first, then static additions
            seen = set()
            merged = []
            for lab in labels + static_labels:
                key = lab.lower()
                if key not in seen:
                    seen.add(key)
                    merged.append(lab)
            labels = merged

        # Cap labels after merge to avoid overwhelming each account if desired
        labels = labels[:max(1, args.max_labels)]
        shards = shard_labels(labels, len(accounts))

        print(f"[INFO] Starting {len(accounts)} account workers. Endpoint={args.endpoint} Inflight={args.per_account_inflight} RunSeconds={args.run_seconds}")
        db_path = Path(args.db)
        tokens_dir = Path(args.tokens_dir)
        csv_path = Path(args.log_csv) if args.log_csv else None

        async def run_all():
            tasks = []
            for i, acc in enumerate(accounts):
                subs = shards[i] if i < len(shards) else []
                if not subs:
                    subs = labels[i::len(accounts)]
                tasks.append(asyncio.create_task(account_worker(
                    account=acc,
                    subreddits=subs,
                    db_path=db_path,
                    tokens_dir=tokens_dir,
                    endpoint=args.endpoint,
                    per_account_inflight=max(1, min(2, args.per_account_inflight)),
                    run_seconds=args.run_seconds,
                    max_age_days=max(0, int(args.max_age_days)),
                    csv_path=csv_path
                )))
            await asyncio.gather(*tasks)

        try:
            asyncio.run(run_all())
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(run_all())
            finally:
                loop.close()

        print("[OK] Multi-runner finished.")

    except Exception as e:
        print("[FATAL] ", str(e))
        print(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
