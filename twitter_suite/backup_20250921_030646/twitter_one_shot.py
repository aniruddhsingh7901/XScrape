#!/usr/bin/env python3
"""
Twitter/X lifecycle demo (cookie-based): use twscrape with an account's cookies to fetch N tweets
and print them as XContent JSON (same shape our miner stores inside DataEntity.content).

What it uses (no official OAuth here):
- twscrape API client (vendored under twitter/twscrape)
- Cookie-based session (ct0 and auth_token recommended)
- Optional username/password/email/email_password for login flow (not required if cookies are good)

Steps shown (teaching mode):
  1) Load account + cookies (and optional proxy)
  2) Initialize twscrape and add the account
  3) Search a query with limit
  4) Transform to XContent (validator format) and print JSON with timestamp obfuscated to the minute
  5) Minimal validation checks for XContent JSON
"""

import os
import sys
import re
import json
import argparse
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from pathlib import Path

# Make vendored twscrape and project modules importable
BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR / "twitter" / "twscrape"))
sys.path.insert(0, str(BASE_DIR))

# twscrape
try:
    from twscrape import API  # type: ignore
    TWSCRAPE_AVAILABLE = True
except Exception as e:
    TWSCRAPE_AVAILABLE = False

# Project model for validator-compatible content
from scraping.x.model import XContent
from scraping import utils as scrape_utils  # obfuscation helper if needed (we use XContent.to_data_entity)
from common.data import DataEntity  # for type hints only


VERBOSE: bool = False


def print_section(title: str):
    print("\\n" + "=" * 80)
    print(title)
    print("=" * 80)


def mask_value(v: str, keep: int = 6) -> str:
    if not isinstance(v, str):
        return str(v)
    if len(v) <= keep:
        return "*" * len(v)
    return v[:keep] + "..." + ("*" * max(0, len(v) - keep - 3))


def extract_hashtags(text: str, fallback: List[str]) -> List[str]:
    """Extract hashtags preserving order; fallback to provided list if twscrape already parsed them."""
    # Prefer provided "fallback" list if present/non-empty (e.g., tweet.hashtags)
    tags = []
    if fallback:
        # Ensure prefixed with '#'
        tags = [f"#{t}" if not str(t).startswith("#") else str(t) for t in fallback]
    else:
        # Regex from raw text
        tags = re.findall(r"#\\w+", text or "")
    # Deduplicate preserving order
    seen = set()
    out = []
    for t in tags:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def normalize_x_url(url: str) -> str:
    """Normalize twitter.com to x.com as per policy."""
    return (url or "").replace("twitter.com/", "x.com/")


def tweet_to_xcontent(tweet: Any) -> XContent:
    """
    Map twscrape Tweet object to XContent (static + optional fields).
    Only include safe/static fields; omit dynamic counters to avoid tolerance penalties.
    """
    # Username with '@'
    username = f"@{getattr(tweet.user, 'username', '')}".strip()

    # Text
    text = getattr(tweet, "rawContent", "") or ""

    # URL normalized to x.com
    url = normalize_x_url(getattr(tweet, "url", "") or f"https://x.com/{getattr(tweet.user, 'username', '')}/status/{getattr(tweet, 'id', '')}")

    # Timestamp (aware UTC)
    ts = getattr(tweet, "date", None)
    if ts is None:
        ts = datetime.now(timezone.utc)
    else:
        # Make it timezone-aware (assume UTC if naive)
        if getattr(ts, "tzinfo", None) is None:
            ts = ts.replace(tzinfo=timezone.utc)

    # Hashtags ordered
    fallback_tags = getattr(tweet, "hashtags", []) or []
    tags = extract_hashtags(text, fallback_tags)

    # Media URLs (photos/videos) best-effort
    media_urls: Optional[List[str]] = []
    try:
        media_attr = getattr(tweet, "media", None)
        if media_attr:
            # twscrape can return a list of media objects or a single object
            if isinstance(media_attr, list):
                for m in media_attr:
                    u = getattr(m, "url", None) or getattr(m, "media_url_https", None)
                    if u:
                        media_urls.append(u)
            else:
                u = getattr(media_attr, "url", None) or getattr(media_attr, "media_url_https", None)
                if u:
                    media_urls.append(u)
    except Exception:
        media_urls = []

    if not media_urls:
        media_urls = None

    # Static tweet ids/flags
    is_reply = getattr(tweet, "inReplyToTweetId", None) is not None
    is_quote = getattr(tweet, "isQuoted", False) if hasattr(tweet, "isQuoted") else False
    tweet_id = str(getattr(tweet, "id", "")) or None
    conversation_id = str(getattr(tweet, "conversationId", "") or getattr(tweet, "id", "")) or None

    # Static user profile fields (optional)
    user_verified = bool(getattr(tweet.user, "verified", False)) if getattr(tweet, "user", None) else None
    profile_image_url = getattr(tweet.user, "profileImageUrl", None) if getattr(tweet, "user", None) else None
    cover_picture_url = None  # not always available

    # Language if exposed
    language = getattr(tweet, "lang", None)

    return XContent(
        username=username,
        text=text,
        url=url,
        timestamp=ts,  # Will be obfuscated to minute in JSON via to_data_entity()
        tweet_hashtags=tags,
        media=media_urls,
        # Enhanced static fields
        user_verified=user_verified,
        tweet_id=tweet_id,
        is_reply=is_reply,
        is_quote=is_quote,
        conversation_id=conversation_id,
        # Additional (optional)
        language=language,
        profile_image_url=profile_image_url,
        cover_picture_url=cover_picture_url,
        # Omit dynamic fields (likes/retweets/etc.) to avoid validator tolerance checks
    )


def validate_xcontent_json(obj: Dict[str, Any]) -> bool:
    """
    Minimal XContent JSON validation (what validators fundamentally require):
      - Required fields exist: username, text, url, tweet_hashtags, timestamp
      - URL domain is x.com
      - timestamp minute-obfuscated (seconds == 00)
      - tweet_hashtags is a list; first tag (if present) will be used as label
    """
    ok = True
    req = ["username", "text", "url", "tweet_hashtags", "timestamp"]
    for k in req:
        if k not in obj or obj[k] in (None, ""):
            print(f"  ❌ Missing/empty required field: {k}")
            ok = False

    url = obj.get("url", "")
    if "x.com" not in url:
        print("  ❌ URL must be x.com")
        ok = False

    ts = obj.get("timestamp", "")
    # Expect ISO like 2025-09-21T01:23:00+00:00 or Z – seconds must be 00
    try:
        # Looser check: seconds characters at position 17-19 == "00" if standard ISO
        if len(ts) >= 19 and ts[17:19] != "00":
            print("  ❌ timestamp must be minute-obfuscated (seconds should be 00)")
            ok = False
    except Exception:
        print("  ❌ timestamp format not recognized")
        ok = False

    tags = obj.get("tweet_hashtags", [])
    if not isinstance(tags, list):
        print("  ❌ tweet_hashtags must be a list")
        ok = False

    if ok:
        print("  ✅ XContent JSON looks valid for validator expectations.")
    return ok


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Demonstrate Twitter scraping (twscrape) + output XContent JSON.")
    # Account via ENV
    p.add_argument("--env", action="store_true", help="Read account/proxy settings from environment variables")
    # Cookies-based (recommended)
    p.add_argument("--username", type=str, help="Account username (used to label the account in pool)")
    p.add_argument("--password", type=str, help="Account password (used if logging in)")
    p.add_argument("--email", type=str, help="Account email (used if logging in)")
    p.add_argument("--email-password", dest="email_password", type=str, help="Email password (IMAP-capable provider) for login flow")
    p.add_argument("--cookies", type=str, help='Cookies string, e.g. "ct0=...; auth_token=..." (recommended)')
    # Proxy / Pool
    p.add_argument("--proxy", type=str, default=None, help="Proxy URL, e.g. http://user:pass@host:port")
    p.add_argument("--pool", type=str, default=None, help="Path to twscrape accounts DB to reuse existing sessions (e.g., scripts/cache/twscrape_refresh.db)")
    # Query
    p.add_argument("--query", type=str, default="#bittensor -filter:retweets", help="Search query (X advanced search syntax)")
    p.add_argument("--limit", type=int, default=1, help="How many tweets to fetch")
    # Verbose
    p.add_argument("--verbose", action="store_true", help="Verbose/teaching mode")
    return p.parse_args()


def load_params(args: argparse.Namespace):
    global VERBOSE
    VERBOSE = bool(args.verbose)

    if args.env:
        username = os.getenv("TW_USERNAME", "") or (args.username or "")
        password = os.getenv("TW_PASSWORD", "") or (args.password or "")
        email = os.getenv("TW_EMAIL", "") or (args.email or "")
        email_password = os.getenv("TW_EMAIL_PASSWORD", "") or (args.email_password or "")
        cookies = os.getenv("TW_COOKIES", "") or (args.cookies or "")
        proxy = os.getenv("TWS_PROXY", "") or (args.proxy or "")
        pool = args.pool or os.getenv("TWS_POOL", "")
    else:
        username = args.username or ""
        password = args.password or ""
        email = args.email or ""
        email_password = args.email_password or ""
        cookies = args.cookies or ""
        proxy = args.proxy or ""
        pool = args.pool or ""

    # Allow three modes:
    #  - cookies-only
    #  - full credentials (username/password/email/email_password)
    #  - pool-only (reuse an existing accounts DB)
    if not cookies and not (username and password and email and email_password) and not pool:
        raise SystemExit("Provide --cookies, or full credentials (--username --password --email --email-password), or --pool to use an existing twscrape DB.")

    if (cookies or (username and password and email and email_password)) and not username:
        # For cookies-only demo, a label is still required by twscrape; use a placeholder
        username = "cookie_user"

    return username, password, email, email_password, cookies, (proxy or None), (pool or None)


async def run_demo():
    if not TWSCRAPE_AVAILABLE:
        print("❌ twscrape is not available. Ensure vendor is present and importable.")
        sys.exit(1)

    args = parse_args()
    username, password, email, email_password, cookies, proxy, pool = load_params(args)

    print("Step 1: Initialize twscrape API with optional proxy ...")
    if pool or proxy:
        api = API(pool=pool, proxy=proxy)
    else:
        api = API()
    if proxy:
        if VERBOSE:
            print(f"  Using proxy: {mask_value(proxy, keep=12)}")
    print("  ✅ twscrape API ready.")

    # Step 2 only when adding a fresh account; if using --pool only, skip
    if cookies or (username and password and email and email_password):
        print("Step 2: Add account to pool (cookies-based is recommended) ...")
        try:
            # Will create the account; if cookies string contains ct0/auth_token, account becomes active immediately
            await api.pool.add_account(
                username=username,
                password=password or "x",
                email=email or "example@nowhere.invalid",
                email_password=email_password or "x",
                cookies=cookies if cookies else None,
                proxy=proxy or None,
            )
            print("  ✅ Account added to pool.")
        except Exception as e:
            # Could be "already exists"; continue
            print(f"  ℹ️ add_account note: {e}")

        if not cookies and (username and password and email and email_password):
            print("Step 2b: Login flow (only if credentials provided, not required for cookies)...")
            res = await api.pool.login_all()
            print(f"  ✅ Login attempted: {res}")
    else:
        if VERBOSE:
            print("Step 2: Using existing pool sessions (no add_account/login performed).")

    print("Step 3: Search query and paginate ...")
    if VERBOSE:
        print(f"  Query: {args.query}")
        print(f"  Limit: {args.limit}")

    # Collect up to N tweets
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

    print_section("Step 4: Transform each to XContent JSON (validator-ready)")
    for idx, t in enumerate(tweets, 1):
        try:
            xcontent = tweet_to_xcontent(t)

            # Obfuscate timestamp in serialized JSON by converting through DataEntity
            de: DataEntity = XContent.to_data_entity(xcontent)
            json_str = de.content.decode("utf-8")
            obj = json.loads(json_str)

            print(f"\\n--- [{idx}/{len(tweets)}] ---")
            print(json.dumps(obj, indent=2))

            print("Validation:")
            validate_xcontent_json(obj)
        except Exception as e:
            print(f"  ❌ Transform/print failed: {e}")

    print("\\nDone.")
    print("\\nNotes:")
    print("- This uses cookie/login-based session + GraphQL via twscrape, not official OAuth.")
    print("- The JSON printed is the exact XContent we store inside DataEntity.content (timestamp obfuscated to minute).")
    print("- URL is normalized to x.com; first hashtag (if present) becomes the label during miner storage.")


def main():
    import asyncio
    asyncio.run(run_demo())


if __name__ == "__main__":
    main()
