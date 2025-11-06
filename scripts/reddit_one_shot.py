#!/usr/bin/env python3
"""
Reddit OAuth lifecycle demo: obtain OAuth token (no cookies), verify it, fetch 1 entity,
and print it as RedditContent JSON (same shape our scraper stores).

What it uses (no session cookies):
- Reddit official OAuth "password" grant with a SCRIPT app:
  - client_id, client_secret, username, password, user_agent
- Returns a Bearer access_token which is used to call the API (no cookies needed).

Requirements:
- The Reddit app type must be "script" (https://www.reddit.com/prefs/apps).
- The username/password must belong to the same account that owns the script app.
- The account must NOT have 2FA enabled (password grant cannot provide TOTP).
- Python 3 standard library only (urllib), no external deps.

Examples:
  # Pass credentials explicitly
  python scripts/reddit_one_shot.py \
    --client-id YYYYY --client-secret ZZZZZ \
    --username myuser --password mypass \
    --user-agent "MyUA/1.0 by u/myuser" \
    --subreddit bittensor_ --type post

  # Use environment variables instead (set and add --env)
  export REDDIT_CLIENT_ID=YYYYY
  export REDDIT_CLIENT_SECRET=ZZZZZ
  export REDDIT_USERNAME=myuser
  export REDDIT_PASSWORD=mypass
  export REDDIT_USER_AGENT="MyUA/1.0 by u/myuser"
  python scripts/reddit_one_shot.py --env --subreddit cryptocurrency --type comment
"""

import os
import sys
import json
import base64
import argparse
from typing import Any, Dict, Optional, Tuple
from datetime import datetime, timezone, timedelta
import urllib.parse
import urllib.request
import urllib.error
import time
from pathlib import Path

# Verbose/teaching mode (set from CLI flags in main)
VERBOSE: bool = False
SHOW_TOKEN: bool = False  # if true, prints raw access_token (not recommended)

def mask_value(value: str, keep: int = 6) -> str:
    """Mask sensitive values for printing."""
    if not isinstance(value, str):
        return str(value)
    if len(value) <= keep:
        return "*" * len(value)
    return value[:keep] + "..." + ("*" * max(0, len(value) - keep - 3))

def print_section(title: str):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)

def validate_reddit_content(rc: Dict[str, Any], entity_type: str) -> bool:
    """Minimal validation to mirror what validators expect for structure."""
    ok = True
    # Required fields
    required = ["id", "url", "username", "communityName", "createdAt", "dataType"]
    for k in required:
        if k not in rc or rc[k] is None:
            print(f"  ❌ Missing field: {k}")
            ok = False
    # communityName must start with r/
    if rc.get("communityName") and not str(rc["communityName"]).lower().startswith("r/"):
        print("  ❌ communityName must start with 'r/'")
        ok = False
    # createdAt must be minute-obfuscated (seconds == :00)
    ca = rc.get("createdAt")
    if isinstance(ca, str) and len(ca) >= 19:
        # e.g., 2025-09-20T01:58:00Z
        if not (ca[17:19] == "00"):
            print("  ❌ createdAt must be minute-obfuscated (seconds should be 00)")
            ok = False
    # dataType must be post/comment
    if rc.get("dataType") not in ("post", "comment"):
        print("  ❌ dataType must be 'post' or 'comment'")
        ok = False
    # For comment, parentId should exist
    if entity_type == "comment" and not rc.get("parentId"):
        print("  ❌ comment should have parentId")
        ok = False
    if ok:
        print("  ✅ RedditContent structure looks valid for validator expectations.")
    return ok


TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
ME_URL = "https://oauth.reddit.com/api/v1/me"
SUB_NEW_URL = "https://oauth.reddit.com/r/{sub}/new?limit={limit}"
SUB_COMMENTS_URL = "https://oauth.reddit.com/r/{sub}/comments?limit={limit}"


def http_request(
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    form: Optional[Dict[str, str]] = None,
    timeout: int = 60,
) -> Tuple[int, bytes, Dict[str, str]]:
    data = None
    if form is not None:
        encoded = urllib.parse.urlencode(form).encode("utf-8")
        data = encoded
    req = urllib.request.Request(url, data=data, method=method)
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            body = resp.read()
            resp_headers = {k.lower(): v for k, v in resp.getheaders()}
            return status, body, resp_headers
    except urllib.error.HTTPError as e:
        return e.code, e.read(), {k.lower(): v for k, v in e.headers.items()}
    except Exception as e:
        raise RuntimeError(f"HTTP request failed: {e}")


def to_iso_minute(ts_utc_seconds: float) -> str:
    # Minute obfuscation: zero seconds/microseconds, keep UTC
    dt = datetime.fromtimestamp(ts_utc_seconds, tz=timezone.utc)
    dt = dt.replace(second=0, microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def normalize_subreddit_prefixed(s: Optional[str], fallback: Optional[str]) -> Optional[str]:
    if s and isinstance(s, str):
        return s if s.startswith("r/") else f"r/{s}"
    if fallback:
        return f"r/{fallback}"
    return None


def get_token(
    client_id: str,
    client_secret: str,
    username: str,
    password: str,
    user_agent: str,
) -> Tuple[str, int, str]:
    # Basic Auth header
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    headers = {
        "Authorization": f"Basic {basic}",
        "User-Agent": user_agent,
        "Content-Type": "application/x-www-form-urlencoded",
    }
    form = {
        "grant_type": "password",
        "username": username,
        "password": password,
        "scope": "read identity",
    }
    if VERBOSE:
        print_section("Step 1A: OAuth Token Request (POST)")
        print("URL:", TOKEN_URL)
        print("Headers:")
        print("  User-Agent:", user_agent)
        print("  Authorization: Basic", mask_value(f'{client_id}:{client_secret}', keep=6))
        print("  Content-Type: application/x-www-form-urlencoded")
        print("Body:")
        print(" ", f"grant_type=password&username={username}&password=****&scope={form['scope']}")
    status, body, resp_headers = http_request(TOKEN_URL, method="POST", headers=headers, form=form)
    if VERBOSE:
        print_section("Step 1B: OAuth Token Response")
        print("Status:", status)
        try:
            dbg = json.loads(body.decode("utf-8"))
            dbg_print = dict(dbg)
            if not SHOW_TOKEN and "access_token" in dbg_print:
                dbg_print["access_token"] = mask_value(dbg_print["access_token"], keep=8)
            print(json.dumps(dbg_print, indent=2))
        except Exception:
            print(body.decode("utf-8", "ignore"))
    if status != 200:
        # Helpful diagnostics
        www = resp_headers.get("www-authenticate", "")
        raise RuntimeError(f"Token request failed: {status} {body.decode('utf-8', 'ignore')} (WWW-Authenticate: {www})")
    data = json.loads(body.decode("utf-8"))
    token = data.get("access_token")
    if not token:
        raise RuntimeError(f"Token response missing access_token: {data}")
    # Token TTL and scopes
    expires_in = int(data.get("expires_in", 3600))
    scope_str = data.get("scope", "")
    return token, expires_in, scope_str

def get_token_with_retry(
    client_id: str,
    client_secret: str,
    username: str,
    password: str,
    user_agent: str,
    retries: int = 3,
    backoff_seconds: float = 2.0,
) -> Tuple[str, int, str]:
    """
    Wrapper around get_token() that retries on transient server/network errors (e.g., 5xx/timeout).
    """
    last_err = None
    for attempt in range(retries):
        try:
            return get_token(client_id, client_secret, username, password, user_agent)
        except Exception as e:
            msg = str(e)
            last_err = e
            transient = any(x in msg for x in [" 500 ", " 502 ", " 503 ", " 504 ", "timed out", "temporar", "CDN"])
            if attempt < retries - 1 and transient:
                sleep_for = backoff_seconds * (2 ** attempt)
                print(f"  ⚠️ Token request transient error ({msg.strip()}). Retrying in {sleep_for:.1f}s...")
                time.sleep(sleep_for)
                continue
            break
    raise last_err


def get_me(token: str, user_agent: str) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": user_agent,
    }
    if VERBOSE:
        print_section("Step 2A: /api/v1/me Request (GET)")
        print("URL:", ME_URL)
        print("Headers:")
        print("  Authorization: Bearer", mask_value(token, keep=10) if not SHOW_TOKEN else token)
        print("  User-Agent:", user_agent)
    status, body, _ = http_request(ME_URL, method="GET", headers=headers)
    if VERBOSE:
        print_section("Step 2B: /api/v1/me Response")
        print("Status:", status)
        try:
            print(json.dumps(json.loads(body.decode("utf-8")), indent=2))
        except Exception:
            print(body.decode("utf-8", "ignore"))
    if status != 200:
        raise RuntimeError(f"/me failed: {status} {body.decode('utf-8', 'ignore')}")
    return json.loads(body.decode("utf-8"))


def fetch_one_post(subreddit: str, token: str, user_agent: str, limit: int) -> list[Dict[str, Any]]:
    url = SUB_NEW_URL.format(sub=subreddit, limit=max(1, min(100, limit)))
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": user_agent,
    }
    if VERBOSE:
        print_section("Step 3A: Fetch Post(s) Request (GET)")
        print("URL:", url)
        print("Headers:")
        print("  Authorization: Bearer", mask_value(token, keep=10) if not SHOW_TOKEN else token)
        print("  User-Agent:", user_agent)
        print(f"Query: limit={max(1, min(100, limit))} (new posts)")
    status, body, _ = http_request(url, method="GET", headers=headers)
    if status != 200:
        raise RuntimeError(f"Fetch post failed: {status} {body.decode('utf-8', 'ignore')}")
    data = json.loads(body.decode("utf-8"))
    if VERBOSE:
        print_section("Step 3B: Raw Post API Response (excerpt)")
        try:
            child = (data.get("data") or {}).get("children", [])
            raw = child[0]["data"] if child else {}
            excerpt = {
                "name": raw.get("name"),
                "author": raw.get("author"),
                "subreddit_name_prefixed": raw.get("subreddit_name_prefixed"),
                "created_utc": raw.get("created_utc"),
                "permalink": raw.get("permalink"),
                "over_18": raw.get("over_18"),
                "title": raw.get("title"),
                "selftext_len": len(raw.get("selftext") or "")
            }
            print(json.dumps(excerpt, indent=2))
        except Exception:
            print("(could not parse excerpt)")
    children = (data.get("data") or {}).get("children", [])
    if not children:
        raise RuntimeError("No posts found in subreddit.")
    return [c.get("data", {}) for c in children][:max(1, min(100, limit))]


def fetch_one_comment(subreddit: str, token: str, user_agent: str, limit: int) -> list[Dict[str, Any]]:
    url = SUB_COMMENTS_URL.format(sub=subreddit, limit=max(1, min(100, limit)))
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": user_agent,
    }
    if VERBOSE:
        print_section("Step 3A: Fetch Comment(s) Request (GET)")
        print("URL:", url)
        print("Headers:")
        print("  Authorization: Bearer", mask_value(token, keep=10) if not SHOW_TOKEN else token)
        print("  User-Agent:", user_agent)
        print(f"Query: limit={max(1, min(100, limit))} (recent comments)")
    status, body, _ = http_request(url, method="GET", headers=headers)
    if status != 200:
        raise RuntimeError(f"Fetch comment failed: {status} {body.decode('utf-8', 'ignore')}")
    data = json.loads(body.decode("utf-8"))
    if VERBOSE:
        print_section("Step 3B: Raw Comment API Response (excerpt)")
        try:
            child = (data.get("data") or {}).get("children", [])
            raw = child[0]["data"] if child else {}
            excerpt = {
                "name": raw.get("name"),
                "author": raw.get("author"),
                "subreddit_name_prefixed": raw.get("subreddit_name_prefixed"),
                "created_utc": raw.get("created_utc"),
                "permalink": raw.get("permalink"),
                "over_18": raw.get("over_18"),
                "subreddit_over18": raw.get("subreddit_over18"),
                "parent_id": raw.get("parent_id"),
                "body_len": len(raw.get("body") or "")
            }
            print(json.dumps(excerpt, indent=2))
        except Exception:
            print("(could not parse excerpt)")
    children = (data.get("data") or {}).get("children", [])
    if not children:
        raise RuntimeError("No comments found in subreddit.")
    return [c.get("data", {}) for c in children][:max(1, min(100, limit))]


def reddit_content_from_post(item: Dict[str, Any]) -> Dict[str, Any]:
    # Transform Reddit submission to our RedditContent JSON (as stored in DataEntity.content).
    name = item.get("name")  # e.g., t3_xxx
    permalink = item.get("permalink") or ""
    url = f"https://www.reddit.com{permalink}"
    username = item.get("author") or "[deleted]"
    community = normalize_subreddit_prefixed(item.get("subreddit_name_prefixed"), item.get("subreddit"))
    body = item.get("selftext") or None
    title = item.get("title") or None
    created_utc = float(item.get("created_utc") or 0)
    created_at = to_iso_minute(created_utc)
    is_nsfw = bool(item.get("over_18") or False)

    return {
        "id": name,
        "url": url,
        "username": username,
        "communityName": community,
        "body": body,
        "createdAt": created_at,   # minute-obfuscated in serialized content
        "dataType": "post",
        "title": title,
        "parentId": None,
        "media": None,             # left None to avoid validation penalties if uncertain
        "is_nsfw": is_nsfw
    }


def reddit_content_from_comment(item: Dict[str, Any]) -> Dict[str, Any]:
    name = item.get("name")  # e.g., t1_xxx
    permalink = item.get("permalink") or ""
    url = f"https://www.reddit.com{permalink}"
    username = item.get("author") or "[deleted]"
    community = normalize_subreddit_prefixed(item.get("subreddit_name_prefixed"), item.get("subreddit"))
    body = item.get("body") or None
    parent_id = item.get("parent_id") or None
    created_utc = float(item.get("created_utc") or 0)
    created_at = to_iso_minute(created_utc)
    # For comments, NSFW is approximated from flags when available
    is_nsfw = bool(item.get("over_18") or item.get("subreddit_over18") or False)

    return {
        "id": name,
        "url": url,
        "username": username,
        "communityName": community,
        "body": body,
        "createdAt": created_at,   # minute-obfuscated in serialized content
        "dataType": "comment",
        "title": None,
        "parentId": parent_id,
        "media": None,
        "is_nsfw": is_nsfw
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Demonstrate Reddit OAuth + fetch N entities, output RedditContent JSON.")
    p.add_argument("--env", action="store_true", help="Read credentials from environment variables")
    p.add_argument("--client-id", dest="client_id", type=str, help="Reddit script app client_id")
    p.add_argument("--client-secret", dest="client_secret", type=str, help="Reddit script app client_secret")
    p.add_argument("--username", dest="username", type=str, help="Reddit username (same account that owns app)")
    p.add_argument("--password", dest="password", type=str, help="Reddit password (2FA must be OFF)")
    p.add_argument("--user-agent", dest="user_agent", type=str, help='User-Agent, e.g., "MyUA/1.0 by u/<username>"')
    p.add_argument("--subreddit", type=str, default="bittensor_", help="Subreddit to query (without r/)")
    p.add_argument("--type", choices=["post", "comment"], default="post", help="Entity type to fetch")
    p.add_argument("--limit", type=int, default=1, help="How many items to fetch (max 100 per single request)")
    p.add_argument("--verbose", action="store_true", help="Print full lifecycle: requests and raw API responses (token masked)")
    p.add_argument("--show-token", action="store_true", help="Also print raw access_token (not recommended)")
    return p.parse_args()


def load_creds(args: argparse.Namespace) -> Tuple[str, str, str, str, str]:
    if args.env:
        cid = os.getenv("REDDIT_CLIENT_ID", "")
        secret = os.getenv("REDDIT_CLIENT_SECRET", "")
        user = os.getenv("REDDIT_USERNAME", "")
        pwd = os.getenv("REDDIT_PASSWORD", "")
        ua = os.getenv("REDDIT_USER_AGENT", "")
    else:
        cid = args.client_id or ""
        secret = args.client_secret or ""
        user = args.username or ""
        pwd = args.password or ""
        ua = args.user_agent or ""
    missing = [k for k, v in {
        "client_id": cid, "client_secret": secret, "username": user, "password": pwd, "user_agent": ua
    }.items() if not v]
    if missing:
        raise SystemExit(f"Missing credentials: {', '.join(missing)}. Use --env or pass flags.")
    return cid, secret, user, pwd, ua


def main():
    args = parse_args()
    # Set teaching flags
    global VERBOSE, SHOW_TOKEN
    VERBOSE = bool(args.verbose)
    SHOW_TOKEN = bool(args.show_token)
    cid, secret, user, pwd, ua = load_creds(args)

    print("Step 1: Requesting OAuth token (no cookies/session needed)...")
    try:
        # Use a small cache to avoid requesting tokens too frequently
        cache_env = os.getenv("REDDIT_TOKEN_CACHE_PATH", "")
        if cache_env:
            cache_path = Path(cache_env)
        else:
            cache_path = Path(__file__).resolve().parent / "cache" / "reddit_token.json"
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        token: Optional[str] = None
        expires_in: Optional[int] = None
        scope = ""
        # Try cache first
        try:
            with cache_path.open("r", encoding="utf-8") as f:
                cached = json.load(f)
            expiry_at_iso = cached.get("expiry_at_utc")
            scope = cached.get("scope", "")
            if cached.get("token") and expiry_at_iso:
                expiry_dt = datetime.fromisoformat(expiry_at_iso.replace("Z", "+00:00"))
                # Renew if within 120s of expiry
                remaining = (expiry_dt - datetime.now(timezone.utc)).total_seconds()
                if remaining > 120:
                    token = cached["token"]
                    expires_in = int(remaining)
        except Exception:
            pass

        if not token:
            token, expires_in, scope = get_token_with_retry(cid, secret, user, pwd, ua, retries=3, backoff_seconds=2.0)
            expiry_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
            with cache_path.open("w", encoding="utf-8") as f:
                json.dump({
                    "token": token,
                    "scope": scope,
                    "expiry_at_utc": expiry_at.isoformat().replace("+00:00", "Z")
                }, f, indent=2)
            if VERBOSE:
                print("  🔄 New token requested from Reddit.")
        else:
            expiry_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
            if VERBOSE:
                print(f"  ♻️ Using cached token (~{expires_in}s remaining).")

        print(f"  ✅ Got access token (expires_in={expires_in}s, scope='{scope}').")
        print(f"  ⏱️ Token expiry at: {expiry_at.isoformat()}")
    except Exception as e:
        print("  ❌ Failed to obtain token.")
        print(f"  Hint: app must be type 'script'; account must match app owner; 2FA must be OFF; valid User-Agent.")
        print(f"  Error: {e}")
        sys.exit(1)

    print("Step 2: Verifying token with /api/v1/me ...")
    try:
        me = get_me(token, ua)
        print(f"  ✅ Authenticated as: {me.get('name')} (id: {me.get('id')})")
    except Exception as e:
        print("  ❌ /me failed.")
        print(f"  Error: {e}")
        sys.exit(1)

    sub = args.subreddit
    print(f"Step 3: Fetching {args.limit} {args.type}(s) from r/{sub} ...")
    try:
        if args.type == "post":
            items = fetch_one_post(sub, token, ua, args.limit)
            transform = reddit_content_from_post
        else:
            items = fetch_one_comment(sub, token, ua, args.limit)
            transform = reddit_content_from_comment

        print_section("Step 4: Transformed RedditContent JSON list (validator-ready)")
        if not items:
            print("No entities returned.")
        for idx, it in enumerate(items, 1):
            rc = transform(it)
            print(f"\n--- [{idx}/{len(items)}] ---")
            print(json.dumps(rc, indent=2))
            print("Validation:")
            validate_reddit_content(rc, args.type)
    except Exception as e:
        print(f"  ❌ Fetch failed: {e}")
        sys.exit(1)

    print("\nDone.")
    print("\nNotes:")
    print("- This flow uses OAuth (client_id, client_secret, username, password, user_agent) to obtain a Bearer token.")
    print("- No session cookies are required, and Postman-like 'cookie jars' are not needed for script apps.")
    print("- The printed JSON matches our scraper's serialized RedditContent (createdAt is minute-obfuscated).")


if __name__ == "__main__":
    main()
