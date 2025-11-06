#!/usr/bin/env python3
"""
Target Provider for Reddit (Gravity live + cache + static + trending)

Purpose:
- Read live dynamic desirability jobs (dynamic_desirability/total.json) produced by --gravity
- Fallback to a cached copy if live is unavailable
- Fallback to a curated static list if both fail
- Optionally enrich with Reddit trending/popular subreddits via asyncpraw
- Merge, rank (by desirability weight), de-duplicate, and write a final list to scripts/cache/reddit_targets.json

Env required for trending (asyncpraw):
  REDDIT_CLIENT_ID
  REDDIT_CLIENT_SECRET
  REDDIT_USERNAME
  REDDIT_PASSWORD
  REDDIT_USER_AGENT
Optional:
  REDDIT_PROXY = http://user:pass@host:port (if you want proxy binding)

CLI:
  python scripts/target_provider.py --update-reddit-targets --max-labels 60 --longtail-ratio 0.2 --include-trending

Artifacts:
  - dynamic_desirability/total.json (live)
  - scripts/cache/total.json (cached copy of live on success)
  - scripts/static/reddit_labels.json (fallback curated list)
  - scripts/cache/reddit_targets.json (final merged output for scraper consumption)
"""

import os
import json
import asyncio
import datetime as dt
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

# Lazy import asyncpraw only when needed (so running without trending doesn't require creds)
try:
    import asyncpraw  # type: ignore
except Exception:
    asyncpraw = None  # will check later before trending

# Paths
THIS_FILE = Path(__file__).resolve()
REPO_ROOT = THIS_FILE.parents[1]  # .../data-universe
DD_TOTAL_PATH = REPO_ROOT / "dynamic_desirability" / "total.json"
CACHE_DIR = REPO_ROOT / "scripts" / "cache"
STATIC_DIR = REPO_ROOT / "scripts" / "static"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
STATIC_DIR.mkdir(parents=True, exist_ok=True)

CACHE_TOTAL_PATH = CACHE_DIR / "total.json"
STATIC_REDDIT_PATH = STATIC_DIR / "reddit_labels.json"
OUTPUT_REDDIT_TARGETS = CACHE_DIR / "reddit_targets.json"

# Twitter/X targets and trending outputs
OUTPUT_TWITTER_TARGETS = CACHE_DIR / "twitter_targets.json"
OUTPUT_TWITTER_TRENDING = CACHE_DIR / "twitter_trending.json"

# twscrape (for Twitter trending via pool search)
try:
    import sys as _sys  # noqa
    _sys.path.insert(0, str(REPO_ROOT / "twitter" / "twscrape"))
    from twscrape import API as _TW_API  # type: ignore
    _TWSCRAPE_OK = True
except Exception:
    _TWSCRAPE_OK = False


def _is_reddit_label(label: str) -> bool:
    return isinstance(label, str) and label.lower().startswith("r/")


def _normalize_reddit_label(label: str) -> str:
    # Ensure "r/" prefix and lowercase, strip spaces
    lab = label.strip()
    if not lab.lower().startswith("r/"):
        lab = f"r/{lab}"
    return f"r/{lab.split('r/', 1)[1].strip()}".lower()


def load_total_json(path: Path = DD_TOTAL_PATH) -> Optional[List[Dict[str, Any]]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return None
    except Exception:
        return None


def save_cache_total_json(jobs: List[Dict[str, Any]], path: Path = CACHE_TOTAL_PATH) -> None:
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(jobs, f, indent=2)
    except Exception:
        pass


def load_cache_total_json(path: Path = CACHE_TOTAL_PATH) -> Optional[List[Dict[str, Any]]]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return None
    except Exception:
        return None


def load_static_reddit_labels(path: Path = STATIC_REDDIT_PATH) -> List[str]:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [_normalize_reddit_label(x) for x in data if isinstance(x, str)]
    except Exception:
        pass
    # minimal safe fallback set
    return [
        "r/cryptocurrency", "r/bitcoin", "r/btc", "r/ethereum",
        "r/solana", "r/monero", "r/polkadot", "r/economics", "r/datascience"
    ]


def extract_reddit_from_jobs(jobs: List[Dict[str, Any]]) -> List[Tuple[str, float]]:
    """
    From total.json job entries (new format), extract reddit labels with weight.
    Job format assumed:
      {
        "id": "...",
        "weight": float,
        "params": {
          "platform": "reddit" | ...,
          "label": "r/...",
          ...
        }
      }
    Returns list of (label, weight).
    """
    out = []
    for job in jobs:
        try:
            params = job.get("params", {})
            platform = params.get("platform")
            label = params.get("label")
            if platform and platform.lower() == "reddit" and _is_reddit_label(label):
                out.append((_normalize_reddit_label(label), float(job.get("weight", 1.0))))
        except Exception:
            continue
    return out


async def _fetch_trending_reddit_labels_async(limit_subs: int = 100) -> List[str]:
    """
    Use asyncpraw to fetch popular/default subreddits as a proxy for trending.
    Requires env creds (CLIENT_ID/SECRET/USERNAME/PASSWORD/USER_AGENT).
    """
    if asyncpraw is None:
        return []

    client_id = os.getenv("REDDIT_CLIENT_ID")
    client_secret = os.getenv("REDDIT_CLIENT_SECRET")
    username = os.getenv("REDDIT_USERNAME")
    password = os.getenv("REDDIT_PASSWORD")
    user_agent = os.getenv("REDDIT_USER_AGENT")

    if not all([client_id, client_secret, username, password, user_agent]):
        return []

    proxy = os.getenv("REDDIT_PROXY")
    requestor_kwargs = {}
    if proxy:
        # Depending on asyncpraw version, either "proxy" or "proxies" may be valid.
        # Try "proxy" first.
        requestor_kwargs = {"proxy": proxy}

    labels: List[str] = []
    try:
        async with asyncpraw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            password=password,
            user_agent=user_agent,
            requestor_kwargs=requestor_kwargs
        ) as reddit:
            # Popular
            try:
                async for sub in reddit.subreddits.popular(limit=limit_subs):
                    if hasattr(sub, "display_name_prefixed"):
                        labels.append(_normalize_reddit_label(sub.display_name_prefixed))
            except Exception:
                pass

            # Default
            try:
                async for sub in reddit.subreddits.default(limit=limit_subs):
                    if hasattr(sub, "display_name_prefixed"):
                        labels.append(_normalize_reddit_label(sub.display_name_prefixed))
            except Exception:
                pass

    except Exception:
        return []

    # de-dup while preserving order
    seen = set()
    out: List[str] = []
    for lab in labels:
        if lab not in seen:
            seen.add(lab)
            out.append(lab)
    return out


def fetch_trending_reddit_labels(limit_subs: int = 100) -> List[str]:
    try:
        return asyncio.run(_fetch_trending_reddit_labels_async(limit_subs=limit_subs))
    except RuntimeError:
        # If already in an event loop, create a new loop for this call.
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(_fetch_trending_reddit_labels_async(limit_subs=limit_subs))
        finally:
            loop.close()


def _is_twitter_label(label: str) -> bool:
    """
    Accept basic Twitter/X labels we can feed to search:
      - Hashtags like "#bitcoin"
      - Simple advanced strings like "from:@user"
    """
    if not isinstance(label, str):
        return False
    lab = label.strip()
    return lab.startswith("#") or lab.lower().startswith("from:@") or lab.lower().startswith("to:@")


def _normalize_twitter_query(label: str) -> str:
    """
    Normalize to a safe search query and append ' -filter:retweets'
    """
    lab = label.strip()
    # ensure hashtags have '#'
    if lab and not lab.startswith("#") and lab.lower().startswith(("from:@", "to:@")):
        # pass through advanced forms
        q = lab
    else:
        # hashtag or keyword: ensure starts with '#'
        q = lab if lab.startswith("#") else f"#{lab.lstrip('#')}"
    # append RT filter if not present
    if " -filter:retweets" not in q:
        q = f"{q} -filter:retweets"
    return q


def extract_twitter_from_jobs(jobs: List[Dict[str, Any]]) -> List[Tuple[str, float]]:
    """
    From total.json jobs, extract twitter/x labels with weight.

    Expected job format:
      {
        "weight": float,
        "params": { "platform": "x"|"twitter", "label": "#bitcoin"|"from:@user" ... }
      }
    """
    out: List[Tuple[str, float]] = []
    for job in jobs:
        try:
            params = job.get("params", {})
            platform = (params.get("platform") or "").lower()
            if platform not in ("x", "twitter", "twitter/x"):
                continue
            raw = params.get("label")
            if not _is_twitter_label(raw):
                continue
            q = _normalize_twitter_query(str(raw))
            out.append((q, float(job.get("weight", 1.0))))
        except Exception:
            continue
    return out


def merge_rank_labels(
    reddit_jobs: List[Tuple[str, float]],
    trending: List[str],
    static_labels: List[str],
    max_labels: int = 60,
    longtail_ratio: float = 0.2
) -> List[str]:
    """
    Merge reddit labels from jobs (with weight), trending, and static long-tail.
    Strategy:
      - Sort reddit_jobs by weight desc (primary source)
      - Append trending labels (de-dup)
      - Append small % of static labels (long-tail) for uniqueness
    """
    # Primary: job-weighted
    sorted_jobs = sorted(reddit_jobs, key=lambda x: x[1], reverse=True)
    ordered: List[str] = []
    seen = set()
    for lab, _w in sorted_jobs:
        if lab not in seen:
            seen.add(lab)
            ordered.append(lab)
            if len(ordered) >= max_labels:
                return ordered

    # Trending next
    for lab in trending:
        if lab not in seen:
            seen.add(lab)
            ordered.append(lab)
            if len(ordered) >= max_labels:
                return ordered

    # Long-tail static (fraction)
    remaining = max_labels - len(ordered)
    if remaining > 0 and longtail_ratio > 0:
        longtail_count = max(1, int(max_labels * longtail_ratio))
        for lab in static_labels:
            if lab not in seen:
                seen.add(lab)
                ordered.append(lab)
                longtail_count -= 1
                if longtail_count <= 0 or len(ordered) >= max_labels:
                    break

    return ordered[:max_labels]


def write_reddit_targets(labels: List[str], path: Path = OUTPUT_REDDIT_TARGETS) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "count": len(labels),
        "labels": labels,
    }
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Wrote {len(labels)} reddit targets to {path}")


def write_twitter_targets(labels: List[str], path: Path = OUTPUT_TWITTER_TARGETS) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "count": len(labels),
        "labels": labels,
    }
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Wrote {len(labels)} twitter targets to {path}")


async def _fetch_trending_twitter_hashtags_via_pool_async(pool_path: str, seeds: List[str], per_query_limit: int = 20, max_out: int = 100) -> List[str]:
    """
    Heuristic trending via sampling: run a few broad searches and collect top hashtags.
    Requires a valid twscrape pool (cookies loaded).
    """
    if not _TWSCRAPE_OK:
        return []
    api = _TW_API(pool=pool_path)
    counts: Dict[str, int] = {}
    total_seen = 0

    async def consume_query(q: str):
        nonlocal total_seen
        try:
            async for t in api.search(q, limit=per_query_limit):
                total_seen += 1
                # Prefer tweet.hashtags if present; else regex from rawContent
                tags: List[str] = []
                hs = getattr(t, "hashtags", []) or []
                if hs:
                    tags = [f"#{str(x)}" if not str(x).startswith("#") else str(x) for x in hs]
                else:
                    txt = getattr(t, "rawContent", "") or ""
                    import re as _re
                    tags = _re.findall(r"#\w+", txt)
                for tag in tags:
                    tag_lc = tag.lower()
                    counts[tag_lc] = counts.get(tag_lc, 0) + 1
        except Exception:
            # ignore errors per query
            pass

    # run sequentially (pool throttles availability)
    for s in seeds:
        await consume_query(s)

    # rank by count desc
    ranked = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    # Normalize to queries (with RT filter)
    out: List[str] = []
    seen = set()
    for tag, _c in ranked:
        q = _normalize_twitter_query(tag)
        if q not in seen:
            seen.add(q)
            out.append(q)
        if len(out) >= max_out:
            break
    return out


def fetch_trending_twitter_hashtags_via_pool(pool_path: str, seeds: Optional[List[str]] = None, per_query_limit: int = 20, max_out: int = 100) -> List[str]:
    """
    Wrapper to fetch trending-like hashtags using the pool. Seeds default to broad queries.
    Always uses a dedicated event loop to avoid 'event loop is running' conflicts when called
    from async contexts (e.g., CLI subcommands executed under asyncio.run).
    """
    if seeds is None:
        seeds = ["e -filter:retweets", "news -filter:retweets", "crypto -filter:retweets"]
    if not _TWSCRAPE_OK:
        return []
    import asyncio as _asyncio
    loop = _asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_fetch_trending_twitter_hashtags_via_pool_async(pool_path, seeds, per_query_limit, max_out))
    finally:
        try:
            loop.close()
        except Exception:
            pass


def update_reddit_targets(
    include_trending: bool = True,
    max_labels: int = 60,
    longtail_ratio: float = 0.2,
    trending_limit: int = 100
) -> None:
    """
    Main orchestration for reddit targets:
      - Load live total.json; on success save a cache copy
      - Else try cache; else static only
      - Extract reddit labels+weights from jobs
      - Optionally fetch trending via asyncpraw
      - Merge & write scripts/cache/reddit_targets.json
    """
    jobs = load_total_json(DD_TOTAL_PATH)
    if jobs is not None:
        save_cache_total_json(jobs, CACHE_TOTAL_PATH)
    else:
        jobs = load_cache_total_json(CACHE_TOTAL_PATH)

    static_labels = load_static_reddit_labels(STATIC_REDDIT_PATH)

    reddit_jobs: List[Tuple[str, float]] = []
    if isinstance(jobs, list):
        reddit_jobs = extract_reddit_from_jobs(jobs)

    trending: List[str] = []
    if include_trending:
        trending = fetch_trending_reddit_labels(limit_subs=trending_limit)

    merged = merge_rank_labels(
        reddit_jobs=reddit_jobs,
        trending=trending,
        static_labels=static_labels,
        max_labels=max_labels,
        longtail_ratio=longtail_ratio,
    )
    write_reddit_targets(merged, OUTPUT_REDDIT_TARGETS)


def update_twitter_targets(
    pool_path: str,
    include_trending: bool = True,
    max_labels: int = 60,
    longtail_ratio: float = 0.2,
    per_query_limit: int = 20,
    trending_max_out: int = 100
) -> None:
    """
    Orchestrate Twitter/X targets:
      - Load dynamic_desirability total.json; extract twitter labels with weight
      - Optionally fetch trending-like hashtags via pool sampling
      - Merge (gravity first by weight, then trending) and write scripts/cache/twitter_targets.json
      - Also write scripts/cache/twitter_trending.json with raw trending labels if available
    """
    jobs = load_total_json(DD_TOTAL_PATH) or load_cache_total_json(CACHE_TOTAL_PATH) or []
    tw_jobs = extract_twitter_from_jobs(jobs if isinstance(jobs, list) else [])
    # gravity first
    gravity_sorted = sorted(tw_jobs, key=lambda x: x[1], reverse=True)
    gravity_labels = []
    seen = set()
    for q, _w in gravity_sorted:
        if q not in seen:
            seen.add(q)
            gravity_labels.append(q)
            if len(gravity_labels) >= max_labels:
                break

    trending_labels: List[str] = []
    if include_trending and _TWSCRAPE_OK and pool_path:
        trending_labels = fetch_trending_twitter_hashtags_via_pool(pool_path, per_query_limit=per_query_limit, max_out=trending_max_out)
        # write raw trending snapshot
        try:
            OUTPUT_TWITTER_TRENDING.parent.mkdir(parents=True, exist_ok=True)
            with OUTPUT_TWITTER_TRENDING.open("w", encoding="utf-8") as f:
                json.dump({
                    "updated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "count": len(trending_labels),
                    "labels": trending_labels,
                }, f, indent=2)
            print(f"Wrote trending snapshot to {OUTPUT_TWITTER_TRENDING}")
        except Exception:
            pass

    # merge gravity + trending
    merged: List[str] = []
    seen2 = set()
    for q in gravity_labels:
        if q not in seen2:
            seen2.add(q)
            merged.append(q)
            if len(merged) >= max_labels:
                break
    if len(merged) < max_labels:
        for q in trending_labels:
            if q not in seen2:
                seen2.add(q)
                merged.append(q)
                if len(merged) >= max_labels:
                    break

    write_twitter_targets(merged, OUTPUT_TWITTER_TARGETS)


def get_reddit_labels_from_cache() -> List[str]:
    """
    Helper for scrapers: read scripts/cache/reddit_targets.json and return labels list.
    """
    try:
        with OUTPUT_REDDIT_TARGETS.open("r", encoding="utf-8") as f:
            data = json.load(f)
        labels = data.get("labels", [])
        if isinstance(labels, list):
            return [_normalize_reddit_label(l) for l in labels if isinstance(l, str)]
    except Exception:
        pass
    # fall back to static if cache missing
    return load_static_reddit_labels(STATIC_REDDIT_PATH)


def _parse_args():
    import argparse
    p = argparse.ArgumentParser(description="Target Provider for Reddit (Gravity + Cache + Static + Trending)")
    p.add_argument("--update-reddit-targets", action="store_true", help="Update scripts/cache/reddit_targets.json")
    p.add_argument("--max-labels", type=int, default=60, help="Max number of reddit labels to emit")
    p.add_argument("--longtail-ratio", type=float, default=0.2, help="Fraction of static labels to mix in for uniqueness")
    p.add_argument("--include-trending", action="store_true", help="Include reddit popular/default subreddits via asyncpraw")
    p.add_argument("--trending-limit", type=int, default=100, help="Limit for popular/default subreddits per call")
    # Twitter/X options
    p.add_argument("--update-twitter-targets", action="store_true", help="Update scripts/cache/twitter_targets.json")
    p.add_argument("--include-twitter-trending", action="store_true", help="Include trending-like hashtags via pool sampling")
    p.add_argument("--pool", type=str, default=str(REPO_ROOT / "scripts" / "cache" / "twscrape_refresh.db"), help="Path to twscrape pool DB for Twitter sampling")
    p.add_argument("--twitter-max-labels", type=int, default=60, help="Max number of twitter labels to emit")
    p.add_argument("--twitter-per-query-limit", type=int, default=20, help="Tweets to sample per seed query when building trending")
    p.add_argument("--twitter-trending-max-out", type=int, default=100, help="Max trending labels to keep before merging")
    return p.parse_args()


def main():
    args = _parse_args()
    acted = False
    if args.update_reddit_targets:
        update_reddit_targets(
            include_trending=args.include_trending,
            max_labels=args.max_labels,
            longtail_ratio=args.longtail_ratio,
            trending_limit=args.trending_limit,
        )
        acted = True
    if getattr(args, "update_twitter_targets", False):
        update_twitter_targets(
            pool_path=args.pool,
            include_trending=args.include_twitter_trending,
            max_labels=args.twitter_max_labels,
            longtail_ratio=0.0,  # currently merging gravity first, then trending; no static long-tail
            per_query_limit=args.twitter_per_query_limit,
            trending_max_out=args.twitter_trending_max_out,
        )
        acted = True

    if not acted:
        print("No action specified. Use --update-reddit-targets and/or --update-twitter-targets.")


if __name__ == "__main__":
    main()
