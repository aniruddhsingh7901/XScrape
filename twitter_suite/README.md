# Twitter/X Scraping Suite (Production-Ready)

This folder provides an end-to-end, modular Twitter/X scraping suite designed for high-volume operation (target: 10M DataEntities/day). It includes:

- Session validation and ct0 refresh (via IMAP re-login)
- Pool loading with per-account proxies
- Pool status and lock reset
- One-shot sanity scrape (validator-ready JSON)
- Batch scrape to SQLite (DataEntity schema)
- Reporting: CSV/JSON exports for account health and pool metrics
- Acceptance runner: orchestrated test flow for new batches

Everything is exposed via a single CLI with subcommands, and you can run the reporting tool to obtain CSVs/JSON for monitoring.

## Directory Structure

- `twitter_suite/cli.py` — Unified CLI (subcommands: `refresh`, `load`, `reset-locks`, `status`, `one-shot`, `batch`, `report`, `accept`)
- `twitter_suite/report.py` — Reporting module (exports CSV + JSON with pool metrics and per-account status)
- `twitter/X_scrapping/twitteracc.txt` — Input accounts file (from vendor)
- `twitter/X_scrapping/proxy.txt` — Proxies list
- `scripts/cache/twscrape_refresh.db` — twscrape pool database (sessions, proxies, locks)
- `scripts/twitter_miner_data.sqlite` — SQLite database where DataEntities are stored (validator-ready content)

## File Formats

- Accounts file (one per line):
  ```
  username:password:email:email_password:auth_token:ct0
  ```
- Proxies file (one per line):
  ```
  host:port:user:pass
  ```

## CLI Usage (Examples)

1) Validate/refresh a new batch (ct0 refresh via IMAP re-login if needed)
```
python twitter_suite/cli.py refresh \
  --accounts-in twitter/X_scrapping/twitteracc.txt \
  --accounts-out twitter/X_scrapping/twitteracc.refreshed.txt \
  --proxies twitter/X_scrapping/proxy.txt \
  --verbose
```

2) Load N accounts into the pool with per-account proxies (round-robin)
```
python twitter_suite/cli.py load \
  --accounts twitter/X_scrapping/twitteracc.refreshed.txt \
  --proxies twitter/X_scrapping/proxy.txt \
  --pool scripts/cache/twscrape_refresh.db \
  --limit 150
```

3) Reset locks and check pool status
```
python twitter_suite/cli.py reset-locks --pool scripts/cache/twscrape_refresh.db
python twitter_suite/cli.py status --pool scripts/cache/twscrape_refresh.db --queue SearchTimeline --limit 20
```

4) One-shot sanity (prints validator-ready XContent JSON)
```
python twitter_suite/cli.py one-shot \
  --pool scripts/cache/twscrape_refresh.db \
  --query "#bitcoin -filter:retweets" \
  --limit 3 --verbose
```

5) Batch scraping to SQLite (DataEntity schema)
```
python twitter_suite/cli.py batch \
  --pool scripts/cache/twscrape_refresh.db \
  --queries "#bitcoin -filter:retweets,#crypto -filter:retweets" \
  --limit-per-query 5 \
  --db scripts/twitter_miner_data.sqlite \
  --verbose
```

6) Acceptance runner (orchestrates end-to-end sample test)
```
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
```

## Reporting (CSV/JSON: working vs banned/locked, etc.)

Use the `report` subcommand to export production metrics:

```
python twitter_suite/cli.py report \
  --pool scripts/cache/twscrape_refresh.db \
  --out-dir twitter_suite/reports \
  --queues SearchTimeline \
  --db scripts/twitter_miner_data.sqlite \
  --verbose
```

- Outputs:
  - `twitter_suite/reports/accounts_status.csv`
    - Columns: `username, active, last_used, proxy, has_auth_token, has_ct0, locked_queues, unlock_at__SearchTimeline, ...`
    - “active” shows the pool’s active flag. A persistently inactive account is typically “dead/banned”.
    - “locked_queues” indicates accounts temporarily locked by twscrape for rate/endpoint issues.
  - `twitter_suite/reports/pool_metrics.json`
    - Contains totals: `total_accounts, active_accounts, inactive_accounts`
    - `locked_counts` per queue, `next_available_at` timestamps per queue
    - Optional DB summary (`dataentity_count`, latest rows)

This is designed so you can quickly see:
- How many accounts are working (active)
- How many are inactive (likely banned/broken)
- How many are currently locked for a queue
- When the pool will next be available
- How your DB is growing (DataEntity counts)

## What the suite stores and where

- Sessions/cookies (auth_token + ct0 per account), proxies, last_used, per-queue locks:
  - `scripts/cache/twscrape_refresh.db`
- Refreshed snapshot of accounts:
  - `twitter/X_scrapping/twitteracc.refreshed.txt`
- Scraped data (validator-compatible DataEntity):
  - `scripts/twitter_miner_data.sqlite` (table `DataEntity`, `source=2` for X)
- Reports:
  - `twitter_suite/reports/accounts_status.csv`
  - `twitter_suite/reports/pool_metrics.json`

## Operational Runbook for 10M/day

- Accounts & proxies:
  - 120–150 accounts with 1:1 residential/ISP proxies (region-matched)
  - 2FA OFF, no SMS challenges, IMAP-enabled mailbox included
  - Delivery format: `username:password:email:email_password:auth_token:ct0`

- Rate & scheduling:
  - Warm-up: 2–4 req/min/account for 24–48h
  - Steady: 6–8 req/min/account with ±15–25% jitter
  - Expand queries and stagger time windows to reduce duplicates
  - Monitor pool `status` and CSV metrics; if locks/401s rise, step down temporarily

- Maintenance:
  - Refresh ct0 periodically or on 401 spikes:
    ```
    python twitter_suite/cli.py refresh --accounts-in ... --accounts-out ... --proxies ... --verbose
    ```
  - Reset locks as needed:
    ```
    python twitter_suite/cli.py reset-locks --pool scripts/cache/twscrape_refresh.db
    ```

## Ready for Miner Production

- DataEntities in `scripts/twitter_miner_data.sqlite` are validator-ready: the JSON stored in `content` is the same printed by `one-shot` (timestamp minute-obfuscated, `x.com` URLs, safe fields).
- You can wire your miner to read from this SQLite file (or adapt to Postgres later if required).

## Deleting Legacy Scripts (Proposed)

We consolidated Twitter functionality into `twitter_suite`. The following legacy scripts are now superseded by the CLI and can be removed after approval:

- `scripts/twitter_refresh_ct0.py`   → replace with `twitter_suite/cli.py refresh`
- `scripts/tws_pool_load_accounts.py` → replace with `twitter_suite/cli.py load`
- `scripts/tws_pool_reset_locks.py`   → replace with `twitter_suite/cli.py reset-locks`
- `scripts/tws_pool_status.py`        → replace with `twitter_suite/cli.py status`
- `scripts/twitter_one_shot.py`       → replace with `twitter_suite/cli.py one-shot`
- `scripts/twitter_scrape_batch.py`   → replace with `twitter_suite/cli.py batch`
- `scripts/twitter_acceptance_runner.py` → replace with `twitter_suite/cli.py accept`

If you approve, we will delete these files and update any docs/paths accordingly.

## Next Steps

1) Approve deletion of the legacy scripts listed above.
2) Run the `report` command and confirm CSV/JSON satisfy your monitoring needs.
3) Provide any extra metrics you want in CSV/JSON (e.g., per-account healthcheck status columns), and we’ll extend `report.py`.
4) If desired, we can add a simple “monitor loop” script or a PM2 config to run batch + report on a schedule.
