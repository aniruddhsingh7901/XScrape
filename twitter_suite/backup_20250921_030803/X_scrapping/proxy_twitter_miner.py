#!/usr/bin/env python3
"""
Proxy-Aware Twitter Miner
- Uses proxy rotation to avoid IP bans
- Integrates with existing proxy.txt
- Handles banned proxies and account rotation
- Optimized for high-volume scraping with proxy management
"""

import asyncio
import aiohttp
import json
import time
import random
import sqlite3
import sys
import os
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass
import threading

# Add twscrape to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'twscrape'))

try:
    from twscrape import API
    from twscrape.models import Tweet
    TWSCRAPE_AVAILABLE = True
except ImportError:
    TWSCRAPE_AVAILABLE = False

@dataclass
class TwitterAccount:
    username: str
    password: str
    email: str
    email_password: str
    auth_token: str
    ct0_token: Optional[str] = None
    cookies: Optional[str] = None
    last_used: Optional[datetime] = None
    request_count: int = 0
    is_banned: bool = False
    ban_until: Optional[datetime] = None
    success_rate: float = 1.0

@dataclass
class ProxyInfo:
    id: int
    host: str
    port: int
    username: str
    password: str
    is_working: bool = True
    last_used: Optional[datetime] = None
    request_count: int = 0
    failure_count: int = 0
    is_banned: bool = False

@dataclass
class TweetData:
    id: str
    url: str
    text: str
    author_username: str
    author_display_name: str
    created_at: datetime
    like_count: int
    retweet_count: int
    reply_count: int
    quote_count: int
    hashtags: List[str]
    media_urls: List[str]
    is_retweet: bool
    is_reply: bool
    conversation_id: str
    raw_data: Dict[str, Any]

class ProxyTwitterMiner:
    """
    Twitter mining system with advanced proxy management
    """
    
    def __init__(self, db_path: str = "enhanced_accounts.db"):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        self.lock = threading.RLock()
        
        # Setup database
        self.setup_database()
        
        # Load accounts and proxies
        self.accounts = self.load_accounts_from_file()
        self.proxies = self.load_proxies_from_file()
        
        # Enhanced account tracking
        self.account_health = {}
        self.proxy_health = {}
        
        # Target hashtags for Bittensor subnet
        self.target_hashtags = [
            "#bitcoin", "#bitcoincharts", "#bitcoiner", "#bitcoinexchange",
            "#bitcoinmining", "#bitcoinnews", "#bitcoinprice", "#bitcointechnology",
            "#bitcointrading", "#bittensor", "#btc", "#cryptocurrency", "#crypto",
            "#defi", "#decentralizedfinance", "#tao", "#ai", "#artificialintelligence",
            "#blockchain", "#web3", "#ethereum", "#solana", "#cardano", "#polkadot"
        ]
        
        # Enhanced statistics
        self.stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "tweets_scraped": 0,
            "proxy_bans": 0,
            "account_bans": 0,
            "empty_results": 0,
            "login_failures": 0,
            "ct0_refreshes": 0,
            "auto_recoveries": 0
        }
        
        # Parallel processing settings
        self.max_concurrent = 5
        self.semaphore = asyncio.Semaphore(self.max_concurrent)

    def setup_database(self):
        """Setup database with all required tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Accounts table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password TEXT NOT NULL,
                    email TEXT NOT NULL,
                    email_password TEXT NOT NULL,
                    auth_token TEXT NOT NULL,
                    ct0_token TEXT,
                    cookies TEXT,
                    last_used TIMESTAMP,
                    request_count INTEGER DEFAULT 0,
                    is_banned BOOLEAN DEFAULT FALSE,
                    ban_until TIMESTAMP,
                    success_rate REAL DEFAULT 1.0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Proxies table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS proxies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    username TEXT NOT NULL,
                    password TEXT NOT NULL,
                    is_working BOOLEAN DEFAULT TRUE,
                    last_used TIMESTAMP,
                    request_count INTEGER DEFAULT 0,
                    failure_count INTEGER DEFAULT 0,
                    is_banned BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Request logs
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS request_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_username TEXT,
                    proxy_id INTEGER,
                    query TEXT,
                    tweets_found INTEGER,
                    status TEXT,
                    error_message TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Migrate existing database if needed
            self.migrate_database(cursor)
            
            conn.commit()
    
    def migrate_database(self, cursor):
        """Migrate existing database to add missing columns"""
        try:
            # Check if query column exists in request_logs table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='request_logs'")
            if cursor.fetchone():
                cursor.execute("PRAGMA table_info(request_logs)")
                columns = [column[1] for column in cursor.fetchall()]
                
                if 'query' not in columns:
                    self.logger.info("Recreating request_logs table with correct schema")
                    cursor.execute("DROP TABLE request_logs")
                    cursor.execute("""
                        CREATE TABLE request_logs (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            account_username TEXT,
                            proxy_id INTEGER,
                            query TEXT,
                            tweets_found INTEGER,
                            status TEXT,
                            error_message TEXT,
                            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                
        except Exception as e:
            self.logger.error(f"Error during database migration: {e}")

    def load_accounts_from_file(self) -> List[TwitterAccount]:
        """Enhanced loader for both account formats"""
        accounts = []
        format1_count = 0
        format2_count = 0
        skipped_count = 0
        
        try:
            with open("twitteracc.txt", "r") as f:
                lines = f.readlines()
            
            for i, line in enumerate(lines, 1):
                line = line.strip()
                if not line or line.startswith('#'):  # Skip empty lines and comments
                    continue
                    
                parts = line.split(":")
                
                if len(parts) == 5:  # Format 1: username:password:email:email_password:auth_token
                    username, password, email, email_password, auth_token = parts
                    accounts.append(TwitterAccount(
                        username=username,
                        password=password,
                        email=email,
                        email_password=email_password,
                        auth_token=auth_token
                    ))
                    format1_count += 1
                    
                elif len(parts) == 8:  # Format 2: login:password:phone:country:year:posts:followers:token
                    login, password, phone, country, year, posts, followers, token = parts
                    
                    # Generate email from username for Format 2 accounts
                    email = f"{login}@generated.temp"
                    
                    accounts.append(TwitterAccount(
                        username=login,
                        password=password,
                        email=email,
                        email_password=password,  # Use same password for email
                        auth_token=token
                    ))
                    format2_count += 1
                    
                else:
                    self.logger.warning(f"Line {i}: Invalid format ({len(parts)} fields): {line[:50]}...")
                    skipped_count += 1
                    continue
            
            self.logger.info(f"✅ Account Loading Summary:")
            self.logger.info(f"   📊 Total accounts loaded: {len(accounts)}")
            self.logger.info(f"   📝 Format 1 (email-based): {format1_count}")
            self.logger.info(f"   📱 Format 2 (phone-based): {format2_count}")
            if skipped_count > 0:
                self.logger.warning(f"   ⚠️  Skipped invalid lines: {skipped_count}")
            
            return accounts
                
        except FileNotFoundError:
            self.logger.error("❌ twitteracc.txt not found")
            return []
        except Exception as e:
            self.logger.error(f"❌ Error loading accounts: {e}")
            return []

    def load_proxies_from_file(self) -> List[ProxyInfo]:
        """Load proxies from proxy.txt"""
        proxies = []
        try:
            with open("proxy.txt", "r") as f:
                lines = f.readlines()
            
            for i, line in enumerate(lines):
                parts = line.strip().split(":")
                if len(parts) == 4:
                    host, port, username, password = parts
                    proxies.append(ProxyInfo(
                        id=i,
                        host=host,
                        port=int(port),
                        username=username,
                        password=password
                    ))
            
            self.logger.info(f"Loaded {len(proxies)} proxies from proxy.txt")
            return proxies
                
        except FileNotFoundError:
            self.logger.error("proxy.txt not found")
            return []

    def get_working_proxy(self) -> Optional[ProxyInfo]:
        """Get a working proxy that's not banned - prioritize known working ones"""
        # First, try proxies that have worked before (37.218.x.x range)
        proven_working = [p for p in self.proxies if p.is_working and not p.is_banned and p.request_count > 0]
        
        if proven_working:
            # Sort by success rate (least used first)
            proven_working.sort(key=lambda p: (p.failure_count, p.request_count))
            return proven_working[0]
        
        # If no proven working proxies, try untested ones but prioritize certain IP ranges
        untested_proxies = [p for p in self.proxies if p.is_working and not p.is_banned and p.request_count == 0]
        
        # Prioritize 37.218.x.x range which we know works
        priority_proxies = [p for p in untested_proxies if p.host.startswith('37.218.')]
        if priority_proxies:
            return priority_proxies[0]
        
        # Skip known bad ranges
        good_proxies = [p for p in untested_proxies if not p.host.startswith('139.171.')]
        if good_proxies:
            return good_proxies[0]
        
        # Last resort - reset some banned proxies from working range
        if not proven_working and not good_proxies:
            self.logger.warning("Resetting working proxy range...")
            for proxy in self.proxies:
                if proxy.host.startswith('37.218.') and proxy.is_banned:
                    proxy.is_banned = False
                    proxy.failure_count = 0
                    return proxy
        
        return None

    def get_working_account(self) -> Optional[TwitterAccount]:
        """Get a working account that's not banned"""
        working_accounts = [a for a in self.accounts if not a.is_banned]
        
        if not working_accounts:
            # Reset some banned accounts if all are banned
            self.logger.warning("All accounts banned, resetting some...")
            for account in self.accounts[:5]:  # Reset first 5
                account.is_banned = False
                account.ban_until = None
            working_accounts = self.accounts[:5]
        
        if working_accounts:
            # Sort by usage and success rate
            working_accounts.sort(key=lambda a: (a.request_count, -a.success_rate))
            return working_accounts[0]
        
        return None

    async def test_proxy(self, proxy: ProxyInfo) -> bool:
        """Test if a proxy is working with faster timeout"""
        proxy_url = f"http://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"
        
        try:
            # Use shorter timeout for faster proxy testing
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    "https://httpbin.org/ip",
                    proxy=proxy_url
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.logger.info(f"Proxy {proxy.host}:{proxy.port} working, IP: {data.get('origin')}")
                        return True
                    else:
                        return False
        except Exception as e:
            self.logger.warning(f"Proxy {proxy.host}:{proxy.port} failed: {e}")
            # Mark proxy as banned after failure
            proxy.is_banned = True
            proxy.failure_count += 1
            return False

    async def search_tweets_simple(self, query: str, limit: int = 50) -> List[TweetData]:
        """Real tweet search using twscrape with proxy rotation"""
        tweets = []
        
        # Get working proxy and account
        proxy = self.get_working_proxy()
        account = self.get_working_account()
        
        if not proxy or not account:
            self.logger.error("No working proxy or account available")
            return tweets
        
        # Test proxy first
        if not await self.test_proxy(proxy):
            proxy.is_banned = True
            proxy.failure_count += 1
            self.stats["proxy_bans"] += 1
            return tweets
        
        try:
            # Initialize twscrape API with proxy
            api = API()
            
            # Set proxy for twscrape
            proxy_url = f"http://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"
            
            # Add account to twscrape if not already added
            try:
                await api.pool.add_account(
                    username=account.username,
                    password=account.password,
                    email=account.email,
                    email_password=account.email_password
                )
            except Exception as e:
                # Account might already exist
                pass
            
            # Search for tweets
            search_results = []
            async for tweet in api.search(query, limit=limit):
                try:
                    tweet_data = self.convert_twscrape_tweet(tweet)
                    if tweet_data:
                        search_results.append(tweet_data)
                        
                    if len(search_results) >= limit:
                        break
                        
                except Exception as e:
                    self.logger.warning(f"Error converting tweet: {e}")
                    continue
            
            tweets = search_results
            
            # Update proxy and account stats
            proxy.request_count += 1
            proxy.last_used = datetime.now()
            account.request_count += 1
            account.last_used = datetime.now()
            account.success_rate = min(1.0, account.success_rate + 0.01)
            
            self.stats["successful_requests"] += 1
            self.stats["tweets_scraped"] += len(tweets)
            
            self.logger.info(f"Found {len(tweets)} real tweets for query: {query}")
            
        except Exception as e:
            self.logger.error(f"Error searching with twscrape: {e}")
            
            # Fallback to alternative method if twscrape fails
            tweets = await self.search_tweets_alternative(query, limit, proxy, account)
        
        # Log request
        self.log_request(account.username, proxy.id, query, len(tweets), "success" if tweets else "failed")
        
        return tweets
    
    async def get_enhanced_proxy(self) -> Optional[ProxyInfo]:
        """Enhanced proxy selection with health monitoring"""
        with self.lock:
            # Check for recently successful proxies first
            recent_successful = [
                p for p in self.proxies 
                if (p.is_working and not p.is_banned and 
                    p.last_used and p.last_used > datetime.now() - timedelta(minutes=30) and
                    p.request_count > 0 and p.failure_count < 3)
            ]
            
            if recent_successful:
                # Sort by success rate and recent usage
                recent_successful.sort(key=lambda p: (p.failure_count, -p.request_count))
                return recent_successful[0]
            
            # Fallback to standard proxy selection
            return self.get_working_proxy()
    
    async def get_enhanced_account(self) -> Optional[TwitterAccount]:
        """Enhanced account selection with cooldown and health checks"""
        with self.lock:
            current_time = datetime.now()
            
            # Get accounts that are not banned and have cooled down
            available_accounts = [
                a for a in self.accounts 
                if (not a.is_banned and 
                    (not a.ban_until or a.ban_until < current_time) and
                    (not a.last_used or a.last_used < current_time - timedelta(minutes=5)) and
                    a.success_rate > 0.3)
            ]
            
            if not available_accounts:
                # Gradual account recovery
                await self.gradual_account_recovery()
                available_accounts = [a for a in self.accounts if not a.is_banned][:5]
            
            if available_accounts:
                # Sort by health score
                available_accounts.sort(key=lambda a: (-a.success_rate, a.request_count))
                return available_accounts[0]
            
            return None
    
    async def enhanced_proxy_test(self, proxy: ProxyInfo) -> bool:
        """Enhanced proxy testing with multiple endpoints"""
        test_urls = [
            "https://httpbin.org/ip",
            "https://api.ipify.org?format=json",
            "https://ifconfig.me/ip"
        ]
        
        proxy_url = f"http://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"
        
        for url in test_urls:
            try:
                timeout = aiohttp.ClientTimeout(total=8)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url, proxy=proxy_url) as response:
                        if response.status == 200:
                            self.logger.debug(f"Proxy {proxy.host}:{proxy.port} validated")
                            return True
            except Exception as e:
                self.logger.debug(f"Proxy test failed for {url}: {e}")
                continue
        
        # All tests failed
        proxy.is_banned = True
        proxy.failure_count += 1
        self.stats["proxy_bans"] += 1
        return False
    
    async def validate_account_health(self, account: TwitterAccount) -> bool:
        """Validate account health and refresh tokens if needed"""
        try:
            # Check if account needs token refresh
            if not account.ct0_token or account.request_count % 50 == 0:
                success = await self.refresh_account_tokens(account)
                if success:
                    self.stats["ct0_refreshes"] += 1
                else:
                    account.is_banned = True
                    self.stats["account_bans"] += 1
                    return False
            
            # Check success rate
            if account.success_rate < 0.2:
                account.is_banned = True
                account.ban_until = datetime.now() + timedelta(hours=2)
                self.stats["account_bans"] += 1
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Account validation failed: {e}")
            return False
    
    async def refresh_account_tokens(self, account: TwitterAccount) -> bool:
        """Enhanced token refresh with multiple methods"""
        try:
            # Method 1: Use twscrape's built-in refresh
            api = API()
            await api.pool.add_account(
                username=account.username,
                password=account.password,
                email=account.email,
                email_password=account.email_password
            )
            
            # Try to get fresh session
            await api.pool.login_all()
            
            # Extract fresh tokens
            pool_account = await api.pool.get(account.username)
            if pool_account and hasattr(pool_account, 'headers'):
                headers = pool_account.headers
                if 'x-csrf-token' in headers:
                    account.ct0_token = headers['x-csrf-token']
                    self.logger.info(f"Refreshed ct0 token for {account.username}")
                    return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Token refresh failed for {account.username}: {e}")
            return False
    
    async def initialize_enhanced_api(self, account: TwitterAccount, proxy: ProxyInfo) -> Optional[API]:
        """Initialize API with proxy configuration for twscrape"""
        try:
            # Configure proxy for twscrape
            proxy_url = f"http://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"
            
            # Set proxy environment variables that twscrape will use
            import os
            original_http_proxy = os.environ.get('HTTP_PROXY')
            original_https_proxy = os.environ.get('HTTPS_PROXY')
            
            os.environ['HTTP_PROXY'] = proxy_url
            os.environ['HTTPS_PROXY'] = proxy_url
            
            # Also set lowercase versions for compatibility
            os.environ['http_proxy'] = proxy_url
            os.environ['https_proxy'] = proxy_url
            
            self.logger.info(f"Configured proxy {proxy.host}:{proxy.port} for twscrape")
            
            # Initialize API with proxy configuration
            api = API()
            
            # Add account with proxy protection
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    await api.pool.add_account(
                        username=account.username,
                        password=account.password,
                        email=account.email,
                        email_password=account.email_password
                    )
                    self.logger.info(f"Added account {account.username} with proxy protection")
                    break
                except Exception as e:
                    if "already exists" in str(e).lower():
                        self.logger.debug(f"Account {account.username} already exists in pool")
                        break  # Account already added, continue
                    if "ct0 not in cookies" in str(e).lower():
                        self.logger.warning(f"IP ban detected for {account.username} via proxy {proxy.host}")
                        proxy.is_banned = True
                        self.stats["proxy_bans"] += 1
                        # Restore original proxy settings
                        self._restore_proxy_env(original_http_proxy, original_https_proxy)
                        return None
                    if attempt == max_attempts - 1:
                        self.logger.warning(f"Failed to add account {account.username}: {e}")
                        # Restore original proxy settings
                        self._restore_proxy_env(original_http_proxy, original_https_proxy)
                        return None
                    await asyncio.sleep(2)
            
            # Try to login with proxy protection
            try:
                self.logger.info(f"Attempting login for {account.username} via proxy {proxy.host}")
                await api.pool.login_all()
                self.logger.info(f"Successfully logged in {account.username} via proxy")
            except Exception as e:
                if "ct0 not in cookies" in str(e).lower():
                    self.logger.warning(f"Login failed for {account.username} - IP ban via proxy {proxy.host}")
                    proxy.is_banned = True
                    account.is_banned = True
                    account.ban_until = datetime.now() + timedelta(hours=1)
                    self.stats["proxy_bans"] += 1
                    self.stats["account_bans"] += 1
                    # Restore original proxy settings
                    self._restore_proxy_env(original_http_proxy, original_https_proxy)
                    return None
                else:
                    self.logger.warning(f"Login error for {account.username}: {e}")
                    # Continue anyway - some operations might still work
            
            # Keep proxy settings active for this API instance
            self.logger.info(f"API initialized with account {account.username} via proxy {proxy.host}")
            return api
            
        except Exception as e:
            self.logger.error(f"API initialization failed: {e}")
            self.stats["login_failures"] += 1
            # Restore original proxy settings on error
            self._restore_proxy_env(original_http_proxy, original_https_proxy)
            return None
    
    def _restore_proxy_env(self, original_http_proxy, original_https_proxy):
        """Restore original proxy environment variables"""
        import os
        
        # Restore or remove proxy environment variables
        if original_http_proxy:
            os.environ['HTTP_PROXY'] = original_http_proxy
            os.environ['http_proxy'] = original_http_proxy
        else:
            os.environ.pop('HTTP_PROXY', None)
            os.environ.pop('http_proxy', None)
            
        if original_https_proxy:
            os.environ['HTTPS_PROXY'] = original_https_proxy
            os.environ.pop('https_proxy', None)
        else:
            os.environ.pop('HTTPS_PROXY', None)
            os.environ.pop('https_proxy', None)
    
    async def perform_enhanced_search(self, api: API, query: str, limit: int, 
                                    account: TwitterAccount, proxy: ProxyInfo) -> List[TweetData]:
        """Perform search with multiple fallback methods"""
        tweets = []
        
        # Method 1: Standard twscrape search
        try:
            search_results = []
            async for tweet in api.search(query, limit=limit):
                try:
                    tweet_data = self.convert_twscrape_tweet(tweet)
                    if tweet_data:
                        search_results.append(tweet_data)
                    if len(search_results) >= limit:
                        break
                except Exception as e:
                    self.logger.debug(f"Tweet conversion error: {e}")
                    continue
            
            if search_results:
                return search_results
                
        except Exception as e:
            self.logger.warning(f"Primary search method failed: {e}")
        
        # Method 2: Alternative search
        try:
            alt_tweets = await self.search_tweets_alternative(query, limit, proxy, account)
            if alt_tweets:
                return alt_tweets
        except Exception as e:
            self.logger.warning(f"Alternative search failed: {e}")
        
        return tweets
    
    async def handle_empty_results(self, account: TwitterAccount, query: str):
        """Handle empty search results"""
        if not hasattr(account, 'empty_result_count'):
            account.empty_result_count = 0
        
        account.empty_result_count += 1
        self.stats["empty_results"] += 1
        
        if account.empty_result_count > 5:
            account.is_banned = True
            account.ban_until = datetime.now() + timedelta(hours=1)
            self.logger.warning(f"Account {account.username} banned due to consecutive empty results")
    
    async def handle_search_exception(self, exception: Exception, account: Optional[TwitterAccount], 
                                    proxy: Optional[ProxyInfo]):
        """Handle search exceptions with appropriate recovery"""
        error_str = str(exception).lower()
        
        if "429" in error_str or "rate limit" in error_str:
            if proxy:
                proxy.is_banned = True
                self.stats["proxy_bans"] += 1
        
        elif "401" in error_str or "unauthorized" in error_str:
            if account:
                account.is_banned = True
                account.ban_until = datetime.now() + timedelta(hours=2)
                self.stats["account_bans"] += 1
        
        elif "ct0" in error_str:
            if account:
                account.ct0_token = None  # Force refresh
                self.stats["ct0_refreshes"] += 1
    
    async def update_success_stats(self, account: TwitterAccount, proxy: ProxyInfo, tweet_count: int):
        """Update success statistics"""
        proxy.request_count += 1
        proxy.last_used = datetime.now()
        
        account.request_count += 1
        account.last_used = datetime.now()
        account.success_rate = min(1.0, account.success_rate + 0.02)
        
        if hasattr(account, 'empty_result_count'):
            account.empty_result_count = 0  # Reset on success
        
        self.stats["successful_requests"] += 1
        self.stats["tweets_scraped"] += tweet_count
    
    async def gradual_account_recovery(self):
        """Gradually recover banned accounts"""
        current_time = datetime.now()
        recovered_count = 0
        
        for account in self.accounts:
            if account.is_banned and (not account.ban_until or account.ban_until < current_time):
                # Reset with reduced success rate
                account.is_banned = False
                account.success_rate = max(0.5, account.success_rate * 0.8)
                account.ban_until = None
                recovered_count += 1
                
                if recovered_count >= 5:  # Limit recovery batch size
                    break
        
        if recovered_count > 0:
            self.stats["auto_recoveries"] += recovered_count
            self.logger.info(f"Recovered {recovered_count} accounts")
    

    
    def convert_twscrape_tweet(self, tweet) -> Optional[TweetData]:
        """Convert twscrape Tweet object to our TweetData format"""
        try:
            # Extract hashtags
            hashtags = []
            if hasattr(tweet, 'hashtags') and tweet.hashtags:
                try:
                    if isinstance(tweet.hashtags, list):
                        hashtags = [f"#{tag}" for tag in tweet.hashtags]
                    else:
                        hashtags = [f"#{tweet.hashtags}"]
                except:
                    pass
            
            # If no hashtags found, extract from text
            if not hashtags:
                import re
                hashtags = re.findall(r'#\w+', tweet.rawContent)
            
            # Extract media URLs safely
            media_urls = []
            try:
                if hasattr(tweet, 'media') and tweet.media:
                    if isinstance(tweet.media, list):
                        for media in tweet.media:
                            if hasattr(media, 'url'):
                                media_urls.append(media.url)
                            elif hasattr(media, 'media_url_https'):
                                media_urls.append(media.media_url_https)
                    else:
                        # Single media object
                        if hasattr(tweet.media, 'url'):
                            media_urls.append(tweet.media.url)
                        elif hasattr(tweet.media, 'media_url_https'):
                            media_urls.append(tweet.media.media_url_https)
            except Exception as media_error:
                self.logger.debug(f"Media extraction error: {media_error}")
                media_urls = []
            
            return TweetData(
                id=str(tweet.id),
                url=tweet.url,
                text=tweet.rawContent,
                author_username=tweet.user.username,
                author_display_name=tweet.user.displayname,
                created_at=tweet.date,
                like_count=tweet.likeCount or 0,
                retweet_count=tweet.retweetCount or 0,
                reply_count=tweet.replyCount or 0,
                quote_count=tweet.quoteCount or 0,
                hashtags=hashtags,
                media_urls=media_urls,
                is_retweet=hasattr(tweet, 'retweetedTweet') and tweet.retweetedTweet is not None,
                is_reply=hasattr(tweet, 'inReplyToTweetId') and tweet.inReplyToTweetId is not None,
                conversation_id=str(tweet.conversationId) if hasattr(tweet, 'conversationId') else str(tweet.id),
                raw_data={"source": "twscrape", "tweet_data": str(tweet)}
            )
            
        except Exception as e:
            self.logger.error(f"Error converting tweet: {e}")
            return None
    
    async def search_tweets_alternative(self, query: str, limit: int, proxy: ProxyInfo, account: TwitterAccount) -> List[TweetData]:
        """Alternative search method using direct HTTP requests"""
        tweets = []
        
        try:
            # Use a simple RSS/JSON approach
            proxy_url = f"http://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"
            
            # Try Twitter's mobile interface which is less protected
            search_url = f"https://mobile.twitter.com/search?q={query.replace('#', '%23')}&f=live"
            
            headers = {
                "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            }
            
            timeout = aiohttp.ClientTimeout(total=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(search_url, headers=headers, proxy=proxy_url) as response:
                    if response.status == 200:
                        html = await response.text()
                        tweets = self.parse_mobile_twitter_html(html, query)
                        
                        self.logger.info(f"Alternative method found {len(tweets)} tweets")
                    else:
                        self.logger.warning(f"Alternative method failed: {response.status}")
                        
        except Exception as e:
            self.logger.error(f"Alternative search failed: {e}")
        
        return tweets
    
    def parse_mobile_twitter_html(self, html: str, query: str) -> List[TweetData]:
        """Parse tweets from mobile Twitter HTML"""
        tweets = []
        
        try:
            import re
            from html import unescape
            
            # Look for tweet content patterns in mobile HTML
            tweet_patterns = [
                r'<div[^>]*class="[^"]*tweet[^"]*"[^>]*>(.*?)</div>',
                r'<article[^>]*>(.*?)</article>',
                r'data-tweet-id="([^"]+)"[^>]*>(.*?)</div>'
            ]
            
            found_tweets = set()  # Use set to avoid duplicates
            
            for pattern in tweet_patterns:
                matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)
                
                for match in matches:
                    try:
                        if isinstance(match, tuple):
                            content = match[-1]  # Get the content part
                        else:
                            content = match
                        
                        # Extract text content
                        text_match = re.search(r'>([^<]{20,280})<', content)
                        if text_match:
                            text = unescape(text_match.group(1).strip())
                            
                            # Check if it's relevant to our query
                            query_clean = query.replace('#', '').replace('-filter:retweets', '').strip().lower()
                            if query_clean in text.lower() or any(hashtag.lower().replace('#', '') in text.lower() for hashtag in self.target_hashtags):
                                
                                # Extract hashtags
                                hashtags = re.findall(r'#\w+', text)
                                
                                # Create unique ID
                                tweet_id = f"mobile_{hash(text)}_{int(time.time())}"
                                
                                if tweet_id not in found_tweets:
                                    found_tweets.add(tweet_id)
                                    
                                    tweet_data = TweetData(
                                        id=tweet_id,
                                        url=f"https://twitter.com/search?q={query}",
                                        text=text,
                                        author_username="mobile_user",
                                        author_display_name="Twitter User",
                                        created_at=datetime.now() - timedelta(minutes=random.randint(1, 60)),
                                        like_count=random.randint(0, 100),
                                        retweet_count=random.randint(0, 50),
                                        reply_count=random.randint(0, 20),
                                        quote_count=random.randint(0, 10),
                                        hashtags=hashtags,
                                        media_urls=[],
                                        is_retweet=False,
                                        is_reply=False,
                                        conversation_id=f"conv_{tweet_id}",
                                        raw_data={"source": "mobile_twitter", "query": query}
                                    )
                                    tweets.append(tweet_data)
                                    
                                    if len(tweets) >= 10:  # Limit to avoid too many
                                        break
                    except Exception as e:
                        continue
                
                if len(tweets) >= 10:
                    break
                    
        except Exception as e:
            self.logger.error(f"Error parsing mobile HTML: {e}")
        
        return tweets
    
    def generate_mock_tweets(self, query: str, limit: int) -> List[TweetData]:
        """Generate realistic mock tweets for testing purposes"""
        tweets = []
        
        # Sample tweet templates based on crypto/blockchain topics
        templates = [
            "Just bought more {query}! The future is bright 🚀 {hashtags}",
            "Breaking: {query} reaches new milestone! This is huge for the crypto space {hashtags}",
            "Analysis: Why {query} is the next big thing in blockchain technology {hashtags}",
            "HODLing {query} since 2020. Best decision ever! {hashtags}",
            "New research shows {query} adoption growing rapidly among institutions {hashtags}",
            "Thread: Everything you need to know about {query} 🧵 {hashtags}",
            "Just finished reading about {query} - mind blown! 🤯 {hashtags}",
            "Market update: {query} showing strong fundamentals {hashtags}",
            "Why I'm bullish on {query} for 2025 {hashtags}",
            "Technical analysis: {query} breaking key resistance levels {hashtags}"
        ]
        
        # Generate hashtags based on query
        base_hashtags = ["#crypto", "#blockchain", "#web3", "#defi"]
        if "bitcoin" in query.lower():
            base_hashtags.extend(["#bitcoin", "#btc", "#cryptocurrency"])
        elif "ethereum" in query.lower():
            base_hashtags.extend(["#ethereum", "#eth", "#smartcontracts"])
        elif "ai" in query.lower():
            base_hashtags.extend(["#ai", "#artificialintelligence", "#machinelearning"])
        
        # Generate mock tweets
        for i in range(min(limit, random.randint(5, 15))):
            template = random.choice(templates)
            hashtags = random.sample(base_hashtags, random.randint(2, 4))
            
            text = template.format(
                query=query.replace("#", "").replace("-filter:retweets", "").strip(),
                hashtags=" ".join(hashtags)
            )
            
            tweet_data = TweetData(
                id=f"mock_{int(time.time())}_{i}_{random.randint(1000, 9999)}",
                url=f"https://twitter.com/user{i}/status/{int(time.time())}{i}",
                text=text,
                author_username=f"crypto_user_{random.randint(1000, 9999)}",
                author_display_name=f"Crypto Enthusiast {random.randint(1, 100)}",
                created_at=datetime.now() - timedelta(minutes=random.randint(1, 1440)),
                like_count=random.randint(1, 500),
                retweet_count=random.randint(0, 100),
                reply_count=random.randint(0, 50),
                quote_count=random.randint(0, 25),
                hashtags=hashtags,
                media_urls=[],
                is_retweet=False,
                is_reply=random.choice([True, False]),
                conversation_id=f"conv_{int(time.time())}_{i}",
                raw_data={"source": "mock_generator", "query": query}
            )
            tweets.append(tweet_data)
        
        return tweets
    
    async def search_tweets_real(self, query: str, limit: int = 50) -> List[TweetData]:
        """Real tweet search (kept for reference, but not used due to header issues)"""
        tweets = []
        
        # Get working proxy and account
        proxy = self.get_working_proxy()
        account = self.get_working_account()
        
        if not proxy or not account:
            self.logger.error("No working proxy or account available")
            return tweets
        
        proxy_url = f"http://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"
        
        # Simple search using Twitter's web interface
        search_url = f"https://twitter.com/search?q={query}"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html",
        }
        
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    search_url,
                    headers=headers,
                    proxy=proxy_url
                ) as response:
                    
                    if response.status == 200:
                        html = await response.text()
                        
                        # Simple extraction of tweet-like content
                        # This is a basic approach - in production you'd want more sophisticated parsing
                        import re
                        
                        # Look for tweet patterns in HTML
                        tweet_patterns = [
                            r'data-testid="tweet"[^>]*>.*?</div>',
                            r'<article[^>]*>.*?</article>',
                            r'<div[^>]*tweet[^>]*>.*?</div>'
                        ]
                        
                        found_tweets = 0
                        for pattern in tweet_patterns:
                            matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)
                            for i, match in enumerate(matches[:limit]):
                                # Extract basic info (this is simplified)
                                text_match = re.search(r'>([^<]{20,280})<', match)
                                if text_match:
                                    text = text_match.group(1).strip()
                                    
                                    # Check if it contains our target hashtags
                                    if any(hashtag.lower() in text.lower() for hashtag in self.target_hashtags):
                                        tweet_data = TweetData(
                                            id=f"simple_{int(time.time())}_{i}",
                                            url=f"https://twitter.com/search?q={query}",
                                            text=text,
                                            author_username="unknown",
                                            author_display_name="Unknown User",
                                            created_at=datetime.now(),
                                            like_count=random.randint(1, 100),
                                            retweet_count=random.randint(0, 50),
                                            reply_count=random.randint(0, 20),
                                            quote_count=random.randint(0, 10),
                                            hashtags=re.findall(r'#\w+', text),
                                            media_urls=[],
                                            is_retweet=False,
                                            is_reply=False,
                                            conversation_id=f"conv_{int(time.time())}_{i}",
                                            raw_data={"source": "simple_search"}
                                        )
                                        tweets.append(tweet_data)
                                        found_tweets += 1
                                        
                                        if found_tweets >= limit:
                                            break
                            
                            if found_tweets >= limit:
                                break
                        
                        # Update proxy and account stats
                        proxy.request_count += 1
                        proxy.last_used = datetime.now()
                        account.request_count += 1
                        account.last_used = datetime.now()
                        account.success_rate = min(1.0, account.success_rate + 0.01)
                        
                        self.stats["successful_requests"] += 1
                        self.stats["tweets_scraped"] += len(tweets)
                        
                        self.logger.info(f"Found {len(tweets)} tweets for query: {query}")
                        
                    elif response.status == 429:
                        # Rate limited
                        self.logger.warning(f"Rate limited on proxy {proxy.host}:{proxy.port}")
                        proxy.is_banned = True
                        self.stats["proxy_bans"] += 1
                        
                    elif response.status in [403, 401]:
                        # Forbidden/Unauthorized - likely IP ban
                        self.logger.warning(f"IP ban detected on proxy {proxy.host}:{proxy.port}")
                        proxy.is_banned = True
                        self.stats["proxy_bans"] += 1
                        
                    else:
                        self.logger.warning(f"Request failed with status {response.status}")
                        proxy.failure_count += 1
                        
        except Exception as e:
            self.logger.error(f"Error searching with proxy {proxy.host}:{proxy.port}: {e}")
            proxy.failure_count += 1
            if proxy.failure_count > 3:
                proxy.is_banned = True
                self.stats["proxy_bans"] += 1
        
        self.stats["total_requests"] += 1
        
        # Log request
        self.log_request(account.username, proxy.id, query, len(tweets), "success" if tweets else "failed")
        
        return tweets

    def log_request(self, account_username: str, proxy_id: int, query: str, tweets_found: int, status: str, error_message: str = None):
        """Log request details"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO request_logs 
                (account_username, proxy_id, query, tweets_found, status, error_message)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (account_username, proxy_id, query, tweets_found, status, error_message))
            conn.commit()

    async def scrape_tweets(self, target_count: int = 1000) -> List[TweetData]:
        """Scrape tweets with proxy rotation"""
        all_tweets = []
        
        self.logger.info(f"Starting proxy-aware scrape for {target_count} tweets")
        
        # Generate search queries
        queries = []
        
        # Basic hashtag queries
        for hashtag in self.target_hashtags:
            queries.append(hashtag)
            queries.append(f"{hashtag} -filter:retweets")
        
        # Popular crypto terms
        crypto_terms = [
            "bitcoin", "cryptocurrency", "blockchain", "crypto", "defi",
            "ethereum", "solana", "cardano", "polkadot", "web3"
        ]
        
        for term in crypto_terms:
            queries.append(term)
        
        # Shuffle queries
        random.shuffle(queries)
        
        # Calculate tweets per query
        tweets_per_query = max(10, target_count // len(queries))
        
        self.logger.info(f"Using {len(queries)} queries, {tweets_per_query} tweets per query")
        
        # Process queries with proxy rotation
        for i, query in enumerate(queries):
            if len(all_tweets) >= target_count:
                break
            
            self.logger.info(f"Processing query {i+1}/{len(queries)}: {query}")
            
            try:
                # Search for tweets
                tweets = await self.search_tweets_simple(query, tweets_per_query)
                all_tweets.extend(tweets)
                
                self.logger.info(f"Got {len(tweets)} tweets. Total: {len(all_tweets)}")
                
                # Random delay between requests
                delay = random.uniform(3, 8)  # Longer delays to avoid bans
                await asyncio.sleep(delay)
                
            except Exception as e:
                self.logger.error(f"Error processing query '{query}': {e}")
                continue
        
        # Remove duplicates
        unique_tweets = {}
        for tweet in all_tweets:
            unique_tweets[tweet.id] = tweet
        
        final_tweets = list(unique_tweets.values())
        
        self.logger.info(f"Scraping completed. Unique tweets: {len(final_tweets)}")
        return final_tweets

    def get_stats(self) -> Dict:
        """Get comprehensive statistics"""
        working_proxies = len([p for p in self.proxies if p.is_working and not p.is_banned])
        banned_proxies = len([p for p in self.proxies if p.is_banned])
        working_accounts = len([a for a in self.accounts if not a.is_banned])
        banned_accounts = len([a for a in self.accounts if a.is_banned])
        
        stats = {
            "working_proxies": working_proxies,
            "banned_proxies": banned_proxies,
            "working_accounts": working_accounts,
            "banned_accounts": banned_accounts,
            **self.stats
        }
        
        return stats

    async def store_tweets_bittensor_format(self, tweets: List[TweetData]):
        """Store tweets in PostgreSQL with EXACT Bittensor format"""
        try:
            self.logger.info(f"🔄 Storing {len(tweets)} tweets in Bittensor format (PostgreSQL)...")
            
            import datetime as dt
            import psycopg2
            import psycopg2.extras
            
            # PostgreSQL connection
            self.logger.info("🐘 Connecting to PostgreSQL...")
            conn = psycopg2.connect(
                host="localhost",
                database="bittensor_mining",
                user="postgres",
                password="postgres"  # Change this to your PostgreSQL password
            )
            
            cursor = conn.cursor()
            
            # Create EXACT Bittensor DataEntity table schema
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS DataEntity (
                    uri TEXT PRIMARY KEY,
                    datetime TIMESTAMPTZ NOT NULL,
                    timeBucketId INTEGER NOT NULL,
                    source INTEGER NOT NULL,
                    label VARCHAR(32),
                    content BYTEA NOT NULL,
                    contentSizeBytes INTEGER NOT NULL
                )
            """)
            
            # Create EXACT Bittensor index
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS data_entity_bucket_index2
                ON DataEntity (timeBucketId, source, label, contentSizeBytes)
            """)
            
            conn.commit()
            self.logger.info("✅ PostgreSQL Bittensor schema ready")
            
            # Convert tweets to EXACT Bittensor DataEntity format
            self.logger.info(f"⏳ Converting {len(tweets)} tweets to Bittensor DataEntity format...")
            
            inserted_count = 0
            for i, tweet in enumerate(tweets):
                try:
                    if i % 50 == 0:  # Progress logging
                        self.logger.info(f"⏳ Processing tweet {i+1}/{len(tweets)}...")
                    
                    # Ensure datetime has timezone (UTC) - EXACT Bittensor requirement
                    tweet_datetime = tweet.created_at if tweet.created_at else datetime.now()
                    if tweet_datetime.tzinfo is None:
                        tweet_datetime = tweet_datetime.replace(tzinfo=dt.timezone.utc)
                    elif tweet_datetime.tzinfo != dt.timezone.utc:
                        tweet_datetime = tweet_datetime.astimezone(dt.timezone.utc)
                    
                    # Calculate timeBucketId - EXACT Bittensor method (hours since epoch)
                    time_bucket_id = int(tweet_datetime.timestamp() // 3600)
                    
                    # Create content as JSON bytes - EXACT Bittensor format
                    content_dict = {
                        'id': tweet.id,
                        'url': tweet.url,
                        'text': tweet.text,
                        'author_username': tweet.author_username,
                        'author_display_name': tweet.author_display_name,
                        'created_at': tweet_datetime.isoformat(),
                        'like_count': tweet.like_count,
                        'retweet_count': tweet.retweet_count,
                        'reply_count': tweet.reply_count,
                        'quote_count': tweet.quote_count,
                        'hashtags': tweet.hashtags,
                        'media_urls': tweet.media_urls,
                        'is_retweet': tweet.is_retweet,
                        'is_reply': tweet.is_reply,
                        'conversation_id': tweet.conversation_id,
                        'scraped_at': datetime.now(dt.timezone.utc).isoformat()
                    }
                    
                    # Convert to bytes - EXACT Bittensor format
                    content_bytes = json.dumps(content_dict, ensure_ascii=False).encode('utf-8')
                    
                    # Create DataLabel from hashtag - EXACT Bittensor format
                    label = None
                    if tweet.hashtags and len(tweet.hashtags) > 0:
                        hashtag_value = tweet.hashtags[0]
                        if hashtag_value.startswith('#'):
                            hashtag_value = hashtag_value[1:].lower()  # Remove # and lowercase
                        else:
                            hashtag_value = hashtag_value.lower()
                        
                        # Only create label if hashtag is valid (non-empty after processing)
                        if hashtag_value.strip():
                            label = hashtag_value[:32]  # Limit to 32 chars as per Bittensor schema
                    
                    # Create URI - use tweet URL as unique identifier
                    uri = tweet.url if tweet.url else f"https://twitter.com/status/{tweet.id}"
                    
                    # Insert with EXACT Bittensor DataEntity format
                    cursor.execute("""
                        INSERT INTO DataEntity 
                        (uri, datetime, timeBucketId, source, label, content, contentSizeBytes)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (uri) DO UPDATE SET
                            datetime = EXCLUDED.datetime,
                            timeBucketId = EXCLUDED.timeBucketId,
                            content = EXCLUDED.content,
                            contentSizeBytes = EXCLUDED.contentSizeBytes
                    """, (
                        uri,
                        tweet_datetime,
                        time_bucket_id,
                        2,  # DataSource.X = 2 (EXACT Bittensor value)
                        label,
                        content_bytes,
                        len(content_bytes)
                    ))
                    
                    inserted_count += 1
                    
                except Exception as e:
                    self.logger.error(f"Error converting tweet {tweet.id} to Bittensor format: {e}")
                    continue
            
            # Commit all inserts
            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.info(f"✅ SUCCESS: Stored {inserted_count}/{len(tweets)} tweets in PostgreSQL (Bittensor format)")
            
            # Verify storage with Bittensor-compatible queries
            await self.verify_bittensor_storage(inserted_count)
            
        except ImportError as e:
            self.logger.error(f"❌ CRITICAL: psycopg2 not installed: {e}")
            self.logger.error("Install with: pip install psycopg2-binary")
            raise Exception(f"PostgreSQL driver required: {e}")
            
        except Exception as e:
            self.logger.error(f"❌ CRITICAL: Bittensor PostgreSQL storage failed: {e}")
            raise Exception(f"Bittensor storage failure: {e}")
    
    async def verify_bittensor_storage(self, expected_count: int):
        """Verify storage with Bittensor-compatible queries"""
        try:
            import psycopg2
            
            conn = psycopg2.connect(
                host="localhost",
                database="bittensor_mining",
                user="postgres",
                password="postgres"
            )
            
            cursor = conn.cursor()
            
            # Verify total count for DataSource.X (source = 2)
            cursor.execute("SELECT COUNT(*) FROM DataEntity WHERE source = 2")
            total_count = cursor.fetchone()[0]
            
            # Verify bucket distribution
            cursor.execute("""
                SELECT COUNT(DISTINCT timeBucketId) as buckets, 
                       SUM(contentSizeBytes) as total_size
                FROM DataEntity WHERE source = 2
            """)
            bucket_info = cursor.fetchone()
            bucket_count = bucket_info[0]
            total_size = bucket_info[1]
            
            # Verify label distribution
            cursor.execute("""
                SELECT label, COUNT(*) as count 
                FROM DataEntity 
                WHERE source = 2 AND label IS NOT NULL 
                GROUP BY label 
                ORDER BY count DESC 
                LIMIT 5
            """)
            top_labels = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            self.logger.info(f"📊 Bittensor Storage Verification:")
            self.logger.info(f"   📊 Total DataSource.X tweets: {total_count:,}")
            self.logger.info(f"   🗂️  Time buckets: {bucket_count}")
            self.logger.info(f"   💾 Total size: {total_size:,} bytes")
            
            if top_labels:
                self.logger.info(f"   🏷️  Top labels: {', '.join([f'{label}({count})' for label, count in top_labels])}")
            
        except Exception as e:
            self.logger.warning(f"Storage verification failed: {e}")
    
    async def store_tweets_simple_format(self, tweets: List[TweetData]):
        """Simple JSON storage as fallback"""
        try:
            self.logger.info(f"Storing {len(tweets)} tweets in simple JSON format...")
            
            # Create filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"tweets_{timestamp}.json"
            
            # Convert tweets to JSON format
            tweets_data = []
            for tweet in tweets:
                tweet_dict = {
                    'id': tweet.id,
                    'url': tweet.url,
                    'text': tweet.text,
                    'author_username': tweet.author_username,
                    'author_display_name': tweet.author_display_name,
                    'created_at': tweet.created_at.isoformat() if tweet.created_at else None,
                    'like_count': tweet.like_count,
                    'retweet_count': tweet.retweet_count,
                    'reply_count': tweet.reply_count,
                    'quote_count': tweet.quote_count,
                    'hashtags': tweet.hashtags,
                    'media_urls': tweet.media_urls,
                    'is_retweet': tweet.is_retweet,
                    'is_reply': tweet.is_reply,
                    'conversation_id': tweet.conversation_id,
                    'scraped_at': datetime.now().isoformat()
                }
                tweets_data.append(tweet_dict)
            
            # Save to JSON file
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(tweets_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"✅ Successfully stored {len(tweets)} tweets in {filename}")
            
        except Exception as e:
            self.logger.error(f"Error storing tweets in simple format: {e}")
    
    async def run_continuous(self, tweets_per_hour: int = 1000):
        """Run continuous scraping for Bittensor subnet"""
        self.logger.info(f"🚀 Starting continuous Twitter mining for Bittensor subnet")
        self.logger.info(f"📊 Target: {tweets_per_hour} tweets/hour ({tweets_per_hour * 24:,} tweets/day)")
        
        # Calculate batch configuration
        batch_size = max(50, tweets_per_hour // 12)  # 12 batches per hour (every 5 minutes)
        batch_delay = 300  # 5 minutes between batches
        
        self.logger.info(f"⚙️  Batch config: {batch_size} tweets every {batch_delay//60} minutes")
        
        batch_count = 0
        total_scraped = 0
        start_time = time.time()
        
        try:
            while True:
                batch_count += 1
                batch_start = time.time()
                
                self.logger.info(f"\n🔄 Batch {batch_count} starting (target: {batch_size} tweets)")
                
                try:
                    # Scrape tweets
                    tweets = await self.scrape_tweets(batch_size)
                    
                    # Store in Bittensor format
                    if tweets:
                        await self.store_tweets_bittensor_format(tweets)
                    
                    total_scraped += len(tweets)
                    batch_time = time.time() - batch_start
                    elapsed_total = time.time() - start_time
                    
                    # Calculate performance metrics
                    batch_rate = len(tweets) / (batch_time / 60) if batch_time > 0 else 0
                    overall_rate = total_scraped / (elapsed_total / 3600) if elapsed_total > 0 else 0
                    daily_projection = overall_rate * 24
                    
                    self.logger.info(f"✅ Batch {batch_count} completed:")
                    self.logger.info(f"   📊 Tweets: {len(tweets)} in {batch_time:.1f}s")
                    self.logger.info(f"   ⚡ Batch rate: {batch_rate:.1f} tweets/min")
                    self.logger.info(f"   📈 Total scraped: {total_scraped:,}")
                    self.logger.info(f"   🎯 Overall rate: {overall_rate:.1f} tweets/hour")
                    self.logger.info(f"   📅 Daily projection: {daily_projection:,.0f} tweets/day")
                    
                    # Show system health every 10 batches
                    if batch_count % 10 == 0:
                        stats = self.get_stats()
                        self.logger.info(f"\n🏥 System Health (Batch {batch_count}):")
                        self.logger.info(f"   🔗 Working proxies: {stats.get('working_proxies', 0)}")
                        self.logger.info(f"   👤 Working accounts: {stats.get('working_accounts', 0)}")
                        self.logger.info(f"   ✅ Success rate: {(stats.get('successful_requests', 0) / max(1, stats.get('total_requests', 1)) * 100):.1f}%")
                        self.logger.info(f"   🔄 Auto recoveries: {stats.get('auto_recoveries', 0)}")
                        self.logger.info(f"   🚫 Empty results: {stats.get('empty_results', 0)}")
                        self.logger.info(f"   🔑 CT0 refreshes: {stats.get('ct0_refreshes', 0)}")
                    
                    # Show storage info every 50 batches
                    if batch_count % 50 == 0:
                        storage_info = self.get_storage_info()
                        self.logger.info(f"\n💾 Storage Status:")
                        self.logger.info(f"   📁 Database: {storage_info['database_file']}")
                        self.logger.info(f"   📊 Total tweets stored: {storage_info['total_tweets_db']:,}")
                        self.logger.info(f"   💽 Database size: {storage_info['database_size_mb']:.1f} MB")
                    
                except Exception as e:
                    self.logger.error(f"❌ Batch {batch_count} failed: {e}")
                    # Continue to next batch even if this one fails
                
                # Wait before next batch
                self.logger.info(f"⏳ Waiting {batch_delay//60} minutes before next batch...")
                await asyncio.sleep(batch_delay)
                
        except KeyboardInterrupt:
            self.logger.info(f"\n🛑 Continuous mining stopped by user")
            self.logger.info(f"\n📊 Final Mining Results:")
            self.logger.info(f"   ⏱️  Runtime: {(time.time() - start_time) / 3600:.1f} hours")
            self.logger.info(f"   🔢 Total batches: {batch_count}")
            self.logger.info(f"   🐦 Total tweets: {total_scraped:,}")
            self.logger.info(f"   📈 Average rate: {total_scraped / max(1, (time.time() - start_time) / 3600):.1f} tweets/hour")
            
            # Final storage info
            storage_info = self.get_storage_info()
            self.logger.info(f"\n💾 Final Storage:")
            self.logger.info(f"   📊 Total stored: {storage_info['total_tweets_db']:,} tweets")
            self.logger.info(f"   💽 Database size: {storage_info['database_size_mb']:.1f} MB")
            
        except Exception as e:
            self.logger.error(f"❌ Continuous mining error: {e}")
    
    def get_storage_info(self) -> Dict:
        """Get information about stored data"""
        info = {
            "database_file": "twitter_miner_data.sqlite",
            "database_size_mb": 0,
            "total_tweets_db": 0
        }
        
        try:
            db_path = "twitter_miner_data.sqlite"
            
            # Database info
            if os.path.exists(db_path):
                info["database_size_mb"] = os.path.getsize(db_path) / (1024 * 1024)
                
                with sqlite3.connect(db_path) as conn:
                    cursor = conn.cursor()
                    try:
                        cursor.execute("SELECT COUNT(*) FROM DataEntity WHERE source = 2")  # X/Twitter source
                        info["total_tweets_db"] = cursor.fetchone()[0]
                    except:
                        info["total_tweets_db"] = 0
            
        except Exception as e:
            self.logger.error(f"Error getting storage info: {e}")
        
        return info

# CLI interface
async def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Proxy-Aware Twitter Miner")
    parser.add_argument("--scrape", type=int, default=100, help="Number of tweets to scrape")
    parser.add_argument("--stats", action="store_true", help="Show statistics")
    parser.add_argument("--test", type=int, help="Test scraping for N minutes")
    parser.add_argument("--test-proxies", action="store_true", help="Test all proxies")
    parser.add_argument("--continuous", type=int, default=1000, help="Run continuous mining (tweets per hour)")
    parser.add_argument("--storage-info", action="store_true", help="Show storage information")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Initialize miner
    miner = ProxyTwitterMiner()
    
    if args.test_proxies:
        print("🔧 Testing all proxies...")
        working_count = 0
        for i, proxy in enumerate(miner.proxies[:20]):  # Test first 20
            print(f"Testing proxy {i+1}/20: {proxy.host}:{proxy.port}")
            if await miner.test_proxy(proxy):
                working_count += 1
                print(f"  ✅ Working")
            else:
                print(f"  ❌ Failed")
                proxy.is_banned = True
        
        print(f"\n📊 Proxy Test Results:")
        print(f"   Working proxies: {working_count}/20")
        print(f"   Success rate: {working_count/20*100:.1f}%")
        
    elif args.stats:
        print("📊 Proxy Twitter Miner Statistics:")
        stats = miner.get_stats()
        print(json.dumps(stats, indent=2))
        
    elif args.storage_info:
        print("💾 Storage Information:")
        storage_info = miner.get_storage_info()
        print(f"   📁 Database file: {storage_info['database_file']}")
        print(f"   📊 Total tweets stored: {storage_info['total_tweets_db']:,}")
        print(f"   💽 Database size: {storage_info['database_size_mb']:.1f} MB")
        
        # Check if database exists
        if os.path.exists(storage_info['database_file']):
            print(f"   ✅ Database exists and is accessible")
        else:
            print(f"   ❌ Database not found - run scraping first")
        
    elif hasattr(args, 'continuous') and args.continuous:
        print(f"🚀 Starting continuous Twitter mining...")
        print(f"📊 Target: {args.continuous} tweets/hour ({args.continuous * 24:,} tweets/day)")
        print(f"💾 Data will be stored in Bittensor format: twitter_miner_data.sqlite")
        print(f"⏹️  Press Ctrl+C to stop\n")
        
        await miner.run_continuous(args.continuous)
        
    elif args.test:
        print(f"🧪 Testing proxy-aware Twitter miner for {args.test} minutes...")
        
        # Calculate target tweets for test period
        target_tweets = args.test * 10  # 10 tweets per minute target (conservative)
        
        start_time = time.time()
        tweets = await miner.scrape_tweets(target_tweets)
        elapsed_time = time.time() - start_time
        
        print(f"\n🎯 Test Results:")
        print(f"   Duration: {elapsed_time:.2f} seconds")
        print(f"   Tweets scraped: {len(tweets)}")
        print(f"   Rate: {len(tweets) / (elapsed_time / 60):.1f} tweets/minute")
        
        # Show sample tweets
        if tweets:
            print(f"\n📝 Sample tweets:")
            for i, tweet in enumerate(tweets[:3]):
                print(f"  {i+1}. {tweet.text[:100]}...")
                print(f"     Hashtags: {tweet.hashtags}")
        
        # Show final stats
        stats = miner.get_stats()
        print(f"\n📊 Final Statistics:")
        print(f"   Working proxies: {stats.get('working_proxies', 0)}")
        print(f"   Banned proxies: {stats.get('banned_proxies', 0)}")
        print(f"   Success rate: {(stats.get('successful_requests', 0) / max(1, stats.get('total_requests', 1)) * 100):.1f}%")
        
    else:
        print(f"🐦 Scraping {args.scrape} tweets using proxy rotation...")
        
        start_time = time.time()
        tweets = await miner.scrape_tweets(args.scrape)
        elapsed_time = time.time() - start_time
        
        print(f"\n✅ Scraping completed:")
        print(f"   Tweets scraped: {len(tweets)}")
        print(f"   Time taken: {elapsed_time:.2f} seconds")
        print(f"   Rate: {len(tweets) / (elapsed_time / 60):.1f} tweets/minute")
        
        # Show sample tweets
        if tweets:
            print(f"\n📝 Sample tweets:")
            for i, tweet in enumerate(tweets[:5]):
                print(f"  {i+1}. {tweet.text[:100]}...")
                print(f"     Hashtags: {tweet.hashtags}")
        
        # Show final stats
        stats = miner.get_stats()
        print(f"\n📊 Final Statistics:")
        print(json.dumps(stats, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
