#!/usr/bin/env python3
"""
Test script for Twitter scraper implementation
Tests account loading, proxy setup, and basic scraping functionality
"""

import asyncio
import logging
import sys
from datetime import datetime

# Import our Twitter components
from enhanced_account_manager import EnhancedAccountManager
from enhanced_twitter_scraper import EnhancedTwitterScraper
from X_scrapping.optimized_data_storage import OptimizedDataStorage

# Apply compatibility fixes directly
def apply_compatibility_fixes():
    """Apply compatibility fixes to EnhancedAccountManager"""
    import sqlite3
    
    def get_available_accounts(self):
        """Get list of available accounts"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT username FROM accounts WHERE is_banned = FALSE")
            return [row[0] for row in cursor.fetchall()]
    
    def get_available_proxies(self):
        """Get list of available proxies"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT host, port FROM proxies WHERE is_working = TRUE")
            return [f"{row[0]}:{row[1]}" for row in cursor.fetchall()]
    
    def get_account_proxy_pair(self):
        """Get account-proxy pair for compatibility"""
        pair = self.get_available_account_proxy_pair()
        if pair:
            account, proxy = pair
            # Create simple objects for compatibility
            class SimpleAccount:
                def __init__(self, username):
                    self.username = username
            
            class SimpleProxy:
                def __init__(self, host, port):
                    self.host = host
                    self.port = port
            
            return SimpleAccount(account.username), SimpleProxy(proxy.host, proxy.port)
        return None, None
    
    # Add methods to class
    EnhancedAccountManager.get_available_accounts = get_available_accounts
    EnhancedAccountManager.get_available_proxies = get_available_proxies
    EnhancedAccountManager.get_account_proxy_pair = get_account_proxy_pair

# Apply fixes before running tests
apply_compatibility_fixes()

async def test_account_manager():
    """Test account manager functionality"""
    print("🔧 Testing Account Manager...")
    
    try:
        manager = EnhancedAccountManager()
        
        # Test account loading
        accounts = manager.get_available_accounts()
        print(f"✅ Loaded {len(accounts)} Twitter accounts")
        
        # Test proxy loading
        proxies = manager.get_available_proxies()
        print(f"✅ Loaded {len(proxies)} proxies")
        
        # Test account-proxy pairing
        if accounts and proxies:
            account, proxy = manager.get_account_proxy_pair()
            print(f"✅ Account-proxy pairing working")
            print(f"   Account: {account.username}")
            print(f"   Proxy: {proxy.host}:{proxy.port}")
        
        # Get stats
        stats = manager.get_account_stats()
        print(f"✅ Account stats: {stats}")
        
        return True
        
    except Exception as e:
        print(f"❌ Account Manager test failed: {e}")
        return False

async def test_twitter_scraper():
    """Test Twitter scraper functionality"""
    print("\n🐦 Testing Twitter Scraper...")
    
    try:
        # Initialize components
        account_manager = EnhancedAccountManager()
        scraper = EnhancedTwitterScraper(account_manager)
        
        # Test small scrape (10 tweets)
        print("📡 Testing small scrape (10 tweets)...")
        tweets = await scraper.scrape_for_target(10)
        
        print(f"✅ Scraped {len(tweets)} tweets")
        
        # Show sample tweets
        if tweets:
            print("\n📝 Sample tweets:")
            for i, tweet in enumerate(tweets[:3]):
                print(f"  {i+1}. @{tweet.author_username}: {tweet.text[:100]}...")
                print(f"     Created: {tweet.created_at}")
                print(f"     Hashtags: {tweet.hashtags}")
        
        # Get scraper stats
        stats = scraper.get_stats()
        print(f"\n📊 Scraper stats: {stats}")
        
        return len(tweets) > 0
        
    except Exception as e:
        print(f"❌ Twitter Scraper test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_data_storage():
    """Test data storage functionality"""
    print("\n💾 Testing Data Storage...")
    
    try:
        # PostgreSQL config
        postgres_config = {
            "dbname": "postgres",
            "user": "postgres",
            "password": "postgres",
            "host": "localhost",
            "port": 5432
        }
        
        storage = OptimizedDataStorage(postgres_config)
        
        # Test database connection
        print("🔗 Testing database connection...")
        
        # Create a sample tweet for testing
        from enhanced_twitter_scraper import TweetData
        
        sample_tweet = TweetData(
            id="test_tweet_123",
            url="https://twitter.com/test/status/123",
            text="This is a test tweet for Bittensor mining #bitcoin #test",
            author_username="test_user",
            author_display_name="Test User",
            created_at=datetime.now(),
            like_count=10,
            retweet_count=5,
            reply_count=2,
            quote_count=1,
            hashtags=["#bitcoin", "#test"],
            media_urls=[],
            is_retweet=False,
            is_reply=False,
            conversation_id="test_conversation_123",
            raw_data={"test": True}
        )
        
        # Test storage
        stored_count = storage.store_tweets_batch([sample_tweet])
        print(f"✅ Stored {stored_count} test tweet(s)")
        
        # Get storage stats
        stats = storage.get_storage_stats()
        print(f"📊 Storage stats: {stats}")
        
        return stored_count > 0
        
    except Exception as e:
        print(f"❌ Data Storage test failed: {e}")
        print("💡 Make sure PostgreSQL is running and accessible")
        return False

async def test_full_pipeline():
    """Test the complete Twitter scraping pipeline"""
    print("\n🚀 Testing Full Pipeline...")
    
    try:
        # Initialize all components
        account_manager = EnhancedAccountManager()
        scraper = EnhancedTwitterScraper(account_manager)
        
        postgres_config = {
            "dbname": "postgres",
            "user": "postgres",
            "password": "postgres",
            "host": "localhost",
            "port": 5432
        }
        storage = OptimizedDataStorage(postgres_config)
        
        # Scrape a small batch
        print("📡 Scraping 5 tweets...")
        tweets = await scraper.scrape_for_target(5)
        
        if not tweets:
            print("❌ No tweets scraped")
            return False
        
        print(f"✅ Scraped {len(tweets)} tweets")
        
        # Store the tweets
        print("💾 Storing tweets...")
        stored_count = storage.store_tweets_batch(tweets)
        
        print(f"✅ Stored {stored_count} tweets")
        
        # Show results
        print(f"\n🎯 Pipeline Results:")
        print(f"   Tweets scraped: {len(tweets)}")
        print(f"   Tweets stored: {stored_count}")
        print(f"   Success rate: {(stored_count/len(tweets)*100):.1f}%")
        
        return stored_count > 0
        
    except Exception as e:
        print(f"❌ Full Pipeline test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run all tests"""
    print("🧪 Twitter Scraper Implementation Test")
    print("=" * 50)
    
    # Run tests
    tests = [
        ("Account Manager", test_account_manager()),
        ("Twitter Scraper", test_twitter_scraper()),
        ("Data Storage", test_data_storage()),
        ("Full Pipeline", test_full_pipeline())
    ]
    
    results = []
    for test_name, test_coro in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            result = await test_coro
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} crashed: {e}")
            results.append((test_name, False))
    
    # Summary
    print(f"\n{'='*50}")
    print("🏁 Test Summary:")
    print("=" * 50)
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1
    
    print(f"\nResults: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("🎉 All tests passed! Twitter implementation is working!")
        print("\n🚀 Ready to run full mining:")
        print("   python multi_platform_miner.py test 10")
        print("   python multi_platform_miner.py")
    else:
        print("⚠️  Some tests failed. Check the errors above.")
        
        # Provide troubleshooting tips
        print("\n🔧 Troubleshooting:")
        print("1. Make sure twitteracc.txt has valid account credentials")
        print("2. Make sure proxy.txt has working proxies")
        print("3. Make sure PostgreSQL is running on localhost:5432")
        print("4. Check that accounts have valid ct0 tokens")

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Run tests
    asyncio.run(main())
