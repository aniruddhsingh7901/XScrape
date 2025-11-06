import psycopg2
import sqlite3
import json
import threading
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass, asdict
from enhanced_twitter_scraper import TweetData
import hashlib
import gzip
import pickle

@dataclass
class DataEntityBittensor:
    """Data entity in Bittensor format"""
    uri: str
    datetime: datetime
    source_id: int  # 2 for Twitter/X
    label_value: str
    content: bytes
    content_size_bytes: int

class OptimizedDataStorage:
    """
    Optimized storage system for 1M+ daily tweets
    Supports both PostgreSQL (main storage) and SQLite (Bittensor compatibility)
    """
    
    def __init__(self, 
                 postgres_config: Dict[str, str],
                 sqlite_path: str = "bittensor_data.db",
                 max_db_size_gb: int = 250):
        
        self.postgres_config = postgres_config
        self.sqlite_path = sqlite_path
        self.max_db_size_bytes = max_db_size_gb * 1024 * 1024 * 1024
        
        self.logger = logging.getLogger(__name__)
        self.lock = threading.RLock()
        
        # Initialize databases
        self.setup_postgres()
        self.setup_sqlite()
        
        # Performance optimization
        self.batch_size = 1000
        self.pending_tweets = []
        self.pending_lock = threading.Lock()
        
        # Statistics
        self.stats = {
            "total_stored": 0,
            "postgres_stored": 0,
            "sqlite_stored": 0,
            "duplicates_skipped": 0,
            "errors": 0
        }

    def setup_postgres(self):
        """Setup PostgreSQL database with optimized schema"""
        try:
            conn = psycopg2.connect(**self.postgres_config)
            cursor = conn.cursor()
            
            # Create optimized tables for high-volume inserts
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_sources (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    weight FLOAT NOT NULL DEFAULT 1.0
                );
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_labels (
                    value VARCHAR(140) PRIMARY KEY
                );
            """)
            
            # Main tweets table with partitioning support
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_entities (
                    uri TEXT PRIMARY KEY,
                    datetime TIMESTAMPTZ NOT NULL,
                    source_id INTEGER NOT NULL REFERENCES data_sources(id),
                    label_value VARCHAR(140) REFERENCES data_labels(value),
                    content BYTEA NOT NULL,
                    content_size_bytes INTEGER NOT NULL CHECK (content_size_bytes >= 0),
                    
                    -- Additional fields for optimization
                    tweet_id BIGINT,
                    author_username TEXT,
                    author_display_name TEXT,
                    like_count INTEGER DEFAULT 0,
                    retweet_count INTEGER DEFAULT 0,
                    reply_count INTEGER DEFAULT 0,
                    quote_count INTEGER DEFAULT 0,
                    hashtags TEXT[],
                    media_urls TEXT[],
                    is_retweet BOOLEAN DEFAULT FALSE,
                    is_reply BOOLEAN DEFAULT FALSE,
                    conversation_id BIGINT,
                    
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Indexes for performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_data_entities_datetime 
                ON data_entities(datetime DESC);
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_data_entities_source_label 
                ON data_entities(source_id, label_value);
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_data_entities_hashtags 
                ON data_entities USING GIN(hashtags);
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_data_entities_author 
                ON data_entities(author_username);
            """)
            
            # Insert default data source for Twitter
            cursor.execute("""
                INSERT INTO data_sources (id, name, weight) 
                VALUES (2, 'Twitter', 0.35) 
                ON CONFLICT (id) DO NOTHING;
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.info("PostgreSQL database setup completed")
            
        except Exception as e:
            self.logger.error(f"Error setting up PostgreSQL: {e}")
            raise

    def setup_sqlite(self):
        """Setup SQLite database for Bittensor compatibility"""
        try:
            conn = sqlite3.connect(self.sqlite_path)
            cursor = conn.cursor()
            
            # Bittensor-compatible schema
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS DataEntity (
                    uri TEXT PRIMARY KEY,
                    datetime TIMESTAMP(6) NOT NULL,
                    timeBucketId INTEGER NOT NULL,
                    source INTEGER NOT NULL,
                    label CHAR(32),
                    content BLOB NOT NULL,
                    contentSizeBytes INTEGER NOT NULL
                ) WITHOUT ROWID;
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS data_entity_bucket_index2
                ON DataEntity (timeBucketId, source, label, contentSizeBytes);
            """)
            
            # HuggingFace metadata table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS HFMetaData (
                    uri TEXT PRIMARY KEY,
                    source INTEGER NOT NULL,
                    updatedAt TIMESTAMP(6) NOT NULL,
                    encodingKey TEXT
                ) WITHOUT ROWID;
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.info("SQLite database setup completed")
            
        except Exception as e:
            self.logger.error(f"Error setting up SQLite: {e}")
            raise

    def calculate_time_bucket_id(self, dt: datetime) -> int:
        """Calculate time bucket ID (hours since epoch)"""
        epoch = datetime(1970, 1, 1, tzinfo=dt.tzinfo)
        hours_since_epoch = int((dt - epoch).total_seconds() / 3600)
        return hours_since_epoch

    def compress_tweet_content(self, tweet: TweetData) -> bytes:
        """Compress tweet data for storage"""
        # Create a comprehensive tweet object
        tweet_obj = {
            "id": tweet.id,
            "url": tweet.url,
            "text": tweet.text,
            "author_username": tweet.author_username,
            "author_display_name": tweet.author_display_name,
            "created_at": tweet.created_at.isoformat(),
            "like_count": tweet.like_count,
            "retweet_count": tweet.retweet_count,
            "reply_count": tweet.reply_count,
            "quote_count": tweet.quote_count,
            "hashtags": tweet.hashtags,
            "media_urls": tweet.media_urls,
            "is_retweet": tweet.is_retweet,
            "is_reply": tweet.is_reply,
            "conversation_id": tweet.conversation_id,
            "raw_data": tweet.raw_data
        }
        
        # Serialize and compress
        json_data = json.dumps(tweet_obj, ensure_ascii=False)
        compressed_data = gzip.compress(json_data.encode('utf-8'))
        
        return compressed_data

    def get_primary_hashtag(self, tweet: TweetData) -> Optional[str]:
        """Get the primary hashtag for labeling"""
        if tweet.hashtags:
            # Return the first hashtag, converted to lowercase
            return tweet.hashtags[0].lower()
        return None

    def store_tweet_postgres(self, tweet: TweetData) -> bool:
        """Store tweet in PostgreSQL"""
        try:
            conn = psycopg2.connect(**self.postgres_config)
            cursor = conn.cursor()
            
            # Get primary hashtag for label
            primary_hashtag = self.get_primary_hashtag(tweet)
            
            # Insert label if it doesn't exist
            if primary_hashtag:
                cursor.execute("""
                    INSERT INTO data_labels (value) 
                    VALUES (%s) 
                    ON CONFLICT (value) DO NOTHING;
                """, (primary_hashtag,))
            
            # Compress content
            compressed_content = self.compress_tweet_content(tweet)
            
            # Insert tweet
            cursor.execute("""
                INSERT INTO data_entities (
                    uri, datetime, source_id, label_value, content, content_size_bytes,
                    tweet_id, author_username, author_display_name,
                    like_count, retweet_count, reply_count, quote_count,
                    hashtags, media_urls, is_retweet, is_reply, conversation_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (uri) DO NOTHING;
            """, (
                tweet.url,
                tweet.created_at,
                2,  # Twitter source ID
                primary_hashtag,
                compressed_content,
                len(compressed_content),
                int(tweet.id) if tweet.id.isdigit() else None,
                tweet.author_username,
                tweet.author_display_name,
                tweet.like_count,
                tweet.retweet_count,
                tweet.reply_count,
                tweet.quote_count,
                tweet.hashtags,
                tweet.media_urls,
                tweet.is_retweet,
                tweet.is_reply,
                int(tweet.conversation_id) if tweet.conversation_id.isdigit() else None
            ))
            
            success = cursor.rowcount > 0
            conn.commit()
            cursor.close()
            conn.close()
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error storing tweet in PostgreSQL: {e}")
            return False

    def store_tweet_sqlite(self, tweet: TweetData) -> bool:
        """Store tweet in SQLite (Bittensor format)"""
        try:
            conn = sqlite3.connect(self.sqlite_path)
            cursor = conn.cursor()
            
            # Calculate time bucket
            time_bucket_id = self.calculate_time_bucket_id(tweet.created_at)
            
            # Get primary hashtag
            primary_hashtag = self.get_primary_hashtag(tweet)
            label = primary_hashtag if primary_hashtag else "NULL"
            
            # Compress content
            compressed_content = self.compress_tweet_content(tweet)
            
            # Insert into Bittensor format
            cursor.execute("""
                INSERT OR IGNORE INTO DataEntity (
                    uri, datetime, timeBucketId, source, label, content, contentSizeBytes
                ) VALUES (?, ?, ?, ?, ?, ?, ?);
            """, (
                tweet.url,
                tweet.created_at,
                time_bucket_id,
                2,  # Twitter source
                label,
                compressed_content,
                len(compressed_content)
            ))
            
            success = cursor.rowcount > 0
            conn.commit()
            cursor.close()
            conn.close()
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error storing tweet in SQLite: {e}")
            return False

    def store_tweet(self, tweet: TweetData) -> bool:
        """Store tweet in both databases"""
        postgres_success = self.store_tweet_postgres(tweet)
        sqlite_success = self.store_tweet_sqlite(tweet)
        
        # Update statistics
        with self.lock:
            if postgres_success:
                self.stats["postgres_stored"] += 1
            if sqlite_success:
                self.stats["sqlite_stored"] += 1
            if postgres_success or sqlite_success:
                self.stats["total_stored"] += 1
            else:
                self.stats["duplicates_skipped"] += 1
        
        return postgres_success or sqlite_success

    def store_tweets_batch(self, tweets: List[TweetData]) -> int:
        """Store multiple tweets efficiently"""
        stored_count = 0
        
        # Group tweets for batch processing
        postgres_data = []
        sqlite_data = []
        labels_to_insert = set()
        
        for tweet in tweets:
            primary_hashtag = self.get_primary_hashtag(tweet)
            if primary_hashtag:
                labels_to_insert.add(primary_hashtag)
            
            compressed_content = self.compress_tweet_content(tweet)
            time_bucket_id = self.calculate_time_bucket_id(tweet.created_at)
            
            # Prepare PostgreSQL data
            postgres_data.append((
                tweet.url, tweet.created_at, 2, primary_hashtag, compressed_content,
                len(compressed_content), int(tweet.id) if tweet.id.isdigit() else None,
                tweet.author_username, tweet.author_display_name,
                tweet.like_count, tweet.retweet_count, tweet.reply_count, tweet.quote_count,
                tweet.hashtags, tweet.media_urls, tweet.is_retweet, tweet.is_reply,
                int(tweet.conversation_id) if tweet.conversation_id.isdigit() else None
            ))
            
            # Prepare SQLite data
            sqlite_data.append((
                tweet.url, tweet.created_at, time_bucket_id, 2,
                primary_hashtag if primary_hashtag else "NULL",
                compressed_content, len(compressed_content)
            ))
        
        # Batch insert into PostgreSQL
        try:
            conn = psycopg2.connect(**self.postgres_config)
            cursor = conn.cursor()
            
            # Insert labels
            if labels_to_insert:
                label_data = [(label,) for label in labels_to_insert]
                cursor.executemany("""
                    INSERT INTO data_labels (value) 
                    VALUES (%s) 
                    ON CONFLICT (value) DO NOTHING;
                """, label_data)
            
            # Insert tweets
            cursor.executemany("""
                INSERT INTO data_entities (
                    uri, datetime, source_id, label_value, content, content_size_bytes,
                    tweet_id, author_username, author_display_name,
                    like_count, retweet_count, reply_count, quote_count,
                    hashtags, media_urls, is_retweet, is_reply, conversation_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (uri) DO NOTHING;
            """, postgres_data)
            
            postgres_inserted = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.info(f"Inserted {postgres_inserted} tweets into PostgreSQL")
            
        except Exception as e:
            self.logger.error(f"Error batch inserting into PostgreSQL: {e}")
            postgres_inserted = 0
        
        # Batch insert into SQLite
        try:
            conn = sqlite3.connect(self.sqlite_path)
            cursor = conn.cursor()
            
            cursor.executemany("""
                INSERT OR IGNORE INTO DataEntity (
                    uri, datetime, timeBucketId, source, label, content, contentSizeBytes
                ) VALUES (?, ?, ?, ?, ?, ?, ?);
            """, sqlite_data)
            
            sqlite_inserted = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.info(f"Inserted {sqlite_inserted} tweets into SQLite")
            
        except Exception as e:
            self.logger.error(f"Error batch inserting into SQLite: {e}")
            sqlite_inserted = 0
        
        # Update statistics
        with self.lock:
            self.stats["postgres_stored"] += postgres_inserted
            self.stats["sqlite_stored"] += sqlite_inserted
            self.stats["total_stored"] += max(postgres_inserted, sqlite_inserted)
        
        return max(postgres_inserted, sqlite_inserted)

    def add_tweet_to_batch(self, tweet: TweetData):
        """Add tweet to pending batch for efficient storage"""
        with self.pending_lock:
            self.pending_tweets.append(tweet)
            
            # Process batch when it reaches the batch size
            if len(self.pending_tweets) >= self.batch_size:
                tweets_to_process = self.pending_tweets.copy()
                self.pending_tweets.clear()
                
                # Process in background thread
                threading.Thread(
                    target=self.store_tweets_batch,
                    args=(tweets_to_process,),
                    daemon=True
                ).start()

    def flush_pending_tweets(self):
        """Flush any remaining tweets in the batch"""
        with self.pending_lock:
            if self.pending_tweets:
                tweets_to_process = self.pending_tweets.copy()
                self.pending_tweets.clear()
                return self.store_tweets_batch(tweets_to_process)
        return 0

    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        with self.lock:
            stats = self.stats.copy()
        
        # Add database sizes
        try:
            # PostgreSQL size
            conn = psycopg2.connect(**self.postgres_config)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) as total_tweets,
                       SUM(content_size_bytes) as total_size_bytes,
                       MAX(datetime) as latest_tweet,
                       MIN(datetime) as earliest_tweet
                FROM data_entities;
            """)
            pg_stats = cursor.fetchone()
            cursor.close()
            conn.close()
            
            stats.update({
                "postgres_total_tweets": pg_stats[0] or 0,
                "postgres_total_size_bytes": pg_stats[1] or 0,
                "postgres_latest_tweet": pg_stats[2],
                "postgres_earliest_tweet": pg_stats[3]
            })
            
        except Exception as e:
            self.logger.error(f"Error getting PostgreSQL stats: {e}")
        
        try:
            # SQLite size
            conn = sqlite3.connect(self.sqlite_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) as total_tweets,
                       SUM(contentSizeBytes) as total_size_bytes,
                       MAX(datetime) as latest_tweet,
                       MIN(datetime) as earliest_tweet
                FROM DataEntity;
            """)
            sqlite_stats = cursor.fetchone()
            cursor.close()
            conn.close()
            
            stats.update({
                "sqlite_total_tweets": sqlite_stats[0] or 0,
                "sqlite_total_size_bytes": sqlite_stats[1] or 0,
                "sqlite_latest_tweet": sqlite_stats[2],
                "sqlite_earliest_tweet": sqlite_stats[3]
            })
            
        except Exception as e:
            self.logger.error(f"Error getting SQLite stats: {e}")
        
        return stats

    def cleanup_old_data(self, days_to_keep: int = 30):
        """Clean up old data to manage storage size"""
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        try:
            # Clean PostgreSQL
            conn = psycopg2.connect(**self.postgres_config)
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM data_entities 
                WHERE datetime < %s;
            """, (cutoff_date,))
            pg_deleted = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.info(f"Deleted {pg_deleted} old tweets from PostgreSQL")
            
        except Exception as e:
            self.logger.error(f"Error cleaning PostgreSQL: {e}")
        
        try:
            # Clean SQLite
            conn = sqlite3.connect(self.sqlite_path)
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM DataEntity 
                WHERE datetime < ?;
            """, (cutoff_date,))
            sqlite_deleted = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            
            self.logger.info(f"Deleted {sqlite_deleted} old tweets from SQLite")
            
        except Exception as e:
            self.logger.error(f"Error cleaning SQLite: {e}")

    def get_tweets_for_bittensor_validation(self, limit: int = 1000) -> List[DataEntityBittensor]:
        """Get tweets in Bittensor format for validation"""
        try:
            conn = sqlite3.connect(self.sqlite_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT uri, datetime, source, label, content, contentSizeBytes
                FROM DataEntity
                ORDER BY datetime DESC
                LIMIT ?;
            """, (limit,))
            
            results = cursor.fetchall()
            cursor.close()
            conn.close()
            
            entities = []
            for row in results:
                entity = DataEntityBittensor(
                    uri=row[0],
                    datetime=datetime.fromisoformat(row[1]),
                    source_id=row[2],
                    label_value=row[3] if row[3] != "NULL" else None,
                    content=row[4],
                    content_size_bytes=row[5]
                )
                entities.append(entity)
            
            return entities
            
        except Exception as e:
            self.logger.error(f"Error getting tweets for validation: {e}")
            return []

# Example usage
if __name__ == "__main__":
    # PostgreSQL configuration
    postgres_config = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": 5432
    }
    
    # Initialize storage
    storage = OptimizedDataStorage(postgres_config)
    
    # Get statistics
    stats = storage.get_storage_stats()
    print("Storage Statistics:")
    print(json.dumps(stats, indent=2, default=str))
