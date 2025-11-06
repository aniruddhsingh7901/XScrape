-- 1. time_buckets
CREATE TABLE IF NOT EXISTS time_buckets (
    id INTEGER PRIMARY KEY CHECK (id > 0)  -- PositiveInt constraint
);

-- 2. data_sources
CREATE TABLE IF NOT EXISTS data_sources (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    weight FLOAT NOT NULL
);

-- 3. data_labels
CREATE TABLE IF NOT EXISTS data_labels (
    value VARCHAR(140) PRIMARY KEY  -- enforce max length
);

-- 4. data_entities
CREATE TABLE IF NOT EXISTS data_entities (
    uri TEXT PRIMARY KEY,
    datetime TIMESTAMPTZ NOT NULL,
    source_id INTEGER NOT NULL REFERENCES data_sources(id),
    label_value VARCHAR(140) REFERENCES data_labels(value),
    content BYTEA NOT NULL,
    content_size_bytes INTEGER NOT NULL CHECK (content_size_bytes >= 0)
);

-- 5. huggingface_metadata (not used by Twitter but included for completeness)
CREATE TABLE IF NOT EXISTS huggingface_metadata (
    repo_name TEXT PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES data_sources(id),
    updated_at TIMESTAMPTZ NOT NULL,
    encoding_key TEXT
);

-- 6. data_entity_buckets
-- Replace MAX_SIZE with your actual max (e.g., 1_000_000_000 for 1GB)
CREATE TABLE IF NOT EXISTS data_entity_buckets (
    time_bucket_id INTEGER NOT NULL REFERENCES time_buckets(id),
    source_id INTEGER NOT NULL REFERENCES data_sources(id),
    label_value VARCHAR(140) REFERENCES data_labels(value),
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0 AND size_bytes <= 1000000000),
    PRIMARY KEY (time_bucket_id, source_id, label_value)
);

-- 7. compressed_entity_buckets
CREATE TABLE IF NOT EXISTS compressed_entity_buckets (
    id SERIAL PRIMARY KEY,
    label TEXT,
    time_bucket_ids INTEGER[] NOT NULL,
    sizes_bytes INTEGER[] NOT NULL
);

-- 8. compressed_miner_index
CREATE TABLE IF NOT EXISTS compressed_miner_index (
    id SERIAL PRIMARY KEY
    -- Optionally add miner_id if tying to a specific miner
);

CREATE TABLE IF NOT EXISTS compressed_miner_index_sources (
    miner_index_id INTEGER REFERENCES compressed_miner_index(id),
    source_id INTEGER NOT NULL REFERENCES data_sources(id),
    compressed_bucket_id INTEGER NOT NULL REFERENCES compressed_entity_buckets(id),
    PRIMARY KEY (miner_index_id, source_id, compressed_bucket_id)
);
