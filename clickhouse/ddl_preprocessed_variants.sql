-- ClickHouse DDL for preprocessed variant columns
-- This schema uses preprocessed TSV data with extracted typed columns for optimal performance

CREATE DATABASE IF NOT EXISTS bluesky_variants_test;

-- Main table with variant columns (preprocessed data)
CREATE TABLE bluesky_variants_test.bluesky_preprocessed
(
    -- Core identity and timing fields (optimized types)
    did String,  -- DIDs are variable length, use String
    time_us UInt64,       -- Microsecond timestamp as integer
    kind LowCardinality(String),  -- Limited values: 'commit', etc.
    timestamp_col DateTime64(6),  -- Converted timestamp for time-based queries
    
    -- Commit metadata (extracted and typed)
    commit_rev String,
    commit_operation LowCardinality(String),  -- Limited values: 'create', 'update', 'delete'
    commit_collection LowCardinality(String), -- Limited values: feed.post, feed.like, etc.
    commit_rkey String,
    commit_cid String,
    
    -- Record type (for filtering by content type)
    record_type LowCardinality(String),  -- app.bsky.feed.post, app.bsky.feed.like, etc.
    
    -- Original JSON (for complex queries that need full data access)
    original_json JSON
)
ENGINE = MergeTree
ORDER BY (kind, commit_operation, commit_collection, did, timestamp_col)
SETTINGS index_granularity = 8192;

-- Create additional indexes for common query patterns
-- This will speed up filtering and aggregation on these fields
CREATE INDEX idx_timestamp ON bluesky_variants_test.bluesky_preprocessed (timestamp_col) TYPE minmax GRANULARITY 1;
CREATE INDEX idx_collection ON bluesky_variants_test.bluesky_preprocessed (commit_collection) TYPE bloom_filter GRANULARITY 1;
CREATE INDEX idx_operation ON bluesky_variants_test.bluesky_preprocessed (commit_operation) TYPE bloom_filter GRANULARITY 1;

-- Alternative table with pure typed columns (no JSON fallback)
-- This maximizes performance by eliminating JSON entirely
CREATE TABLE bluesky_variants_test.bluesky_pure_variants
(
    -- Core fields
    did String,  -- Changed from FixedString(32) to String
    time_us UInt64,
    kind LowCardinality(String),
    timestamp_col DateTime64(6),
    
    -- Commit fields
    commit_rev String,
    commit_operation LowCardinality(String),
    commit_collection LowCardinality(String),
    commit_rkey String,
    commit_cid String,
    record_type LowCardinality(String)
    
    -- Note: No original_json column for maximum performance
)
ENGINE = MergeTree
ORDER BY (kind, commit_operation, commit_collection, did, timestamp_col)
SETTINGS index_granularity = 8192;

-- Performance comparison table (JSON Object baseline for comparison)
-- Using simple ordering to avoid JSON path expressions in ORDER BY
CREATE TABLE bluesky_variants_test.bluesky_json_baseline
(
    data JSON
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192; 