ATTACH TABLE _ UUID 'a113f16b-8f2a-4cf0-9dd7-833e0d398f8e'
(
    `did` String,
    `time_us` UInt64,
    `kind` LowCardinality(String),
    `timestamp_col` DateTime64(6),
    `commit_rev` String,
    `commit_operation` LowCardinality(String),
    `commit_collection` LowCardinality(String),
    `commit_rkey` String,
    `commit_cid` String,
    `record_type` LowCardinality(String),
    `original_json` JSON,
    INDEX idx_timestamp timestamp_col TYPE minmax GRANULARITY 1,
    INDEX idx_collection commit_collection TYPE bloom_filter GRANULARITY 1,
    INDEX idx_operation commit_operation TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (kind, commit_operation, commit_collection, did, timestamp_col)
SETTINGS index_granularity = 8192
