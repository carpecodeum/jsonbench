ATTACH TABLE _ UUID '722f6f11-d6ed-4cf8-a81c-18e81fb9a0c3'
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
    `record_type` LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (kind, commit_operation, commit_collection, did, timestamp_col)
SETTINGS index_granularity = 8192
