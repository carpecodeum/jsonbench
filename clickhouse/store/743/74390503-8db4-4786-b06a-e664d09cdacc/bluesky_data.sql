ATTACH TABLE _ UUID '86f13ee7-7d4d-4e57-8f1a-5bed2e276e0a'
(
    `did` String,
    `time_us` UInt64,
    `kind` LowCardinality(String),
    `timestamp_col` DateTime64(6),
    `commit_operation` Variant(String),
    `commit_collection` Variant(String),
    `record_data` Variant(JSON, String),
    `commit_info` Variant(JSON),
    `original_json` JSON
)
ENGINE = MergeTree
ORDER BY (kind, did, timestamp_col)
SETTINGS index_granularity = 8192
