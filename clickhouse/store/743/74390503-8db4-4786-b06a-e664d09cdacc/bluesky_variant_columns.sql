ATTACH TABLE _ UUID 'cd4d29f4-ff84-449f-88a3-f7603f9401d2'
(
    `did` String,
    `time_us` UInt64,
    `kind` String,
    `timestamp_col` DateTime64(6),
    `commit_operation` Variant(String),
    `commit_collection` Variant(String),
    `record_type` Variant(JSON, String),
    `metadata` Variant(JSON),
    `record_content` Variant(JSON, String),
    `original_json` JSON
)
ENGINE = MergeTree
ORDER BY (kind, did, timestamp_col)
SETTINGS index_granularity = 8192
