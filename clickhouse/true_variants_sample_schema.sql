-- True ClickHouse Variant columns schema
CREATE DATABASE IF NOT EXISTS bluesky_true_variants;

CREATE TABLE bluesky_true_variants.bluesky_variant_columns
(
    -- Basic fields
    did String,
    time_us UInt64,
    kind String,
    timestamp_col DateTime64(6),

    -- TRUE Variant columns - can store multiple types
    commit_operation Variant(String),
    commit_collection Variant(String),
    record_type Variant(String, JSON),
    metadata Variant(JSON),
    record_content Variant(String, JSON),

    -- Original JSON for comparison
    original_json JSON
)
ENGINE = MergeTree
ORDER BY (kind, did, timestamp_col);
