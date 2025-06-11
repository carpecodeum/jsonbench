-- True Variants Approach: Uses Variant columns without storing raw JSON
-- This approach leverages ClickHouse's Variant type for flexible column storage

DROP DATABASE IF EXISTS bluesky_true_variants;
CREATE DATABASE bluesky_true_variants;

-- Enable Variant type
SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;

-- Create table with Variant columns (no JSON storage)
CREATE TABLE bluesky_true_variants.bluesky_data
(
    did Variant(String),
    time_us Variant(UInt64),
    kind Variant(String),
    timestamp_col Variant(DateTime64(6)),
    commit_rev Variant(String),
    commit_operation Variant(String),
    commit_collection Variant(String),
    commit_rkey Variant(String),
    commit_cid Variant(String),
    record_type Variant(String)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS 
    allow_experimental_variant_type = 1,
    use_variant_as_common_type = 1; 