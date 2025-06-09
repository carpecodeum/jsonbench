-- Test script for fixed true variants approach
-- Create source JSON table
CREATE DATABASE IF NOT EXISTS bluesky_1m;
CREATE TABLE bluesky_1m.bluesky (data JSON) ENGINE = MergeTree ORDER BY tuple();

-- Load sample data
INSERT INTO bluesky_1m.bluesky FROM INFILE 'sample_1k.jsonl' FORMAT JSONEachRow;

-- Verify source data
SELECT 'Source data count:' as check, count() as value FROM bluesky_1m.bluesky;

-- Create true variants schema
CREATE DATABASE IF NOT EXISTS bluesky_true_variants;

DROP TABLE IF EXISTS bluesky_true_variants.bluesky_data;

CREATE TABLE bluesky_true_variants.bluesky_data
(
    -- Core identity fields
    did String,
    time_us UInt64,
    kind LowCardinality(String),
    timestamp_col DateTime64(6),
    
    -- TRUE Variant columns
    commit_operation Variant(String),
    commit_collection Variant(String),
    commit_rev Variant(String),
    commit_rkey Variant(String),
    commit_cid Variant(String),
    
    -- Record data as variant (JSON or String)
    record_data Variant(JSON, String),
    
    -- Original JSON for comparison
    original_json JSON
)
ENGINE = MergeTree
ORDER BY (kind, did, timestamp_col)
SETTINGS 
    allow_experimental_variant_type = 1,
    use_variant_as_common_type = 1;

-- Load data using CAST approach (the proven method)
INSERT INTO bluesky_true_variants.bluesky_data
SELECT 
    data.did::String as did,
    data.time_us::UInt64 as time_us,
    data.kind::String as kind,
    fromUnixTimestamp64Micro(data.time_us) as timestamp_col,
    
    -- Cast to Variant columns
    CAST(data.commit.operation AS Variant(String)) as commit_operation,
    CAST(data.commit.collection AS Variant(String)) as commit_collection,
    CAST(data.commit.rev AS Variant(String)) as commit_rev,
    CAST(data.commit.rkey AS Variant(String)) as commit_rkey,
    CAST(data.commit.cid AS Variant(String)) as commit_cid,
    
    -- Record data as variant
    CAST(data AS Variant(JSON, String)) as record_data,
    
    -- Original JSON
    data as original_json
FROM bluesky_1m.bluesky;

-- Verification queries
SELECT 'Variants data count:' as check, count() as value FROM bluesky_true_variants.bluesky_data;

SELECT 'Variant types:' as check, 
       variantType(commit_operation) as op_type,
       variantType(record_data) as data_type,
       count() as cnt
FROM bluesky_true_variants.bluesky_data 
GROUP BY op_type, data_type;

SELECT 'Top events:' as check,
       variantElement(commit_collection, 'String') as event,
       count() as cnt
FROM bluesky_true_variants.bluesky_data 
WHERE commit_collection IS NOT NULL
GROUP BY event ORDER BY cnt DESC LIMIT 5;

-- Test variant functions
SELECT 'Variant extraction test:' as check,
       variantElement(commit_operation, 'String') as operation,
       variantElement(commit_collection, 'String') as collection,
       count() as cnt
FROM bluesky_true_variants.bluesky_data 
WHERE commit_operation IS NOT NULL AND commit_collection IS NOT NULL
GROUP BY operation, collection 
ORDER BY cnt DESC 
LIMIT 3; 