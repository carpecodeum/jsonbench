-- NULL Statistics for Preprocessed Table - All extracted columns analysis
SELECT 
    'Total Records' as field_name,
    'N/A' as field_type,
    count() as total_records,
    0 as null_count,
    '0.00%' as null_percentage
FROM bluesky_variants_test.bluesky_preprocessed

UNION ALL

SELECT 
    'did' as field_name,
    'String' as field_type,
    count() as total_records,
    countIf(did IS NULL OR did = '') as null_count,
    concat(toString(round(countIf(did IS NULL OR did = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_preprocessed

UNION ALL

SELECT 
    'time_us' as field_name,
    'UInt64' as field_type,
    count() as total_records,
    countIf(time_us IS NULL OR time_us = 0) as null_count,
    concat(toString(round(countIf(time_us IS NULL OR time_us = 0) * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_preprocessed

UNION ALL

SELECT 
    'kind' as field_name,
    'LowCardinality(String)' as field_type,
    count() as total_records,
    countIf(kind IS NULL OR kind = '') as null_count,
    concat(toString(round(countIf(kind IS NULL OR kind = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_preprocessed

UNION ALL

SELECT 
    'timestamp_col' as field_name,
    'DateTime64(6)' as field_type,
    count() as total_records,
    countIf(timestamp_col IS NULL) as null_count,
    concat(toString(round(countIf(timestamp_col IS NULL) * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_preprocessed

UNION ALL

SELECT 
    'commit_rev' as field_name,
    'String' as field_type,
    count() as total_records,
    countIf(commit_rev IS NULL OR commit_rev = '') as null_count,
    concat(toString(round(countIf(commit_rev IS NULL OR commit_rev = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_preprocessed

UNION ALL

SELECT 
    'commit_operation' as field_name,
    'LowCardinality(String)' as field_type,
    count() as total_records,
    countIf(commit_operation IS NULL OR commit_operation = '') as null_count,
    concat(toString(round(countIf(commit_operation IS NULL OR commit_operation = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_preprocessed

UNION ALL

SELECT 
    'commit_collection' as field_name,
    'LowCardinality(String)' as field_type,
    count() as total_records,
    countIf(commit_collection IS NULL OR commit_collection = '') as null_count,
    concat(toString(round(countIf(commit_collection IS NULL OR commit_collection = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_preprocessed

UNION ALL

SELECT 
    'commit_rkey' as field_name,
    'String' as field_type,
    count() as total_records,
    countIf(commit_rkey IS NULL OR commit_rkey = '') as null_count,
    concat(toString(round(countIf(commit_rkey IS NULL OR commit_rkey = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_preprocessed

UNION ALL

SELECT 
    'commit_cid' as field_name,
    'String' as field_type,
    count() as total_records,
    countIf(commit_cid IS NULL OR commit_cid = '') as null_count,
    concat(toString(round(countIf(commit_cid IS NULL OR commit_cid = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_preprocessed

UNION ALL

SELECT 
    'record_type' as field_name,
    'LowCardinality(String)' as field_type,
    count() as total_records,
    countIf(record_type IS NULL OR record_type = '') as null_count,
    concat(toString(round(countIf(record_type IS NULL OR record_type = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_preprocessed

UNION ALL

SELECT 
    'original_json' as field_name,
    'JSON' as field_type,
    count() as total_records,
    countIf(original_json IS NULL) as null_count,
    concat(toString(round(countIf(original_json IS NULL) * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_preprocessed

ORDER BY 
    CASE 
        WHEN field_name = 'Total Records' THEN 0
        ELSE 1 
    END,
    null_count DESC; 