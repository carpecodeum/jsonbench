-- NULL Statistics for Original JSON Table - Comprehensive field analysis
SELECT 
    'Total Records' as field_name,
    'N/A' as field_type,
    count() as total_records,
    0 as null_count,
    '0.00%' as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.did' as field_name,
    'String' as field_type,
    count() as total_records,
    countIf(data.did IS NULL OR data.did::String = '') as null_count,
    concat(toString(round(countIf(data.did IS NULL OR data.did::String = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.time_us' as field_name,
    'UInt64' as field_type,
    count() as total_records,
    countIf(data.time_us IS NULL OR data.time_us::UInt64 = 0) as null_count,
    concat(toString(round(countIf(data.time_us IS NULL OR data.time_us::UInt64 = 0) * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.kind' as field_name,
    'String' as field_type,
    count() as total_records,
    countIf(data.kind IS NULL OR data.kind::String = '') as null_count,
    concat(toString(round(countIf(data.kind IS NULL OR data.kind::String = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.commit' as field_name,
    'JSON Object' as field_type,
    count() as total_records,
    countIf(data.commit IS NULL) as null_count,
    concat(toString(round(countIf(data.commit IS NULL) * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.commit.rev' as field_name,
    'String' as field_type,
    count() as total_records,
    countIf(data.commit.rev IS NULL OR data.commit.rev::String = '') as null_count,
    concat(toString(round(countIf(data.commit.rev IS NULL OR data.commit.rev::String = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.commit.operation' as field_name,
    'String' as field_type,
    count() as total_records,
    countIf(data.commit.operation IS NULL OR data.commit.operation::String = '') as null_count,
    concat(toString(round(countIf(data.commit.operation IS NULL OR data.commit.operation::String = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.commit.collection' as field_name,
    'String' as field_type,
    count() as total_records,
    countIf(data.commit.collection IS NULL OR data.commit.collection::String = '') as null_count,
    concat(toString(round(countIf(data.commit.collection IS NULL OR data.commit.collection::String = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.commit.rkey' as field_name,
    'String' as field_type,
    count() as total_records,
    countIf(data.commit.rkey IS NULL OR data.commit.rkey::String = '') as null_count,
    concat(toString(round(countIf(data.commit.rkey IS NULL OR data.commit.rkey::String = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.commit.cid' as field_name,
    'String' as field_type,
    count() as total_records,
    countIf(data.commit.cid IS NULL OR data.commit.cid::String = '') as null_count,
    concat(toString(round(countIf(data.commit.cid IS NULL OR data.commit.cid::String = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.commit.record' as field_name,
    'JSON Object' as field_type,
    count() as total_records,
    countIf(data.commit.record IS NULL) as null_count,
    concat(toString(round(countIf(data.commit.record IS NULL) * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.commit.record.$type' as field_name,
    'String' as field_type,
    count() as total_records,
    countIf(data.commit.record.`$type` IS NULL OR data.commit.record.`$type`::String = '') as null_count,
    concat(toString(round(countIf(data.commit.record.`$type` IS NULL OR data.commit.record.`$type`::String = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.commit.record.createdAt' as field_name,
    'String' as field_type,
    count() as total_records,
    countIf(data.commit.record.createdAt IS NULL OR data.commit.record.createdAt::String = '') as null_count,
    concat(toString(round(countIf(data.commit.record.createdAt IS NULL OR data.commit.record.createdAt::String = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.commit.record.text' as field_name,
    'String' as field_type,
    count() as total_records,
    countIf(data.commit.record.text IS NULL OR data.commit.record.text::String = '') as null_count,
    concat(toString(round(countIf(data.commit.record.text IS NULL OR data.commit.record.text::String = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.commit.record.langs' as field_name,
    'Array' as field_type,
    count() as total_records,
    countIf(data.commit.record.langs IS NULL OR length(JSONExtract(data.commit.record.langs, 'Array(String)')) = 0) as null_count,
    concat(toString(round(countIf(data.commit.record.langs IS NULL OR length(JSONExtract(data.commit.record.langs, 'Array(String)')) = 0) * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.commit.record.subject' as field_name,
    'JSON Object' as field_type,
    count() as total_records,
    countIf(data.commit.record.subject IS NULL) as null_count,
    concat(toString(round(countIf(data.commit.record.subject IS NULL) * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.commit.record.reply' as field_name,
    'JSON Object' as field_type,
    count() as total_records,
    countIf(data.commit.record.reply IS NULL) as null_count,
    concat(toString(round(countIf(data.commit.record.reply IS NULL) * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.identity' as field_name,
    'JSON Object' as field_type,
    count() as total_records,
    countIf(data.identity IS NULL) as null_count,
    concat(toString(round(countIf(data.identity IS NULL) * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.identity.handle' as field_name,
    'String' as field_type,
    count() as total_records,
    countIf(data.identity.handle IS NULL OR data.identity.handle::String = '') as null_count,
    concat(toString(round(countIf(data.identity.handle IS NULL OR data.identity.handle::String = '') * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

UNION ALL

SELECT 
    'data.identity.seq' as field_name,
    'UInt64' as field_type,
    count() as total_records,
    countIf(data.identity.seq IS NULL OR data.identity.seq::UInt64 = 0) as null_count,
    concat(toString(round(countIf(data.identity.seq IS NULL OR data.identity.seq::UInt64 = 0) * 100.0 / count(), 2)), '%') as null_percentage
FROM bluesky_variants_test.bluesky_json_baseline

ORDER BY 
    CASE 
        WHEN field_name = 'Total Records' THEN 0
        ELSE 1 
    END,
    null_count DESC; 