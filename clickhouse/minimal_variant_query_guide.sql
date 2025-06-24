-- =====================================================
-- MINIMAL VARIANT QUERY GUIDE
-- How to query data stored in Variant(JSON) column
-- =====================================================

-- Table structure: 
-- CREATE TABLE bluesky_data (data Variant(JSON))

-- =====================================================
-- BASIC EXTRACTION PATTERNS
-- =====================================================

-- 1. Extract top-level string fields
SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') as kind
FROM bluesky_minimal_1m.bluesky_data;

SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'did') as did
FROM bluesky_minimal_1m.bluesky_data;

-- 2. Extract nested string fields
SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') as collection
FROM bluesky_minimal_1m.bluesky_data;

SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'operation') as operation
FROM bluesky_minimal_1m.bluesky_data;

-- 3. Extract numeric fields
SELECT JSONExtractUInt(toString(variantElement(data, 'JSON')), 'time_us') as time_us
FROM bluesky_minimal_1m.bluesky_data;

-- 4. Extract and convert to different types
SELECT 
    JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') as kind,
    JSONExtractUInt(toString(variantElement(data, 'JSON')), 'time_us') as time_us,
    JSONExtractString(toString(variantElement(data, 'JSON')), 'did') as did
FROM bluesky_minimal_1m.bluesky_data;

-- =====================================================
-- FILTERING AND WHERE CLAUSES
-- =====================================================

-- Filter by string equality
SELECT count() 
FROM bluesky_minimal_1m.bluesky_data 
WHERE JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') = 'commit';

-- Filter by nested field
SELECT count() 
FROM bluesky_minimal_1m.bluesky_data 
WHERE JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'operation') = 'create';

-- Filter by numeric comparison
SELECT count() 
FROM bluesky_minimal_1m.bluesky_data 
WHERE JSONExtractUInt(toString(variantElement(data, 'JSON')), 'time_us') > 1700000000000000;

-- Filter with multiple conditions
SELECT count() 
FROM bluesky_minimal_1m.bluesky_data 
WHERE JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') = 'commit'
  AND JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') = 'app.bsky.feed.post';

-- =====================================================
-- AGGREGATIONS AND GROUP BY
-- =====================================================

-- Count by field
SELECT 
    JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') as kind, 
    count() 
FROM bluesky_minimal_1m.bluesky_data 
GROUP BY kind 
ORDER BY count() DESC;

-- Count by nested field
SELECT 
    JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') as collection, 
    count() 
FROM bluesky_minimal_1m.bluesky_data 
WHERE collection != '' 
GROUP BY collection 
ORDER BY count() DESC;

-- Group by multiple fields
SELECT 
    JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'operation') as op,
    JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') as coll,
    count() 
FROM bluesky_minimal_1m.bluesky_data 
WHERE op != '' AND coll != '' 
GROUP BY op, coll 
ORDER BY count() DESC;

-- =====================================================
-- ADVANCED PATTERNS
-- =====================================================

-- Extract arrays (if they exist)
SELECT JSONExtractArrayRaw(toString(variantElement(data, 'JSON')), 'some_array_field')
FROM bluesky_minimal_1m.bluesky_data;

-- Check if field exists
SELECT count()
FROM bluesky_minimal_1m.bluesky_data
WHERE JSONHas(toString(variantElement(data, 'JSON')), 'commit');

-- Extract with default value
SELECT 
    JSONExtractString(toString(variantElement(data, 'JSON')), 'kind'),
    JSONExtractString(toString(variantElement(data, 'JSON')), 'nonexistent_field') as missing_field
FROM bluesky_minimal_1m.bluesky_data 
LIMIT 3;

-- =====================================================
-- PERFORMANCE CONSIDERATIONS
-- =====================================================

-- Bad: Don't extract the same field multiple times
-- SELECT 
--     JSONExtractString(toString(variantElement(data, 'JSON')), 'kind'),
--     JSONExtractString(toString(variantElement(data, 'JSON')), 'kind')  -- Duplicate!

-- Good: Extract once and reuse
SELECT 
    kind,
    kind as kind_copy
FROM (
    SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') as kind
    FROM bluesky_minimal_1m.bluesky_data
);

-- =====================================================
-- COMPARISON WITH JSON BASELINE
-- =====================================================

-- Minimal Variant approach (complex):
SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') as kind
FROM bluesky_minimal_1m.bluesky_data;

-- JSON Baseline approach (simple):
-- SELECT toString(data.kind) as kind
-- FROM bluesky_1m.bluesky;

-- =====================================================
-- MEMORY OPTIMIZATION (if needed)
-- =====================================================

-- Add these settings for large queries to prevent memory issues:
-- SETTINGS max_threads = 1, max_memory_usage = 4000000000

SELECT 
    JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') as kind, 
    count() 
FROM bluesky_minimal_1m.bluesky_data 
GROUP BY kind 
ORDER BY count() DESC
SETTINGS max_threads = 1, max_memory_usage = 4000000000; 