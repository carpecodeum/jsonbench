-- =====================================================
-- VARIANT QUERY ALTERNATIVES GUIDE
-- Different ways to query Variant(JSON) data in ClickHouse
-- =====================================================

-- Table: bluesky_minimal_1m.bluesky_data (data Variant(JSON))

-- =====================================================
-- METHOD COMPARISON (Performance for 1M records)
-- =====================================================

-- Method 1: toString() + JSONExtractString (Original)
-- Performance: ~2.34s
SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') as kind
FROM bluesky_minimal_1m.bluesky_data;

-- Method 2: CAST + JSONExtractString  
-- Performance: ~2.45s (slightly slower)
SELECT JSONExtractString(CAST(variantElement(data, 'JSON') AS String), 'kind') as kind
FROM bluesky_minimal_1m.bluesky_data;

-- Method 3: toString() + JSON_VALUE (JSONPath syntax)
-- Performance: ~2.55s (slightly slower)
SELECT JSON_VALUE(toString(variantElement(data, 'JSON')), '$.kind') as kind
FROM bluesky_minimal_1m.bluesky_data;

-- =====================================================
-- WORKING ALTERNATIVES
-- =====================================================

-- 1. CAST Method (cleaner syntax)
SELECT 
    JSONExtractString(CAST(variantElement(data, 'JSON') AS String), 'kind') as kind,
    JSONExtractString(CAST(variantElement(data, 'JSON') AS String), 'did') as did
FROM bluesky_minimal_1m.bluesky_data;

-- 2. JSON_VALUE with JSONPath (more flexible)
SELECT 
    JSON_VALUE(toString(variantElement(data, 'JSON')), '$.kind') as kind,
    JSON_VALUE(toString(variantElement(data, 'JSON')), '$.commit.collection') as collection
FROM bluesky_minimal_1m.bluesky_data;

-- 3. JSONPath for nested fields  
SELECT 
    JSON_VALUE(toString(variantElement(data, 'JSON')), '$.commit.operation') as operation,
    JSON_VALUE(toString(variantElement(data, 'JSON')), '$.commit.collection') as collection
FROM bluesky_minimal_1m.bluesky_data;

-- =====================================================
-- CREATING QUERY FUNCTIONS (for cleaner code)
-- =====================================================

-- You can create a function to simplify the syntax:
-- Note: This is a conceptual example - ClickHouse may not support all UDF features

-- Conceptual function (if supported):
-- CREATE FUNCTION extractFromVariant(data Variant(JSON), path String) -> String
-- AS JSON_VALUE(toString(variantElement(data, 'JSON')), concat('$.', path));

-- Usage would be:
-- SELECT extractFromVariant(data, 'kind') FROM bluesky_minimal_1m.bluesky_data;

-- =====================================================
-- WORKING WITH DIFFERENT DATA TYPES
-- =====================================================

-- String fields
SELECT JSON_VALUE(toString(variantElement(data, 'JSON')), '$.kind') as kind
FROM bluesky_minimal_1m.bluesky_data;

-- Numeric fields  
SELECT CAST(JSON_VALUE(toString(variantElement(data, 'JSON')), '$.time_us') AS UInt64) as time_us
FROM bluesky_minimal_1m.bluesky_data;

-- Boolean fields (if they exist)
SELECT CAST(JSON_VALUE(toString(variantElement(data, 'JSON')), '$.some_flag') AS Bool) as flag
FROM bluesky_minimal_1m.bluesky_data;

-- =====================================================
-- COMPLEX QUERIES WITH ALTERNATIVES
-- =====================================================

-- Group by with CAST method
SELECT 
    JSONExtractString(CAST(variantElement(data, 'JSON') AS String), 'kind') as kind,
    count() 
FROM bluesky_minimal_1m.bluesky_data 
GROUP BY kind 
ORDER BY count() DESC;

-- Group by with JSON_VALUE method
SELECT 
    JSON_VALUE(toString(variantElement(data, 'JSON')), '$.kind') as kind,
    count() 
FROM bluesky_minimal_1m.bluesky_data 
GROUP BY kind 
ORDER BY count() DESC;

-- Nested aggregation with JSON_VALUE
SELECT 
    JSON_VALUE(toString(variantElement(data, 'JSON')), '$.commit.operation') as op,
    JSON_VALUE(toString(variantElement(data, 'JSON')), '$.commit.collection') as coll,
    count()
FROM bluesky_minimal_1m.bluesky_data 
WHERE op != '' AND coll != ''
GROUP BY op, coll 
ORDER BY count() DESC;

-- =====================================================
-- NON-WORKING ALTERNATIVES (for reference)
-- =====================================================

-- These DON'T work:

-- Direct field access (fails - not a tuple)
-- SELECT variantElement(data, 'JSON').kind FROM bluesky_minimal_1m.bluesky_data;

-- Direct JSONExtract without string conversion (fails - wrong type)
-- SELECT JSONExtractString(variantElement(data, 'JSON'), 'kind') FROM bluesky_minimal_1m.bluesky_data;

-- Subscript notation (fails - not an array)  
-- SELECT data['JSON'] FROM bluesky_minimal_1m.bluesky_data;

-- Cast to JSON then access (fails - JSON type doesn't support dot notation)
-- SELECT CAST(variantElement(data, 'JSON') AS JSON).kind FROM bluesky_minimal_1m.bluesky_data;

-- =====================================================
-- PERFORMANCE SUMMARY
-- =====================================================

-- All methods are similarly slow (~2.3-2.6s for 1M records) because they all require:
-- 1. Extract JSON from Variant 
-- 2. Convert to String
-- 3. Parse JSON string
-- 4. Extract field

-- For comparison, native JSON Object access:
-- SELECT toString(data.kind) FROM bluesky_1m.bluesky;  -- ~0.12s (20x faster!)

-- =====================================================
-- RECOMMENDATIONS
-- =====================================================

-- 1. For readability: Use JSON_VALUE with JSONPath syntax
--    JSON_VALUE(toString(variantElement(data, 'JSON')), '$.field')

-- 2. For consistency: Use CAST instead of toString()  
--    JSONExtractString(CAST(variantElement(data, 'JSON') AS String), 'field')

-- 3. For performance: Consider using native JSON Object type instead of Variant(JSON)
--    if you only need to store JSON data

-- 4. For complex nested paths: JSON_VALUE supports more flexible JSONPath expressions
--    JSON_VALUE(data_str, '$.commit.record.text') vs JSONExtractString(data_str, 'commit', 'record', 'text') 