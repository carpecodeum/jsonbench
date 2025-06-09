-- ClickHouse TRUE Variant Column Example
-- This shows what actual Variant columns look like in ClickHouse

CREATE DATABASE IF NOT EXISTS bluesky_true_variant;

-- Example of TRUE ClickHouse Variant columns
CREATE TABLE bluesky_true_variant.bluesky_variant_example
(
    id UInt64,
    
    -- This is a TRUE Variant column - can store String, UInt64, or Array(String)
    metadata Variant(String, UInt64, Array(String)),
    
    -- Another Variant column - can store different types of values
    content Variant(String, JSON, Array(UInt64)),
    
    -- Traditional approach for comparison
    original_json JSON
)
ENGINE = MergeTree
ORDER BY id;

-- Example inserts showing Variant column usage
INSERT INTO bluesky_true_variant.bluesky_variant_example VALUES
(1, 'string_value', 'text content', '{"original": "json"}'),
(2, 12345, ['array', 'of', 'strings'], '{"original": "json2"}'),
(3, ['tag1', 'tag2'], 99999, '{"original": "json3"}');

-- Query Variant columns with type checking
SELECT 
    id,
    variantType(metadata) as metadata_type,
    variantElement(metadata, 'String') as metadata_string,
    variantElement(metadata, 'UInt64') as metadata_number,
    variantType(content) as content_type
FROM bluesky_true_variant.bluesky_variant_example; 