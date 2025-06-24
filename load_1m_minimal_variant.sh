#!/bin/bash
"""
Load 1M Records into Minimal Variant Schema
===========================================
This script loads 1M Bluesky records into ClickHouse using the Variant(JSON) type.
This demonstrates ClickHouse's new Variant type for JSON analytics.
"""

set -e

# Configuration
DATABASE="bluesky_minimal_1m"
TABLE="bluesky_data"
DATA_FILE="bluesky_1m_baseline.jsonl"
RECORDS_TO_LOAD=1000000

echo "========================================"
echo "LOADING 1M RECORDS - MINIMAL VARIANT"
echo "========================================"
echo "Database: $DATABASE"
echo "Table: $TABLE"
echo "Data file: $DATA_FILE"
echo "Records to load: $RECORDS_TO_LOAD"
echo ""

# Check if data file exists
if [ ! -f "$DATA_FILE" ]; then
    echo "❌ Error: Data file $DATA_FILE not found!"
    echo "Please ensure the 1M baseline data file is available."
    exit 1
fi

echo "✓ Data file found: $(du -h $DATA_FILE | cut -f1)"

# Step 1: Create database and table schema
echo ""
echo "1. Creating database and table schema..."
clickhouse client --query "
CREATE DATABASE IF NOT EXISTS $DATABASE;

DROP TABLE IF EXISTS $DATABASE.$TABLE;

CREATE TABLE $DATABASE.$TABLE (
    data Variant(JSON)
) ENGINE = MergeTree 
ORDER BY tuple()
SETTINGS 
    allow_experimental_variant_type = 1, 
    use_variant_as_common_type = 1
COMMENT 'Minimal Variant - using ClickHouse Variant(JSON) type for flexible JSON storage';
"

if [ $? -eq 0 ]; then
    echo "✓ Schema created successfully"
else
    echo "❌ Schema creation failed"
    exit 1
fi

# Step 2: Load data with proper JSON wrapping
echo ""
echo "2. Loading $RECORDS_TO_LOAD records into minimal variant..."
echo "   This will take a few minutes..."

start_time=$(date +%s)

# Use sed to wrap each JSON record in {"data": ...} format for JSONEachRow
cat "$DATA_FILE" | head -$RECORDS_TO_LOAD | sed 's/^/{"data":/' | sed 's/$/}/' | \
clickhouse client --query "INSERT INTO $DATABASE.$TABLE FORMAT JSONEachRow"

load_result=$?
end_time=$(date +%s)
load_duration=$((end_time - start_time))

if [ $load_result -eq 0 ]; then
    echo "✓ Data loaded successfully in ${load_duration}s"
else
    echo "❌ Data loading failed"
    exit 1
fi

# Step 3: Verify data integrity
echo ""
echo "3. Verifying data integrity..."

# Check record count
record_count=$(clickhouse client --query "SELECT count() FROM $DATABASE.$TABLE")
echo "   Records loaded: $record_count"

if [ "$record_count" -ne "$RECORDS_TO_LOAD" ]; then
    echo "❌ Warning: Expected $RECORDS_TO_LOAD records, got $record_count"
fi

# Check data content using both query methods
echo "   Checking data content..."

# Method 1: Direct JSON field access (fastest)
echo "   Testing direct JSON field access..."
sample_data_direct=$(clickhouse client --query "
SELECT 
    toString(data.JSON.did) as did,
    toString(data.JSON.kind) as kind,
    toString(data.JSON.time_us) as time_us
FROM $DATABASE.$TABLE 
LIMIT 3
")

if [ $? -eq 0 ] && [ -n "$sample_data_direct" ]; then
    echo "✓ Direct access verified:"
    echo "$sample_data_direct" | head -3
else
    echo "❌ Direct access verification failed"
    exit 1
fi

# Method 2: JSONExtract method (slower but compatible)
echo "   Testing JSONExtract method..."
sample_data_extract=$(clickhouse client --query "
SELECT 
    JSONExtractString(toString(data.JSON), 'did') as did,
    JSONExtractString(toString(data.JSON), 'kind') as kind,
    JSONExtractString(toString(data.JSON), 'time_us') as time_us
FROM $DATABASE.$TABLE 
LIMIT 3
SETTINGS max_threads = 1, max_memory_usage = 4000000000
")

if [ $? -eq 0 ] && [ -n "$sample_data_extract" ]; then
    echo "✓ JSONExtract method verified:"
    echo "$sample_data_extract" | head -3
else
    echo "❌ JSONExtract method verification failed"
fi

# Step 4: Show table statistics
echo ""
echo "4. Table statistics:"

# Get table size
table_size=$(clickhouse client --query "
SELECT formatReadableSize(sum(bytes_on_disk)) 
FROM system.parts 
WHERE database = '$DATABASE' AND table = '$TABLE' AND active = 1
")

# Check variant type distribution
echo "   Table size: $table_size"
echo "   Variant type distribution:"
clickhouse client --query "
SELECT variantType(data) as variant_type, count() 
FROM $DATABASE.$TABLE 
GROUP BY variant_type
"

echo ""
echo "========================================"
echo "✅ MINIMAL VARIANT LOADING COMPLETED"
echo "========================================"
echo "Database: $DATABASE"
echo "Table: $TABLE" 
echo "Records: $record_count"
echo "Size: $table_size"
echo "Load time: ${load_duration}s"
echo ""
echo "Query Methods Available:"
echo "1. Direct JSON field access (FASTEST):"
echo "   SELECT toString(data.JSON.kind), count() FROM $DATABASE.$TABLE GROUP BY toString(data.JSON.kind)"
echo ""
echo "2. JSONExtract method (SLOWER but compatible):"
echo "   SELECT JSONExtractString(toString(data.JSON), 'kind'), count() FROM $DATABASE.$TABLE GROUP BY JSONExtractString(toString(data.JSON), 'kind') SETTINGS max_threads=1, max_memory_usage=4000000000"
echo "" 