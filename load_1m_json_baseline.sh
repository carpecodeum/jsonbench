#!/bin/bash
"""
Load 1M Records into JSON Baseline Schema
=========================================
This script loads 1M Bluesky records into ClickHouse using the native JSON Object type.
The JSON baseline provides the fastest query performance for JSON analytics.
"""

set -e

# Configuration
DATABASE="bluesky_1m"
TABLE="bluesky"
DATA_FILE="bluesky_1m_baseline.jsonl"
RECORDS_TO_LOAD=1000000

echo "========================================"
echo "LOADING 1M RECORDS - JSON BASELINE"
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
    data JSON
) ENGINE = MergeTree 
ORDER BY tuple()
COMMENT 'JSON Object baseline - native ClickHouse JSON type for fastest performance';
"

if [ $? -eq 0 ]; then
    echo "✓ Schema created successfully"
else
    echo "❌ Schema creation failed"
    exit 1
fi

# Step 2: Load data with proper JSON wrapping
echo ""
echo "2. Loading $RECORDS_TO_LOAD records into JSON baseline..."
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

# Check data content by extracting sample fields
echo "   Checking data content..."
sample_data=$(clickhouse client --query "
SELECT 
    toString(data.did) as did,
    toString(data.kind) as kind,
    toString(data.time_us) as time_us
FROM $DATABASE.$TABLE 
LIMIT 3
")

if [ $? -eq 0 ] && [ -n "$sample_data" ]; then
    echo "✓ Data content verified:"
    echo "$sample_data" | head -3
else
    echo "❌ Data content verification failed"
    exit 1
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

# Get sample record structure
echo "   Table size: $table_size"
echo "   Sample record structure:"
clickhouse client --query "
SELECT JSONAllPathsWithTypes(data) as paths 
FROM $DATABASE.$TABLE 
LIMIT 1
" --format Vertical

echo ""
echo "========================================"
echo "✅ JSON BASELINE LOADING COMPLETED"
echo "========================================"
echo "Database: $DATABASE"
echo "Table: $TABLE" 
echo "Records: $record_count"
echo "Size: $table_size"
echo "Load time: ${load_duration}s"
echo ""
echo "Ready for benchmarking! Use queries like:"
echo "  SELECT toString(data.kind), count() FROM $DATABASE.$TABLE GROUP BY toString(data.kind)"
echo "  SELECT count() FROM $DATABASE.$TABLE WHERE toString(data.kind) = 'commit'"
echo "" 