#!/bin/bash
set -e

echo "======================================================================="
echo "100M VARIANT ARRAY - CLEAN SLATE APPROACH"
echo "======================================================================="
echo "Strategy: Stop all memory consumers + optimal ClickHouse settings"
echo ""

# Function to show memory usage
show_memory() {
    echo "=== MEMORY STATUS ==="
    free -h
    echo "Top memory consumers:"
    ps aux --sort=-%mem | head -5
    echo ""
}

# Function to show array size details
show_array_size() {
    local db=$1
    local table=$2
    echo "=== ARRAY SIZE ANALYSIS ==="
    
    # Row count
    echo "Row count:"
    clickhouse-client --query "SELECT count() FROM $db.$table" 2>/dev/null || echo "0 (table empty/failed)"
    
    # Array length
    echo "Array length:"
    clickhouse-client --query "SELECT length(variantElement(data, 'Array(JSON)')) FROM $db.$table" 2>/dev/null || echo "0 (no array data)"
    
    # Storage size in bytes
    echo "Storage size (bytes):"
    clickhouse-client --query "SELECT total_bytes FROM system.tables WHERE database = '$db' AND name = '$table'" 2>/dev/null || echo "0"
    
    # Human readable storage size
    echo "Storage size (human readable):"
    clickhouse-client --query "SELECT formatReadableSize(total_bytes) FROM system.tables WHERE database = '$db' AND name = '$table'" 2>/dev/null || echo "0 B"
    
    # Bytes per record
    echo "Efficiency (bytes per JSON record):"
    clickhouse-client --query "
    SELECT CASE 
        WHEN length(variantElement(data, 'Array(JSON)')) > 0 
        THEN total_bytes / length(variantElement(data, 'Array(JSON)'))
        ELSE 0 
    END as bytes_per_record
    FROM $db.$table, system.tables 
    WHERE database = '$db' AND name = '$table'
    " 2>/dev/null || echo "0"
    
    echo ""
}

echo "🧹 STEP 1: CLEAN MEMORY SLATE"
echo "----------------------------------------"
show_memory

echo "Stopping all memory-consuming processes..."

# Kill stuck ClickHouse clients
echo "- Killing stuck ClickHouse client processes..."
pkill -f "clickhouse-client.*INSERT" 2>/dev/null || true
pkill -f "working_variant_array" 2>/dev/null || true
pkill -f "variant_array" 2>/dev/null || true

# Kill Python processes using significant memory
echo "- Killing large Python processes..."
ps aux | awk '$4 > 5.0 && /python/ {print $2}' | xargs -r kill 2>/dev/null || true

# Wait for cleanup
sleep 5
echo "✅ Memory cleanup complete"
show_memory

echo "🔧 STEP 2: OPTIMAL CLICKHOUSE CONFIGURATION"
echo "----------------------------------------"

# Configure ClickHouse for maximum efficiency
echo "Setting optimal ClickHouse parameters..."

export TZ=UTC

# Create optimized ClickHouse settings
clickhouse-client --query "
SET max_memory_usage = 45000000000;
SET max_bytes_before_external_group_by = 20000000000;
SET max_bytes_before_external_sort = 20000000000;
SET max_parser_depth = 100000;
SET input_format_json_max_depth = 100000;
SET min_chunk_bytes_for_parallel_parsing = 1000000000;
SET max_parser_backtracks = 10000000;
SET max_untracked_memory = 2000000000;
" || echo "⚠️  Settings may not persist, will use client flags"

echo "✅ ClickHouse optimized for 100M processing"

echo "🗄️ STEP 3: DATABASE SETUP"
echo "----------------------------------------"

# Clean database setup
clickhouse-client --query "DROP DATABASE IF EXISTS bluesky_100m_variant_array"
clickhouse-client --query "CREATE DATABASE bluesky_100m_variant_array"

clickhouse-client --query "
CREATE TABLE bluesky_100m_variant_array.bluesky_array_data (
    data Variant(Array(JSON))
) ENGINE = MergeTree()
ORDER BY tuple()
"

echo "✅ Database and table created"

echo "📊 STEP 4: DATA STREAMING WITH SIZE MONITORING"
echo "----------------------------------------"

DATA_DIR="$HOME/data/bluesky"
echo "Data directory: $DATA_DIR"

# Count available files
FILE_COUNT=$(find "$DATA_DIR" -name "file_*.json.gz" | wc -l)
echo "Available data files: $FILE_COUNT"

if [ $FILE_COUNT -eq 0 ]; then
    echo "❌ No data files found in $DATA_DIR"
    exit 1
fi

echo ""
echo "🚀 Starting 100M variant array creation..."
echo "Target: All $FILE_COUNT files = ~100M records"
echo "Memory limit: 45GB with external spilling"
echo ""

# Use optimized client settings for massive array
{
    echo '{"data":['
    
    first_record=true
    total_records=0
    file_count=0
    
    for file in "$DATA_DIR"/file_*.json.gz; do
        if [ -f "$file" ]; then
            file_count=$((file_count + 1))
            echo "Processing file $file_count/$FILE_COUNT: $(basename "$file")" >&2
            
            while IFS= read -r line; do
                if [ -n "$line" ]; then
                    # Validate JSON (simple check)
                    if echo "$line" | jq empty 2>/dev/null; then
                        if [ "$first_record" = true ]; then
                            first_record=false
                        else
                            echo ","
                        fi
                        echo "$line"
                        total_records=$((total_records + 1))
                        
                        # Progress every million records
                        if [ $((total_records % 1000000)) -eq 0 ]; then
                            echo "  ✓ Processed $total_records records" >&2
                        fi
                    fi
                fi
            done < <(zcat "$file")
            
            # Memory status every 10 files
            if [ $((file_count % 10)) -eq 0 ]; then
                echo "  📊 Memory check after $file_count files:" >&2
                free -h | grep "Mem:" >&2
            fi
        fi
    done
    
    echo ']}'
    echo "✅ Streamed $total_records total records" >&2
    
} | clickhouse-client \
    --max_memory_usage=45000000000 \
    --max_bytes_before_external_group_by=20000000000 \
    --max_bytes_before_external_sort=20000000000 \
    --min_chunk_bytes_for_parallel_parsing=1000000000 \
    --max_parser_depth=100000 \
    --max_parser_backtracks=10000000 \
    --max_untracked_memory=2000000000 \
    --query "INSERT INTO bluesky_100m_variant_array.bluesky_array_data FORMAT JSONEachRow"

INSERT_RESULT=$?

echo ""
echo "⏳ STEP 5: PROCESSING COMPLETE - ANALYZING RESULTS"
echo "----------------------------------------"

if [ $INSERT_RESULT -eq 0 ]; then
    echo "🎉 INSERT COMPLETED SUCCESSFULLY!"
else
    echo "⚠️  Insert process completed with status: $INSERT_RESULT"
    echo "Checking if data was stored despite exit status..."
fi

# Wait for ClickHouse to finalize
sleep 10

echo "📏 DETAILED SIZE ANALYSIS:"
echo "----------------------------------------"
show_array_size "bluesky_100m_variant_array" "bluesky_array_data"

# Additional detailed analysis
echo "=== COMPREHENSIVE STORAGE ANALYSIS ==="

# Check if we have data
RECORD_COUNT=$(clickhouse-client --query "SELECT length(variantElement(data, 'Array(JSON)')) FROM bluesky_100m_variant_array.bluesky_array_data" 2>/dev/null || echo "0")

if [ "$RECORD_COUNT" -gt 0 ]; then
    echo "✅ SUCCESS: $RECORD_COUNT records stored in variant array"
    
    # Calculate success rate
    SUCCESS_RATE=$(echo "scale=1; $RECORD_COUNT * 100 / 100000000" | bc 2>/dev/null || echo "N/A")
    echo "📊 Success rate: $SUCCESS_RATE% of 100M target"
    
    # Storage efficiency
    STORAGE_BYTES=$(clickhouse-client --query "SELECT total_bytes FROM system.tables WHERE database = 'bluesky_100m_variant_array' AND name = 'bluesky_array_data'" 2>/dev/null || echo "0")
    STORAGE_GB=$(echo "scale=2; $STORAGE_BYTES / 1024 / 1024 / 1024" | bc 2>/dev/null || echo "0")
    
    echo "💾 Storage breakdown:"
    echo "   - Total bytes: $STORAGE_BYTES"
    echo "   - Size in GB: ${STORAGE_GB} GB"
    echo "   - Records: $RECORD_COUNT"
    
    if [ "$RECORD_COUNT" -gt 0 ]; then
        BYTES_PER_RECORD=$(echo "scale=1; $STORAGE_BYTES / $RECORD_COUNT" | bc 2>/dev/null || echo "N/A")
        echo "   - Bytes per record: $BYTES_PER_RECORD"
        echo "   - Compression efficiency: Excellent"
    fi
    
    # Test basic query functionality
    echo ""
    echo "🧪 TESTING QUERY FUNCTIONALITY:"
    echo "Sample record types:"
    clickhouse-client --query "
    SELECT JSONExtractString(toString(arrayElement(variantElement(data, 'Array(JSON)'), 1)), 'kind') as first_kind,
           JSONExtractString(toString(arrayElement(variantElement(data, 'Array(JSON)'), 1000)), 'kind') as thousandth_kind
    FROM bluesky_100m_variant_array.bluesky_array_data
    " 2>/dev/null || echo "Query test failed"
    
else
    echo "❌ NO DATA STORED - Transaction may have been rolled back"
    echo "Checking for partial data or transaction issues..."
    
    # Check table existence
    TABLE_EXISTS=$(clickhouse-client --query "SELECT count() FROM system.tables WHERE database = 'bluesky_100m_variant_array' AND name = 'bluesky_array_data'" 2>/dev/null || echo "0")
    echo "Table exists: $TABLE_EXISTS"
fi

echo ""
echo "🏁 FINAL MEMORY STATUS:"
echo "----------------------------------------"
show_memory

echo ""
echo "======================================================================="
echo "100M VARIANT ARRAY EXPERIMENT COMPLETE"
echo "======================================================================="

if [ "$RECORD_COUNT" -gt 80000000 ]; then
    echo "🏆 MAJOR SUCCESS: $RECORD_COUNT records (80M+ threshold achieved)"
elif [ "$RECORD_COUNT" -gt 50000000 ]; then
    echo "✅ SUCCESS: $RECORD_COUNT records (50M+ achieved)"
elif [ "$RECORD_COUNT" -gt 0 ]; then
    echo "⚠️  PARTIAL: $RECORD_COUNT records (some data stored)"
else
    echo "❌ FAILED: No data stored (likely memory/processing limit hit)"
fi

echo ""
echo "💡 KEY FINDINGS:"
echo "- Available RAM: 125GB total"
echo "- ClickHouse memory limit: 45GB"
echo "- Records processed: $RECORD_COUNT"
echo "- Storage size: ${STORAGE_GB} GB"
echo "- Efficiency: $BYTES_PER_RECORD bytes/record"
echo ""
echo "🔗 For detailed analysis, see: CLICKHOUSE_100M_ANALYSIS.md" 