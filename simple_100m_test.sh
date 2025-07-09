#!/bin/bash

# Simple 100M variant array test with detailed size monitoring
echo "🚀 100M VARIANT ARRAY - DIRECT APPROACH"
echo "Memory available: 116GB | Target: 100M records"

DATA_DIR="$HOME/data/bluesky"
FILE_COUNT=$(find "$DATA_DIR" -name "file_*.json.gz" | wc -l)
echo "Data files available: $FILE_COUNT"

echo ""
echo "📊 Starting data streaming to ClickHouse..."

# Start timing
start_time=$(date +%s)

# Create the massive JSON array and pipe to ClickHouse
{
    echo '{"data":['
    
    first_record=true
    total_records=0
    file_num=0
    
    for file in "$DATA_DIR"/file_*.json.gz; do
        if [ -f "$file" ]; then
            file_num=$((file_num + 1))
            echo "File $file_num/$FILE_COUNT: $(basename "$file")" >&2
            
            while IFS= read -r line; do
                if [ -n "$line" ]; then
                    if [ "$first_record" = true ]; then
                        first_record=false
                    else
                        echo ","
                    fi
                    echo "$line"
                    total_records=$((total_records + 1))
                    
                    # Progress every million
                    if [ $((total_records % 1000000)) -eq 0 ]; then
                        echo "  ✓ $total_records records streamed" >&2
                    fi
                fi
            done < <(zcat "$file")
            
            # Memory check every 20 files
            if [ $((file_num % 20)) -eq 0 ]; then
                echo "  Memory after $file_num files:" >&2
                free -h | grep Mem: >&2
            fi
        fi
    done
    
    echo ']}'
    echo "✅ Total records streamed: $total_records" >&2
    
} | TZ=UTC clickhouse-client \
    --max_memory_usage=40000000000 \
    --max_bytes_before_external_group_by=15000000000 \
    --max_bytes_before_external_sort=15000000000 \
    --query "INSERT INTO bluesky_100m_variant_array.bluesky_array_data FORMAT JSONEachRow"

insert_result=$?
end_time=$(date +%s)
duration=$((end_time - start_time))

echo ""
echo "⏱️  Processing completed in $duration seconds"
echo "Insert exit code: $insert_result"

# Wait for ClickHouse to finalize
echo "⏳ Waiting for ClickHouse to finalize..."
sleep 15

echo ""
echo "📏 ARRAY SIZE ANALYSIS:"
echo "======================="

# Check basic stats
echo "1. Row count:"
TZ=UTC clickhouse-client --query "SELECT count() FROM bluesky_100m_variant_array.bluesky_array_data" 2>/dev/null || echo "0"

echo ""
echo "2. Array length (JSON objects):"
ARRAY_LENGTH=$(TZ=UTC clickhouse-client --query "SELECT length(variantElement(data, 'Array(JSON)')) FROM bluesky_100m_variant_array.bluesky_array_data" 2>/dev/null || echo "0")
echo "$ARRAY_LENGTH"

echo ""
echo "3. Storage size in bytes:"
STORAGE_BYTES=$(TZ=UTC clickhouse-client --query "SELECT total_bytes FROM system.tables WHERE database = 'bluesky_100m_variant_array' AND name = 'bluesky_array_data'" 2>/dev/null || echo "0")
echo "$STORAGE_BYTES"

echo ""
echo "4. Human-readable storage size:"
TZ=UTC clickhouse-client --query "SELECT formatReadableSize(total_bytes) FROM system.tables WHERE database = 'bluesky_100m_variant_array' AND name = 'bluesky_array_data'" 2>/dev/null || echo "0 B"

echo ""
echo "5. Storage efficiency (bytes per JSON record):"
if [ "$ARRAY_LENGTH" -gt 0 ] && [ "$STORAGE_BYTES" -gt 0 ]; then
    EFFICIENCY=$(echo "scale=1; $STORAGE_BYTES / $ARRAY_LENGTH" | bc)
    echo "$EFFICIENCY bytes per record"
else
    echo "Cannot calculate (no data stored)"
fi

echo ""
echo "6. Success rate:"
if [ "$ARRAY_LENGTH" -gt 0 ]; then
    SUCCESS_RATE=$(echo "scale=1; $ARRAY_LENGTH * 100 / 100000000" | bc)
    echo "$SUCCESS_RATE% of 100M target"
    
    if [ "$ARRAY_LENGTH" -ge 80000000 ]; then
        echo "🏆 EXCELLENT: 80M+ records achieved!"
    elif [ "$ARRAY_LENGTH" -ge 50000000 ]; then
        echo "✅ GOOD: 50M+ records achieved!"
    else
        echo "⚠️  PARTIAL: $(echo $ARRAY_LENGTH | cut -c1-2)M records achieved"
    fi
else
    echo "❌ FAILED: No data stored"
fi

echo ""
echo "🧪 QUERY FUNCTIONALITY TEST:"
echo "Sample data access:"
TZ=UTC clickhouse-client --query "
SELECT 'First record kind: ' || JSONExtractString(toString(arrayElement(variantElement(data, 'Array(JSON)'), 1)), 'kind') as test
FROM bluesky_100m_variant_array.bluesky_array_data
" 2>/dev/null || echo "Query failed"

echo ""
echo "🎯 FINAL SUMMARY:"
echo "================="
echo "• Records stored: $ARRAY_LENGTH"
echo "• Storage size: $STORAGE_BYTES bytes"
echo "• Storage size (GB): $(echo "scale=2; $STORAGE_BYTES / 1024 / 1024 / 1024" | bc) GB"
echo "• Processing time: $duration seconds"
echo "• Memory limit used: 40GB"
echo "• Available system RAM: 116GB"

if [ "$ARRAY_LENGTH" -gt 0 ]; then
    echo "✅ VARIANT ARRAY SUCCESSFULLY CREATED!"
else
    echo "❌ VARIANT ARRAY CREATION FAILED"
fi 