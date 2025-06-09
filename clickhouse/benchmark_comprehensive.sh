#!/bin/bash

echo "=========================================="
echo "Comprehensive ClickHouse Benchmark"
echo "Testing 4 Approaches:"
echo "1. JSON Baseline (JSON Object type)"
echo "2. Typed Columns (Extracted fields + JSON)"
echo "3. Pure Variants (Typed fields only)" 
echo "4. True Variant Columns (ClickHouse Variant type)"
echo "=========================================="

# Check ClickHouse is running
if ! clickhouse client --query "SELECT 1" > /dev/null 2>&1; then
    echo "Error: ClickHouse is not running or not accessible"
    exit 1
fi

# Function to run a query and measure time
run_query() {
    local approach=$1
    local query_num=$2
    local query_file=$3
    local output_file="benchmark_results_$(date +%Y%m%d_%H%M%S).txt"
    
    echo -n "  Q${query_num}: "
    
    # Extract the specific query (each query is separated by semicolon)
    query=$(sed -n "${query_num}p" <<< "$(grep -v '^--' "$query_file" | tr '\n' ' ' | sed 's/;/\n/g' | sed '/^\s*$/d')")
    
    if [ -z "$query" ]; then
        echo "ERROR - Query $query_num not found"
        return
    fi
    
    # Run query 3 times and take the best time
    best_time=""
    for i in {1..3}; do
        start_time=$(date +%s.%N)
        clickhouse client --query "$query" > /dev/null 2>&1
        if [ $? -eq 0 ]; then
            end_time=$(date +%s.%N)
            elapsed=$(echo "$end_time - $start_time" | bc -l)
            if [ -z "$best_time" ] || [ $(echo "$elapsed < $best_time" | bc -l) -eq 1 ]; then
                best_time=$elapsed
            fi
        else
            echo "ERROR - Query failed"
            return
        fi
    done
    
    printf "%.3fs\n" "$best_time"
    echo "$approach,Q$query_num,$best_time" >> "$output_file"
}

# Function to get table size
get_table_size() {
    local database=$1
    local table=$2
    
    size_bytes=$(clickhouse client --query "
        SELECT formatReadableSize(sum(bytes_on_disk)) as size
        FROM system.parts 
        WHERE database = '$database' AND table = '$table' AND active = 1
    " 2>/dev/null)
    
    if [ $? -eq 0 ] && [ -n "$size_bytes" ]; then
        echo "$size_bytes"
    else
        echo "Unknown"
    fi
}

# Function to get record count
get_record_count() {
    local database=$1
    local table=$2
    
    count=$(clickhouse client --query "SELECT count() FROM $database.$table" 2>/dev/null)
    if [ $? -eq 0 ] && [ -n "$count" ]; then
        echo "$count"
    else
        echo "0"
    fi
}

echo ""
echo "Checking table status..."

# JSON Baseline
json_count=$(get_record_count "bluesky_variants_test" "bluesky_json_baseline")
json_size=$(get_table_size "bluesky_variants_test" "bluesky_json_baseline")
echo "JSON Baseline: $json_count records, $json_size"

# Typed Columns 
typed_count=$(get_record_count "bluesky_variants_test" "bluesky_preprocessed")
typed_size=$(get_table_size "bluesky_variants_test" "bluesky_preprocessed")
echo "Typed Columns: $typed_count records, $typed_size"

# Pure Variants
pure_count=$(get_record_count "bluesky_variants_test" "bluesky_pure_variants")
pure_size=$(get_table_size "bluesky_variants_test" "bluesky_pure_variants")
echo "Pure Variants: $pure_count records, $pure_size"

# True Variants
true_count=$(get_record_count "bluesky_true_variants" "bluesky_data")
true_size=$(get_table_size "bluesky_true_variants" "bluesky_data")
echo "True Variants: $true_count records, $true_size"

echo ""
echo "Running benchmarks..."
echo ""

# 1. JSON Baseline
echo "1. JSON Baseline (JSON Object type):"
if [ -f "queries_json_baseline.sql" ] && [ "$json_count" -gt 0 ]; then
    for i in {1..5}; do
        run_query "JSON_Baseline" "$i" "queries_json_baseline.sql"
    done
else
    echo "  Skipping - queries file missing or no data"
fi

echo ""

# 2. Typed Columns (Preprocessed)
echo "2. Typed Columns (Extracted fields + JSON fallback):"
if [ -f "queries_preprocessed_variants.sql" ] && [ "$typed_count" -gt 0 ]; then
    for i in {1..5}; do
        run_query "Typed_Columns" "$i" "queries_preprocessed_variants.sql"
    done
else
    echo "  Skipping - queries file missing or no data"
fi

echo ""

# 3. Pure Variants (Typed only)
echo "3. Pure Variants (Typed fields only):"
if [ -f "queries_pure_variants.sql" ] && [ "$pure_count" -gt 0 ]; then
    for i in {1..5}; do
        run_query "Pure_Variants" "$i" "queries_pure_variants.sql"
    done
else
    echo "  Skipping - queries file missing or no data"
fi

echo ""

# 4. True Variant Columns
echo "4. True Variant Columns (ClickHouse Variant type):"
if [ -f "queries_true_variants.sql" ] && [ "$true_count" -gt 0 ]; then
    for i in {1..5}; do
        run_query "True_Variants" "$i" "queries_true_variants.sql"
    done
else
    echo "  Skipping - queries file missing or no data"
fi

echo ""
echo "=========================================="
echo "Benchmark completed!"
echo ""
echo "Summary:"
echo "JSON Baseline: $json_count records ($json_size)"
echo "Typed Columns: $typed_count records ($typed_size)" 
echo "Pure Variants: $pure_count records ($pure_size)"
echo "True Variants: $true_count records ($true_size)"
echo ""
echo "Results have been saved to benchmark_results_*.txt"
echo "==========================================" 