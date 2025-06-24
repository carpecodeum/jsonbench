#!/bin/bash

echo "======================================================================="
echo "CORRECTED DATA COMPREHENSIVE BENCHMARK"
echo "Testing all approaches with properly loaded data"
echo "10 runs per query, calculating averages"
echo "======================================================================="
echo

# Function to run query multiple times and calculate average
run_query_benchmark() {
    local query="$1"
    local approach="$2"
    local query_name="$3"
    local runs=10
    
    echo "Testing: $query_name ($approach)"
    echo "Query: $query"
    
    local total_time=0
    local times=()
    
    for i in $(seq 1 $runs); do
        echo -n "  Run $i/10... "
        
        # Capture timing from ClickHouse
        local result=$(clickhouse client --time --query "$query" --format Null 2>&1)
        local time=$(echo "$result" | grep "Elapsed:" | awk '{print $2}' | sed 's/s//')
        
        if [ -n "$time" ]; then
            times+=($time)
            total_time=$(echo "$total_time + $time" | bc -l)
            printf "%.4fs\n" "$time"
        else
            echo "FAILED"
            return 1
        fi
    done
    
    local avg_time=$(echo "scale=4; $total_time / $runs" | bc -l)
    printf "  Average: %.4fs\n" "$avg_time"
    echo
    
    # Store result for summary
    echo "$approach,$query_name,$avg_time" >> benchmark_results.csv
}

# Initialize results file
echo "Approach,Query,Average_Time_Seconds" > benchmark_results.csv

echo "======================================================================="
echo "1. JSON BASELINE APPROACH"
echo "======================================================================="

run_query_benchmark "SELECT data.kind, count() FROM bluesky_variants_test.bluesky_json_baseline GROUP BY data.kind ORDER BY count() DESC" "JSON_Baseline" "Count_by_Kind"

run_query_benchmark "SELECT data.kind, data.commit.collection, count() FROM bluesky_variants_test.bluesky_json_baseline WHERE data.commit.collection != '' GROUP BY data.kind, data.commit.collection ORDER BY count() DESC LIMIT 10" "JSON_Baseline" "Kind_Collection_Stats"

run_query_benchmark "SELECT count() FROM bluesky_variants_test.bluesky_json_baseline WHERE data.kind = 'commit'" "JSON_Baseline" "Filter_by_Kind"

run_query_benchmark "SELECT count() FROM bluesky_variants_test.bluesky_json_baseline WHERE data.time_us > 1700000000000000" "JSON_Baseline" "Time_Range_Filter"

run_query_benchmark "SELECT data.commit.operation, data.commit.collection, count() FROM bluesky_variants_test.bluesky_json_baseline WHERE data.commit.operation != '' AND data.commit.collection != '' GROUP BY data.commit.operation, data.commit.collection ORDER BY count() DESC LIMIT 5" "JSON_Baseline" "Complex_Grouping"

echo "======================================================================="
echo "2. MINIMAL VARIANT APPROACH"
echo "======================================================================="

run_query_benchmark "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') as kind, count() FROM bluesky_minimal_1m.bluesky_data GROUP BY kind ORDER BY count() DESC" "Minimal_Variant" "Count_by_Kind"

run_query_benchmark "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') as kind, JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') as collection, count() FROM bluesky_minimal_1m.bluesky_data WHERE collection != '' GROUP BY kind, collection ORDER BY count() DESC LIMIT 10" "Minimal_Variant" "Kind_Collection_Stats"

run_query_benchmark "SELECT count() FROM bluesky_minimal_1m.bluesky_data WHERE JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') = 'commit'" "Minimal_Variant" "Filter_by_Kind"

run_query_benchmark "SELECT count() FROM bluesky_minimal_1m.bluesky_data WHERE JSONExtractUInt(toString(variantElement(data, 'JSON')), 'time_us') > 1700000000000000" "Minimal_Variant" "Time_Range_Filter"

run_query_benchmark "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'operation') as op, JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') as coll, count() FROM bluesky_minimal_1m.bluesky_data WHERE op != '' AND coll != '' GROUP BY op, coll ORDER BY count() DESC LIMIT 5" "Minimal_Variant" "Complex_Grouping"

echo "======================================================================="
echo "3. PREPROCESSED VARIANTS APPROACH"
echo "======================================================================="

run_query_benchmark "SELECT kind, count() FROM bluesky_variants_test.bluesky_preprocessed GROUP BY kind ORDER BY count() DESC" "Preprocessed_Variants" "Count_by_Kind"

run_query_benchmark "SELECT kind, commit_collection, count() FROM bluesky_variants_test.bluesky_preprocessed WHERE commit_collection != '' GROUP BY kind, commit_collection ORDER BY count() DESC LIMIT 10" "Preprocessed_Variants" "Kind_Collection_Stats"

run_query_benchmark "SELECT count() FROM bluesky_variants_test.bluesky_preprocessed WHERE kind = 'commit'" "Preprocessed_Variants" "Filter_by_Kind"

run_query_benchmark "SELECT count() FROM bluesky_variants_test.bluesky_preprocessed WHERE time_us > 1700000000000000" "Preprocessed_Variants" "Time_Range_Filter"

run_query_benchmark "SELECT commit_operation, commit_collection, count() FROM bluesky_variants_test.bluesky_preprocessed WHERE commit_operation != '' AND commit_collection != '' GROUP BY commit_operation, commit_collection ORDER BY count() DESC LIMIT 5" "Preprocessed_Variants" "Complex_Grouping"

echo "======================================================================="
echo "4. PURE VARIANTS APPROACH"
echo "======================================================================="

run_query_benchmark "SELECT kind, count() FROM bluesky_variants_test.bluesky_pure_variants GROUP BY kind ORDER BY count() DESC" "Pure_Variants" "Count_by_Kind"

run_query_benchmark "SELECT kind, commit_collection, count() FROM bluesky_variants_test.bluesky_pure_variants WHERE commit_collection != '' GROUP BY kind, commit_collection ORDER BY count() DESC LIMIT 10" "Pure_Variants" "Kind_Collection_Stats"

run_query_benchmark "SELECT count() FROM bluesky_variants_test.bluesky_pure_variants WHERE kind = 'commit'" "Pure_Variants" "Filter_by_Kind"

run_query_benchmark "SELECT count() FROM bluesky_variants_test.bluesky_pure_variants WHERE time_us > 1700000000000000" "Pure_Variants" "Time_Range_Filter"

run_query_benchmark "SELECT commit_operation, commit_collection, count() FROM bluesky_variants_test.bluesky_pure_variants WHERE commit_operation != '' AND commit_collection != '' GROUP BY commit_operation, commit_collection ORDER BY count() DESC LIMIT 5" "Pure_Variants" "Complex_Grouping"

echo "======================================================================="
echo "STORAGE COMPARISON"
echo "======================================================================="

clickhouse client --query "
SELECT 
    CASE 
        WHEN database = 'bluesky_variants_test' AND table = 'bluesky_json_baseline' THEN 'JSON_Baseline'
        WHEN database = 'bluesky_minimal_1m' AND table = 'bluesky_data' THEN 'Minimal_Variant'
        WHEN database = 'bluesky_variants_test' AND table = 'bluesky_preprocessed' THEN 'Preprocessed_Variants'
        WHEN database = 'bluesky_variants_test' AND table = 'bluesky_pure_variants' THEN 'Pure_Variants'
    END as approach,
    formatReadableSize(sum(bytes_on_disk)) as storage_size,
    formatReadableQuantity(sum(rows)) as records
FROM system.parts 
WHERE ((database = 'bluesky_variants_test' AND table IN ('bluesky_json_baseline', 'bluesky_preprocessed', 'bluesky_pure_variants')) 
       OR (database = 'bluesky_minimal_1m' AND table = 'bluesky_data'))
  AND active = 1
GROUP BY database, table
ORDER BY sum(bytes_on_disk) ASC
"

echo
echo "======================================================================="
echo "PERFORMANCE SUMMARY"
echo "======================================================================="

# Generate summary report
echo "Average query times by approach:"
echo
awk -F',' 'NR>1 {sum[$1]+=$3; count[$1]++} END {for(i in sum) printf "%-20s: %.4f seconds\n", i, sum[i]/count[i]}' benchmark_results.csv | sort -k3 -n

echo
echo "Detailed results saved to: benchmark_results.csv"
echo "Benchmark completed at: $(date)" 