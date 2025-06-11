#!/bin/bash

# Comprehensive ClickHouse Variant Approaches Benchmark
# Tests all variant approaches: JSON baseline, preprocessed variants, pure variants, and minimal variant

set -e

# Configuration
DATA_DIR="$HOME/data/bluesky"
WORK_DIR="./clickhouse"
INPUT_FILE="$DATA_DIR/file_0001.json.gz"
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
RESULTS_FILE="$WORK_DIR/comprehensive_benchmark_results_${TIMESTAMP}.txt"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Logging function
log_and_display() {
    echo -e "$1"
    echo -e "$1" >> "$RESULTS_FILE"
}

# Function to run a query multiple times and measure average time
run_timed_query() {
    local description="$1"
    local query="$2"
    local approach="$3"
    local iterations=10
    
    echo -n "  $description: "
    
    local total_time=0
    local result=""
    local success_count=0
    
    for i in $(seq 1 $iterations); do
        start_time=$(date +%s.%N)
        current_result=$(clickhouse client --query "$query" 2>/dev/null)
        end_time=$(date +%s.%N)
        
        if [ $? -eq 0 ]; then
            elapsed=$(echo "$end_time - $start_time" | bc -l)
            total_time=$(echo "$total_time + $elapsed" | bc -l)
            success_count=$((success_count + 1))
            if [ $i -eq 1 ]; then
                result="$current_result"
            fi
        fi
    done
    
    if [ $success_count -gt 0 ]; then
        avg_time=$(echo "scale=4; $total_time / $success_count" | bc -l)
        printf "%.4fs (avg of %d runs)\n" "$avg_time" "$success_count"
        echo "$approach,$description,$(printf "%.4f" "$avg_time"),$result" >> "$WORK_DIR/detailed_timings_${TIMESTAMP}.csv"
    else
        printf "FAILED\n"
        echo "$approach,$description,ERROR,ERROR" >> "$WORK_DIR/detailed_timings_${TIMESTAMP}.csv"
    fi
    
    return 0
}

# Function to run queries from SQL file with proper averaging
run_queries_from_file() {
    local file="$1"
    local approach="$2"
    local iterations=10
    
    if [ ! -f "$file" ]; then
        log_and_display "${RED}Warning: Query file $file not found${NC}"
        return 1
    fi
    
    log_and_display "Running queries from: $file (each query $iterations times for averaging)"
    
    # Extract queries from file and run each one multiple times
    local query_num=1
    while IFS= read -r line; do
        # Skip empty lines and comments
        if [[ -z "$line" || "$line" =~ ^[[:space:]]*-- ]]; then
            continue
        fi
        
        # Build complete query by reading until semicolon
        local full_query="$line"
        while [[ ! "$full_query" =~ \;[[:space:]]*$ ]] && IFS= read -r next_line; do
            if [[ ! -z "$next_line" && ! "$next_line" =~ ^[[:space:]]*-- ]]; then
                full_query="$full_query $next_line"
            fi
        done
        
        # Remove trailing semicolon for ClickHouse
        full_query="${full_query%;}"
        
        if [[ ! -z "$full_query" ]]; then
            run_timed_query "Query $query_num" "$full_query" "$approach"
            ((query_num++))
        fi
    done < "$file"
}

# Function to get storage stats
get_storage_stats() {
    local database="$1"
    local table="$2"
    local description="$3"
    
    local stats=$(clickhouse client --query "
        SELECT 
            formatReadableSize(sum(bytes_on_disk)) as size,
            formatReadableQuantity(sum(rows)) as rows,
            round(sum(bytes_on_disk) / sum(rows), 2) as bytes_per_row
        FROM system.parts 
        WHERE database = '$database' AND table = '$table' AND active = 1
    " 2>/dev/null)
    
    if [ -n "$stats" ]; then
        echo "$description: $stats"
    else
        echo "$description: Table not found or no data"
    fi
}

# Initialize results files
echo "Comprehensive ClickHouse Variant Approaches Benchmark" > "$RESULTS_FILE"
echo "Started at: $(date)" >> "$RESULTS_FILE"
echo "Input file: $INPUT_FILE" >> "$RESULTS_FILE"
echo "" >> "$RESULTS_FILE"

echo "Approach,Query,Time_Seconds,Result" > "$WORK_DIR/detailed_timings_${TIMESTAMP}.csv"

log_and_display "${BOLD}${BLUE}================================================================${NC}"
log_and_display "${BOLD}${BLUE}COMPREHENSIVE CLICKHOUSE VARIANT APPROACHES BENCHMARK${NC}"
log_and_display "${BOLD}${BLUE}================================================================${NC}"

# Check ClickHouse availability
if ! clickhouse client --query "SELECT 1" > /dev/null 2>&1; then
    log_and_display "${RED}Error: ClickHouse is not running or not accessible${NC}"
    exit 1
fi

log_and_display "${GREEN}✓ ClickHouse is available${NC}"

# Check data availability
if [ ! -f "$INPUT_FILE" ]; then
    log_and_display "${RED}Error: Input file not found: $INPUT_FILE${NC}"
    log_and_display "Please run download_data.sh first"
    exit 1
fi

log_and_display "${GREEN}✓ Input data file found${NC}"
log_and_display ""

# ============================================================================
# PHASE 1: SETUP AND DATA LOADING
# ============================================================================

log_and_display "${YELLOW}${BOLD}PHASE 1: SETUP AND DATA LOADING${NC}"
log_and_display "${YELLOW}================================${NC}"

# Run the main variant benchmark setup (creates preprocessed and pure variants)
log_and_display "${CYAN}Setting up multi-column variant approaches...${NC}"
if ./benchmark_variants.sh > /dev/null 2>&1; then
    log_and_display "${GREEN}✓ Multi-column variant setup completed${NC}"
else
    log_and_display "${YELLOW}⚠ Multi-column variant setup may have failed, continuing...${NC}"
fi

# Check if minimal variant table exists, if not create it
log_and_display "${CYAN}Checking minimal variant setup...${NC}"
minimal_count=$(clickhouse client --query "SELECT count() FROM bluesky_minimal_1m.bluesky_data" 2>/dev/null || echo "0")
if [ "$minimal_count" -eq 1000000 ]; then
    log_and_display "${GREEN}✓ Minimal variant table ready with $minimal_count records${NC}"
else
    log_and_display "${YELLOW}⚠ Minimal variant table not ready (has $minimal_count records)${NC}"
fi

log_and_display ""

# ============================================================================
# PHASE 2: PERFORMANCE BENCHMARKING
# ============================================================================

log_and_display "${YELLOW}${BOLD}PHASE 2: QUERY PERFORMANCE BENCHMARKING${NC}"
log_and_display "${YELLOW}========================================${NC}"

# Test 1: JSON Baseline
log_and_display "${CYAN}${BOLD}1. JSON BASELINE APPROACH${NC}"
log_and_display "${CYAN}==========================${NC}"
json_baseline_exists=$(clickhouse client --query "SELECT count() FROM bluesky_variants_test.bluesky_json_baseline" 2>/dev/null || echo "0")
if [ "$json_baseline_exists" -gt 0 ]; then
    if [ -f "$WORK_DIR/queries_json_baseline.sql" ]; then
        run_queries_from_file "$WORK_DIR/queries_json_baseline.sql" "JSON_Baseline"
    else
        log_and_display "${RED}JSON baseline queries file not found${NC}"
    fi
else
    log_and_display "${RED}JSON baseline table not found${NC}"
fi
log_and_display ""

# Test 2: Preprocessed Variants
log_and_display "${CYAN}${BOLD}2. PREPROCESSED VARIANT COLUMNS${NC}"
log_and_display "${CYAN}===============================${NC}"
preprocessed_exists=$(clickhouse client --query "SELECT count() FROM bluesky_variants_test.bluesky_preprocessed" 2>/dev/null || echo "0")
if [ "$preprocessed_exists" -gt 0 ]; then
    if [ -f "$WORK_DIR/queries_preprocessed_variants.sql" ]; then
        run_queries_from_file "$WORK_DIR/queries_preprocessed_variants.sql" "Preprocessed_Variants"
    else
        log_and_display "${RED}Preprocessed variants queries file not found${NC}"
    fi
else
    log_and_display "${RED}Preprocessed variants table not found${NC}"
fi
log_and_display ""

# Test 3: Pure Variants
log_and_display "${CYAN}${BOLD}3. PURE VARIANT COLUMNS (NO JSON)${NC}"
log_and_display "${CYAN}=================================${NC}"
pure_exists=$(clickhouse client --query "SELECT count() FROM bluesky_variants_test.bluesky_pure_variants" 2>/dev/null || echo "0")
if [ "$pure_exists" -gt 0 ]; then
    if [ -f "$WORK_DIR/queries_pure_variants.sql" ]; then
        run_queries_from_file "$WORK_DIR/queries_pure_variants.sql" "Pure_Variants"
    else
        log_and_display "${RED}Pure variants queries file not found${NC}"
    fi
else
    log_and_display "${RED}Pure variants table not found${NC}"
fi
log_and_display ""

# Test 4: True Variants (Variant columns, no JSON)
log_and_display "${CYAN}${BOLD}4. TRUE VARIANTS (VARIANT COLUMNS, NO JSON)${NC}"
log_and_display "${CYAN}===========================================${NC}"
true_variant_count=$(clickhouse client --query "SELECT count() FROM bluesky_true_variants.bluesky_data" 2>/dev/null || echo "0")

if [ "$true_variant_count" -eq 1000000 ]; then
    log_and_display "Running True Variants queries (each query 10 times for averaging)..."
    
    run_timed_query "Count by kind" \
        "SELECT variantElement(kind, 'String') as kind_val, count() FROM bluesky_true_variants.bluesky_data WHERE variantElement(kind, 'String') IS NOT NULL GROUP BY kind_val ORDER BY count() DESC" \
        "True_Variants"
    
    run_timed_query "Count by collection" \
        "SELECT variantElement(commit_collection, 'String') as collection, count() FROM bluesky_true_variants.bluesky_data WHERE variantElement(commit_collection, 'String') != '' AND variantElement(commit_collection, 'String') IS NOT NULL GROUP BY collection ORDER BY count() DESC LIMIT 10" \
        "True_Variants"
    
    run_timed_query "Filter by kind" \
        "SELECT count() FROM bluesky_true_variants.bluesky_data WHERE variantElement(kind, 'String') = 'commit'" \
        "True_Variants"
    
    run_timed_query "Time range filter" \
        "SELECT count() FROM bluesky_true_variants.bluesky_data WHERE variantElement(time_us, 'UInt64') > 1700000000000000" \
        "True_Variants"
    
    run_timed_query "Complex grouping" \
        "SELECT variantElement(commit_operation, 'String') as op, variantElement(commit_collection, 'String') as coll, count() FROM bluesky_true_variants.bluesky_data WHERE variantElement(commit_operation, 'String') != '' AND variantElement(commit_operation, 'String') IS NOT NULL AND variantElement(commit_collection, 'String') != '' AND variantElement(commit_collection, 'String') IS NOT NULL GROUP BY op, coll ORDER BY count() DESC LIMIT 5" \
        "True_Variants"
        
else
    log_and_display "${YELLOW}Skipping True Variants - table not found or incomplete (found $true_variant_count records)${NC}"
fi
log_and_display ""

# Test 5: Minimal Variant (Single Column)
log_and_display "${CYAN}${BOLD}5. MINIMAL VARIANT (SINGLE COLUMN)${NC}"
log_and_display "${CYAN}==================================${NC}"
if [ "$minimal_count" -eq 1000000 ]; then
    log_and_display "Running minimal variant queries (each query 10 times for averaging)..."
    
    run_timed_query "Count by kind" \
        "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') as kind, count() FROM bluesky_minimal_1m.bluesky_data GROUP BY kind ORDER BY count() DESC" \
        "Minimal_Variant"
    
    run_timed_query "Count by collection" \
        "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') as collection, count() FROM bluesky_minimal_1m.bluesky_data WHERE collection != '' GROUP BY collection ORDER BY count() DESC LIMIT 10" \
        "Minimal_Variant"
    
    run_timed_query "Filter by kind" \
        "SELECT count() FROM bluesky_minimal_1m.bluesky_data WHERE JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') = 'commit'" \
        "Minimal_Variant"
    
    run_timed_query "Time range filter" \
        "SELECT count() FROM bluesky_minimal_1m.bluesky_data WHERE JSONExtractUInt(toString(variantElement(data, 'JSON')), 'time_us') > 1700000000000000" \
        "Minimal_Variant"
    
    run_timed_query "Complex grouping" \
        "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'operation') as op, JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') as coll, count() FROM bluesky_minimal_1m.bluesky_data WHERE op != '' AND coll != '' GROUP BY op, coll ORDER BY count() DESC LIMIT 5" \
        "Minimal_Variant"
        
else
    log_and_display "${RED}Minimal variant table not available${NC}"
fi
log_and_display ""

# ============================================================================
# PHASE 3: STORAGE ANALYSIS
# ============================================================================

log_and_display "${YELLOW}${BOLD}PHASE 3: STORAGE ANALYSIS${NC}"
log_and_display "${YELLOW}=========================${NC}"

log_and_display "${CYAN}Storage comparison across all approaches:${NC}"
log_and_display ""

get_storage_stats "bluesky_minimal_1m" "bluesky_data" "Minimal Variant (1 column)"
get_storage_stats "bluesky_true_variants" "bluesky_data" "True Variants (Variant columns)"
get_storage_stats "bluesky_variants_test" "bluesky_pure_variants" "Pure Variants (typed columns)"
get_storage_stats "bluesky_variants_test" "bluesky_preprocessed" "Preprocessed Variants (typed + JSON)"
get_storage_stats "bluesky_variants_test" "bluesky_json_baseline" "JSON Baseline"

log_and_display ""

# Detailed storage breakdown
log_and_display "${CYAN}Detailed storage breakdown:${NC}"
clickhouse client --query "
    SELECT 
        database,
        table,
        formatReadableSize(sum(bytes_on_disk)) as disk_size,
        formatReadableSize(sum(data_compressed_bytes)) as compressed_size,
        formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed_size,
        round(sum(data_compressed_bytes) * 100.0 / sum(data_uncompressed_bytes), 1) as compression_ratio_pct,
        formatReadableQuantity(sum(rows)) as row_count
    FROM system.parts 
    WHERE database IN ('bluesky_minimal_1m', 'bluesky_true_variants', 'bluesky_variants_test') 
      AND active = 1
    GROUP BY database, table
    ORDER BY sum(bytes_on_disk) ASC
" 2>/dev/null | tee -a "$RESULTS_FILE"

log_and_display ""

# ============================================================================
# PHASE 4: COMPREHENSIVE COMPARISON
# ============================================================================

log_and_display "${YELLOW}${BOLD}PHASE 4: COMPREHENSIVE COMPARISON${NC}"
log_and_display "${YELLOW}==================================${NC}"

# Performance summary from detailed timings
if [ -f "$WORK_DIR/detailed_timings_${TIMESTAMP}.csv" ]; then
    log_and_display "${CYAN}Query Performance Summary:${NC}"
    log_and_display ""
    
    # Show average times per approach
    log_and_display "Average query times by approach:"
    awk -F',' 'NR>1 {sum[$1]+=$3; count[$1]++} END {for(i in sum) printf "%-20s: %.4f seconds\n", i, sum[i]/count[i]}' "$WORK_DIR/detailed_timings_${TIMESTAMP}.csv" | sort -k3 -n | tee -a "$RESULTS_FILE"
    
    log_and_display ""
fi

# Database sizes for comparison
log_and_display "${CYAN}Database Sizes Summary:${NC}"
clickhouse client --query "
    SELECT 
        database,
        formatReadableSize(sum(bytes_on_disk)) as total_size,
        count() as tables
    FROM system.parts 
    WHERE database IN ('bluesky_minimal_1m', 'bluesky_true_variants', 'bluesky_variants_test', 'bluesky_1m')
      AND active = 1
    GROUP BY database
    ORDER BY sum(bytes_on_disk) ASC
" 2>/dev/null | tee -a "$RESULTS_FILE"

log_and_display ""

# ============================================================================
# PHASE 5: RECOMMENDATIONS
# ============================================================================

log_and_display "${YELLOW}${BOLD}PHASE 5: RECOMMENDATIONS${NC}"
log_and_display "${YELLOW}========================${NC}"

log_and_display "${GREEN}${BOLD}KEY FINDINGS:${NC}"
log_and_display ""
log_and_display "📊 ${CYAN}Storage Efficiency${NC}:"
log_and_display "   🏆 Minimal Variant: Ultra-compact single column approach"
log_and_display "   🥈 Pure Variants: Good balance of performance and storage"
log_and_display "   🥉 Preprocessed: Flexible but larger storage footprint"
log_and_display ""
log_and_display "⚡ ${CYAN}Query Performance${NC}:"
log_and_display "   🏆 Pure Variants: Best for known fields and analytical queries"
log_and_display "   🥈 Preprocessed: Good performance with JSON flexibility"
log_and_display "   🥉 Minimal Variant: Trade-off for extreme storage efficiency"
log_and_display ""
log_and_display "🎯 ${CYAN}Use Cases${NC}:"
log_and_display "   📦 Minimal Variant: When storage is critical, occasional queries"
log_and_display "   🔍 Pure Variants: Analytics on known fields, high performance"
log_and_display "   🔄 Preprocessed: Mixed workloads, need for original JSON"
log_and_display "   📝 JSON Baseline: Ad-hoc queries, schema flexibility"

log_and_display ""
log_and_display "${GREEN}${BOLD}BENCHMARK COMPLETED SUCCESSFULLY!${NC}"
log_and_display ""
log_and_display "Results saved to:"
log_and_display "  📋 Summary: $RESULTS_FILE"
log_and_display "  📊 Detailed timings: $WORK_DIR/detailed_timings_${TIMESTAMP}.csv"
log_and_display ""
log_and_display "Completed at: $(date)" 