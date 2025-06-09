#!/bin/bash

# ClickHouse Variant Columns Benchmark Script
# This script preprocesses JSON data, loads variant columns, and compares performance

set -e

# Configuration
DATA_DIR="$HOME/data/bluesky"
WORK_DIR="./clickhouse"
INPUT_FILE="$DATA_DIR/file_0001.json.gz"
PREPROCESSED_FILE="$WORK_DIR/bluesky_1m_preprocessed.tsv"
JSON_BASELINE_FILE="$WORK_DIR/bluesky_1m_baseline.jsonl"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}ClickHouse Variant Columns Benchmark${NC}"
echo -e "${BLUE}========================================${NC}"
echo

# Check if ClickHouse is running
if ! clickhouse client --query "SELECT 1" > /dev/null 2>&1; then
    echo -e "${RED}Error: ClickHouse is not running or not accessible${NC}"
    echo "Please start ClickHouse and try again."
    exit 1
fi

# Check if input file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo -e "${RED}Error: Input file not found: $INPUT_FILE${NC}"
    echo "Please run download_data.sh first to download the dataset."
    exit 1
fi

# Step 1: Preprocess JSON to variant columns
echo -e "${YELLOW}Step 1: Preprocessing JSON data to variant columns...${NC}"
echo "Input: $INPUT_FILE"
echo "Output: $PREPROCESSED_FILE"
echo

if [ ! -f "$PREPROCESSED_FILE" ]; then
    python3 "$WORK_DIR/preprocess_json_to_variants.py" "$INPUT_FILE" "$PREPROCESSED_FILE"
    if [ $? -ne 0 ]; then
        echo -e "${RED}Error: Preprocessing failed${NC}"
        exit 1
    fi
    echo -e "${GREEN}Preprocessing completed successfully${NC}"
else
    echo -e "${BLUE}Preprocessed file already exists, skipping preprocessing${NC}"
fi

echo

# Step 2: Create JSON baseline for comparison
echo -e "${YELLOW}Step 2: Creating JSON baseline file...${NC}"
if [ ! -f "$JSON_BASELINE_FILE" ]; then
    gzip -dc "$INPUT_FILE" > "$JSON_BASELINE_FILE"
    echo -e "${GREEN}JSON baseline file created${NC}"
else
    echo -e "${BLUE}JSON baseline file already exists, skipping creation${NC}"
fi

echo

# Step 3: Create database schemas
echo -e "${YELLOW}Step 3: Creating ClickHouse schemas...${NC}"
clickhouse client --queries-file "$WORK_DIR/ddl_preprocessed_variants.sql"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}Schemas created successfully${NC}"
else
    echo -e "${RED}Error creating schemas${NC}"
    exit 1
fi

echo

# Step 4: Load data into all tables
echo -e "${YELLOW}Step 4: Loading data into tables...${NC}"

# Load preprocessed data
echo "Loading preprocessed variant data..."
clickhouse client --query "
    INSERT INTO bluesky_variants_test.bluesky_preprocessed
    FROM INFILE '$PREPROCESSED_FILE'
    FORMAT TSVWithNames
"

# Load pure variant data (subset of columns)
echo "Loading pure variant data..."
clickhouse client --query "
    INSERT INTO bluesky_variants_test.bluesky_pure_variants 
    SELECT 
        did, time_us, kind, timestamp_col,
        commit_rev, commit_operation, commit_collection,
        commit_rkey, commit_cid, record_type
    FROM bluesky_variants_test.bluesky_preprocessed
"

# Load JSON baseline
echo "Loading JSON baseline data..."
clickhouse client --query "
    INSERT INTO bluesky_variants_test.bluesky_json_baseline
    FROM INFILE '$JSON_BASELINE_FILE'
    FORMAT JSONEachRow
"

echo -e "${GREEN}All data loaded successfully${NC}"
echo

# Step 5: Verify data loading
echo -e "${YELLOW}Step 5: Verifying data loading...${NC}"

PREPROCESSED_COUNT=$(clickhouse client --query "SELECT count() FROM bluesky_variants_test.bluesky_preprocessed")
PURE_COUNT=$(clickhouse client --query "SELECT count() FROM bluesky_variants_test.bluesky_pure_variants")
JSON_COUNT=$(clickhouse client --query "SELECT count() FROM bluesky_variants_test.bluesky_json_baseline")

echo "Preprocessed variants table: $PREPROCESSED_COUNT records"
echo "Pure variants table: $PURE_COUNT records"
echo "JSON baseline table: $JSON_COUNT records"

if [ "$PREPROCESSED_COUNT" != "$PURE_COUNT" ] || [ "$PREPROCESSED_COUNT" != "$JSON_COUNT" ]; then
    echo -e "${RED}Warning: Record counts don't match between tables${NC}"
fi

echo

# Step 6: Run performance benchmarks
echo -e "${YELLOW}Step 6: Running performance benchmarks...${NC}"

# Function to extract queries and run benchmarks
run_query_benchmark() {
    local query_file=$1
    local description=$2
    
    echo -e "${BLUE}Testing: $description${NC}"
    
    # Extract individual queries (look for lines starting with SELECT)
    local query_num=1
    while IFS= read -r line; do
        if [[ $line =~ ^SELECT ]]; then
            echo -n "Q$query_num: "
            
            # Build complete query by reading until semicolon
            local full_query="$line"
            while [[ ! $full_query =~ \;$ ]]; do
                read -r next_line
                full_query="$full_query $next_line"
            done
            
            # Remove the semicolon for ClickHouse
            full_query="${full_query%;}"
            
            # Run query and measure time
            time_result=$(clickhouse client --time --query "$full_query" 2>&1 | grep "Elapsed:" | awk '{print $2}')
            echo "$time_result sec"
            
            ((query_num++))
        fi
    done < "$query_file"
    echo
}

# Alternative simple approach - run queries directly
run_simple_benchmark() {
    local query_file=$1
    local description=$2
    
    echo -e "${BLUE}Testing: $description${NC}"
    echo "Running all queries from: $query_file"
    
    # Run queries file directly and capture timing
    clickhouse client --time --queries-file "$query_file" > /dev/null 2>&1
    
    echo "Completed (check individual query times below)"
    echo
}

# Create baseline query file for JSON comparison
if [ ! -f "$WORK_DIR/queries_json_baseline.sql" ]; then
    # Modify original queries to use the baseline table
    sed 's/bluesky_1m\.bluesky/bluesky_variants_test.bluesky_json_baseline/g; s/j->/data->/g' "$WORK_DIR/queries.sql" > "$WORK_DIR/queries_json_baseline.sql"
fi

echo "=== JSON Object Baseline ==="
clickhouse client --time --queries-file "$WORK_DIR/queries_json_baseline.sql"

echo
echo "=== Preprocessed Variant Columns ==="
clickhouse client --time --queries-file "$WORK_DIR/queries_preprocessed_variants.sql"

echo
echo "=== Pure Variant Columns (No JSON) ==="
clickhouse client --time --queries-file "$WORK_DIR/queries_pure_variants.sql"

# Step 7: Storage analysis
echo
echo -e "${YELLOW}Step 7: Storage analysis...${NC}"

echo "Table sizes:"
clickhouse client --query "
    SELECT 
        table,
        formatReadableSize(total_bytes) as size,
        formatReadableQuantity(total_rows) as rows
    FROM system.tables 
    WHERE database = 'bluesky_variants_test'
    ORDER BY total_bytes DESC
"

echo
echo "Compression ratios:"
clickhouse client --query "
    SELECT 
        table,
        round(total_bytes / total_rows, 2) as bytes_per_row,
        round(100 - (total_bytes * 100.0 / sum(total_bytes) OVER ()), 1) as compression_vs_largest
    FROM system.tables 
    WHERE database = 'bluesky_variants_test'
    ORDER BY total_bytes DESC
"

echo
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Benchmark completed successfully!${NC}"
echo -e "${GREEN}========================================${NC}"
echo
echo "Summary:"
echo "- Preprocessed variant columns should show improved performance for simple aggregations"
echo "- Pure variant columns should show maximum performance by eliminating JSON entirely"
echo "- Storage analysis shows compression benefits of typed columns vs JSON"
echo
echo "Check the output above to compare query execution times between approaches." 