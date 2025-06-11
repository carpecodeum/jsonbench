#!/usr/bin/env python3
"""
Single Variant Column Loader for ClickHouse
Implements the approach suggested by the user - entire JSON as one variant column.
"""

import subprocess
import sys
import os
from pathlib import Path

def run_clickhouse_query(query: str, use_local: bool = True):
    """Execute a ClickHouse query using local mode or client."""
    cmd = ['clickhouse', 'local'] if use_local else ['clickhouse', 'client']
    cmd.extend(['--query', query])
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Query failed: {result.stderr}")
        return None
    return result.stdout.strip()

def create_single_variant_schema(use_local: bool = True):
    """Create the single variant schema - entire JSON as one variant column."""
    
    schema_sql = """
-- Create database
CREATE DATABASE IF NOT EXISTS bluesky_single_variant;

-- Create single variant table - ENTIRE JSON as one variant column
CREATE TABLE bluesky_single_variant.bluesky_data
(
    -- Minimal core fields for ordering/partitioning only
    did String,
    time_us UInt64,
    kind LowCardinality(String),
    timestamp_col DateTime64(6),
    
    -- SINGLE variant column containing entire JSON
    data Variant(JSON)
)
ENGINE = MergeTree
ORDER BY (kind, did, timestamp_col)
SETTINGS 
    allow_experimental_variant_type = 1,
    use_variant_as_common_type = 1;
"""

    print("Creating single variant schema...")
    result = run_clickhouse_query(schema_sql, use_local)
    if result is not None:
        print("✓ Single variant schema created successfully")
        return True
    else:
        print("✗ Single variant schema creation failed")
        return False

def load_data_single_variant(source_db: str = "bluesky_1m", source_table: str = "bluesky", 
                           max_records: int = None, use_local: bool = True):
    """Load data using single variant column approach."""
    
    limit_clause = f"LIMIT {max_records}" if max_records else ""
    
    insert_sql = f"""
INSERT INTO bluesky_single_variant.bluesky_data
SELECT 
    -- Extract minimal core fields for indexing/ordering
    data.did::String as did,
    data.time_us::UInt64 as time_us,
    data.kind::String as kind,
    fromUnixTimestamp64Micro(data.time_us) as timestamp_col,
    
    -- Store ENTIRE JSON as single variant
    CAST(data AS Variant(JSON)) as data
FROM {source_db}.{source_table}
{limit_clause};
"""

    print(f"Loading data into single variant column from {source_db}.{source_table}...")
    if max_records:
        print(f"Limiting to {max_records} records")
    
    result = run_clickhouse_query(insert_sql, use_local)
    if result is not None:
        print("✓ Single variant data loaded successfully")
        return True
    else:
        print("✗ Single variant data loading failed")
        return False

def verify_single_variant_data(use_local: bool = True):
    """Verify the single variant data and show query patterns."""
    
    # Check record count
    count_sql = "SELECT count() FROM bluesky_single_variant.bluesky_data"
    count = run_clickhouse_query(count_sql, use_local)
    if count:
        print(f"✓ Loaded {count} records in single variant column")
    
    # Test variant type analysis
    variant_test_sql = """
SELECT 
    variantType(data) as data_type,
    count() as cnt
FROM bluesky_single_variant.bluesky_data 
GROUP BY data_type
"""
    
    print("\nSingle variant type analysis:")
    result = run_clickhouse_query(variant_test_sql, use_local)
    if result:
        print(result)
    
    # Test field extraction from single variant
    extraction_test_sql = """
SELECT 
    variantElement(data, 'JSON').commit.collection::String as collection,
    count() as count
FROM bluesky_single_variant.bluesky_data 
WHERE variantElement(data, 'JSON').commit.collection IS NOT NULL
GROUP BY collection 
ORDER BY count DESC 
LIMIT 3
"""
    
    print("\nTop collections extracted from single variant:")
    result = run_clickhouse_query(extraction_test_sql, use_local)
    if result:
        print(result)
    
    # Show query pattern comparison
    print("\n" + "="*60)
    print("QUERY PATTERN COMPARISON:")
    print("="*60)
    print("\n1. Multi-variant approach:")
    print("   SELECT commit_collection FROM bluesky_true_variants.bluesky_data")
    print("\n2. Single variant approach:")
    print("   SELECT variantElement(data, 'JSON').commit.collection")
    print("   FROM bluesky_single_variant.bluesky_data")
    print("\n3. Benefits of single variant:")
    print("   - Simpler schema")
    print("   - No need to predict fields")
    print("   - True schema-on-read")
    print("   - Easier to add new field extractions")

def benchmark_approaches(use_local: bool = True):
    """Compare performance between single variant vs multi-variant approaches."""
    
    print("\n" + "="*60)
    print("PERFORMANCE COMPARISON")
    print("="*60)
    
    # Test 1: Simple field access
    print("\nTest 1: Simple field access")
    
    # Multi-variant query
    multi_sql = """
SELECT count(DISTINCT commit_collection) 
FROM bluesky_true_variants.bluesky_data 
WHERE commit_collection IS NOT NULL
"""
    
    # Single variant query  
    single_sql = """
SELECT count(DISTINCT variantElement(data, 'JSON').commit.collection::String)
FROM bluesky_single_variant.bluesky_data 
WHERE variantElement(data, 'JSON').commit.collection IS NOT NULL
"""
    
    print("Multi-variant query:")
    print("  ", multi_sql.strip())
    result = run_clickhouse_query(multi_sql, use_local)
    if result:
        print(f"  Result: {result}")
    
    print("\nSingle variant query:")
    print("  ", single_sql.strip())
    result = run_clickhouse_query(single_sql, use_local)
    if result:
        print(f"  Result: {result}")

def main():
    """Main function with command-line interface."""
    
    if len(sys.argv) < 2:
        print("Usage: python3 load_single_variant.py <mode> [options]")
        print("Modes:")
        print("  schema - Create the single variant schema")
        print("  load - Load data into single variant column")
        print("  verify - Verify loaded data and show query patterns")
        print("  benchmark - Compare with multi-variant approach")
        print("  all - Do everything")
        print("")
        print("Options for 'load' mode:")
        print("  --source-db DATABASE (default: bluesky_1m)")
        print("  --source-table TABLE (default: bluesky)")
        print("  --max-records N (default: no limit)")
        print("  --use-client (default: use local mode)")
        return
    
    mode = sys.argv[1]
    
    # Parse options
    source_db = "bluesky_1m"
    source_table = "bluesky"
    max_records = None
    use_local = True
    
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--source-db" and i + 1 < len(sys.argv):
            source_db = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == "--source-table" and i + 1 < len(sys.argv):
            source_table = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == "--max-records" and i + 1 < len(sys.argv):
            max_records = int(sys.argv[i + 1])
            i += 2
        elif sys.argv[i] == "--use-client":
            use_local = False
            i += 1
        else:
            i += 1
    
    print(f"Single Variant Loader - Mode: {mode}")
    print(f"Using: {'ClickHouse Local' if use_local else 'ClickHouse Client'}")
    print("")
    
    if mode in ['schema', 'all']:
        if not create_single_variant_schema(use_local):
            print("Failed to create single variant schema")
            return 1
    
    if mode in ['load', 'all']:
        if not load_data_single_variant(source_db, source_table, max_records, use_local):
            print("Failed to load single variant data")
            return 1
    
    if mode in ['verify', 'all']:
        verify_single_variant_data(use_local)
    
    if mode in ['benchmark', 'all']:
        benchmark_approaches(use_local)
    
    print("\n✓ Single variant approach completed successfully!")
    return 0

if __name__ == '__main__':
    sys.exit(main()) 