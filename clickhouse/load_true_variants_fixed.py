#!/usr/bin/env python3
"""
Fixed True Variants Loader for ClickHouse
Uses the proven SQL CAST approach that actually works.
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

def create_true_variants_schema(use_local: bool = True):
    """Create the true variants schema using the proven approach."""
    
    schema_sql = """
-- Create database
CREATE DATABASE IF NOT EXISTS bluesky_true_variants;

-- Create true variant table with experimental settings
CREATE TABLE bluesky_true_variants.bluesky_data
(
    -- Core identity fields (not variants)
    did String,
    time_us UInt64,
    kind LowCardinality(String),
    timestamp_col DateTime64(6),
    
    -- TRUE Variant columns - these can store different types
    commit_operation Variant(String),
    commit_collection Variant(String),
    commit_rev Variant(String),
    commit_rkey Variant(String),
    commit_cid Variant(String),
    
    -- Record data as variant (can be JSON or simple string)
    record_data Variant(JSON, String),
    
    -- Original JSON for comparison
    original_json JSON
)
ENGINE = MergeTree
ORDER BY (kind, did, timestamp_col)
SETTINGS 
    allow_experimental_variant_type = 1,
    use_variant_as_common_type = 1;
"""

    print("Creating true variants schema...")
    result = run_clickhouse_query(schema_sql, use_local)
    if result is not None:
        print("✓ Schema created successfully")
        return True
    else:
        print("✗ Schema creation failed")
        return False

def load_data_via_cast(source_db: str = "bluesky_1m", source_table: str = "bluesky", 
                      max_records: int = None, use_local: bool = True):
    """Load data using the proven CAST approach."""
    
    # Build the INSERT query using CAST operations
    limit_clause = f"LIMIT {max_records}" if max_records else ""
    
    insert_sql = f"""
INSERT INTO bluesky_true_variants.bluesky_data
SELECT 
    data.did::String as did,
    data.time_us::UInt64 as time_us,
    data.kind::String as kind,
    fromUnixTimestamp64Micro(data.time_us) as timestamp_col,
    
    -- Cast to Variant columns
    CAST(data.commit.operation AS Variant(String)) as commit_operation,
    CAST(data.commit.collection AS Variant(String)) as commit_collection,
    CAST(data.commit.rev AS Variant(String)) as commit_rev,
    CAST(data.commit.rkey AS Variant(String)) as commit_rkey,
    CAST(data.commit.cid AS Variant(String)) as commit_cid,
    
    -- Record data as variant
    CAST(data AS Variant(JSON, String)) as record_data,
    
    -- Original JSON
    data as original_json
FROM {source_db}.{source_table}
{limit_clause};
"""

    print(f"Loading data from {source_db}.{source_table} using CAST approach...")
    if max_records:
        print(f"Limiting to {max_records} records")
    
    result = run_clickhouse_query(insert_sql, use_local)
    if result is not None:
        print("✓ Data loaded successfully")
        return True
    else:
        print("✗ Data loading failed")
        return False

def verify_data(use_local: bool = True):
    """Verify the loaded data."""
    
    # Check record count
    count_sql = "SELECT count() FROM bluesky_true_variants.bluesky_data"
    count = run_clickhouse_query(count_sql, use_local)
    if count:
        print(f"✓ Loaded {count} records")
    
    # Test variant functions
    variant_test_sql = """
SELECT 
    variantType(commit_operation) as op_type,
    variantType(commit_collection) as coll_type,
    variantType(record_data) as data_type,
    count() as cnt
FROM bluesky_true_variants.bluesky_data 
GROUP BY op_type, coll_type, data_type
LIMIT 5
"""
    
    print("\nVariant type analysis:")
    result = run_clickhouse_query(variant_test_sql, use_local)
    if result:
        print(result)
    
    # Test data extraction
    extraction_test_sql = """
SELECT 
    variantElement(commit_collection, 'String') as event,
    count() as count
FROM bluesky_true_variants.bluesky_data 
WHERE commit_collection IS NOT NULL
GROUP BY event 
ORDER BY count DESC 
LIMIT 3
"""
    
    print("\nTop events by variant extraction:")
    result = run_clickhouse_query(extraction_test_sql, use_local)
    if result:
        print(result)

def main():
    """Main function with command-line interface."""
    
    # Parse arguments
    if len(sys.argv) < 2:
        print("Usage: python3 load_true_variants_fixed.py <mode> [options]")
        print("Modes:")
        print("  schema - Create the true variants schema")
        print("  load - Load data from existing JSON table")
        print("  verify - Verify loaded data")
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
    
    print(f"True Variants Loader - Mode: {mode}")
    print(f"Using: {'ClickHouse Local' if use_local else 'ClickHouse Client'}")
    print("")
    
    if mode in ['schema', 'all']:
        if not create_true_variants_schema(use_local):
            print("Failed to create schema")
            return 1
    
    if mode in ['load', 'all']:
        if not load_data_via_cast(source_db, source_table, max_records, use_local):
            print("Failed to load data")
            return 1
    
    if mode in ['verify', 'all']:
        verify_data(use_local)
    
    print("\n✓ True variants loading completed successfully!")
    return 0

if __name__ == '__main__':
    sys.exit(main()) 