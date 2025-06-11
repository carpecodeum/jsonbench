#!/usr/bin/env python3
"""
Minimal Single Variant Column Loader for ClickHouse
Ultra-simple approach: ONLY ONE COLUMN containing entire JSON as variant.
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

def create_minimal_variant_schema(use_local: bool = True):
    """Create the minimal variant schema - ONLY ONE COLUMN."""
    
    schema_sql = """
-- Create database
CREATE DATABASE IF NOT EXISTS bluesky_minimal_variant;

-- Create minimal variant table - ONLY ONE COLUMN
CREATE TABLE bluesky_minimal_variant.bluesky_data
(
    -- ONLY ONE COLUMN containing entire JSON
    data Variant(JSON)
)
ENGINE = MergeTree
ORDER BY tuple()  -- No ordering key needed
SETTINGS 
    allow_experimental_variant_type = 1,
    use_variant_as_common_type = 1;
"""

    print("Creating minimal variant schema (single column only)...")
    result = run_clickhouse_query(schema_sql, use_local)
    if result is not None:
        print("✓ Minimal variant schema created successfully")
        return True
    else:
        print("✗ Minimal variant schema creation failed")
        return False

def load_data_minimal_variant(source_db: str = "bluesky_1m", source_table: str = "bluesky", 
                            max_records: int = None, use_local: bool = True):
    """Load data using minimal single variant column approach."""
    
    limit_clause = f"LIMIT {max_records}" if max_records else ""
    
    insert_sql = f"""
INSERT INTO bluesky_minimal_variant.bluesky_data
SELECT 
    -- Store ENTIRE JSON as single variant - NO OTHER COLUMNS
    CAST(data AS Variant(JSON)) as data
FROM {source_db}.{source_table}
{limit_clause};
"""

    print(f"Loading data into minimal single variant column from {source_db}.{source_table}...")
    if max_records:
        print(f"Limiting to {max_records} records")
    
    result = run_clickhouse_query(insert_sql, use_local)
    if result is not None:
        print("✓ Minimal variant data loaded successfully")
        return True
    else:
        print("✗ Minimal variant data loading failed")
        return False

def verify_minimal_variant_data(use_local: bool = True):
    """Verify the minimal variant data and show query patterns."""
    
    # Check record count
    count_sql = "SELECT count() FROM bluesky_minimal_variant.bluesky_data"
    count = run_clickhouse_query(count_sql, use_local)
    if count:
        print(f"✓ Loaded {count} records in minimal single variant column")
    
    # Test variant type analysis
    variant_test_sql = """
SELECT 
    variantType(data) as data_type,
    count() as cnt
FROM bluesky_minimal_variant.bluesky_data 
GROUP BY data_type
"""
    
    print("\nMinimal variant type analysis:")
    result = run_clickhouse_query(variant_test_sql, use_local)
    if result:
        print(result)
    
    # Test field extraction from minimal variant
    extraction_test_sql = """
SELECT 
    variantElement(data, 'JSON').kind::String as kind,
    count() as count
FROM bluesky_minimal_variant.bluesky_data 
WHERE variantElement(data, 'JSON').kind IS NOT NULL
GROUP BY kind 
ORDER BY count DESC 
LIMIT 3
"""
    
    print("\nTop kinds extracted from minimal variant:")
    result = run_clickhouse_query(extraction_test_sql, use_local)
    if result:
        print(result)
    
    # Test nested field extraction
    nested_test_sql = """
SELECT 
    variantElement(data, 'JSON').commit.collection::String as collection,
    count() as count
FROM bluesky_minimal_variant.bluesky_data 
WHERE variantElement(data, 'JSON').commit.collection IS NOT NULL
GROUP BY collection 
ORDER BY count DESC 
LIMIT 3
"""
    
    print("\nTop collections extracted from minimal variant:")
    result = run_clickhouse_query(nested_test_sql, use_local)
    if result:
        print(result)
    
    # Show the ultra-simple schema
    print("\n" + "="*60)
    print("ULTRA-MINIMAL VARIANT SCHEMA:")
    print("="*60)
    print("\nSchema:")
    print("CREATE TABLE bluesky_minimal_variant.bluesky_data (")
    print("    data Variant(JSON)  -- ONLY ONE COLUMN")
    print(")")
    print("\nQuery examples:")
    print("-- Get kind field:")
    print("SELECT variantElement(data, 'JSON').kind::String FROM bluesky_minimal_variant.bluesky_data")
    print("\n-- Get nested commit.collection:")
    print("SELECT variantElement(data, 'JSON').commit.collection::String FROM bluesky_minimal_variant.bluesky_data")
    print("\n-- Filter by any field:")
    print("SELECT * FROM bluesky_minimal_variant.bluesky_data")
    print("WHERE variantElement(data, 'JSON').kind = 'commit'")

def show_storage_comparison(use_local: bool = True):
    """Compare storage usage of different approaches."""
    
    print("\n" + "="*60)
    print("STORAGE COMPARISON")
    print("="*60)
    
    # Check if other tables exist for comparison
    tables_to_check = [
        ("bluesky_minimal_variant", "bluesky_data", "Minimal (1 column)"),
        ("bluesky_single_variant", "bluesky_data", "Single variant (5 columns)"),
        ("bluesky_true_variants", "bluesky_data", "Multi variant (12 columns)"),
        ("bluesky_1m", "bluesky", "Original JSON (1 column)")
    ]
    
    for db, table, description in tables_to_check:
        size_sql = f"""
SELECT 
    '{description}' as approach,
    formatReadableSize(sum(bytes_on_disk)) as size_on_disk,
    count() as rows
FROM system.parts 
WHERE database = '{db}' AND table = '{table}' AND active = 1
"""
        
        result = run_clickhouse_query(size_sql, use_local)
        if result and result != "":
            print(f"{description}: {result}")

def main():
    """Main function with command-line interface."""
    
    if len(sys.argv) < 2:
        print("Usage: python3 load_minimal_variant.py <mode> [options]")
        print("Modes:")
        print("  schema - Create the minimal variant schema (1 column only)")
        print("  load - Load data into minimal variant column")
        print("  verify - Verify loaded data and show query patterns")
        print("  storage - Compare storage usage with other approaches")
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
    
    print(f"Minimal Variant Loader - Mode: {mode}")
    print(f"Using: {'ClickHouse Local' if use_local else 'ClickHouse Client'}")
    print("")
    
    if mode in ['schema', 'all']:
        if not create_minimal_variant_schema(use_local):
            print("Failed to create minimal variant schema")
            return 1
    
    if mode in ['load', 'all']:
        if not load_data_minimal_variant(source_db, source_table, max_records, use_local):
            print("Failed to load minimal variant data")
            return 1
    
    if mode in ['verify', 'all']:
        verify_minimal_variant_data(use_local)
    
    if mode in ['storage', 'all']:
        show_storage_comparison(use_local)
    
    print("\n✓ Minimal variant approach completed successfully!")
    return 0

if __name__ == '__main__':
    sys.exit(main()) 