#!/usr/bin/env python3
"""
Final True Variants Loader - Most Robust Version
Uses direct JSON loading followed by SQL transformations to avoid escaping issues.
"""

import subprocess
import tempfile
import os
import sys
import json

def run_clickhouse_local_script(sql_commands: list):
    """Run multiple SQL commands in a single ClickHouse local session."""
    
    # Create temporary SQL file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        for cmd in sql_commands:
            f.write(cmd)
            f.write('\n\n')
        temp_file = f.name
    
    try:
        # Execute the SQL file
        result = subprocess.run(
            ['clickhouse', 'local', '--queries-file', temp_file],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"SQL execution failed: {result.stderr}")
            return False, result.stderr
        
        return True, result.stdout
        
    finally:
        # Clean up temp file
        os.unlink(temp_file)

def create_complete_workflow(json_file: str, max_records: int = None):
    """Create a complete workflow to load true variants."""
    
    limit_clause = f"LIMIT {max_records}" if max_records else ""
    
    sql_commands = [
        "-- Step 1: Create source JSON table",
        "CREATE DATABASE IF NOT EXISTS bluesky_source;",
        f"DROP TABLE IF EXISTS bluesky_source.json_data;",
        "CREATE TABLE bluesky_source.json_data (data JSON) ENGINE = MergeTree ORDER BY tuple();",
        
        f"-- Step 2: Load JSON data from file",
        f"INSERT INTO bluesky_source.json_data FROM INFILE '{json_file}' FORMAT JSONEachRow;",
        
        "-- Step 3: Verify source data",
        "SELECT 'Source records loaded:' as status, count() as count FROM bluesky_source.json_data;",
        
        "-- Step 4: Create true variants schema",
        "CREATE DATABASE IF NOT EXISTS bluesky_true_variants;",
        "DROP TABLE IF EXISTS bluesky_true_variants.bluesky_data;",
        """
CREATE TABLE bluesky_true_variants.bluesky_data
(
    -- Core fields
    did String,
    time_us UInt64,
    kind LowCardinality(String),
    timestamp_col DateTime64(6),
    
    -- True Variant columns
    commit_operation Variant(String),
    commit_collection Variant(String),
    commit_rev Variant(String),
    commit_rkey Variant(String),
    commit_cid Variant(String),
    
    -- Complex variant for record data
    record_data Variant(JSON, String),
    
    -- Original for comparison
    original_json JSON
)
ENGINE = MergeTree
ORDER BY (kind, did, timestamp_col)
SETTINGS 
    allow_experimental_variant_type = 1,
    use_variant_as_common_type = 1;
        """,
        
        f"-- Step 5: Transform data using CAST (the proven approach)",
        f"""
INSERT INTO bluesky_true_variants.bluesky_data
SELECT 
    COALESCE(data.did::String, '') as did,
    COALESCE(data.time_us::UInt64, 0) as time_us,
    COALESCE(data.kind::String, '') as kind,
    fromUnixTimestamp64Micro(COALESCE(data.time_us::UInt64, 0)) as timestamp_col,
    
    -- Cast to Variant columns (handle NULLs gracefully)
    CAST(data.commit.operation AS Variant(String)) as commit_operation,
    CAST(data.commit.collection AS Variant(String)) as commit_collection,
    CAST(data.commit.rev AS Variant(String)) as commit_rev,
    CAST(data.commit.rkey AS Variant(String)) as commit_rkey,
    CAST(data.commit.cid AS Variant(String)) as commit_cid,
    
    -- Cast entire data object as variant
    CAST(data AS Variant(JSON, String)) as record_data,
    
    -- Keep original
    data as original_json
FROM bluesky_source.json_data
{limit_clause};
        """,
        
        "-- Step 6: Verification queries",
        "SELECT 'True variants loaded:' as status, count() as count FROM bluesky_true_variants.bluesky_data;",
        
        """
SELECT 'Variant type analysis:' as test,
       variantType(commit_operation) as op_type,
       variantType(commit_collection) as coll_type,
       variantType(record_data) as data_type,
       count() as records
FROM bluesky_true_variants.bluesky_data 
GROUP BY op_type, coll_type, data_type
ORDER BY records DESC
LIMIT 5;
        """,
        
        """
SELECT 'Event distribution:' as test,
       variantElement(commit_collection, 'String') as event_type,
       count() as event_count
FROM bluesky_true_variants.bluesky_data 
WHERE commit_collection IS NOT NULL
GROUP BY event_type 
ORDER BY event_count DESC 
LIMIT 10;
        """,
        
        """
SELECT 'Operation distribution:' as test,
       variantElement(commit_operation, 'String') as operation,
       count() as op_count
FROM bluesky_true_variants.bluesky_data 
WHERE commit_operation IS NOT NULL
GROUP BY operation 
ORDER BY op_count DESC;
        """,
        
        "-- Step 7: Storage analysis",
        """
SELECT 'Storage comparison:' as test,
       'source_json' as table_type,
       formatReadableSize(sum(total_bytes)) as size
FROM system.tables 
WHERE database = 'bluesky_source' AND name = 'json_data'
UNION ALL
SELECT 'Storage comparison:' as test,
       'true_variants' as table_type,
       formatReadableSize(sum(total_bytes)) as size
FROM system.tables 
WHERE database = 'bluesky_true_variants' AND name = 'bluesky_data';
        """
    ]
    
    return sql_commands

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 load_true_variants_final.py <json_file> [max_records]")
        print("")
        print("Example:")
        print("  python3 load_true_variants_final.py sample_1k.jsonl")
        print("  python3 load_true_variants_final.py sample_1k.jsonl 500")
        print("")
        print("This script:")
        print("1. Creates source JSON table")
        print("2. Loads JSON data")
        print("3. Creates true variants table") 
        print("4. Transforms data using proven CAST approach")
        print("5. Runs comprehensive verification")
        return 1
    
    json_file = sys.argv[1]
    max_records = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    if not os.path.exists(json_file):
        print(f"Error: File {json_file} not found")
        return 1
    
    print(f"🚀 Loading True Variants from: {json_file}")
    if max_records:
        print(f"📊 Limiting to: {max_records} records")
    print("")
    
    # Create the workflow
    sql_commands = create_complete_workflow(json_file, max_records)
    
    # Execute everything in one session
    print("⚙️  Executing complete true variants workflow...")
    success, output = run_clickhouse_local_script(sql_commands)
    
    if success:
        print("✅ True variants loading completed successfully!")
        print("")
        print("📈 Results:")
        print(output)
        
        print("\n🎯 Next steps:")
        print("- Query the data: clickhouse local --query \"SELECT * FROM bluesky_true_variants.bluesky_data LIMIT 5\"")
        print("- Test variants: clickhouse local --query \"SELECT variantType(commit_operation), count() FROM bluesky_true_variants.bluesky_data GROUP BY 1\"")
        print("- Extract data: clickhouse local --query \"SELECT variantElement(commit_collection, 'String'), count() FROM bluesky_true_variants.bluesky_data WHERE commit_collection IS NOT NULL GROUP BY 1\"")
        
        return 0
    else:
        print("❌ True variants loading failed!")
        print(f"Error: {output}")
        return 1

if __name__ == '__main__':
    sys.exit(main()) 