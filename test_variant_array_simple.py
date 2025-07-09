#!/usr/bin/env python3
"""
Simple test for variant_array approach using ClickHouse Local mode
Tests the variant(array(json)) schema with sample data
"""

import subprocess
import json
import sys
import os
from pathlib import Path

def run_clickhouse_local(query, input_file=None):
    """Run a ClickHouse local query."""
    try:
        env = os.environ.copy()
        env['TZ'] = 'UTC'
        
        clickhouse_path = "./clickhouse/clickhouse"
        if not Path(clickhouse_path).exists():
            clickhouse_path = "clickhouse"
        
        if input_file:
            cmd = f"{clickhouse_path} local --query \"{query}\" < {input_file}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, env=env)
        else:
            result = subprocess.run([clickhouse_path, 'local', '--query', query], 
                                  capture_output=True, text=True, env=env)
        
        if result.returncode == 0:
            return True, result.stdout.strip()
        else:
            return False, result.stderr.strip()
    except Exception as e:
        return False, str(e)

def main():
    print("=" * 60)
    print("TESTING VARIANT ARRAY APPROACH (SIMPLE)")
    print("=" * 60)
    
    # Test 1: Basic ClickHouse functionality
    print("1. Testing ClickHouse local mode...")
    success, result = run_clickhouse_local("SELECT 1")
    if not success:
        print(f"✗ ClickHouse not working: {result}")
        return 1
    print("✓ ClickHouse local mode working")
    
    # Test 2: Create sample data
    print("\n2. Creating test data...")
    sample_data = [
        {"kind": "commit", "time_us": 1700000001000000, "commit": {"operation": "create", "collection": "app.bsky.feed.post"}},
        {"kind": "identity", "time_us": 1700000002000000, "commit": {"operation": "update", "collection": "app.bsky.actor.profile"}},
        {"kind": "commit", "time_us": 1700000003000000, "commit": {"operation": "delete", "collection": "app.bsky.feed.post"}}
    ]
    
    test_file = "simple_test_data.json"
    with open(test_file, 'w') as f:
        json.dump({"data": sample_data}, f)
    print(f"✓ Test data created: {test_file}")
    
    # Test 3: Test variant array schema with data loading
    print("\n3. Testing variant array schema...")
    
    # Create table and load data
    create_and_load = """
    CREATE TABLE test_array (data Variant(Array(JSON))) ENGINE = Memory() 
    SETTINGS allow_experimental_variant_type = 1, use_variant_as_common_type = 1;
    
    INSERT INTO test_array FORMAT JSONEachRow
    """
    
    success, result = run_clickhouse_local(create_and_load, test_file)
    if not success:
        print(f"✗ Schema/loading failed: {result}")
        return 1
    print("✓ Schema created and data loaded")
    
    # Test 4: Test array access queries
    print("\n4. Testing array access queries...")
    
    queries = [
        ("Count records", "SELECT count() FROM test_array"),
        ("Array length", "SELECT length(data.Array) FROM test_array"),
        ("First element kind", "SELECT toString(arrayElement(data.Array, 1).kind) FROM test_array"),
        ("Count by kind", """
         SELECT toString(arrayElement(data.Array, i).kind) as kind, count() 
         FROM test_array 
         ARRAY JOIN arrayEnumerate(data.Array) AS i 
         GROUP BY kind ORDER BY count() DESC
         """),
        ("Filter commits", """
         SELECT count() 
         FROM test_array 
         ARRAY JOIN arrayEnumerate(data.Array) AS i 
         WHERE toString(arrayElement(data.Array, i).kind) = 'commit'
         """)
    ]
    
    for name, query in queries:
        print(f"  Testing: {name}")
        success, result = run_clickhouse_local(query)
        if success:
            print(f"    ✓ Result: {result}")
        else:
            print(f"    ✗ Failed: {result}")
            return 1
    
    # Cleanup
    if Path(test_file).exists():
        Path(test_file).unlink()
    
    print("\n🎉 All tests passed!")
    print("\n📋 Verified functionality:")
    print("- ✅ Variant(Array(JSON)) schema creation")
    print("- ✅ JSON array data loading")
    print("- ✅ arrayElement() access")
    print("- ✅ ARRAY JOIN with arrayEnumerate()")
    print("- ✅ Filtering and aggregation")
    print("\n🚀 Variant Array approach is ready for benchmarking!")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 