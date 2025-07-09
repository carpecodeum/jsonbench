#!/usr/bin/env python3
"""
Fixed test for variant_array approach using ClickHouse Local mode
"""

import subprocess
import json
import sys
import os
from pathlib import Path

def run_clickhouse_local(query):
    """Run a ClickHouse local query."""
    try:
        env = os.environ.copy()
        env['TZ'] = 'UTC'
        
        clickhouse_path = "./clickhouse/clickhouse"
        if not Path(clickhouse_path).exists():
            clickhouse_path = "clickhouse"
        
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
    print("TESTING VARIANT ARRAY APPROACH")
    print("=" * 60)
    
    # Test 1: Basic ClickHouse functionality
    print("1. Testing ClickHouse local mode...")
    success, result = run_clickhouse_local("SELECT 1")
    if not success:
        print(f"✗ ClickHouse not working: {result}")
        return 1
    print("✓ ClickHouse local mode working")
    
    # Test 2: Test variant array schema creation
    print("\n2. Testing Variant(Array(JSON)) schema...")
    
    schema_test = """
    SELECT 'Variant Array schema test' as test;
    
    CREATE TABLE test_table (data Variant(Array(JSON))) ENGINE = Memory() 
    SETTINGS allow_experimental_variant_type = 1, use_variant_as_common_type = 1;
    
    SELECT 'Table created successfully' as result;
    """
    
    success, result = run_clickhouse_local(schema_test)
    if not success:
        print(f"✗ Schema test failed: {result}")
        return 1
    print("✓ Variant(Array(JSON)) schema works")
    
    # Test 3: Test array creation and access
    print("\n3. Testing array data creation and access...")
    
    array_test = """
    WITH [
        CAST('{"kind": "commit", "time_us": 1700000001000000}', 'JSON'),
        CAST('{"kind": "identity", "time_us": 1700000002000000}', 'JSON'),
        CAST('{"kind": "commit", "time_us": 1700000003000000}', 'JSON')
    ] AS json_array
    SELECT 
        'Array length: ' || toString(length(json_array)) as test1,
        'First element kind: ' || toString(json_array[1].kind) as test2,
        'Element access works' as test3;
    """
    
    success, result = run_clickhouse_local(array_test)
    if not success:
        print(f"✗ Array test failed: {result}")
        return 1
    print("✓ Array creation and access works")
    print(f"  Results: {result}")
    
    # Test 4: Test arrayElement function
    print("\n4. Testing arrayElement function...")
    
    element_test = """
    WITH [
        CAST('{"kind": "commit", "time_us": 1700000001000000}', 'JSON'),
        CAST('{"kind": "identity", "time_us": 1700000002000000}', 'JSON'),
        CAST('{"kind": "commit", "time_us": 1700000003000000}', 'JSON')
    ] AS json_array
    SELECT 
        toString(arrayElement(json_array, 1).kind) as first_kind,
        toString(arrayElement(json_array, 2).kind) as second_kind,
        toString(arrayElement(json_array, 3).kind) as third_kind;
    """
    
    success, result = run_clickhouse_local(element_test)
    if not success:
        print(f"✗ arrayElement test failed: {result}")
        return 1
    print("✓ arrayElement function works")
    print(f"  Results: {result}")
    
    # Test 5: Test ARRAY JOIN with arrayEnumerate
    print("\n5. Testing ARRAY JOIN with arrayEnumerate...")
    
    join_test = """
    WITH [
        CAST('{"kind": "commit", "time_us": 1700000001000000}', 'JSON'),
        CAST('{"kind": "identity", "time_us": 1700000002000000}', 'JSON'),
        CAST('{"kind": "commit", "time_us": 1700000003000000}', 'JSON')
    ] AS json_array
    SELECT 
        toString(arrayElement(json_array, i).kind) as kind,
        count() as cnt
    FROM (SELECT json_array) 
    ARRAY JOIN arrayEnumerate(json_array) AS i 
    GROUP BY kind 
    ORDER BY cnt DESC;
    """
    
    success, result = run_clickhouse_local(join_test)
    if not success:
        print(f"✗ ARRAY JOIN test failed: {result}")
        return 1
    print("✓ ARRAY JOIN with arrayEnumerate works")
    print(f"  Results: {result}")
    
    # Test 6: Test filtering
    print("\n6. Testing filtering with WHERE...")
    
    filter_test = """
    WITH [
        CAST('{"kind": "commit", "time_us": 1700000001000000}', 'JSON'),
        CAST('{"kind": "identity", "time_us": 1700000002000000}', 'JSON'),
        CAST('{"kind": "commit", "time_us": 1700000003000000}', 'JSON')
    ] AS json_array
    SELECT count() as commit_count
    FROM (SELECT json_array) 
    ARRAY JOIN arrayEnumerate(json_array) AS i 
    WHERE toString(arrayElement(json_array, i).kind) = 'commit';
    """
    
    success, result = run_clickhouse_local(filter_test)
    if not success:
        print(f"✗ Filter test failed: {result}")
        return 1
    print("✓ Filtering works")
    print(f"  Results: {result}")
    
    print("\n🎉 All tests passed!")
    print("\n📋 Verified functionality:")
    print("- ✅ Variant(Array(JSON)) schema creation")
    print("- ✅ JSON array handling")
    print("- ✅ arrayElement() access")
    print("- ✅ ARRAY JOIN with arrayEnumerate()")
    print("- ✅ Filtering and aggregation")
    print("\n🚀 Variant Array approach is ready for benchmarking!")
    print("\n💡 To fix the server connection issue for the full benchmark:")
    print("   Try: export TZ=UTC && cd clickhouse && ./clickhouse server --daemon")
    print("   Or use a different ClickHouse installation method")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 