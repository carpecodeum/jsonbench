#!/usr/bin/env python3
"""
Test script for variant_array approach using ClickHouse Local mode
Tests the variant(array(json)) schema with a small sample of data
"""

import subprocess
import time
import json
import sys
import os
from pathlib import Path

class TestVariantArrayLocal:
    def __init__(self):
        self.clickhouse_path = "./clickhouse/clickhouse"
        if not Path(self.clickhouse_path).exists():
            self.clickhouse_path = "clickhouse-client"  # fallback to system-wide
        
    def run_clickhouse_query(self, query, input_file=None):
        """Run a ClickHouse local query and return results."""
        try:
            # Set timezone to avoid issues
            env = os.environ.copy()
            env['TZ'] = 'UTC'
            
            if input_file:
                # For queries that need input data
                cmd = [self.clickhouse_path, 'local', '--query', query]
                with open(input_file, 'r') as f:
                    result = subprocess.run(
                        cmd,
                        stdin=f,
                        capture_output=True,
                        text=True,
                        timeout=30,
                        env=env
                    )
            else:
                # For simple queries
                result = subprocess.run(
                    [self.clickhouse_path, 'local', '--query', query],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    env=env
                )
            
            if result.returncode == 0:
                return True, result.stdout.strip()
            else:
                return False, f"Error: {result.stderr}"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def create_test_data(self):
        """Create test data file with small array."""
        print("Creating test data...")
        
        # Create sample JSON objects
        sample_data = [
            {"kind": "commit", "time_us": 1700000001000000, "commit": {"operation": "create", "collection": "app.bsky.feed.post"}},
            {"kind": "identity", "time_us": 1700000002000000, "commit": {"operation": "update", "collection": "app.bsky.actor.profile"}},
            {"kind": "commit", "time_us": 1700000003000000, "commit": {"operation": "delete", "collection": "app.bsky.feed.post"}},
            {"kind": "account", "time_us": 1700000004000000, "commit": {"operation": "create", "collection": "app.bsky.actor.profile"}},
            {"kind": "commit", "time_us": 1700000005000000, "commit": {"operation": "create", "collection": "app.bsky.feed.like"}}
        ]
        
        # Write as single JSON array wrapped in data field
        test_file = "test_array_data.json"
        with open(test_file, 'w') as f:
            json.dump({"data": sample_data}, f)
        
        print(f"✓ Test data created: {test_file}")
        return test_file
    
    def test_variant_array_schema(self):
        """Test creating the variant array schema and loading data."""
        print("Testing variant array schema...")
        
        # Create test data
        test_file = self.create_test_data()
        
        # Test schema creation and data loading with local table
        schema_and_load_query = f"""
        CREATE TABLE test_data (
            data Variant(Array(JSON))
        ) ENGINE = Memory()
        SETTINGS allow_experimental_variant_type = 1, use_variant_as_common_type = 1;
        
        INSERT INTO test_data FORMAT JSONEachRow
        """
        
        success, result = self.run_clickhouse_query(schema_and_load_query, test_file)
        if success:
            print("✓ Schema created and data loaded successfully")
            return True
        else:
            print(f"✗ Schema/loading failed: {result}")
            return False
    
    def test_array_queries(self):
        """Test the array queries."""
        print("Testing array queries...")
        
        # Create test data
        test_file = self.create_test_data()
        
        # Test each query type
        test_queries = [
            {
                "name": "Q1: Count by kind",
                "query": f"""
                CREATE TABLE test_data (data Variant(Array(JSON))) ENGINE = Memory()
                SETTINGS allow_experimental_variant_type = 1, use_variant_as_common_type = 1;
                
                INSERT INTO test_data FORMAT JSONEachRow
                """
            },
            {
                "name": "Q2: Count by collection", 
                "query": f"""
                CREATE TABLE test_data (data Variant(Array(JSON))) ENGINE = Memory()
                SETTINGS allow_experimental_variant_type = 1, use_variant_as_common_type = 1;
                
                INSERT INTO test_data FORMAT JSONEachRow
                """
            },
            {
                "name": "Q3: Filter by kind",
                "query": f"""
                CREATE TABLE test_data (data Variant(Array(JSON))) ENGINE = Memory()
                SETTINGS allow_experimental_variant_type = 1, use_variant_as_common_type = 1;
                
                INSERT INTO test_data FORMAT JSONEachRow
                """
            },
            {
                "name": "Q4: Time range query",
                "query": f"""
                CREATE TABLE test_data (data Variant(Array(JSON))) ENGINE = Memory()
                SETTINGS allow_experimental_variant_type = 1, use_variant_as_common_type = 1;
                
                INSERT INTO test_data FORMAT JSONEachRow
                """
            },
            {
                "name": "Q5: Complex aggregation",
                "query": f"""
                CREATE TABLE test_data (data Variant(Array(JSON))) ENGINE = Memory()
                SETTINGS allow_experimental_variant_type = 1, use_variant_as_common_type = 1;
                
                INSERT INTO test_data FORMAT JSONEachRow
                """
            }
        ]
        
        # Load data first with a simple insert
        load_query = """
        CREATE TABLE test_data (data Variant(Array(JSON))) ENGINE = Memory()
        SETTINGS allow_experimental_variant_type = 1, use_variant_as_common_type = 1;
        
        INSERT INTO test_data FORMAT JSONEachRow
        """
        
        print("  Loading test data...")
        success, result = self.run_clickhouse_query(load_query, test_file)
        if not success:
            print(f"    ✗ Failed to load data: {result}")
            return False
        print("    ✓ Data loaded successfully")
        
        # Now test the queries
        query_tests = [
            {
                "name": "Q1: Count by kind",
                "query": """
                SELECT toString(arrayElement(data.Array, i).kind) as kind, count() 
                FROM test_data 
                ARRAY JOIN arrayEnumerate(data.Array) AS i 
                GROUP BY kind ORDER BY count() DESC
                """
            },
            {
                "name": "Q2: Count by collection", 
                "query": """
                SELECT toString(arrayElement(data.Array, i).commit.collection) as collection, count() 
                FROM test_data 
                ARRAY JOIN arrayEnumerate(data.Array) AS i 
                WHERE collection != '' 
                GROUP BY collection ORDER BY count() DESC LIMIT 10
                """
            },
            {
                "name": "Q3: Filter by kind",
                "query": """
                SELECT count() 
                FROM test_data 
                ARRAY JOIN arrayEnumerate(data.Array) AS i 
                WHERE toString(arrayElement(data.Array, i).kind) = 'commit'
                """
            },
            {
                "name": "Q4: Time range query",
                "query": """
                SELECT count() 
                FROM test_data 
                ARRAY JOIN arrayEnumerate(data.Array) AS i 
                WHERE toUInt64(arrayElement(data.Array, i).time_us) > 1700000000000000
                """
            },
            {
                "name": "Q5: Complex aggregation",
                "query": """
                SELECT toString(arrayElement(data.Array, i).commit.operation) as op, 
                       toString(arrayElement(data.Array, i).commit.collection) as coll, 
                       count() 
                FROM test_data 
                ARRAY JOIN arrayEnumerate(data.Array) AS i 
                WHERE op != '' AND coll != '' 
                GROUP BY op, coll ORDER BY count() DESC LIMIT 5
                """
            }
        ]
        
        for i, test_case in enumerate(query_tests, 1):
            print(f"  {test_case['name']}...")
            success, result = self.run_clickhouse_query(test_case['query'])
            if success:
                print(f"    ✓ Query {i} succeeded")
                print(f"    Result: {result}")
            else:
                print(f"    ✗ Query {i} failed: {result}")
                return False
        
        return True
    
    def cleanup(self):
        """Clean up test resources."""
        print("Cleaning up...")
        
        # Remove test file
        test_file = "test_array_data.json"
        if Path(test_file).exists():
            Path(test_file).unlink()
            print("✓ Test data file removed")
    
    def run_test(self):
        """Run complete test."""
        print("=" * 60)
        print("TESTING VARIANT ARRAY APPROACH (LOCAL MODE)")
        print("=" * 60)
        
        try:
            # Test basic ClickHouse functionality
            success, result = self.run_clickhouse_query("SELECT 1")
            if not success:
                print(f"✗ ClickHouse local mode not working: {result}")
                return False
            print("✓ ClickHouse local mode is working")
            
            # Test variant array queries
            if not self.test_array_queries():
                return False
            
            print("\n✓ All tests passed!")
            return True
            
        except Exception as e:
            print(f"\n✗ Test failed with exception: {str(e)}")
            return False
        finally:
            self.cleanup()

def main():
    """Main test execution."""
    # Change to project root directory
    os.chdir(Path(__file__).parent)
    
    test = TestVariantArrayLocal()
    success = test.run_test()
    
    if success:
        print("\n🎉 Variant Array implementation is working correctly!")
        print("\n📋 Summary:")
        print("- ✅ Variant(Array(JSON)) schema works")
        print("- ✅ Array element access with arrayElement() works") 
        print("- ✅ ARRAY JOIN with arrayEnumerate() works")
        print("- ✅ All 5 benchmark queries execute successfully")
        print("\n🚀 Ready for 100M dataset benchmarking!")
        return 0
    else:
        print("\n❌ Variant Array implementation has issues.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 