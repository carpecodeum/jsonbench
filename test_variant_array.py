#!/usr/bin/env python3
"""
Test script for variant_array approach
Tests the variant(array(json)) schema with a small sample of data
"""

import subprocess
import time
import json
import sys
from pathlib import Path

class TestVariantArray:
    def __init__(self):
        self.database = 'test_variant_array'
        self.table = 'test_data'
        
    def run_clickhouse_query(self, query):
        """Run a ClickHouse query and return results."""
        try:
            result = subprocess.run(
                ['clickhouse-client', '--query', query],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                return True, result.stdout.strip()
            else:
                return False, f"Error: {result.stderr}"
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    def start_clickhouse_server(self):
        """Start ClickHouse server if not running."""
        print("Starting ClickHouse server...")
        try:
            # Try to test if server is already running
            result = subprocess.run(
                ['clickhouse-client', '--query', 'SELECT 1'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                print("✓ ClickHouse server is already running")
                return True
            
            # Server not running, try to start it
            print("  Starting server...")
            result = subprocess.run(
                ['sudo', 'clickhouse', 'start'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                print("✓ ClickHouse server started successfully")
                return True
            else:
                print(f"✗ Failed to start server: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"✗ Error starting server: {str(e)}")
            return False
    
    def create_test_schema(self):
        """Create test schema for variant array."""
        print("Creating test schema...")
        
        schema_sql = f"""
        CREATE DATABASE IF NOT EXISTS {self.database};
        CREATE TABLE IF NOT EXISTS {self.database}.{self.table} (
            data Variant(Array(JSON))
        ) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS allow_experimental_variant_type = 1, use_variant_as_common_type = 1;
        """
        
        success, result = self.run_clickhouse_query(schema_sql)
        if success:
            print("✓ Schema created successfully")
            return True
        else:
            print(f"✗ Schema creation failed: {result}")
            return False
    
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
    
    def load_test_data(self, test_file):
        """Load test data into ClickHouse."""
        print("Loading test data...")
        
        load_cmd = f"clickhouse-client --query 'INSERT INTO {self.database}.{self.table} FORMAT JSONEachRow' < {test_file}"
        result = subprocess.run(load_cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✓ Test data loaded successfully")
            return True
        else:
            print(f"✗ Test data loading failed: {result.stderr}")
            return False
    
    def test_queries(self):
        """Test the array queries."""
        print("Testing queries...")
        
        test_queries = [
            # Q1: Count by kind
            f"SELECT toString(arrayElement(data.Array, i).kind) as kind, count() FROM {self.database}.{self.table} ARRAY JOIN arrayEnumerate(data.Array) AS i GROUP BY kind ORDER BY count() DESC",
            
            # Q2: Count by collection
            f"SELECT toString(arrayElement(data.Array, i).commit.collection) as collection, count() FROM {self.database}.{self.table} ARRAY JOIN arrayEnumerate(data.Array) AS i WHERE collection != '' GROUP BY collection ORDER BY count() DESC LIMIT 10",
            
            # Q3: Filter by kind
            f"SELECT count() FROM {self.database}.{self.table} ARRAY JOIN arrayEnumerate(data.Array) AS i WHERE toString(arrayElement(data.Array, i).kind) = 'commit'",
            
            # Q4: Time range query
            f"SELECT count() FROM {self.database}.{self.table} ARRAY JOIN arrayEnumerate(data.Array) AS i WHERE toUInt64(arrayElement(data.Array, i).time_us) > 1700000000000000",
            
            # Q5: Complex aggregation
            f"SELECT toString(arrayElement(data.Array, i).commit.operation) as op, toString(arrayElement(data.Array, i).commit.collection) as coll, count() FROM {self.database}.{self.table} ARRAY JOIN arrayEnumerate(data.Array) AS i WHERE op != '' AND coll != '' GROUP BY op, coll ORDER BY count() DESC LIMIT 5"
        ]
        
        for i, query in enumerate(test_queries, 1):
            print(f"  Q{i}: Testing query...")
            success, result = self.run_clickhouse_query(query)
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
        
        # Drop test database
        success, result = self.run_clickhouse_query(f"DROP DATABASE IF EXISTS {self.database}")
        if success:
            print("✓ Test database dropped")
        
        # Remove test file
        test_file = "test_array_data.json"
        if Path(test_file).exists():
            Path(test_file).unlink()
            print("✓ Test data file removed")
    
    def run_test(self):
        """Run complete test."""
        print("=" * 60)
        print("TESTING VARIANT ARRAY APPROACH")
        print("=" * 60)
        
        try:
            # Start server
            if not self.start_clickhouse_server():
                print("⚠ Server not available, skipping test")
                return False
                
            # Create schema
            if not self.create_test_schema():
                return False
            
            # Create test data
            test_file = self.create_test_data()
            if not test_file:
                return False
            
            # Load test data
            if not self.load_test_data(test_file):
                return False
            
            # Test queries
            if not self.test_queries():
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
    test = TestVariantArray()
    success = test.run_test()
    
    if success:
        print("\n🎉 Variant Array implementation is working correctly!")
        return 0
    else:
        print("\n❌ Variant Array implementation has issues or server not available.")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 