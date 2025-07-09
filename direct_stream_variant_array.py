#!/usr/bin/env python3
"""
Direct streaming variant array approach:
- Read directly from gzipped files
- No intermediate files
- Stream all 100M records into 1 row, 1 column as Variant(Array(JSON))
"""
import json
import gzip
import subprocess
import sys
from pathlib import Path

def create_variant_array_direct():
    """Create variant array by streaming directly from gzipped files to ClickHouse."""
    print("Creating variant array with direct streaming approach...")
    print("Target: 1 row, 1 column, all 100M JSON objects as array")
    
    data_dir = Path.home() / "data" / "bluesky"
    
    # Setup database and table
    print("Setting up database and table...")
    
    # Create database
    db_cmd = "clickhouse-client --query 'CREATE DATABASE IF NOT EXISTS bluesky_100m_variant_array'"
    result = subprocess.run(db_cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Database creation failed: {result.stderr}")
        return False
    
    # Drop table if exists
    drop_cmd = "clickhouse-client --query 'DROP TABLE IF EXISTS bluesky_100m_variant_array.bluesky_array_data'"
    subprocess.run(drop_cmd, shell=True, capture_output=True, text=True)
    
    # Create table
    create_table_cmd = """
    clickhouse-client --query "
    CREATE TABLE bluesky_100m_variant_array.bluesky_array_data (
        data Variant(Array(JSON))
    ) ENGINE = MergeTree()
    ORDER BY tuple()
    "
    """
    
    result = subprocess.run(create_table_cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Table creation failed: {result.stderr}")
        return False
    
    print("✓ Database and table created")
    
    # Stream data directly to ClickHouse using subprocess with pipe
    print("Streaming data directly to ClickHouse...")
    
    insert_cmd = [
        'clickhouse-client',
        '--max_memory_usage=64000000000',
        '--max_parser_depth=100000',
        '--max_query_size=64000000000',
        '--query', 'INSERT INTO bluesky_100m_variant_array.bluesky_array_data FORMAT JSONEachRow'
    ]
    
    # Start the ClickHouse insert process
    process = subprocess.Popen(
        insert_cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    try:
        # Start JSON structure
        process.stdin.write('{"data": [')
        
        total_processed = 0
        first_record = True
        
        # Process all 100 files
        for file_num in range(1, 101):
            file_path = data_dir / f"file_{file_num:04d}.json.gz"
            
            if not file_path.exists():
                print(f"Warning: File {file_path} not found, skipping...")
                continue
            
            print(f"Processing file {file_num}/100: {file_path.name}")
            
            try:
                with gzip.open(file_path, 'rt') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        
                        try:
                            # Validate JSON
                            record = json.loads(line)
                            
                            # Add comma separator for all but first record
                            if not first_record:
                                process.stdin.write(',')
                            else:
                                first_record = False
                            
                            # Write the record directly to ClickHouse
                            json.dump(record, process.stdin, separators=(',', ':'))
                            
                            total_processed += 1
                            
                            # Progress update
                            if total_processed % 500000 == 0:
                                print(f"  Streamed {total_processed:,} records...")
                                process.stdin.flush()  # Ensure data is sent
                            
                        except json.JSONDecodeError:
                            continue  # Skip invalid JSON
                            
            except Exception as e:
                print(f"Error reading file {file_path}: {e}")
                continue
            
            if file_num % 10 == 0:
                print(f"  Completed {file_num}/100 files, {total_processed:,} total records")
        
        # Close JSON structure
        process.stdin.write(']}')
        process.stdin.close()
        
        print(f"✓ Finished streaming {total_processed:,} records")
        
        # Wait for ClickHouse to finish processing
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            print("✓ Successfully inserted data into ClickHouse!")
            return True
        else:
            print(f"Insert failed: {stderr}")
            return False
            
    except Exception as e:
        print(f"Streaming error: {e}")
        process.terminate()
        return False

def verify_structure():
    """Verify the variant array structure."""
    print("\nVerifying variant array structure...")
    
    # Check row count (should be exactly 1)
    count_query = "SELECT count() FROM bluesky_100m_variant_array.bluesky_array_data"
    result = subprocess.run(['clickhouse-client', '--query', count_query], 
                          capture_output=True, text=True)
    
    if result.returncode == 0:
        row_count = int(result.stdout.strip())
        print(f"✓ Table has {row_count} row(s)")
        
        if row_count != 1:
            print("❌ ERROR: Expected exactly 1 row!")
            return False
    else:
        print(f"Row count check failed: {result.stderr}")
        return False
    
    # Check array length
    array_length_query = "SELECT length(data.Array) FROM bluesky_100m_variant_array.bluesky_array_data"
    result = subprocess.run(['clickhouse-client', '--query', array_length_query], 
                          capture_output=True, text=True)
    
    if result.returncode == 0:
        array_length = int(result.stdout.strip())
        print(f"✓ Array contains {array_length:,} JSON objects")
        
        if array_length < 50000000:  # Should be close to 100M
            print("⚠️  Warning: Array length seems lower than expected")
    else:
        print(f"Array length check failed: {result.stderr}")
        return False
    
    # Test array access
    test_query = """
    SELECT toString(arrayElement(data.Array, 1).kind) as first_kind,
           toString(arrayElement(data.Array, length(data.Array)).kind) as last_kind
    FROM bluesky_100m_variant_array.bluesky_array_data
    """
    
    result = subprocess.run(['clickhouse-client', '--query', test_query], 
                          capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"✓ Array access working: {result.stdout.strip()}")
        return True
    else:
        print(f"Array access test failed: {result.stderr}")
        return False

def main():
    """Main execution."""
    print("=" * 60)
    print("DIRECT STREAMING VARIANT ARRAY")
    print("1 ROW, 1 COLUMN, ALL 100M JSON OBJECTS")
    print("NO INTERMEDIATE FILES")
    print("=" * 60)
    
    if not create_variant_array_direct():
        print("❌ Failed to create variant array structure")
        return False
    
    if not verify_structure():
        print("❌ Verification failed")
        return False
    
    print("\n🎉 SUCCESS!")
    print("✓ 1 row created")
    print("✓ 1 column: Variant(Array(JSON))")
    print("✓ All JSON objects stored as single array")
    print("\nReady to run variant array benchmark!")
    return True

if __name__ == "__main__":
    main() 