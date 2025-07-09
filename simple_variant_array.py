#!/usr/bin/env python3
"""
Simple variant array approach: Start with a small subset to verify the concept works.
Then scale up to all 100M records.
"""
import json
import gzip
import subprocess
import tempfile
import os
from pathlib import Path

def create_simple_variant_array(max_files=5):
    """Create variant array with a limited number of files first."""
    print(f"Creating variant array with first {max_files} files...")
    
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
    
    # Collect all records into memory first
    print("Reading JSON records into memory...")
    
    all_records = []
    total_processed = 0
    
    for file_num in range(1, max_files + 1):
        file_path = data_dir / f"file_{file_num:04d}.json.gz"
        
        if not file_path.exists():
            print(f"Warning: File {file_path} not found, skipping...")
            continue
        
        print(f"Processing file {file_num}/{max_files}: {file_path.name}")
        
        try:
            with gzip.open(file_path, 'rt') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        record = json.loads(line)
                        all_records.append(record)
                        total_processed += 1
                        
                        if total_processed % 100000 == 0:
                            print(f"  Loaded {total_processed:,} records...")
                            
                    except json.JSONDecodeError:
                        continue  # Skip invalid JSON
                        
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
            continue
    
    print(f"✓ Loaded {total_processed:,} records into memory")
    
    # Create the JSON structure for ClickHouse
    array_data = {"data": all_records}
    
    # Write to temporary file
    print("Creating temporary JSON file...")
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
        temp_filename = temp_file.name
        json.dump(array_data, temp_file, separators=(',', ':'))
    
    print(f"✓ Created temporary file: {temp_filename}")
    
    # Insert into ClickHouse
    print("Inserting into ClickHouse...")
    
    insert_cmd = f"""clickhouse-client \
        --max_memory_usage=32000000000 \
        --max_parser_depth=50000 \
        --query 'INSERT INTO bluesky_100m_variant_array.bluesky_array_data FORMAT JSONEachRow' < {temp_filename}"""
    
    result = subprocess.run(insert_cmd, shell=True, capture_output=True, text=True)
    
    # Clean up temp file
    os.unlink(temp_filename)
    
    if result.returncode == 0:
        print("✓ Successfully inserted data!")
        return True
    else:
        print(f"Insert failed: {result.stderr}")
        return False

def verify_and_expand():
    """Verify the structure and expand to full dataset if working."""
    print("\nVerifying structure...")
    
    # Check row count
    count_query = "SELECT count() FROM bluesky_100m_variant_array.bluesky_array_data"
    result = subprocess.run(['clickhouse-client', '--query', count_query], 
                          capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Row count check failed: {result.stderr}")
        return False
    
    row_count = int(result.stdout.strip())
    print(f"✓ Table has {row_count} row(s)")
    
    if row_count != 1:
        print("❌ ERROR: Expected exactly 1 row!")
        return False
    
    # Check array length
    array_length_query = "SELECT length(data.Array) FROM bluesky_100m_variant_array.bluesky_array_data"
    result = subprocess.run(['clickhouse-client', '--query', array_length_query], 
                          capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Array length check failed: {result.stderr}")
        return False
    
    array_length = int(result.stdout.strip())
    print(f"✓ Array contains {array_length:,} JSON objects")
    
    # Test array access
    test_query = """
    SELECT toString(arrayElement(data.Array, 1).kind) as first_kind
    FROM bluesky_100m_variant_array.bluesky_array_data
    """
    
    result = subprocess.run(['clickhouse-client', '--query', test_query], 
                          capture_output=True, text=True)
    
    if result.returncode == 0:
        first_kind = result.stdout.strip()
        print(f"✓ Array access working, first record kind: {first_kind}")
    else:
        print(f"Array access test failed: {result.stderr}")
        return False
    
    # If we're here, the basic concept works. Ask user if they want to expand
    print(f"\n✅ SUCCESS with {array_length:,} records!")
    print("The variant array concept is working!")
    
    if array_length < 1000000:  # Less than 1M records
        response = input("\nExpand to all 100 files? (y/n): ")
        if response.lower() == 'y':
            print("Expanding to all 100 files...")
            return create_simple_variant_array(max_files=100)
    
    return True

def main():
    """Main execution."""
    print("=" * 60)
    print("SIMPLE VARIANT ARRAY TEST")
    print("1 ROW, 1 COLUMN, JSON ARRAY STRUCTURE")
    print("=" * 60)
    
    # Start with just 5 files to test the concept
    if not create_simple_variant_array(max_files=5):
        print("❌ Failed to create variant array structure")
        return False
    
    if not verify_and_expand():
        print("❌ Verification or expansion failed")
        return False
    
    print("\n🎉 Variant array ready for benchmarking!")
    return True

if __name__ == "__main__":
    main() 