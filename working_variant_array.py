#!/usr/bin/env python3
"""
Working variant array approach with proper ClickHouse settings for large JSON objects.
"""
import json
import gzip
import subprocess
import tempfile
import os
from pathlib import Path

def create_working_variant_array(max_files=10):
    """Create variant array with proper settings for large JSON objects."""
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
                        
                        if total_processed % 500000 == 0:
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
    
    file_size = os.path.getsize(temp_filename)
    print(f"✓ Created temporary file: {temp_filename} ({file_size:,} bytes)")
    
    # Insert into ClickHouse with proper settings for large JSON
    print("Inserting into ClickHouse with large JSON settings...")
    
    insert_cmd = f"""clickhouse-client \
        --max_memory_usage=64000000000 \
        --max_parser_depth=100000 \
        --min_chunk_bytes_for_parallel_parsing=1073741824 \
        --max_read_buffer_size=1073741824 \
        --input_format_parallel_parsing=0 \
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

def verify_structure():
    """Verify the variant array structure."""
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
    print("WORKING VARIANT ARRAY WITH LARGE JSON SUPPORT")
    print("1 ROW, 1 COLUMN, JSON ARRAY STRUCTURE")
    print("=" * 60)
    
    # Start with 10 files (about 10M records)
    if not create_working_variant_array(max_files=10):
        print("❌ Failed to create variant array structure")
        return False
    
    if not verify_structure():
        print("❌ Verification failed")
        return False
    
    print("\n🎉 SUCCESS! Variant array working!")
    print("✓ 1 row created")
    print("✓ 1 column: Variant(Array(JSON))")
    print("✓ Large JSON objects supported")
    
    # Ask if user wants to expand to all 100 files
    response = input("\nExpand to all 100 files? This will take some time (y/n): ")
    if response.lower() == 'y':
        print("Expanding to all 100 files...")
        if create_working_variant_array(max_files=100):
            print("\n🎉 ALL 100M RECORDS LOADED!")
            print("Ready for benchmarking!")
        else:
            print("❌ Failed to expand to 100 files")
    
    return True

if __name__ == "__main__":
    main() 