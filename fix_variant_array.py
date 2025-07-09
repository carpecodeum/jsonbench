#!/usr/bin/env python3
"""
Fix variant array approach by properly creating and loading the array data.
"""
import json
import gzip
import time
import subprocess
from pathlib import Path

def create_variant_array_data():
    """Create the variant array JSON file properly."""
    print("Creating variant array data file...")
    
    data_dir = Path.home() / "data" / "bluesky"
    array_data_file = "bluesky_100m_array.json"
    
    # Remove incomplete file if it exists
    if Path(array_data_file).exists():
        print(f"Removing incomplete file: {array_data_file}")
        Path(array_data_file).unlink()
    
    processed = 0
    
    # Write JSON array directly
    with open(array_data_file, 'w') as output_file:
        output_file.write('{"data": [')
        
        first_object = True
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
                        if line:
                            try:
                                # Validate JSON
                                json.loads(line)
                                
                                if not first_object:
                                    output_file.write(',')
                                output_file.write(line)
                                first_object = False
                                processed += 1
                                
                                if processed % 1000000 == 0:
                                    print(f"Processed {processed:,} records...")
                            except json.JSONDecodeError as e:
                                print(f"Skipping invalid JSON: {line[:50]}... Error: {e}")
                                continue
            except Exception as e:
                print(f"Error reading file {file_path}: {e}")
                continue
        
        output_file.write(']}')
    
    print(f"Array data file created with {processed:,} JSON objects: {array_data_file}")
    
    # Check file size
    file_size = Path(array_data_file).stat().st_size
    print(f"Array file size: {file_size:,} bytes ({file_size/1024/1024:.1f} MB)")
    
    return array_data_file, processed

def load_variant_array_data(array_file):
    """Load the variant array data into ClickHouse."""
    print("Loading variant array data into ClickHouse...")
    
    table_name = "bluesky_100m_variant_array.bluesky_array_data"
    
    # Clear any existing data first
    clear_cmd = f"clickhouse-client --query 'TRUNCATE TABLE {table_name}'"
    print("Clearing existing data...")
    result = subprocess.run(clear_cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Warning: Could not clear table: {result.stderr}")
    
    # Load array data
    start_time = time.time()
    load_cmd = f"clickhouse-client --max_memory_usage=16000000000 --max_parser_depth=10000 --query 'INSERT INTO {table_name} FORMAT JSONEachRow' < {array_file}"
    
    print("Loading data (this may take a few minutes)...")
    result = subprocess.run(load_cmd, shell=True, capture_output=True, text=True)
    
    load_time = time.time() - start_time
    
    if result.returncode == 0:
        print(f"✓ Variant array data loaded successfully in {load_time:.1f}s")
        return True
    else:
        print(f"✗ Loading failed: {result.stderr}")
        if result.stdout:
            print(f"Stdout: {result.stdout}")
        return False

def verify_data():
    """Verify the loaded data."""
    print("Verifying loaded data...")
    
    # Check record count
    count_query = "SELECT count() FROM bluesky_100m_variant_array.bluesky_array_data"
    result = subprocess.run(['clickhouse-client', '--query', count_query], 
                          capture_output=True, text=True)
    
    if result.returncode == 0:
        count = int(result.stdout.strip())
        print(f"Records in variant_array table: {count:,}")
        
        if count > 0:
            # Test array access
            test_query = "SELECT arrayElement(data.Array, 1).kind as first_kind, arrayElement(data.Array, arrayJoin(range(1, least(6, length(data.Array)+1)))).kind as sample_kinds FROM bluesky_100m_variant_array.bluesky_array_data LIMIT 5"
            test_result = subprocess.run(['clickhouse-client', '--query', test_query], 
                                       capture_output=True, text=True)
            if test_result.returncode == 0:
                print("✓ Array access working:")
                print(test_result.stdout)
            else:
                print(f"Array access test failed: {test_result.stderr}")
                
        return count > 0
    else:
        print(f"Verification failed: {result.stderr}")
        return False

def main():
    """Main execution."""
    print("=" * 60)
    print("FIXING VARIANT ARRAY APPROACH")
    print("=" * 60)
    
    # Step 1: Create array data file
    array_file, processed = create_variant_array_data()
    
    if processed == 0:
        print("No data processed. Exiting.")
        return False
    
    # Step 2: Load into ClickHouse
    if not load_variant_array_data(array_file):
        return False
    
    # Step 3: Verify
    if not verify_data():
        return False
    
    print("\n✓ Variant array approach fixed successfully!")
    print("You can now run the full benchmark.")
    return True

if __name__ == "__main__":
    main() 