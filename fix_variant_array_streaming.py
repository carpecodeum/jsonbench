#!/usr/bin/env python3
"""
Fix variant array approach using streaming batches to avoid disk space issues.
Instead of one massive array, create multiple smaller arrays.
"""
import json
import gzip
import time
import subprocess
from pathlib import Path

def load_variant_array_streaming():
    """Load variant array data using streaming approach with smaller batches."""
    print("Loading variant array data using streaming approach...")
    
    data_dir = Path.home() / "data" / "bluesky"
    table_name = "bluesky_100m_variant_array.bluesky_array_data"
    
    # Clear existing data
    clear_cmd = f"clickhouse-client --query 'TRUNCATE TABLE {table_name}'"
    print("Clearing existing data...")
    result = subprocess.run(clear_cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Warning: Could not clear table: {result.stderr}")
    
    batch_size = 1000000  # 1M records per array
    total_processed = 0
    batch_num = 0
    
    current_batch = []
    
    print(f"Processing in batches of {batch_size:,} records...")
    
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
                            parsed = json.loads(line)
                            current_batch.append(parsed)
                            total_processed += 1
                            
                            # When batch is full, load it
                            if len(current_batch) >= batch_size:
                                if load_batch(current_batch, table_name, batch_num):
                                    batch_num += 1
                                    print(f"Loaded batch {batch_num} ({total_processed:,} total records)")
                                    current_batch = []
                                else:
                                    return False
                                    
                        except json.JSONDecodeError as e:
                            # Skip invalid JSON silently for cleaner output
                            continue
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
            continue
    
    # Load final partial batch if any
    if current_batch:
        if load_batch(current_batch, table_name, batch_num):
            batch_num += 1
            print(f"Loaded final batch {batch_num} ({total_processed:,} total records)")
        else:
            return False
    
    print(f"✓ Streaming load complete: {total_processed:,} records in {batch_num} batches")
    return True

def load_batch(batch_records, table_name, batch_num):
    """Load a batch of records as a single array."""
    if not batch_records:
        return True
    
    # Create the batch JSON with array structure
    batch_data = {"data": batch_records}
    
    # Create temporary file for this batch
    temp_file = f"temp_batch_{batch_num}.json"
    try:
        with open(temp_file, 'w') as f:
            json.dump(batch_data, f)
        
        # Load batch into ClickHouse
        load_cmd = f"clickhouse-client --max_memory_usage=8000000000 --max_parser_depth=10000 --query 'INSERT INTO {table_name} FORMAT JSONEachRow' < {temp_file}"
        result = subprocess.run(load_cmd, shell=True, capture_output=True, text=True)
        
        # Clean up temp file
        Path(temp_file).unlink()
        
        if result.returncode == 0:
            return True
        else:
            print(f"Batch load failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"Error creating batch file: {e}")
        if Path(temp_file).exists():
            Path(temp_file).unlink()
        return False

def verify_streaming_data():
    """Verify the loaded streaming data."""
    print("Verifying loaded streaming data...")
    
    # Check total records across all array rows
    count_query = """
    SELECT 
        count() as num_arrays,
        sum(length(data.Array)) as total_elements
    FROM bluesky_100m_variant_array.bluesky_array_data
    """
    
    result = subprocess.run(['clickhouse-client', '--query', count_query], 
                          capture_output=True, text=True)
    
    if result.returncode == 0:
        print("Array statistics:")
        print(result.stdout)
        
        # Test array access with a simple query
        test_query = """
        SELECT 
            toString(arrayElement(data.Array, 1).kind) as first_kind,
            count() as arrays_with_data
        FROM bluesky_100m_variant_array.bluesky_array_data 
        WHERE length(data.Array) > 0
        GROUP BY first_kind
        LIMIT 5
        """
        
        test_result = subprocess.run(['clickhouse-client', '--query', test_query], 
                                   capture_output=True, text=True)
        if test_result.returncode == 0:
            print("✓ Array access working:")
            print(test_result.stdout)
            return True
        else:
            print(f"Array access test failed: {test_result.stderr}")
            return False
    else:
        print(f"Verification failed: {result.stderr}")
        return False

def main():
    """Main execution."""
    print("=" * 60)
    print("FIXING VARIANT ARRAY APPROACH - STREAMING VERSION")
    print("=" * 60)
    
    # Load using streaming approach
    if not load_variant_array_streaming():
        print("Streaming load failed")
        return False
    
    # Verify
    if not verify_streaming_data():
        print("Verification failed")
        return False
    
    print("\n✓ Variant array approach fixed using streaming batches!")
    print("The data is now loaded as multiple arrays instead of one massive array.")
    print("You can now run the benchmark with this approach.")
    return True

if __name__ == "__main__":
    main() 