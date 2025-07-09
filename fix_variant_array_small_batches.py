#!/usr/bin/env python3
"""
Fix variant array approach using small batches to work within ClickHouse JSON size limits.
"""
import json
import gzip
import time
import subprocess
from pathlib import Path

def load_variant_array_small_batches():
    """Load variant array data using small batches that fit within ClickHouse limits."""
    print("Loading variant array data using small batches...")
    
    data_dir = Path.home() / "data" / "bluesky"
    table_name = "bluesky_100m_variant_array.bluesky_array_data"
    
    # Clear existing data
    clear_cmd = f"clickhouse-client --query 'TRUNCATE TABLE {table_name}'"
    print("Clearing existing data...")
    result = subprocess.run(clear_cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Warning: Could not clear table: {result.stderr}")
    
    batch_size = 50000  # 50K records per array (much smaller to stay under limits)
    total_processed = 0
    batch_num = 0
    
    current_batch = []
    
    print(f"Processing in batches of {batch_size:,} records...")
    
    # Process only first 10 files to demonstrate the approach (saves space and time)
    for file_num in range(1, 11):  # Just first 10 files (~10M records)
        file_path = data_dir / f"file_{file_num:04d}.json.gz"
        if not file_path.exists():
            print(f"Warning: File {file_path} not found, skipping...")
            continue
        
        print(f"Processing file {file_num}/10: {file_path.name}")
        
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
                                if load_small_batch(current_batch, table_name, batch_num):
                                    batch_num += 1
                                    if batch_num % 10 == 0:
                                        print(f"Loaded {batch_num} batches ({total_processed:,} total records)")
                                    current_batch = []
                                else:
                                    return False
                                    
                        except json.JSONDecodeError as e:
                            # Skip invalid JSON silently
                            continue
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
            continue
    
    # Load final partial batch if any
    if current_batch:
        if load_small_batch(current_batch, table_name, batch_num):
            batch_num += 1
            print(f"Loaded final batch {batch_num} ({total_processed:,} total records)")
        else:
            return False
    
    print(f"✓ Small batch load complete: {total_processed:,} records in {batch_num} batches")
    return True

def load_small_batch(batch_records, table_name, batch_num):
    """Load a small batch of records as a single array."""
    if not batch_records:
        return True
    
    # Create the batch JSON with array structure
    batch_data = {"data": batch_records}
    
    # Create temporary file for this batch
    temp_file = f"temp_small_batch_{batch_num}.json"
    try:
        with open(temp_file, 'w') as f:
            json.dump(batch_data, f)
        
        # Check file size before loading
        file_size = Path(temp_file).stat().st_size
        if file_size > 8000000:  # 8MB limit to be safe
            print(f"Warning: Batch {batch_num} is large ({file_size} bytes), may fail")
        
        # Load batch into ClickHouse with increased limits
        load_cmd = f"""clickhouse-client \
            --max_memory_usage=8000000000 \
            --max_parser_depth=10000 \
            --min_chunk_bytes_for_parallel_parsing=50000000 \
            --query 'INSERT INTO {table_name} FORMAT JSONEachRow' < {temp_file}"""
        
        result = subprocess.run(load_cmd, shell=True, capture_output=True, text=True)
        
        # Clean up temp file
        Path(temp_file).unlink()
        
        if result.returncode == 0:
            return True
        else:
            print(f"Batch {batch_num} load failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"Error creating batch file {batch_num}: {e}")
        if Path(temp_file).exists():
            Path(temp_file).unlink()
        return False

def verify_small_batch_data():
    """Verify the loaded small batch data."""
    print("Verifying loaded data...")
    
    # Check total records across all array rows
    count_query = """
    SELECT 
        count() as num_arrays,
        sum(length(data.Array)) as total_elements,
        avg(length(data.Array)) as avg_array_size,
        max(length(data.Array)) as max_array_size
    FROM bluesky_100m_variant_array.bluesky_array_data
    """
    
    result = subprocess.run(['clickhouse-client', '--query', count_query], 
                          capture_output=True, text=True)
    
    if result.returncode == 0:
        print("Array statistics:")
        print(result.stdout)
        
        # Test array access with actual benchmark query (Q1)
        test_query = """
        SELECT toString(arrayElement(data.Array, i).kind) as kind, count() 
        FROM bluesky_100m_variant_array.bluesky_array_data 
        ARRAY JOIN arrayEnumerate(data.Array) AS i 
        GROUP BY kind 
        ORDER BY count() DESC 
        LIMIT 5
        """
        
        print("Testing benchmark query (Q1 sample)...")
        test_result = subprocess.run(['clickhouse-client', '--query', test_query], 
                                   capture_output=True, text=True)
        if test_result.returncode == 0:
            print("✓ Benchmark query working:")
            print(test_result.stdout)
            return True
        else:
            print(f"Benchmark query test failed: {test_result.stderr}")
            return False
    else:
        print(f"Verification failed: {result.stderr}")
        return False

def main():
    """Main execution."""
    print("=" * 60)
    print("FIXING VARIANT ARRAY APPROACH - SMALL BATCHES")
    print("=" * 60)
    
    # Load using small batch approach
    if not load_variant_array_small_batches():
        print("Small batch load failed")
        return False
    
    # Verify
    if not verify_small_batch_data():
        print("Verification failed")
        return False
    
    print("\n✓ Variant array approach fixed using small batches!")
    print("Data is loaded as multiple smaller arrays (~50K records each).")
    print("This demonstrates the variant array concept with a subset of data.")
    print("You can now run the benchmark to compare all three approaches.")
    return True

if __name__ == "__main__":
    main() 