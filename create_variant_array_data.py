#!/usr/bin/env python3
"""
Create the variant array data structure: 1 row with 1 column containing all 100M JSON objects as an array.
"""
import json
import gzip
import subprocess
from pathlib import Path

def create_combined_jsonl():
    """Create combined JSONL file from all gzipped files."""
    print("Creating combined JSONL file from all data files...")
    
    data_dir = Path.home() / "data" / "bluesky"
    output_file = "bluesky_100m_combined.jsonl"
    
    # Remove existing file if it exists
    if Path(output_file).exists():
        Path(output_file).unlink()
    
    total_records = 0
    
    with open(output_file, 'w') as outf:
        for file_num in range(1, 101):  # Process all 100 files
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
                            # Validate JSON and write to combined file
                            try:
                                json.loads(line)  # Validate
                                outf.write(line + '\n')
                                total_records += 1
                            except json.JSONDecodeError:
                                continue  # Skip invalid JSON
            except Exception as e:
                print(f"Error reading file {file_path}: {e}")
                continue
            
            if file_num % 10 == 0:
                print(f"  Processed {total_records:,} records so far...")
    
    print(f"✓ Combined JSONL created: {total_records:,} records")
    return total_records

def create_variant_array_structure():
    """Create the variant array data structure with all 100M records in 1 row."""
    print("Creating variant array structure (1 row, 1 column, all JSON objects)...")
    
    # First ensure we have the combined JSONL
    if not Path("bluesky_100m_combined.jsonl").exists():
        create_combined_jsonl()
    
    # Create the database and table
    print("Creating database and table...")
    
    # Create database
    db_cmd = "clickhouse-client --query 'CREATE DATABASE IF NOT EXISTS bluesky_100m_variant_array'"
    result = subprocess.run(db_cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Database creation failed: {result.stderr}")
        return False
    
    # Drop table if exists
    drop_cmd = "clickhouse-client --query 'DROP TABLE IF EXISTS bluesky_100m_variant_array.bluesky_array_data'"
    subprocess.run(drop_cmd, shell=True, capture_output=True, text=True)
    
    # Create table with variant array structure
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
    
    # Now create the array structure
    print("Reading all JSON records into memory...")
    
    all_records = []
    record_count = 0
    
    with open("bluesky_100m_combined.jsonl", 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    record = json.loads(line)
                    all_records.append(record)
                    record_count += 1
                    
                    if record_count % 1000000 == 0:
                        print(f"  Loaded {record_count:,} records into memory...")
                        
                except json.JSONDecodeError:
                    continue
    
    print(f"✓ Loaded {record_count:,} records into memory")
    
    # Create the single-row array structure
    print("Creating single-row array structure...")
    
    array_data = {"data": all_records}
    
    # Write to temporary file
    temp_file = "variant_array_single_row.json"
    with open(temp_file, 'w') as f:
        json.dump(array_data, f)
    
    print(f"✓ Created array structure file")
    
    # Insert into ClickHouse
    print("Inserting single row with all data...")
    
    insert_cmd = f"""clickhouse-client \
        --max_memory_usage=16000000000 \
        --max_parser_depth=20000 \
        --query 'INSERT INTO bluesky_100m_variant_array.bluesky_array_data FORMAT JSONEachRow' < {temp_file}"""
    
    result = subprocess.run(insert_cmd, shell=True, capture_output=True, text=True)
    
    # Clean up temp file
    Path(temp_file).unlink()
    
    if result.returncode == 0:
        print("✓ Successfully inserted single row with all data!")
        return True
    else:
        print(f"Insert failed: {result.stderr}")
        return False

def verify_variant_array():
    """Verify the variant array structure."""
    print("Verifying variant array structure...")
    
    # Check row count (should be 1)
    count_query = "SELECT count() FROM bluesky_100m_variant_array.bluesky_array_data"
    result = subprocess.run(['clickhouse-client', '--query', count_query], 
                          capture_output=True, text=True)
    
    if result.returncode == 0:
        row_count = int(result.stdout.strip())
        print(f"✓ Table has {row_count} row(s)")
        
        if row_count != 1:
            print("❌ Expected exactly 1 row!")
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
            print("❌ Array length seems too small!")
            return False
    else:
        print(f"Array length check failed: {result.stderr}")
        return False
    
    # Test a simple query
    test_query = """
    SELECT toString(arrayElement(data.Array, 1).kind) as first_kind
    FROM bluesky_100m_variant_array.bluesky_array_data
    """
    
    result = subprocess.run(['clickhouse-client', '--query', test_query], 
                          capture_output=True, text=True)
    
    if result.returncode == 0:
        first_kind = result.stdout.strip()
        print(f"✓ Array access working, first record kind: {first_kind}")
        return True
    else:
        print(f"Array access test failed: {result.stderr}")
        return False

def main():
    """Main execution."""
    print("=" * 60)
    print("CREATING VARIANT ARRAY DATA STRUCTURE")
    print("1 ROW, 1 COLUMN, ALL 100M JSON OBJECTS")
    print("=" * 60)
    
    # Create the variant array structure
    if not create_variant_array_structure():
        print("Failed to create variant array structure")
        return False
    
    # Verify it
    if not verify_variant_array():
        print("Verification failed")
        return False
    
    print("\n✅ SUCCESS: Variant array structure created!")
    print("- 1 row in the table")
    print("- 1 column of type Variant(Array(JSON))")
    print("- All ~100M JSON objects stored as a single array")
    print("\nReady to run benchmark!")
    return True

if __name__ == "__main__":
    main() 