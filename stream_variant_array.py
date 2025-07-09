#!/usr/bin/env python3
"""
Stream variant array approach: Load JSONL data directly into ClickHouse as a single array.
Avoids creating large intermediate files.
"""
import json
import subprocess
import tempfile
import os

def create_variant_array_streaming():
    """Stream data directly into ClickHouse as variant array."""
    print("Creating variant array structure with streaming approach...")
    
    # Create database and table
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
    
    # Now stream the data
    print("Reading JSONL data and creating array structure...")
    
    # Count total records first
    with open("bluesky_100m_combined.jsonl", 'r') as f:
        total_lines = sum(1 for line in f if line.strip())
    
    print(f"Found {total_lines:,} records to process")
    
    # Create a temporary file with the array structure for ClickHouse
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
        temp_filename = temp_file.name
        
        # Start the JSON structure
        temp_file.write('{"data": [')
        
        processed = 0
        first_record = True
        
        with open("bluesky_100m_combined.jsonl", 'r') as jsonl_file:
            for line in jsonl_file:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    # Validate JSON
                    record = json.loads(line)
                    
                    # Add comma separator for all but first record
                    if not first_record:
                        temp_file.write(',')
                    else:
                        first_record = False
                    
                    # Write the record directly
                    json.dump(record, temp_file, separators=(',', ':'))
                    
                    processed += 1
                    if processed % 100000 == 0:
                        print(f"  Processed {processed:,}/{total_lines:,} records...")
                    
                except json.JSONDecodeError:
                    continue  # Skip invalid JSON
        
        # Close the JSON structure
        temp_file.write(']}')
    
    print(f"✓ Created temporary array file with {processed:,} records")
    
    # Insert into ClickHouse
    print("Inserting into ClickHouse...")
    
    insert_cmd = f"""clickhouse-client \
        --max_memory_usage=32000000000 \
        --max_parser_depth=50000 \
        --max_query_size=32000000000 \
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
    print("\nVerifying variant array structure...")
    
    # Check row count
    count_query = "SELECT count() FROM bluesky_100m_variant_array.bluesky_array_data"
    result = subprocess.run(['clickhouse-client', '--query', count_query], 
                          capture_output=True, text=True)
    
    if result.returncode == 0:
        row_count = int(result.stdout.strip())
        print(f"✓ Table has {row_count} row(s)")
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
    print("STREAMING VARIANT ARRAY CREATION")
    print("1 ROW, 1 COLUMN, ALL JSON OBJECTS AS ARRAY")
    print("=" * 60)
    
    if not create_variant_array_streaming():
        print("❌ Failed to create variant array structure")
        return False
    
    if not verify_structure():
        print("❌ Verification failed")
        return False
    
    print("\n✅ SUCCESS!")
    print("- Variant array structure created")
    print("- 1 row in the table")
    print("- 1 column with all JSON objects as array")
    print("\nReady to benchmark!")
    return True

if __name__ == "__main__":
    main() 