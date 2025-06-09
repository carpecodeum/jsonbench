#!/usr/bin/env python3
"""
Load Bluesky data into true ClickHouse Variant columns for benchmarking.
"""

import json
import gzip
import sys
from datetime import datetime

def create_true_variant_schema():
    """Create a proper true Variant schema for Bluesky data."""
    schema = """
-- True ClickHouse Variant columns for Bluesky data
DROP TABLE IF EXISTS bluesky_true_variants.bluesky_data;

CREATE TABLE bluesky_true_variants.bluesky_data
(
    -- Basic identifier fields (not variants)
    did String,
    time_us UInt64,
    kind LowCardinality(String),
    timestamp_col DateTime64(6),
    
    -- TRUE Variant columns - same field can store different types
    commit_operation Variant(String),  -- Usually string but could be NULL
    commit_collection Variant(String), -- Usually string but could be NULL
    record_data Variant(String, JSON), -- Could be text or complex JSON
    
    -- Additional metadata as variants
    commit_info Variant(JSON),          -- Full commit object
    
    -- Original JSON for comparison
    original_json JSON
)
ENGINE = MergeTree
ORDER BY (kind, did, timestamp_col)
SETTINGS 
    allow_experimental_variant_type = 1,
    use_variant_as_common_type = 1;
"""
    return schema

def load_sample_data(input_file: str, max_records: int = 10000):
    """Load sample data into true Variant columns."""
    
    print(f"Loading {max_records} records into true Variant columns...")
    
    open_func = gzip.open if input_file.endswith('.gz') else open
    mode = 'rt' if input_file.endswith('.gz') else 'r'
    
    # Prepare batch insert
    values = []
    processed = 0
    
    with open_func(input_file, mode) as f:
        for line_num, line in enumerate(f):
            if processed >= max_records:
                break
                
            try:
                record = json.loads(line.strip())
                
                # Extract basic fields
                did = record.get('did', '').replace("'", "''")
                time_us = record.get('time_us', 0)
                kind = record.get('kind', '').replace("'", "''")
                
                # Convert timestamp
                try:
                    timestamp_col = datetime.fromtimestamp(time_us / 1_000_000).strftime('%Y-%m-%d %H:%M:%S.%f')
                except:
                    timestamp_col = '1970-01-01 00:00:00.000000'
                
                # Variant fields - these can be different types
                commit_operation = record.get('commit', {}).get('operation', '') if record.get('commit') else ''
                commit_collection = record.get('commit', {}).get('collection', '') if record.get('commit') else ''
                
                # Record data as variant - sometimes simple text, sometimes complex JSON
                record_obj = record.get('record', {})
                if record_obj:
                    if '$type' in record_obj and record_obj['$type'] == 'app.bsky.feed.post':
                        # Simple text for posts
                        record_data = record_obj.get('text', '')[:100]  # Truncate for demo
                    else:
                        # Complex JSON for other types
                        record_data = json.dumps(record_obj)
                else:
                    record_data = ''
                
                # Commit info as JSON variant
                commit_info = json.dumps(record.get('commit', {})) if record.get('commit') else '{}'
                
                # Original JSON
                original_json = json.dumps(record)
                
                # Build row values
                row_values = [
                    f"'{did}'",
                    str(time_us),
                    f"'{kind}'",
                    f"'{timestamp_col}'",
                    f"'{commit_operation.replace(chr(39), chr(39)+chr(39))}'" if commit_operation else 'NULL',
                    f"'{commit_collection.replace(chr(39), chr(39)+chr(39))}'" if commit_collection else 'NULL',
                    f"'{record_data.replace(chr(39), chr(39)+chr(39))}'" if record_data else 'NULL',
                    f"'{commit_info.replace(chr(39), chr(39)+chr(39))}'",
                    f"'{original_json.replace(chr(39), chr(39)+chr(39))}'"
                ]
                
                values.append(f"({', '.join(row_values)})")
                processed += 1
                
                # Insert in batches of 1000
                if len(values) >= 1000:
                    insert_batch(values)
                    values = []
                    print(f"Loaded {processed} records...")
                
            except Exception as e:
                print(f"Error processing line {line_num + 1}: {e}")
                continue
    
    # Insert remaining values
    if values:
        insert_batch(values)
    
    print(f"Successfully loaded {processed} records into true Variant columns")
    return processed

def insert_batch(values):
    """Insert a batch of values."""
    import subprocess
    
    query = f"""
    INSERT INTO bluesky_true_variants.bluesky_data VALUES 
    {', '.join(values)}
    """
    
    # Write to temp file and execute
    with open('/tmp/batch_insert.sql', 'w') as f:
        f.write(query)
    
    result = subprocess.run(
        ['clickhouse', 'client', '--queries-file', '/tmp/batch_insert.sql'],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"Insert error: {result.stderr}")

def main():
    import subprocess
    
    # Create schema
    schema = create_true_variant_schema()
    with open('/tmp/create_variants.sql', 'w') as f:
        f.write(schema)
    
    print("Creating true Variant columns schema...")
    result = subprocess.run(
        ['clickhouse', 'client', '--queries-file', '/tmp/create_variants.sql'],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"Schema creation error: {result.stderr}")
        return
    
    print("Schema created successfully!")
    
    # Load data
    input_file = sys.argv[1] if len(sys.argv) > 1 else "~/data/bluesky/file_0001.json.gz"
    max_records = int(sys.argv[2]) if len(sys.argv) > 2 else 10000
    
    load_sample_data(input_file, max_records)

if __name__ == '__main__':
    main() 