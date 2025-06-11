#!/usr/bin/env python3
"""
Load Bluesky data into true ClickHouse Variant columns for benchmarking.
"""

import gzip
import json
import sys
from datetime import datetime
import subprocess

def extract_fields(record):
    """Extract and convert fields for True Variants storage"""
    try:
        # Basic fields
        did = record.get('did', '')
        time_us = record.get('time_us', 0)
        kind = record.get('kind', '')
        
        # Convert timestamp
        timestamp_col = None
        if time_us and time_us > 0:
            timestamp_col = datetime.fromtimestamp(time_us / 1_000_000).strftime('%Y-%m-%d %H:%M:%S.%f')
        
        # Commit fields
        commit = record.get('commit', {})
        commit_rev = commit.get('rev', '') if commit else ''
        commit_operation = commit.get('operation', '') if commit else ''
        commit_collection = commit.get('collection', '') if commit else ''
        commit_rkey = commit.get('rkey', '') if commit else ''
        commit_cid = commit.get('cid', '') if commit else ''
        
        # Record type
        record_data = commit.get('record', {}) if commit else {}
        record_type = record_data.get('$type', '') if record_data else ''
        
        return {
            'did': did,
            'time_us': time_us,
            'kind': kind,
            'timestamp_col': timestamp_col,
            'commit_rev': commit_rev,
            'commit_operation': commit_operation,
            'commit_collection': commit_collection,
            'commit_rkey': commit_rkey,
            'commit_cid': commit_cid,
            'record_type': record_type
        }
    except Exception as e:
        print(f"Error extracting fields: {e}", file=sys.stderr)
        return None

def main():
    input_file = sys.argv[1] if len(sys.argv) > 1 else '/Users/adityabhatnagar/data/bluesky/file_0001.json.gz'
    
    print("Loading True Variants table (Variant columns, no JSON)...")
    
    # Create database and table
    subprocess.run(['clickhouse', 'client', '--queries-file', 'create_true_variants.sql'], check=True)
    
    batch_size = 100
    batch = []
    total_processed = 0
    
    with gzip.open(input_file, 'rt', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                record = json.loads(line.strip())
                fields = extract_fields(record)
                
                if fields:
                    batch.append(fields)
                    
                    if len(batch) >= batch_size:
                        insert_batch(batch)
                        total_processed += len(batch)
                        print(f"Processed {total_processed} records...")
                        batch = []
                        
                        # Stop at 1M records for consistency
                        if total_processed >= 1_000_000:
                            break
                            
            except json.JSONDecodeError as e:
                print(f"JSON decode error at line {line_num}: {e}", file=sys.stderr)
                continue
            except Exception as e:
                print(f"Error at line {line_num}: {e}", file=sys.stderr)
                continue
    
    # Insert remaining batch
    if batch:
        insert_batch(batch)
        total_processed += len(batch)
    
    print(f"Total records loaded: {total_processed}")

def insert_batch(batch):
    """Insert a batch of records using INSERT VALUES"""
    if not batch:
        return
    
    # Build VALUES clause
    values = []
    for record in batch:
        # Format values, handling None/null values
        did = f"'{record['did']}'" if record['did'] else 'NULL'
        time_us = str(record['time_us']) if record['time_us'] else 'NULL'
        kind = f"'{record['kind']}'" if record['kind'] else 'NULL'
        timestamp_col = f"'{record['timestamp_col']}'" if record['timestamp_col'] else 'NULL'
        commit_rev = f"'{record['commit_rev']}'" if record['commit_rev'] else 'NULL'
        commit_operation = f"'{record['commit_operation']}'" if record['commit_operation'] else 'NULL'
        commit_collection = f"'{record['commit_collection']}'" if record['commit_collection'] else 'NULL'
        commit_rkey = f"'{record['commit_rkey']}'" if record['commit_rkey'] else 'NULL'
        commit_cid = f"'{record['commit_cid']}'" if record['commit_cid'] else 'NULL'
        record_type = f"'{record['record_type']}'" if record['record_type'] else 'NULL'
        
        values.append(f"({did}, {time_us}, {kind}, {timestamp_col}, {commit_rev}, {commit_operation}, {commit_collection}, {commit_rkey}, {commit_cid}, {record_type})")
    
    query = f"""
    INSERT INTO bluesky_true_variants.bluesky_data 
    (did, time_us, kind, timestamp_col, commit_rev, commit_operation, commit_collection, commit_rkey, commit_cid, record_type)
    VALUES {', '.join(values)}
    """
    
    try:
        subprocess.run(['clickhouse', 'client', '--query', query], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Insert error: {e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    main() 