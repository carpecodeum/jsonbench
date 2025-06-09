#!/usr/bin/env python3
"""
JSON to Variant Columns Preprocessor for ClickHouse
Converts JSON Bluesky data to TSV format with extracted typed columns
"""

import json
import gzip
import sys
import argparse
from datetime import datetime
from typing import Optional

def extract_fields(record: dict) -> tuple:
    """
    Extract fields from JSON record for variant columns.
    Returns tuple of values in order matching the schema.
    """
    did = record.get('did', '')
    time_us = record.get('time_us', 0)
    kind = record.get('kind', '')
    
    timestamp_col = datetime.fromtimestamp(time_us / 1_000_000) if time_us else None
    
    commit = record.get('commit', {})
    commit_rev = commit.get('rev', '') if commit else ''
    commit_operation = commit.get('operation', '') if commit else ''
    commit_collection = commit.get('collection', '') if commit else ''
    commit_rkey = commit.get('rkey', '') if commit else ''
    commit_cid = commit.get('cid', '') if commit else ''
    
    record_type = ''
    if commit and 'record' in commit:
        record_data = commit['record']
        if isinstance(record_data, dict):
            record_type = record_data.get('$type', '')
    
    original_json = json.dumps(record, separators=(',', ':'))
    
    return (
        did,
        time_us,
        kind,
        timestamp_col.isoformat() if timestamp_col else '',
        commit_rev,
        commit_operation,
        commit_collection,
        commit_rkey,
        commit_cid,
        record_type,
        original_json
    )

def escape_tsv_value(value) -> str:
    """Escape value for TSV format"""
    if value is None:
        return ''
    
    str_value = str(value)
    # Escape tabs, newlines, and backslashes
    str_value = str_value.replace('\\', '\\\\')
    str_value = str_value.replace('\t', '\\t')
    str_value = str_value.replace('\n', '\\n')
    str_value = str_value.replace('\r', '\\r')
    
    return str_value

def process_file(input_file: str, output_file: str, max_records: Optional[int] = None):
    """Process JSON file and convert to TSV with variant columns"""
    
    print(f"Processing {input_file} -> {output_file}")
    records_processed = 0
    
    if input_file.endswith('.gz'):
        input_handle = gzip.open(input_file, 'rt', encoding='utf-8')
    else:
        input_handle = open(input_file, 'r', encoding='utf-8')
    
    try:
        with input_handle, open(output_file, 'w', encoding='utf-8') as out_f:
            headers = [
                'did', 'time_us', 'kind', 'timestamp_col',
                'commit_rev', 'commit_operation', 'commit_collection',
                'commit_rkey', 'commit_cid', 'record_type', 'original_json'
            ]
            out_f.write('\t'.join(headers) + '\n')
            
            for line in input_handle:
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    record = json.loads(line)
                    fields = extract_fields(record)
                    
                    # Escape and write fields
                    escaped_fields = [escape_tsv_value(field) for field in fields]
                    out_f.write('\t'.join(escaped_fields) + '\n')
                    
                    records_processed += 1
                    
                    if records_processed % 50000 == 0:
                        print(f"Processed {records_processed} records...")
                    
                    if max_records and records_processed >= max_records:
                        break
                        
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON on line {records_processed + 1}: {e}")
                    continue
                except Exception as e:
                    print(f"Error processing record {records_processed + 1}: {e}")
                    continue
    
    finally:
        input_handle.close()
    
    print(f"Successfully processed {records_processed} records")
    return records_processed

def main():
    parser = argparse.ArgumentParser(description='Convert JSON Bluesky data to TSV with variant columns')
    parser.add_argument('input_file', help='Input JSON file (.json or .json.gz)')
    parser.add_argument('output_file', help='Output TSV file')
    parser.add_argument('--max-records', type=int, help='Maximum number of records to process')
    
    args = parser.parse_args()
    
    try:
        records = process_file(args.input_file, args.output_file, args.max_records)
        print(f"Conversion completed successfully. {records} records written to {args.output_file}")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 