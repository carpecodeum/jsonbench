#!/usr/bin/env python3
"""
Fixed JSON to True Variants Preprocessor
Simplified approach that avoids complex escaping issues.
"""

import json
import gzip
import sys
import argparse
from datetime import datetime
from typing import Optional

def safe_json_escape(value):
    """Safely escape JSON for SQL insertion."""
    if value is None:
        return 'NULL'
    
    # Convert to JSON string, then escape single quotes
    json_str = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
    return "'" + json_str.replace("'", "''") + "'"

def safe_string_escape(value):
    """Safely escape string for SQL insertion."""
    if not value:
        return 'NULL'
    return "'" + str(value).replace("'", "''").replace('\\', '\\\\') + "'"

def extract_variant_fields(record: dict) -> dict:
    """Extract fields that will become variant columns."""
    
    # Get commit data
    commit = record.get('commit', {})
    
    return {
        'did': record.get('did', ''),
        'time_us': record.get('time_us', 0),
        'kind': record.get('kind', ''),
        'timestamp_col': None,  # Will be calculated
        'commit_operation': commit.get('operation'),
        'commit_collection': commit.get('collection'),
        'commit_rev': commit.get('rev'),
        'commit_rkey': commit.get('rkey'),
        'commit_cid': commit.get('cid'),
        'record_data': record.get('record'),
        'original_json': record
    }

def create_schema() -> str:
    """Generate the true variants schema."""
    return """
-- True ClickHouse Variant columns schema (Fixed)
CREATE DATABASE IF NOT EXISTS bluesky_true_variants;

DROP TABLE IF EXISTS bluesky_true_variants.bluesky_data;

CREATE TABLE bluesky_true_variants.bluesky_data
(
    -- Core identity fields
    did String,
    time_us UInt64,
    kind LowCardinality(String),
    timestamp_col DateTime64(6),
    
    -- TRUE Variant columns
    commit_operation Variant(String),
    commit_collection Variant(String),
    commit_rev Variant(String),
    commit_rkey Variant(String),
    commit_cid Variant(String),
    
    -- Record data as variant (JSON or String)
    record_data Variant(JSON, String),
    
    -- Original JSON for comparison
    original_json JSON
)
ENGINE = MergeTree
ORDER BY (kind, did, timestamp_col)
SETTINGS 
    allow_experimental_variant_type = 1,
    use_variant_as_common_type = 1;
"""

def process_file(input_file: str, output_file: str, max_records: Optional[int] = None):
    """Process JSON file and convert to loadable SQL format."""
    
    print(f"Processing {input_file} -> {output_file}")
    if max_records:
        print(f"Limiting to {max_records} records")
    
    processed = 0
    
    # Determine file type
    open_func = gzip.open if input_file.endswith('.gz') else open
    mode = 'rt' if input_file.endswith('.gz') else 'r'
    
    with open_func(input_file, mode, encoding='utf-8') as in_f, \
         open(output_file, 'w', encoding='utf-8') as out_f:
        
        # Write SQL header
        out_f.write("-- Generated SQL for true variants loading\n")
        out_f.write("-- Use: clickhouse local --queries-file <this_file>\n\n")
        out_f.write(create_schema())
        out_f.write("\n\n-- Data insertion\n")
        out_f.write("INSERT INTO bluesky_true_variants.bluesky_data VALUES\n")
        
        first_record = True
        
        for line_num, line in enumerate(in_f):
            if max_records and processed >= max_records:
                break
                
            line = line.strip()
            if not line:
                continue
            
            try:
                record = json.loads(line)
                fields = extract_variant_fields(record)
                
                # Calculate timestamp
                try:
                    timestamp_col = datetime.fromtimestamp(fields['time_us'] / 1_000_000)
                    timestamp_str = timestamp_col.strftime('%Y-%m-%d %H:%M:%S.%f')
                except:
                    timestamp_str = '1970-01-01 00:00:00.000000'
                
                # Build SQL VALUES clause
                values = [
                    safe_string_escape(fields['did']),
                    str(fields['time_us']),
                    safe_string_escape(fields['kind']),
                    f"'{timestamp_str}'",
                    safe_string_escape(fields['commit_operation']),
                    safe_string_escape(fields['commit_collection']),
                    safe_string_escape(fields['commit_rev']),
                    safe_string_escape(fields['commit_rkey']),
                    safe_string_escape(fields['commit_cid']),
                    safe_json_escape(fields['record_data']),
                    safe_json_escape(fields['original_json'])
                ]
                
                # Add comma for continuation
                prefix = ",\n" if not first_record else ""
                out_f.write(f"{prefix}({', '.join(values)})")
                first_record = False
                
                processed += 1
                
                if processed % 10000 == 0:
                    print(f"Processed {processed} records...")
                    
            except json.JSONDecodeError as e:
                print(f"JSON error on line {line_num + 1}: {e}")
                continue
            except Exception as e:
                print(f"Processing error on line {line_num + 1}: {e}")
                continue
        
        # End the INSERT statement
        out_f.write(";\n\n")
        
        # Add verification queries
        out_f.write("""
-- Verification queries
SELECT 'Record count:' as check, count() as value FROM bluesky_true_variants.bluesky_data;

SELECT 'Variant types:' as check, 
       variantType(commit_operation) as op_type,
       variantType(record_data) as data_type,
       count() as cnt
FROM bluesky_true_variants.bluesky_data 
GROUP BY op_type, data_type;

SELECT 'Top events:' as check,
       variantElement(commit_collection, 'String') as event,
       count() as cnt
FROM bluesky_true_variants.bluesky_data 
WHERE commit_collection IS NOT NULL
GROUP BY event ORDER BY cnt DESC LIMIT 5;
""")
    
    print(f"Successfully processed {processed} records")
    print(f"Generated SQL file: {output_file}")
    print(f"To load: clickhouse local --queries-file {output_file}")
    
    return processed

def main():
    parser = argparse.ArgumentParser(description='Convert JSON to True Variants SQL')
    parser.add_argument('input_file', help='Input JSON file (.json or .json.gz)')
    parser.add_argument('output_file', help='Output SQL file')
    parser.add_argument('--max-records', type=int, help='Maximum records to process')
    parser.add_argument('--sample', action='store_true', help='Process only 10,000 records for testing')
    
    args = parser.parse_args()
    
    if args.sample:
        args.max_records = 10000
        print("Sample mode: processing only 10,000 records")
    
    try:
        records = process_file(args.input_file, args.output_file, args.max_records)
        print(f"\n✓ Conversion completed successfully!")
        print(f"Records processed: {records}")
        print(f"Output: {args.output_file}")
        
        # Show next steps
        print(f"\nNext steps:")
        print(f"1. Load data: clickhouse local --queries-file {args.output_file}")
        print(f"2. Or use the fixed loader: python3 load_true_variants_fixed.py all")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main() 