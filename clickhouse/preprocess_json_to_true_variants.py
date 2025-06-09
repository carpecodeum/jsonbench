#!/usr/bin/env python3
"""
True JSON to ClickHouse Variant columns converter.
This demonstrates actual Variant column usage, not just field extraction.
"""

import json
import gzip
import sys
from typing import Optional, Any, Union
from datetime import datetime

def analyze_json_field_types(records: list, field_path: str) -> set:
    """Analyze what types a JSON field contains across records."""
    types_found = set()
    
    for record in records:
        value = get_nested_value(record, field_path)
        if value is not None:
            if isinstance(value, str):
                types_found.add('String')
            elif isinstance(value, int):
                types_found.add('UInt64')
            elif isinstance(value, float):
                types_found.add('Float64')
            elif isinstance(value, bool):
                types_found.add('Bool')
            elif isinstance(value, list):
                types_found.add('Array(String)')  # Simplified
            elif isinstance(value, dict):
                types_found.add('JSON')
    
    return types_found

def get_nested_value(obj: dict, path: str) -> Any:
    """Get value from nested dictionary using dot notation."""
    keys = path.split('.')
    current = obj
    
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    
    return current

def convert_to_variant_value(value: Any) -> str:
    """Convert Python value to ClickHouse Variant column format."""
    if value is None:
        return 'NULL'
    elif isinstance(value, str):
        # Escape quotes for SQL
        escaped = value.replace("'", "''").replace('\\', '\\\\')
        return f"'{escaped}'"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, bool):
        return 'true' if value else 'false'
    elif isinstance(value, list):
        # Convert list to Array(String) format
        escaped_items = [item.replace("'", "''") if isinstance(item, str) else str(item) for item in value]
        return '[' + ', '.join([f"'{item}'" for item in escaped_items]) + ']'
    elif isinstance(value, dict):
        # Convert dict to JSON string
        json_str = json.dumps(value).replace("'", "''")
        return f"'{json_str}'"
    else:
        return f"'{str(value)}'"

def create_variant_schema(sample_records: list, max_samples: int = 1000) -> str:
    """Analyze sample records and create optimal Variant column schema."""
    
    # Take sample for analysis
    samples = sample_records[:max_samples]
    
    # Define fields we want to analyze for Variant columns
    variant_fields = {
        'commit_operation': 'commit.operation',
        'commit_collection': 'commit.collection', 
        'record_type': 'record.$type',
        'metadata': 'commit',  # Entire commit object as variant
        'record_content': 'record'  # Entire record as variant
    }
    
    schema_lines = [
        "-- True ClickHouse Variant columns schema",
        "CREATE DATABASE IF NOT EXISTS bluesky_true_variants;",
        "",
        "CREATE TABLE bluesky_true_variants.bluesky_variant_columns",
        "(",
        "    -- Basic fields",
        "    did String,",
        "    time_us UInt64,",
        "    kind String,",
        "    timestamp_col DateTime64(6),",
        "",
        "    -- TRUE Variant columns - can store multiple types"
    ]
    
    # Analyze each field and create appropriate Variant types
    for field_name, field_path in variant_fields.items():
        types_found = analyze_json_field_types(samples, field_path)
        if types_found:
            variant_types = ', '.join(sorted(types_found))
            schema_lines.append(f"    {field_name} Variant({variant_types}),")
        else:
            schema_lines.append(f"    {field_name} Variant(String, JSON),")
    
    schema_lines.extend([
        "",
        "    -- Original JSON for comparison",
        "    original_json JSON",
        ")",
        "ENGINE = MergeTree",
        "ORDER BY (kind, did, timestamp_col);",
        ""
    ])
    
    return '\n'.join(schema_lines)

def process_json_to_variants(input_file: str, output_file: str, max_records: Optional[int] = None):
    """Convert JSON records to Variant column format."""
    
    print(f"Converting JSON to TRUE Variant columns: {input_file} -> {output_file}")
    
    # First pass: analyze schema from sample
    print("Analyzing JSON structure for Variant types...")
    sample_records = []
    
    open_func = gzip.open if input_file.endswith('.gz') else open
    mode = 'rt' if input_file.endswith('.gz') else 'r'
    
    with open_func(input_file, mode) as f:
        for i, line in enumerate(f):
            if i >= 1000:  # Sample first 1000 records
                break
            try:
                record = json.loads(line.strip())
                sample_records.append(record)
            except json.JSONDecodeError:
                continue
    
    # Generate schema
    schema = create_variant_schema(sample_records)
    schema_file = output_file.replace('.tsv', '_schema.sql')
    with open(schema_file, 'w') as f:
        f.write(schema)
    print(f"Generated schema: {schema_file}")
    
    # Second pass: convert data
    print("Converting records to Variant format...")
    processed = 0
    
    with open(output_file, 'w') as out_f:
        # Write header
        out_f.write("did\ttime_us\tkind\ttimestamp_col\tcommit_operation\tcommit_collection\trecord_type\tmetadata\trecord_content\toriginal_json\n")
        
        with open_func(input_file, mode) as in_f:
            for line_num, line in enumerate(in_f):
                if max_records and processed >= max_records:
                    break
                
                try:
                    record = json.loads(line.strip())
                    
                    # Extract basic fields
                    did = record.get('did', '')
                    time_us = record.get('time_us', 0)
                    kind = record.get('kind', '')
                    
                    # Convert timestamp
                    try:
                        timestamp_col = datetime.fromtimestamp(time_us / 1_000_000).strftime('%Y-%m-%d %H:%M:%S.%f')
                    except:
                        timestamp_col = '1970-01-01 00:00:00.000000'
                    
                    # Extract values for Variant columns
                    commit_operation = get_nested_value(record, 'commit.operation')
                    commit_collection = get_nested_value(record, 'commit.collection')
                    record_type = get_nested_value(record, 'record.$type')
                    metadata = record.get('commit')  # Entire commit object
                    record_content = record.get('record')  # Entire record object
                    
                    # Convert to Variant format
                    values = [
                        f"'{did}'",
                        str(time_us),
                        f"'{kind}'",
                        f"'{timestamp_col}'",
                        convert_to_variant_value(commit_operation),
                        convert_to_variant_value(commit_collection),
                        convert_to_variant_value(record_type),
                        convert_to_variant_value(metadata),
                        convert_to_variant_value(record_content),
                        f"'{json.dumps(record).replace(chr(39), chr(39)+chr(39))}'"  # Escape quotes
                    ]
                    
                    out_f.write('\t'.join(values) + '\n')
                    processed += 1
                    
                    if processed % 50000 == 0:
                        print(f"Processed {processed} records...")
                
                except json.JSONDecodeError as e:
                    print(f"Skipping malformed JSON at line {line_num + 1}: {e}")
                    continue
                except Exception as e:
                    print(f"Error processing line {line_num + 1}: {e}")
                    continue
    
    print(f"Successfully converted {processed} records to Variant format")
    return processed

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 preprocess_json_to_true_variants.py input.json[.gz] output.tsv [max_records]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    max_records = int(sys.argv[3]) if len(sys.argv) > 3 else None
    
    process_json_to_variants(input_file, output_file, max_records)

if __name__ == '__main__':
    main() 