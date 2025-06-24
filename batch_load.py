
import sys
import json
import subprocess
import tempfile
import os
import time

def load_batch(batch_lines, table_name, batch_size_mb=500):
    """Load a batch of lines into ClickHouse with adaptive memory management."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        valid_lines = 0
        for line in batch_lines:
            try:
                # Validate JSON first
                json.loads(line)
                f.write('{"data":' + line + '}\n')
                valid_lines += 1
            except json.JSONDecodeError:
                # Skip invalid JSON lines
                continue
        temp_file = f.name
    
    if valid_lines == 0:
        os.unlink(temp_file)
        return True, "No valid JSON lines in batch"
    
    # Try loading with progressively smaller memory limits if it fails
    memory_limits = [f"{batch_size_mb}000000", "300000000", "150000000", "100000000"]
    
    for memory_limit in memory_limits:
        # Load batch into ClickHouse with memory limits
        cmd = f"clickhouse client --max_memory_usage={memory_limit} --max_parser_depth=10000 --input_format_json_read_objects_as_strings=1 --query 'INSERT INTO {table_name} FORMAT JSONEachRow' < {temp_file}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            # Success
            os.unlink(temp_file)
            return True, f"Loaded {valid_lines} records with memory limit {memory_limit}"
        elif "MEMORY_LIMIT_EXCEEDED" in result.stderr:
            # Try with lower memory limit
            print(f"Memory limit {memory_limit} exceeded, trying lower limit...", file=sys.stderr)
            time.sleep(1)  # Brief pause to let ClickHouse recover
            continue
        else:
            # Other error, don't retry
            break
    
    # Clean up temp file
    os.unlink(temp_file)
    return False, result.stderr

def split_batch(batch_lines, num_parts=2):
    """Split a batch into smaller parts."""
    part_size = len(batch_lines) // num_parts
    parts = []
    for i in range(0, len(batch_lines), part_size):
        parts.append(batch_lines[i:i + part_size])
    return parts

# Process in batches with adaptive sizing
initial_batch_size = 500000  # Start with 500K records per batch
current_batch_size = initial_batch_size
batch_lines = []
processed = 0
total_loaded = 0
failed_records = 0

table_name = sys.argv[1] if len(sys.argv) > 1 else "bluesky_100m.bluesky"

for line in sys.stdin:
    line = line.strip()
    if line:
        batch_lines.append(line)
        processed += 1
        
        if len(batch_lines) >= current_batch_size:
            print(f"Loading batch: {total_loaded + 1:,} to {total_loaded + len(batch_lines):,} records (batch size: {len(batch_lines):,})", file=sys.stderr)
            success, message = load_batch(batch_lines, table_name)
            
            if success:
                total_loaded += len(batch_lines)
                print(f"✓ Successfully loaded {total_loaded:,} records total", file=sys.stderr)
                # If successful, gradually increase batch size back up
                if current_batch_size < initial_batch_size:
                    current_batch_size = min(current_batch_size + 100000, initial_batch_size)
            else:
                print(f"✗ Batch failed: {message}", file=sys.stderr)
                # Try splitting the batch in half and loading smaller parts
                if len(batch_lines) > 50000:  # Only split if batch is reasonably large
                    print(f"Splitting batch into smaller parts...", file=sys.stderr)
                    parts = split_batch(batch_lines, 4)  # Split into 4 parts
                    for i, part in enumerate(parts):
                        if part:
                            print(f"Loading split part {i+1}/4 ({len(part):,} records)...", file=sys.stderr)
                            part_success, part_message = load_batch(part, table_name, 200)  # Lower memory limit
                            if part_success:
                                total_loaded += len(part)
                                print(f"✓ Part {i+1} loaded, total: {total_loaded:,}", file=sys.stderr)
                            else:
                                failed_records += len(part)
                                print(f"✗ Part {i+1} failed: {part_message}", file=sys.stderr)
                else:
                    failed_records += len(batch_lines)
                
                # Reduce batch size for next batches
                current_batch_size = max(current_batch_size // 2, 100000)
                print(f"Reducing batch size to {current_batch_size:,} for next batches", file=sys.stderr)
            
            batch_lines = []

# Load remaining records
if batch_lines:
    print(f"Loading final batch: {total_loaded + 1:,} to {total_loaded + len(batch_lines):,} records", file=sys.stderr)
    success, message = load_batch(batch_lines, table_name)
    if success:
        total_loaded += len(batch_lines)
        print(f"✓ Final total: {total_loaded:,} records loaded", file=sys.stderr)
    else:
        print(f"✗ Final batch failed: {message}", file=sys.stderr)
        # Try splitting final batch too
        if len(batch_lines) > 50000:
            parts = split_batch(batch_lines, 4)
            for i, part in enumerate(parts):
                if part:
                    part_success, part_message = load_batch(part, table_name, 200)
                    if part_success:
                        total_loaded += len(part)
                    else:
                        failed_records += len(part)
        else:
            failed_records += len(batch_lines)

print(f"FINAL SUMMARY:", file=sys.stderr)
print(f"- Processed: {processed:,} input records", file=sys.stderr)
print(f"- Loaded: {total_loaded:,} records", file=sys.stderr)
print(f"- Failed: {failed_records:,} records", file=sys.stderr)
print(f"- Success rate: {(total_loaded/processed)*100:.1f}%", file=sys.stderr)
