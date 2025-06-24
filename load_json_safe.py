
import sys
import json

max_size = 1024 * 1024  # 1MB limit per record
processed = 0
skipped = 0

for line in sys.stdin:
    line = line.strip()
    if line:
        processed += 1
        if len(line) > max_size:
            skipped += 1
            if skipped <= 10:  # Only show first 10 skipped records
                print(f"Skipping large record {processed} (size: {len(line):,} bytes)", file=sys.stderr)
            continue
        
        try:
            # Validate JSON before wrapping
            json.loads(line)
            print('{"data":' + line + '}')
        except json.JSONDecodeError:
            skipped += 1
            if skipped <= 10:
                print(f"Skipping invalid JSON at record {processed}", file=sys.stderr)
            continue
        
        if processed % 100000 == 0:
            print(f"Processed {processed:,} records, skipped {skipped:,}", file=sys.stderr)

print(f"Final: Processed {processed:,} records, skipped {skipped:,}", file=sys.stderr)
