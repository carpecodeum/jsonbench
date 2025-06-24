#!/usr/bin/env python3
"""
100M Record Benchmark for ClickHouse JSON Approaches
Tests the two best approaches with 100M records to see how they scale.
Focuses on JSON Object baseline vs Variant Direct JSON Access.
"""

import subprocess
import time
import json
import statistics
import sys
from pathlib import Path

class Benchmark100M:
    def __init__(self):
        self.approaches = {
            'json_baseline': {
                'database': 'bluesky_100m',
                'table': 'bluesky',
                'description': 'JSON Object (baseline)',
                'queries_file': None
            },
            'variant_direct': {
                'database': 'bluesky_100m_variant',
                'table': 'bluesky_data',
                'description': 'Variant Direct JSON Access ⭐',
                'queries_file': None
            }
        }
        self.iterations = 5  # Reduced iterations for 100M dataset
        self.results = {}

    def run_clickhouse_query(self, query: str, timeout: int = 900):  # Increased timeout for 100M
        """Run a ClickHouse query and return execution time."""
        start_time = time.time()
        try:
            result = subprocess.run(
                ['clickhouse', 'client', '--query', query],
                capture_output=True,
                text=True,
                timeout=timeout
            )
            end_time = time.time()
            
            if result.returncode == 0:
                return end_time - start_time, result.stdout.strip()
            else:
                return -1, f"Error: {result.stderr}"
        except subprocess.TimeoutExpired:
            return -1, "Error: Query timeout"
        except Exception as e:
            return -1, f"Error: {str(e)}"

    def check_table_status(self):
        """Check the status of all tables."""
        print("=" * 60)
        print("TABLE STATUS CHECK (100M DATASET)")
        print("=" * 60)
        
        for approach_name, config in self.approaches.items():
            db = config['database']
            table = config['table']
            
            # Check if table exists and get record count
            count_query = f"SELECT count() FROM {db}.{table}"
            exec_time, result = self.run_clickhouse_query(count_query)
            
            if exec_time > 0:
                count = int(result)
                # Get table size
                size_query = f"SELECT formatReadableSize(sum(bytes_on_disk)) FROM system.parts WHERE database = '{db}' AND table = '{table}' AND active = 1"
                _, size_result = self.run_clickhouse_query(size_query)
                print(f"{approach_name:15} ({config['description']:30}): {count:>10,} records, {size_result}")
            else:
                print(f"{approach_name:15} ({config['description']:30}): NOT AVAILABLE - {result}")
        print()

    def create_schemas(self):
        """Create database schemas for 100M dataset."""
        print("Creating database schemas for 100M dataset...")
        
        schemas = [
            # JSON Baseline (100M)
            """
            CREATE DATABASE IF NOT EXISTS bluesky_100m;
            CREATE TABLE IF NOT EXISTS bluesky_100m.bluesky (
                data JSON
            ) ENGINE = MergeTree ORDER BY tuple();
            """,
            
            # Variant Direct (100M)
            """
            CREATE DATABASE IF NOT EXISTS bluesky_100m_variant;
            CREATE TABLE IF NOT EXISTS bluesky_100m_variant.bluesky_data (
                data Variant(JSON)
            ) ENGINE = MergeTree ORDER BY tuple()
            SETTINGS allow_experimental_variant_type = 1, use_variant_as_common_type = 1;
            """
        ]
        
        for schema in schemas:
            exec_time, result = self.run_clickhouse_query(schema)
            if exec_time < 0:
                print(f"Schema creation error: {result}")
                return False
        return True

    def prepare_100m_data(self):
        """Prepare and combine the 100 files into JSONL format."""
        print("Preparing 100M dataset...")
        
        # First decompress and combine all 100 files
        data_dir = Path.home() / "data" / "bluesky"
        output_file = "bluesky_100m_combined.jsonl"
        
        if not Path(output_file).exists():
            print("Decompressing and combining 100 files...")
            # Use gunzip -c instead of zcat for macOS compatibility
            cmd = f"gunzip -c {data_dir}/file_*.json.gz > {output_file}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"Error combining files: {result.stderr}")
                return False
            print(f"Combined 100M dataset created: {output_file}")
        else:
            print(f"Combined dataset already exists: {output_file}")
        
        return True

    def load_data_with_batch_script(self, table_name, description):
        """Load data using the batch script with better error reporting."""
        print(f"Loading {description}...")
        print("   This will take several minutes...")
        
        # Create the batch loading script
        batch_script = '''
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
                f.write('{"data":' + line + '}\\n')
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
initial_batch_size = 100000  # Reduced to 100K records per batch for better stability
current_batch_size = initial_batch_size
batch_lines = []
processed = 0
total_loaded = 0
failed_records = 0

table_name = sys.argv[1] if len(sys.argv) > 1 else "bluesky_100m.bluesky"

print(f"Starting data loading for table: {table_name}", file=sys.stderr)

for line in sys.stdin:
    line = line.strip()
    if line:
        batch_lines.append(line)
        processed += 1
        
        # Print progress more frequently
        if processed % 50000 == 0:
            print(f"Processed {processed:,} records so far...", file=sys.stderr)
        
        if len(batch_lines) >= current_batch_size:
            print(f"Loading batch: {total_loaded + 1:,} to {total_loaded + len(batch_lines):,} records (batch size: {len(batch_lines):,})", file=sys.stderr)
            success, message = load_batch(batch_lines, table_name)
            
            if success:
                total_loaded += len(batch_lines)
                print(f"✓ Successfully loaded {total_loaded:,} records total", file=sys.stderr)
                # If successful, gradually increase batch size back up
                if current_batch_size < initial_batch_size:
                    current_batch_size = min(current_batch_size + 25000, initial_batch_size)
            else:
                print(f"✗ Batch failed: {message}", file=sys.stderr)
                # Try splitting the batch in half and loading smaller parts
                if len(batch_lines) > 10000:  # Only split if batch is reasonably large
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
                current_batch_size = max(current_batch_size // 2, 25000)
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
        if len(batch_lines) > 10000:
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
'''
        
        with open('batch_load_improved.py', 'w') as f:
            f.write(batch_script)
        
        load_cmd = f"python3 batch_load_improved.py {table_name} < bluesky_100m_combined.jsonl"
        
        start_time = time.time()
        result = subprocess.run(load_cmd, shell=True, capture_output=True, text=True)
        load_time = time.time() - start_time
        
        if result.returncode == 0:
            print(f"   ✓ {description} loaded in {load_time:.1f}s")
            if result.stderr:
                print(f"   Loading details:\n{result.stderr}")
            return True
        else:
            print(f"   ✗ {description} failed: {result.stderr}")
            if result.stdout:
                print(f"   Stdout: {result.stdout}")
            return False

    def load_100m_data(self):
        """Load 100M records into both table approaches without filtering."""
        print("=" * 60)
        print("LOADING 100M RECORDS (FULL DATASET)")
        print("=" * 60)
        
        if not self.prepare_100m_data():
            print("Failed to prepare 100M data")
            return False
        
        # Clear existing data first
        print("0. Clearing existing tables...")
        clear_queries = [
            "TRUNCATE TABLE IF EXISTS bluesky_100m.bluesky",
            "TRUNCATE TABLE IF EXISTS bluesky_100m_variant.bluesky_data"
        ]
        for query in clear_queries:
            exec_time, result = self.run_clickhouse_query(query)
            if exec_time < 0:
                print(f"   Warning: {result}")
        
        # Recreate schemas
        if not self.create_schemas():
            print("Failed to create schemas")
            return False
        
        # 1. Load JSON baseline
        print("1. Loading JSON baseline (100M records)...")
        success1 = self.load_data_with_batch_script('bluesky_100m.bluesky', 'JSON baseline')
        
        # 2. Load Variant Direct
        print("2. Loading Variant Direct (100M records)...")
        success2 = self.load_data_with_batch_script('bluesky_100m_variant.bluesky_data', 'Variant Direct')
        
        # Clean up temporary scripts
        Path('batch_load_improved.py').unlink(missing_ok=True)
        
        if success1 and success2:
            print("\n✓ 100M data loading completed successfully!")
            return True
        else:
            print(f"\n⚠ 100M data loading completed with issues (JSON: {'✓' if success1 else '✗'}, Variant: {'✓' if success2 else '✗'})")
            return success1 or success2  # Return True if at least one succeeded

    def create_json_baseline_queries_100m(self):
        """Create query file for JSON baseline approach (100M scale)."""
        queries = [
            # Q1: Count by kind
            "SELECT toString(data.kind) as kind, count() FROM bluesky_100m.bluesky GROUP BY toString(data.kind) ORDER BY count() DESC",
            
            # Q2: Count by collection (top 10)
            "SELECT toString(data.commit.collection) as collection, count() FROM bluesky_100m.bluesky WHERE toString(data.commit.collection) != '' GROUP BY toString(data.commit.collection) ORDER BY count() DESC LIMIT 10",
            
            # Q3: Filter by kind
            "SELECT count() FROM bluesky_100m.bluesky WHERE toString(data.kind) = 'commit'",
            
            # Q4: Time range query
            "SELECT count() FROM bluesky_100m.bluesky WHERE toUInt64(data.time_us) > 1700000000000000",
            
            # Q5: Complex aggregation
            "SELECT toString(data.commit.operation) as op, toString(data.commit.collection) as coll, count() FROM bluesky_100m.bluesky WHERE toString(data.commit.operation) != '' AND toString(data.commit.collection) != '' GROUP BY toString(data.commit.operation), toString(data.commit.collection) ORDER BY count() DESC LIMIT 5"
        ]
        
        with open('queries_json_baseline_100m.sql', 'w') as f:
            for query in queries:
                f.write(query + ';\n')
        
        return 'queries_json_baseline_100m.sql'

    def create_variant_direct_queries_100m(self):
        """Create query file for variant direct JSON access approach (100M scale)."""
        queries = [
            # Q1: Count by kind - using direct JSON field access
            "SELECT toString(data.JSON.kind) as kind, count() FROM bluesky_100m_variant.bluesky_data GROUP BY kind ORDER BY count() DESC",
            
            # Q2: Count by collection - using direct JSON field access  
            "SELECT toString(data.JSON.commit.collection) as collection, count() FROM bluesky_100m_variant.bluesky_data WHERE collection != '' GROUP BY collection ORDER BY count() DESC LIMIT 10",
            
            # Q3: Filter by kind - using direct JSON field access
            "SELECT count() FROM bluesky_100m_variant.bluesky_data WHERE toString(data.JSON.kind) = 'commit'",
            
            # Q4: Time range query - using direct JSON field access
            "SELECT count() FROM bluesky_100m_variant.bluesky_data WHERE toUInt64(data.JSON.time_us) > 1700000000000000",
            
            # Q5: Complex aggregation - using direct JSON field access
            "SELECT toString(data.JSON.commit.operation) as op, toString(data.JSON.commit.collection) as coll, count() FROM bluesky_100m_variant.bluesky_data WHERE op != '' AND coll != '' GROUP BY op, coll ORDER BY count() DESC LIMIT 5"
        ]
        
        with open('queries_variant_direct_100m.sql', 'w') as f:
            for query in queries:
                f.write(query + ';\n')
        
        return 'queries_variant_direct_100m.sql'

    def load_queries_from_file(self, filename):
        """Load queries from SQL file."""
        if not Path(filename).exists():
            return []
        
        with open(filename, 'r') as f:
            content = f.read()
        
        # Split by semicolon and clean up
        queries = [q.strip() for q in content.split(';') if q.strip() and not q.strip().startswith('--')]
        return queries

    def run_query_benchmark(self, approach_name, query, query_num):
        """Run a single query multiple times and return statistics."""
        times = []
        errors = 0
        
        print(f"      Running {self.iterations} iterations...")
        
        for iteration in range(self.iterations):
            print(f"        Iteration {iteration + 1}/{self.iterations}...", end='', flush=True)
            exec_time, result = self.run_clickhouse_query(query)
            if exec_time > 0:
                times.append(exec_time)
                print(f" {exec_time:.2f}s")
            else:
                errors += 1
                print(f" ERROR")
                if iteration == 0:  # Show error for first iteration
                    print(f"        Error details: {result}")
        
        if len(times) == 0:
            return None
        
        return {
            'mean': statistics.mean(times),
            'median': statistics.median(times),
            'min': min(times),
            'max': max(times),
            'std': statistics.stdev(times) if len(times) > 1 else 0,
            'errors': errors,
            'successful_runs': len(times)
        }

    def run_benchmarks(self):
        """Run comprehensive benchmarks for 100M dataset."""
        print("=" * 60)
        print(f"RUNNING 100M BENCHMARKS ({self.iterations} iterations per query)")
        print("=" * 60)
        
        # Create query files
        self.approaches['json_baseline']['queries_file'] = self.create_json_baseline_queries_100m()
        self.approaches['variant_direct']['queries_file'] = self.create_variant_direct_queries_100m()
        
        for approach_name, config in self.approaches.items():
            print(f"\n=== {config['description']} (100M Records) ===")
            
            # Check if table has data
            count_query = f"SELECT count() FROM {config['database']}.{config['table']}"
            exec_time, result = self.run_clickhouse_query(count_query)
            
            if exec_time <= 0 or int(result) == 0:
                print(f"  Skipping - no data available")
                continue
            
            print(f"  Records: {int(result):,}")
            
            # Load queries
            if config['queries_file'] and Path(config['queries_file']).exists():
                queries = self.load_queries_from_file(config['queries_file'])
            else:
                print(f"  Skipping - queries file not found: {config['queries_file']}")
                continue
            
            approach_results = {}
            
            for i, query in enumerate(queries, 1):
                print(f"  Q{i}: Running 100M scale query...")
                
                stats = self.run_query_benchmark(approach_name, query, i)
                if stats:
                    approach_results[f'Q{i}'] = stats
                    print(f"      RESULT: avg={stats['mean']:.3f}s (min={stats['min']:.3f}, max={stats['max']:.3f}, std={stats['std']:.3f})")
                    if stats['errors'] > 0:
                        print(f"      Errors: {stats['errors']}/{self.iterations}")
                else:
                    print("      FAILED - all iterations failed")
            
            self.results[approach_name] = {
                'config': config,
                'queries': approach_results
            }

    def generate_report(self):
        """Generate comprehensive benchmark report for 100M dataset."""
        print("\n" + "=" * 80)
        print("COMPREHENSIVE 100M BENCHMARK RESULTS")
        print("=" * 80)
        
        # Summary table
        print(f"\n{'Approach':<35} {'Q1':<10} {'Q2':<10} {'Q3':<10} {'Q4':<10} {'Q5':<10} {'Avg':<10}")
        print("-" * 95)
        
        approach_averages = {}
        
        for approach_name, data in self.results.items():
            config = data['config']
            queries = data['queries']
            
            times = []
            row = f"{config['description']:<35}"
            
            for i in range(1, 6):
                q_key = f'Q{i}'
                if q_key in queries:
                    avg_time = queries[q_key]['mean']
                    times.append(avg_time)
                    row += f" {avg_time:.3f}s"
                else:
                    row += f" {'---':<9}"
            
            if times:
                overall_avg = statistics.mean(times)
                approach_averages[approach_name] = overall_avg
                row += f" {overall_avg:.3f}s"
            else:
                row += f" {'---':<9}"
            
            print(row)
        
        # Performance comparison
        if approach_averages:
            print(f"\n{'100M DATASET PERFORMANCE RANKING:':<40}")
            sorted_approaches = sorted(approach_averages.items(), key=lambda x: x[1])
            baseline_time = sorted_approaches[0][1]
            
            for i, (approach_name, avg_time) in enumerate(sorted_approaches, 1):
                config = self.results[approach_name]['config']
                ratio = avg_time / baseline_time
                print(f"  {i}. {config['description']:<40} {avg_time:.3f}s ({ratio:.2f}x)")
        
        # Scale comparison with 1M results
        print(f"\n{'SCALE ANALYSIS (100M vs 1M):':<40}")
        print("Based on previous 1M results:")
        print("  JSON Object (1M):     ~0.11s average")
        print("  Variant Direct (1M):  ~0.11s average")
        print("")
        if approach_averages:
            for approach_name, avg_time in approach_averages.items():
                scale_factor = avg_time / 0.11  # Assuming 0.11s for 1M
                print(f"  {self.results[approach_name]['config']['description']} (100M): {avg_time:.3f}s ({scale_factor:.1f}x slower than 1M)")

    def save_results(self):
        """Save results to JSON file."""
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_results_100m_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        print(f"\nResults saved to: {filename}")

def main():
    """Main benchmark execution."""
    print("CLICKHOUSE 100M RECORD BENCHMARK")
    print("Testing JSON Object vs Variant Direct JSON Access")
    print("=" * 60)
    
    benchmark = Benchmark100M()
    
    # Check current status
    benchmark.check_table_status()
    
    # Determine if we need to load data
    need_loading = False
    for approach_name, config in benchmark.approaches.items():
        count_query = f"SELECT count() FROM {config['database']}.{config['table']}"
        exec_time, result = benchmark.run_clickhouse_query(count_query)
        if exec_time <= 0 or int(result) < 1000000:  # Less than 1M records
            need_loading = True
            break
    
    if need_loading:
        print("\n🔄 Data loading is needed for proper benchmarking")
        response = input("Do you want to load 100M records? This will take significant time (y/N): ")
        if response.lower() in ['y', 'yes']:
            success = benchmark.load_100m_data()
            if success:
                print("\nRechecking table status after loading...")
                benchmark.check_table_status()
            else:
                print("\n⚠ Data loading had issues. Checking current status...")
                benchmark.check_table_status()
        else:
            print("⚠ Skipping data loading. Benchmark may not be meaningful with limited data.")
    else:
        print("\n✓ Tables appear to have sufficient data for benchmarking")
    
    # Final status check
    print("\nFinal table status:")
    benchmark.check_table_status()
    
    # Determine if we can run benchmarks
    can_benchmark = False
    for approach_name, config in benchmark.approaches.items():
        count_query = f"SELECT count() FROM {config['database']}.{config['table']}"
        exec_time, result = benchmark.run_clickhouse_query(count_query)
        if exec_time > 0 and int(result) > 0:
            can_benchmark = True
            break
    
    if can_benchmark:
        response = input("Do you want to run the 100M benchmarks? This will take time (y/N): ")
        if response.lower() in ['y', 'yes']:
            benchmark.run_benchmarks()
            benchmark.generate_report()
            benchmark.save_results()
        else:
            print("Skipping benchmarks.")
    else:
        print("⚠ No data available for benchmarking. Please load data first.")
    
    print("\n" + "=" * 60)
    print("100M BENCHMARK COMPLETED!")
    print("=" * 60)

if __name__ == '__main__':
    main() 