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

    def load_100m_data(self):
        """Load 100M records into both table approaches with better error handling."""
        print("=" * 60)
        print("LOADING 100M RECORDS (WITH ERROR HANDLING)")
        print("=" * 60)
        
        if not self.prepare_100m_data():
            print("Failed to prepare 100M data")
            return
        
        # Clear existing data first
        print("0. Clearing existing tables...")
        clear_queries = [
            "DROP TABLE IF EXISTS bluesky_100m.bluesky",
            "DROP TABLE IF EXISTS bluesky_100m_variant.bluesky_data"
        ]
        for query in clear_queries:
            self.run_clickhouse_query(query)
        
        # Recreate schemas
        self.create_schemas()
        
        # 1. Load JSON baseline with error handling
        print("1. Loading JSON baseline (100M records)...")
        print("   This will take several minutes...")
        print("   Filtering out overly large JSON records...")
        
        # Create a more robust loading script that filters large records
        loading_script = """
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
"""
        
        # Write the loading script to a temporary file
        with open('load_json_safe.py', 'w') as f:
            f.write(loading_script)
        
        json_load_cmd = "python3 load_json_safe.py < bluesky_100m_combined.jsonl | clickhouse client --query 'INSERT INTO bluesky_100m.bluesky FORMAT JSONEachRow'"
        
        start_time = time.time()
        result = subprocess.run(json_load_cmd, shell=True, capture_output=True, text=True)
        load_time = time.time() - start_time
        
        if result.returncode == 0:
            print(f"   ✓ JSON baseline loaded in {load_time:.1f}s")
            if result.stderr:
                print(f"   Loading info: {result.stderr.strip()}")
        else:
            print(f"   ✗ JSON baseline failed: {result.stderr}")
            # Don't return here, try to load variant anyway
        
        # 2. Load Variant Direct with same error handling
        print("2. Loading Variant Direct (100M records)...")
        print("   This will take several minutes...")
        
        variant_load_cmd = "python3 load_json_safe.py < bluesky_100m_combined.jsonl | clickhouse client --query 'INSERT INTO bluesky_100m_variant.bluesky_data FORMAT JSONEachRow'"
        
        start_time = time.time()
        result = subprocess.run(variant_load_cmd, shell=True, capture_output=True, text=True)
        load_time = time.time() - start_time
        
        if result.returncode == 0:
            print(f"   ✓ Variant Direct loaded in {load_time:.1f}s")
            if result.stderr:
                print(f"   Loading info: {result.stderr.strip()}")
        else:
            print(f"   ✗ Variant Direct failed: {result.stderr}")
        
        # Clean up temporary file
        try:
            Path('load_json_safe.py').unlink()
        except:
            pass
        
        print("\n100M data loading completed!")

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
    
    # Ask user if they want to load data
    response = input("Do you want to load 100M records? This will take significant time (y/N): ")
    if response.lower() in ['y', 'yes']:
        benchmark.create_schemas()
        benchmark.load_100m_data()
        print("\nRechecking table status after loading...")
        benchmark.check_table_status()
    
    # Check again if tables have data
    benchmark.check_table_status()
    
    # Ask user if they want to run benchmarks
    response = input("Do you want to run the 100M benchmarks? This will take time (y/N): ")
    if response.lower() in ['y', 'yes']:
        benchmark.run_benchmarks()
        benchmark.generate_report()
        benchmark.save_results()
    
    print("\n" + "=" * 60)
    print("100M BENCHMARK COMPLETED!")
    print("=" * 60)

if __name__ == '__main__':
    main() 