#!/usr/bin/env python3
"""
Comprehensive 1M Record Benchmark for All ClickHouse JSON Approaches
Tests all approaches with 1M records, running each query 10 times for statistical significance.
"""

import subprocess
import time
import json
import statistics
import sys
from pathlib import Path

class ComprehensiveBenchmark:
    def __init__(self):
        self.approaches = {
            'json_baseline': {
                'database': 'bluesky_1m',
                'table': 'bluesky',
                'description': 'JSON Object (baseline)',
                'queries_file': 'queries_json_baseline.sql'
            },
            'typed_columns': {
                'database': 'bluesky_variants_test',
                'table': 'bluesky_preprocessed',
                'description': 'Typed Columns + JSON fallback',
                'queries_file': 'queries_preprocessed_variants.sql'
            },
            'pure_variants': {
                'database': 'bluesky_variants_test',
                'table': 'bluesky_pure_variants',
                'description': 'Pure Typed Columns (no JSON)',
                'queries_file': 'queries_pure_variants.sql'
            },
            'true_variants': {
                'database': 'bluesky_true_variants',
                'table': 'bluesky_data',
                'description': 'ClickHouse Variant type',
                'queries_file': 'queries_true_variants.sql'
            },
            'minimal_variant': {
                'database': 'bluesky_minimal_1m',
                'table': 'bluesky_data',
                'description': 'Minimal Variant (1 column)',
                'queries_file': None  # Will create custom queries
            }
        }
        self.iterations = 10
        self.results = {}

    def run_clickhouse_query(self, query: str, timeout: int = 300):
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
        print("TABLE STATUS CHECK")
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
                print(f"{approach_name:15} ({config['description']:30}): {count:>8,} records, {size_result}")
            else:
                print(f"{approach_name:15} ({config['description']:30}): NOT AVAILABLE - {result}")
        print()

    def create_schemas(self):
        """Create all necessary database schemas."""
        print("Creating database schemas...")
        
        schemas = [
            # JSON Baseline
            """
            CREATE DATABASE IF NOT EXISTS bluesky_1m;
            CREATE TABLE IF NOT EXISTS bluesky_1m.bluesky (
                data JSON
            ) ENGINE = MergeTree ORDER BY tuple();
            """,
            
            # Typed columns + Variants
            """
            CREATE DATABASE IF NOT EXISTS bluesky_variants_test;
            """,
            
            # True variants
            """
            CREATE DATABASE IF NOT EXISTS bluesky_true_variants;
            """,
            
            # Minimal variant (1M)
            """
            CREATE DATABASE IF NOT EXISTS bluesky_minimal_1m;
            CREATE TABLE IF NOT EXISTS bluesky_minimal_1m.bluesky_data (
                data Variant(JSON)
            ) ENGINE = MergeTree ORDER BY tuple()
            SETTINGS allow_experimental_variant_type = 1, use_variant_as_common_type = 1;
            """
        ]
        
        for schema in schemas:
            exec_time, result = self.run_clickhouse_query(schema)
            if exec_time < 0:
                print(f"Schema creation error: {result}")

    def load_all_data(self):
        """Load 1M records into all table approaches."""
        print("=" * 60)
        print("LOADING 1M RECORDS INTO ALL APPROACHES")
        print("=" * 60)
        
        # 1. Load JSON baseline
        print("1. Loading JSON baseline (1M records)...")
        json_load_cmd = f"head -1000000 bluesky_1m_baseline.jsonl | clickhouse client --query 'INSERT INTO bluesky_1m.bluesky FORMAT JSONEachRow'"
        result = subprocess.run(json_load_cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print("   ✓ JSON baseline loaded")
        else:
            print(f"   ✗ JSON baseline failed: {result.stderr}")
        
        # 2. Load typed columns
        print("2. Loading typed columns (1M records)...")
        # First need to create the schema
        typed_schema = Path('ddl_preprocessed_variants.sql')
        if typed_schema.exists():
            subprocess.run(['clickhouse', 'client', '--queries-file', str(typed_schema)])
        
        typed_load_cmd = f"head -1000000 bluesky_1m_preprocessed.tsv | clickhouse client --query 'INSERT INTO bluesky_variants_test.bluesky_preprocessed FORMAT TSVWithNames'"
        result = subprocess.run(typed_load_cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print("   ✓ Typed columns loaded")
        else:
            print(f"   ✗ Typed columns failed: {result.stderr}")
        
        # 3. Load pure variants (subset of typed)
        print("3. Loading pure variants...")
        pure_query = """
        INSERT INTO bluesky_variants_test.bluesky_pure_variants 
        SELECT 
            did, time_us, kind, timestamp_col,
            commit_rev, commit_operation, commit_collection,
            commit_rkey, commit_cid, record_type
        FROM bluesky_variants_test.bluesky_preprocessed
        """
        exec_time, result = self.run_clickhouse_query(pure_query)
        if exec_time > 0:
            print("   ✓ Pure variants loaded")
        else:
            print(f"   ✗ Pure variants failed: {result}")
        
        # 4. Load true variants
        print("4. Loading true variants...")
        subprocess.run(['python3', 'load_true_variants_fixed.py', 'all', '--source-db', 'bluesky_1m', '--use-client'])
        
        # 5. Load minimal variant
        print("5. Loading minimal variant...")
        minimal_query = """
        INSERT INTO bluesky_minimal_1m.bluesky_data
        SELECT CAST(data AS Variant(JSON)) as data
        FROM bluesky_1m.bluesky
        """
        exec_time, result = self.run_clickhouse_query(minimal_query)
        if exec_time > 0:
            print("   ✓ Minimal variant loaded")
        else:
            print(f"   ✗ Minimal variant failed: {result}")
        
        print("\nData loading completed!")

    def create_minimal_variant_queries(self):
        """Create query file for minimal variant approach."""
        queries = [
            # Q1: Count by kind
            "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') as kind, count() FROM bluesky_minimal_1m.bluesky_data GROUP BY kind ORDER BY count() DESC",
            
            # Q2: Count by collection 
            "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') as collection, count() FROM bluesky_minimal_1m.bluesky_data WHERE collection != '' GROUP BY collection ORDER BY count() DESC LIMIT 10",
            
            # Q3: Filter by kind
            "SELECT count() FROM bluesky_minimal_1m.bluesky_data WHERE JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') = 'commit'",
            
            # Q4: Time range query
            "SELECT count() FROM bluesky_minimal_1m.bluesky_data WHERE JSONExtractUInt(toString(variantElement(data, 'JSON')), 'time_us') > 1700000000000000",
            
            # Q5: Complex aggregation
            "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'operation') as op, JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') as coll, count() FROM bluesky_minimal_1m.bluesky_data WHERE op != '' AND coll != '' GROUP BY op, coll ORDER BY count() DESC LIMIT 5"
        ]
        
        with open('queries_minimal_variant.sql', 'w') as f:
            for query in queries:
                f.write(query + ';\n')
        
        return 'queries_minimal_variant.sql'

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
        
        for iteration in range(self.iterations):
            exec_time, result = self.run_clickhouse_query(query)
            if exec_time > 0:
                times.append(exec_time)
            else:
                errors += 1
                if iteration == 0:  # Show error for first iteration
                    print(f"      Error: {result}")
        
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
        """Run comprehensive benchmarks for all approaches."""
        print("=" * 60)
        print(f"RUNNING BENCHMARKS ({self.iterations} iterations per query)")
        print("=" * 60)
        
        # Create minimal variant queries
        self.approaches['minimal_variant']['queries_file'] = self.create_minimal_variant_queries()
        
        for approach_name, config in self.approaches.items():
            print(f"\n=== {config['description']} ===")
            
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
                print(f"  Q{i}: ", end='', flush=True)
                
                stats = self.run_query_benchmark(approach_name, query, i)
                if stats:
                    approach_results[f'Q{i}'] = stats
                    print(f"avg={stats['mean']:.3f}s (min={stats['min']:.3f}, max={stats['max']:.3f}, std={stats['std']:.3f})")
                    if stats['errors'] > 0:
                        print(f"      Errors: {stats['errors']}/{self.iterations}")
                else:
                    print("FAILED - all iterations failed")
            
            self.results[approach_name] = {
                'config': config,
                'queries': approach_results
            }

    def generate_report(self):
        """Generate comprehensive benchmark report."""
        print("\n" + "=" * 80)
        print("COMPREHENSIVE BENCHMARK RESULTS (1M RECORDS)")
        print("=" * 80)
        
        # Summary table
        print(f"\n{'Approach':<30} {'Q1':<8} {'Q2':<8} {'Q3':<8} {'Q4':<8} {'Q5':<8} {'Avg':<8}")
        print("-" * 80)
        
        approach_averages = {}
        
        for approach_name, data in self.results.items():
            config = data['config']
            queries = data['queries']
            
            times = []
            row = f"{config['description']:<30}"
            
            for i in range(1, 6):
                q_key = f'Q{i}'
                if q_key in queries:
                    avg_time = queries[q_key]['mean']
                    times.append(avg_time)
                    row += f" {avg_time:.3f}s"
                else:
                    row += f" {'---':<7}"
            
            if times:
                overall_avg = statistics.mean(times)
                approach_averages[approach_name] = overall_avg
                row += f" {overall_avg:.3f}s"
            else:
                row += f" {'---':<7}"
            
            print(row)
        
        # Performance comparison
        if approach_averages:
            print(f"\n{'Performance Ranking:':<30}")
            sorted_approaches = sorted(approach_averages.items(), key=lambda x: x[1])
            baseline_time = sorted_approaches[0][1]
            
            for i, (approach_name, avg_time) in enumerate(sorted_approaches, 1):
                config = self.results[approach_name]['config']
                ratio = avg_time / baseline_time
                print(f"  {i}. {config['description']:<35} {avg_time:.3f}s ({ratio:.2f}x)")
        
        # Detailed statistics
        print(f"\n{'DETAILED STATISTICS':<30}")
        print("-" * 50)
        
        for approach_name, data in self.results.items():
            config = data['config']
            print(f"\n{config['description']}:")
            
            for q_key, stats in data['queries'].items():
                print(f"  {q_key}: mean={stats['mean']:.3f}s, median={stats['median']:.3f}s, "
                      f"std={stats['std']:.3f}s, success={stats['successful_runs']}/{self.iterations}")

    def save_results(self):
        """Save results to JSON file."""
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_results_1m_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        print(f"\nResults saved to: {filename}")

def main():
    """Main benchmark execution."""
    print("COMPREHENSIVE CLICKHOUSE JSON BENCHMARK - 1M RECORDS")
    print("Testing all approaches with 10 iterations per query")
    print("=" * 60)
    
    benchmark = ComprehensiveBenchmark()
    
    # Check current status
    benchmark.check_table_status()
    
    # Ask user if they want to reload data
    response = input("Do you want to reload all data (1M records)? This will take time. (y/N): ")
    if response.lower() in ['y', 'yes']:
        benchmark.create_schemas()
        benchmark.load_all_data()
        print("\nRechecking table status after loading...")
        benchmark.check_table_status()
    
    # Run benchmarks
    benchmark.run_benchmarks()
    
    # Generate report
    benchmark.generate_report()
    
    # Save results
    benchmark.save_results()
    
    print("\n" + "=" * 60)
    print("BENCHMARK COMPLETED!")
    print("=" * 60)

if __name__ == '__main__':
    main() 