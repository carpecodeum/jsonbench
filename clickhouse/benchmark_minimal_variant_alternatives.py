#!/usr/bin/env python3
"""
Benchmark Alternative Query Methods for Minimal Variant Approach
Tests 3 different ways to query Variant(JSON) data for performance comparison.
"""

import subprocess
import time
import statistics
import sys

class MinimalVariantBenchmark:
    def __init__(self):
        self.database = 'bluesky_minimal_1m'
        self.table = 'bluesky_data'
        self.iterations = 10
        # Memory optimization settings for all queries
        self.settings = "SETTINGS max_threads = 1, max_memory_usage = 4000000000"
        
        self.methods = {
            'toString_method': {
                'name': 'toString() + JSONExtractString (Original)',
                'queries': [
                    f"SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') as kind, count() FROM bluesky_minimal_1m.bluesky_data GROUP BY kind ORDER BY count() DESC {self.settings}",
                    f"SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') as collection, count() FROM bluesky_minimal_1m.bluesky_data WHERE collection != '' GROUP BY collection ORDER BY count() DESC LIMIT 10 {self.settings}",
                    f"SELECT count() FROM bluesky_minimal_1m.bluesky_data WHERE JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') = 'commit' {self.settings}",
                    f"SELECT count() FROM bluesky_minimal_1m.bluesky_data WHERE JSONExtractUInt(toString(variantElement(data, 'JSON')), 'time_us') > 1700000000000000 {self.settings}",
                    f"SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'operation') as op, JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') as coll, count() FROM bluesky_minimal_1m.bluesky_data WHERE op != '' AND coll != '' GROUP BY op, coll ORDER BY count() DESC LIMIT 5 {self.settings}"
                ]
            },
            'cast_method': {
                'name': 'CAST() + JSONExtractString (Alternative 1)',
                'queries': [
                    f"SELECT JSONExtractString(CAST(variantElement(data, 'JSON') AS String), 'kind') as kind, count() FROM bluesky_minimal_1m.bluesky_data GROUP BY kind ORDER BY count() DESC {self.settings}",
                    f"SELECT JSONExtractString(CAST(variantElement(data, 'JSON') AS String), 'commit', 'collection') as collection, count() FROM bluesky_minimal_1m.bluesky_data WHERE collection != '' GROUP BY collection ORDER BY count() DESC LIMIT 10 {self.settings}",
                    f"SELECT count() FROM bluesky_minimal_1m.bluesky_data WHERE JSONExtractString(CAST(variantElement(data, 'JSON') AS String), 'kind') = 'commit' {self.settings}",
                    f"SELECT count() FROM bluesky_minimal_1m.bluesky_data WHERE JSONExtractUInt(CAST(variantElement(data, 'JSON') AS String), 'time_us') > 1700000000000000 {self.settings}",
                    f"SELECT JSONExtractString(CAST(variantElement(data, 'JSON') AS String), 'commit', 'operation') as op, JSONExtractString(CAST(variantElement(data, 'JSON') AS String), 'commit', 'collection') as coll, count() FROM bluesky_minimal_1m.bluesky_data WHERE op != '' AND coll != '' GROUP BY op, coll ORDER BY count() DESC LIMIT 5 {self.settings}"
                ]
            },
            'json_value_method': {
                'name': 'JSON_VALUE with JSONPath (Alternative 2)',
                'queries': [
                    f"SELECT JSON_VALUE(toString(variantElement(data, 'JSON')), '$.kind') as kind, count() FROM bluesky_minimal_1m.bluesky_data GROUP BY kind ORDER BY count() DESC {self.settings}",
                    f"SELECT JSON_VALUE(toString(variantElement(data, 'JSON')), '$.commit.collection') as collection, count() FROM bluesky_minimal_1m.bluesky_data WHERE collection != '' GROUP BY collection ORDER BY count() DESC LIMIT 10 {self.settings}",
                    f"SELECT count() FROM bluesky_minimal_1m.bluesky_data WHERE JSON_VALUE(toString(variantElement(data, 'JSON')), '$.kind') = 'commit' {self.settings}",
                    f"SELECT count() FROM bluesky_minimal_1m.bluesky_data WHERE CAST(JSON_VALUE(toString(variantElement(data, 'JSON')), '$.time_us') AS UInt64) > 1700000000000000 {self.settings}",
                    f"SELECT JSON_VALUE(toString(variantElement(data, 'JSON')), '$.commit.operation') as op, JSON_VALUE(toString(variantElement(data, 'JSON')), '$.commit.collection') as coll, count() FROM bluesky_minimal_1m.bluesky_data WHERE op != '' AND coll != '' GROUP BY op, coll ORDER BY count() DESC LIMIT 5 {self.settings}"
                ]
            }
        }
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

    def check_data_availability(self):
        """Check if the minimal variant table has data."""
        print("=" * 60)
        print("CHECKING DATA AVAILABILITY")
        print("=" * 60)
        
        count_query = f"SELECT count() FROM {self.database}.{self.table}"
        exec_time, result = self.run_clickhouse_query(count_query)
        
        if exec_time > 0:
            count = int(result)
            size_query = f"SELECT formatReadableSize(sum(bytes_on_disk)) FROM system.parts WHERE database = '{self.database}' AND table = '{self.table}' AND active = 1"
            _, size_result = self.run_clickhouse_query(size_query)
            print(f"Minimal Variant Table: {count:,} records, {size_result}")
            return count > 0
        else:
            print(f"Error accessing table: {result}")
            return False

    def run_query_benchmark(self, method_name, query, query_num):
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
        """Run benchmarks for all three methods."""
        print("=" * 60)
        print(f"MINIMAL VARIANT QUERY METHOD COMPARISON ({self.iterations} iterations per query)")
        print("With memory optimization: max_threads=1, max_memory_usage=4GB")
        print("=" * 60)
        
        for method_key, method_config in self.methods.items():
            print(f"\n=== {method_config['name']} ===")
            
            method_results = {}
            method_times = []
            
            for i, query in enumerate(method_config['queries'], 1):
                print(f"  Q{i}: ", end='', flush=True)
                
                stats = self.run_query_benchmark(method_key, query, i)
                if stats:
                    method_results[f'Q{i}'] = stats
                    method_times.append(stats['mean'])
                    print(f"avg={stats['mean']:.3f}s (min={stats['min']:.3f}, max={stats['max']:.3f}, std={stats['std']:.3f})")
                    if stats['errors'] > 0:
                        print(f"      Errors: {stats['errors']}/{self.iterations}")
                else:
                    print("FAILED - all iterations failed")
            
            # Calculate overall average for this method
            if method_times:
                overall_avg = statistics.mean(method_times)
                method_results['overall_avg'] = overall_avg
                print(f"  Overall Average: {overall_avg:.3f}s")
            
            self.results[method_key] = {
                'config': method_config,
                'queries': method_results
            }

    def generate_comparison_report(self):
        """Generate detailed comparison report."""
        print("\n" + "=" * 80)
        print("MINIMAL VARIANT QUERY METHOD COMPARISON REPORT")
        print("=" * 80)
        
        # Summary table
        print(f"\n{'Method':<40} {'Q1':<8} {'Q2':<8} {'Q3':<8} {'Q4':<8} {'Q5':<8} {'Avg':<8}")
        print("-" * 80)
        
        method_averages = {}
        
        for method_key, data in self.results.items():
            config = data['config']
            queries = data['queries']
            
            times = []
            row = f"{config['name']:<40}"
            
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
                method_averages[method_key] = overall_avg
                row += f" {overall_avg:.3f}s"
            else:
                row += f" {'---':<7}"
            
            print(row)
        
        # Performance comparison
        if method_averages:
            print(f"\n{'Performance Ranking:':<40}")
            sorted_methods = sorted(method_averages.items(), key=lambda x: x[1])
            baseline_time = sorted_methods[0][1]
            
            for i, (method_key, avg_time) in enumerate(sorted_methods, 1):
                config = self.results[method_key]['config']
                ratio = avg_time / baseline_time
                print(f"  {i}. {config['name']:<45} {avg_time:.3f}s ({ratio:.2f}x)")
        
        # Method-specific insights
        print(f"\n{'METHOD ANALYSIS:':<40}")
        print("-" * 50)
        
        for method_key, data in self.results.items():
            config = data['config']
            print(f"\n{config['name']}:")
            
            success_rate = 0
            total_queries = 0
            for q_key, stats in data['queries'].items():
                if q_key.startswith('Q'):
                    total_queries += 1
                    if stats['successful_runs'] == self.iterations:
                        success_rate += 1
            
            if total_queries > 0:
                success_percentage = (success_rate / total_queries) * 100
                print(f"  Success Rate: {success_rate}/{total_queries} queries ({success_percentage:.0f}%)")
            
            # Show detailed stats for each query
            for q_key, stats in data['queries'].items():
                if q_key.startswith('Q'):
                    print(f"  {q_key}: {stats['mean']:.3f}s ± {stats['std']:.3f}s")

def main():
    """Main benchmark execution."""
    print("MINIMAL VARIANT QUERY METHOD BENCHMARK WITH MEMORY OPTIMIZATION")
    print("Comparing toString(), CAST(), and JSON_VALUE approaches")
    print("=" * 60)
    
    benchmark = MinimalVariantBenchmark()
    
    # Check data availability
    if not benchmark.check_data_availability():
        print("Error: No data available in minimal variant table. Exiting.")
        return
    
    # Run benchmarks
    benchmark.run_benchmarks()
    
    # Generate comparison report
    benchmark.generate_comparison_report()
    
    print("\n" + "=" * 60)
    print("BENCHMARK COMPLETED!")
    print("=" * 60)

if __name__ == '__main__':
    main() 