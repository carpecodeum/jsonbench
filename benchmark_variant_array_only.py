#!/usr/bin/env python3
"""
Benchmark script for ONLY the variant array approach.
Tests 1 row, 1 column, all 100M JSON objects in a single array.
"""
import subprocess
import time
import json
from datetime import datetime

def run_query(query, description):
    """Execute a query and measure performance."""
    print(f"\n📊 {description}")
    print(f"Query: {query}")
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            ['clickhouse-client', '--query', query, '--format', 'JSONCompact'],
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        if result.returncode == 0:
            # Parse result
            try:
                result_data = json.loads(result.stdout)
                row_count = len(result_data.get('data', []))
                print(f"✅ SUCCESS: {duration:.3f}s, {row_count} result rows")
                return {
                    'query': description,
                    'duration_seconds': duration,
                    'result_rows': row_count,
                    'status': 'success'
                }
            except json.JSONDecodeError:
                print(f"✅ SUCCESS: {duration:.3f}s (non-JSON result)")
                return {
                    'query': description,
                    'duration_seconds': duration,
                    'result_rows': 'N/A',
                    'status': 'success'
                }
        else:
            print(f"❌ FAILED: {result.stderr}")
            return {
                'query': description,
                'duration_seconds': duration,
                'error': result.stderr,
                'status': 'failed'
            }
            
    except subprocess.TimeoutExpired:
        print(f"⏰ TIMEOUT: Query exceeded 5 minutes")
        return {
            'query': description,
            'duration_seconds': 300,
            'error': 'Query timeout',
            'status': 'timeout'
        }

def verify_data_structure():
    """Verify that we have the correct 1-row array structure."""
    print("🔍 Verifying variant array data structure...")
    
    # Check row count
    result = run_query(
        "SELECT count() FROM bluesky_100m_variant_array.bluesky_array_data",
        "Row count verification"
    )
    
    if result['status'] != 'success':
        return False
    
    # Check array length
    result = run_query(
        "SELECT length(data.Array) FROM bluesky_100m_variant_array.bluesky_array_data",
        "Array length verification"
    )
    
    return result['status'] == 'success'

def run_variant_array_benchmark():
    """Run the complete benchmark suite for variant array approach."""
    print("🚀 Starting Variant Array Benchmark")
    print("=" * 60)
    
    # Verify data structure first
    if not verify_data_structure():
        print("❌ Data structure verification failed!")
        return None
    
    results = []
    
    # Query 1: Count by kind
    results.append(run_query(
        """
        SELECT toString(arrayElement(data.Array, i).kind) as kind, count() 
        FROM bluesky_100m_variant_array.bluesky_array_data 
        ARRAY JOIN arrayEnumerate(data.Array) AS i 
        GROUP BY kind 
        ORDER BY count() DESC
        """,
        "Q1: Count by kind"
    ))
    
    # Query 2: Top collections
    results.append(run_query(
        """
        SELECT toString(arrayElement(data.Array, i).commit.collection) as collection, count() 
        FROM bluesky_100m_variant_array.bluesky_array_data 
        ARRAY JOIN arrayEnumerate(data.Array) AS i 
        WHERE collection != '' 
        GROUP BY collection 
        ORDER BY count() DESC 
        LIMIT 10
        """,
        "Q2: Top collections"
    ))
    
    # Query 3: Filter commits
    results.append(run_query(
        """
        SELECT count() 
        FROM bluesky_100m_variant_array.bluesky_array_data 
        ARRAY JOIN arrayEnumerate(data.Array) AS i 
        WHERE toString(arrayElement(data.Array, i).kind) = 'commit'
        """,
        "Q3: Count commits"
    ))
    
    # Query 4: Time range query
    results.append(run_query(
        """
        SELECT count() 
        FROM bluesky_100m_variant_array.bluesky_array_data 
        ARRAY JOIN arrayEnumerate(data.Array) AS i 
        WHERE toUInt64(arrayElement(data.Array, i).time_us) > 1700000000000000
        """,
        "Q4: Time range filter"
    ))
    
    # Query 5: Complex aggregation
    results.append(run_query(
        """
        SELECT toString(arrayElement(data.Array, i).commit.operation) as op, 
               toString(arrayElement(data.Array, i).commit.collection) as coll, 
               count() 
        FROM bluesky_100m_variant_array.bluesky_array_data 
        ARRAY JOIN arrayEnumerate(data.Array) AS i 
        WHERE op != '' AND coll != '' 
        GROUP BY op, coll 
        ORDER BY count() DESC 
        LIMIT 5
        """,
        "Q5: Operations by collection"
    ))
    
    return results

def save_results(results):
    """Save benchmark results to file."""
    if not results:
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"variant_array_results_{timestamp}.json"
    
    summary = {
        'timestamp': timestamp,
        'approach': 'variant_array',
        'description': '1 row, 1 column, all JSON objects in single array',
        'queries': results
    }
    
    with open(filename, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n💾 Results saved to: {filename}")
    
    # Print summary
    print("\n📈 BENCHMARK SUMMARY")
    print("=" * 40)
    
    successful_queries = [r for r in results if r['status'] == 'success']
    failed_queries = [r for r in results if r['status'] == 'failed']
    timeout_queries = [r for r in results if r['status'] == 'timeout']
    
    print(f"✅ Successful: {len(successful_queries)}")
    print(f"❌ Failed: {len(failed_queries)}")
    print(f"⏰ Timeout: {len(timeout_queries)}")
    
    if successful_queries:
        total_time = sum(r['duration_seconds'] for r in successful_queries)
        avg_time = total_time / len(successful_queries)
        print(f"⏱️  Average query time: {avg_time:.3f}s")
        print(f"⏱️  Total execution time: {total_time:.3f}s")
    
    print("\nQuery Details:")
    for result in results:
        status_icon = "✅" if result['status'] == 'success' else "❌" if result['status'] == 'failed' else "⏰"
        print(f"{status_icon} {result['query']}: {result['duration_seconds']:.3f}s")

def main():
    """Main execution."""
    print("=" * 60)
    print("VARIANT ARRAY BENCHMARK")
    print("Testing: 1 row, 1 column, all 100M JSON objects")
    print("=" * 60)
    
    # Run benchmark
    results = run_variant_array_benchmark()
    
    if results:
        save_results(results)
        print("\n🎉 Benchmark completed!")
    else:
        print("\n💥 Benchmark failed!")

if __name__ == "__main__":
    main() 