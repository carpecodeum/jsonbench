#!/usr/bin/env python3
"""
Benchmark script for minimal variant table.
Tests performance of various query patterns on the ultra-simple single variant column.
"""

import subprocess
import time
import sys
from typing import List, Tuple

def run_clickhouse_query(query: str, iterations: int = 3) -> Tuple[float, str]:
    """Run a ClickHouse query multiple times and return average time and result."""
    times = []
    result = ""
    
    for i in range(iterations):
        start_time = time.time()
        cmd = ['clickhouse', 'client', '--query', query]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        end_time = time.time()
        
        if proc.returncode != 0:
            return -1, f"Error: {proc.stderr}"
        
        times.append(end_time - start_time)
        if i == 0:  # Store result from first run
            result = proc.stdout.strip()
    
    avg_time = sum(times) / len(times)
    return avg_time, result

def test_basic_queries():
    """Test basic variant queries."""
    print("=" * 60)
    print("BASIC VARIANT QUERIES")
    print("=" * 60)
    
    queries = [
        ("Record Count", "SELECT count() FROM bluesky_minimal_variant.bluesky_data"),
        ("Variant Type", "SELECT variantType(data), count() FROM bluesky_minimal_variant.bluesky_data GROUP BY variantType(data)"),
        ("Data Size", "SELECT formatReadableSize(sum(data_compressed_bytes)) as compressed_size, formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed_size FROM system.columns WHERE database = 'bluesky_minimal_variant'"),
    ]
    
    for name, query in queries:
        print(f"\n{name}:")
        avg_time, result = run_clickhouse_query(query)
        if avg_time > 0:
            print(f"  Time: {avg_time:.4f}s")
            print(f"  Result: {result}")
        else:
            print(f"  Error: {result}")

def test_json_extraction():
    """Test JSON field extraction patterns."""
    print("\n" + "=" * 60)
    print("JSON FIELD EXTRACTION")
    print("=" * 60)
    
    # Test different JSON extraction methods
    extraction_queries = [
        ("JSON Extract - kind", "SELECT JSONExtractString(variantElement(data, 'JSON'), 'kind') as kind, count() as cnt FROM bluesky_minimal_variant.bluesky_data GROUP BY kind ORDER BY cnt DESC LIMIT 5"),
        ("JSON Extract - did", "SELECT JSONExtractString(variantElement(data, 'JSON'), 'did') as did FROM bluesky_minimal_variant.bluesky_data LIMIT 3"),
        ("JSON Extract - time_us", "SELECT JSONExtractUInt(variantElement(data, 'JSON'), 'time_us') as time_us FROM bluesky_minimal_variant.bluesky_data WHERE JSONExtractUInt(variantElement(data, 'JSON'), 'time_us') > 0 LIMIT 5"),
        ("JSON Extract - collection", "SELECT JSONExtractString(variantElement(data, 'JSON'), 'commit', 'collection') as collection, count() FROM bluesky_minimal_variant.bluesky_data WHERE JSONExtractString(variantElement(data, 'JSON'), 'commit', 'collection') != '' GROUP BY collection ORDER BY count() DESC LIMIT 5"),
    ]
    
    for name, query in extraction_queries:
        print(f"\n{name}:")
        avg_time, result = run_clickhouse_query(query)
        if avg_time > 0:
            print(f"  Time: {avg_time:.4f}s")
            print(f"  Result: {result}")
        else:
            print(f"  Error: {result}")

def test_filtering_queries():
    """Test filtering performance on variant data."""
    print("\n" + "=" * 60)
    print("FILTERING PERFORMANCE")
    print("=" * 60)
    
    filter_queries = [
        ("Filter by kind", "SELECT count() FROM bluesky_minimal_variant.bluesky_data WHERE JSONExtractString(variantElement(data, 'JSON'), 'kind') = 'commit'"),
        ("Filter by collection", "SELECT count() FROM bluesky_minimal_variant.bluesky_data WHERE JSONExtractString(variantElement(data, 'JSON'), 'commit', 'collection') = 'app.bsky.feed.post'"),
        ("Complex filter", "SELECT count() FROM bluesky_minimal_variant.bluesky_data WHERE JSONExtractString(variantElement(data, 'JSON'), 'kind') = 'commit' AND JSONExtractString(variantElement(data, 'JSON'), 'commit', 'collection') LIKE '%post%'"),
        ("Time range filter", "SELECT count() FROM bluesky_minimal_variant.bluesky_data WHERE JSONExtractUInt(variantElement(data, 'JSON'), 'time_us') > 1600000000000000"),
    ]
    
    for name, query in filter_queries:
        print(f"\n{name}:")
        avg_time, result = run_clickhouse_query(query)
        if avg_time > 0:
            print(f"  Time: {avg_time:.4f}s")
            print(f"  Result: {result}")
        else:
            print(f"  Error: {result}")

def test_aggregation_queries():
    """Test aggregation performance."""
    print("\n" + "=" * 60)
    print("AGGREGATION PERFORMANCE")
    print("=" * 60)
    
    agg_queries = [
        ("Count by kind", "SELECT JSONExtractString(variantElement(data, 'JSON'), 'kind') as kind, count() FROM bluesky_minimal_variant.bluesky_data GROUP BY kind ORDER BY count() DESC"),
        ("Count by collection", "SELECT JSONExtractString(variantElement(data, 'JSON'), 'commit', 'collection') as collection, count() FROM bluesky_minimal_variant.bluesky_data WHERE collection != '' GROUP BY collection ORDER BY count() DESC LIMIT 10"),
        ("Time stats", "SELECT min(JSONExtractUInt(variantElement(data, 'JSON'), 'time_us')), max(JSONExtractUInt(variantElement(data, 'JSON'), 'time_us')), avg(JSONExtractUInt(variantElement(data, 'JSON'), 'time_us')) FROM bluesky_minimal_variant.bluesky_data WHERE JSONExtractUInt(variantElement(data, 'JSON'), 'time_us') > 0"),
    ]
    
    for name, query in agg_queries:
        print(f"\n{name}:")
        avg_time, result = run_clickhouse_query(query)
        if avg_time > 0:
            print(f"  Time: {avg_time:.4f}s")
            print(f"  Result: {result}")
        else:
            print(f"  Error: {result}")

def compare_with_json_table():
    """Compare minimal variant performance with regular JSON table."""
    print("\n" + "=" * 60)
    print("COMPARISON: MINIMAL VARIANT vs REGULAR JSON")
    print("=" * 60)
    
    # Test same queries on both tables
    test_queries = [
        ("Count records", 
         "SELECT count() FROM bluesky_minimal_variant.bluesky_data",
         "SELECT count() FROM bluesky_sample.bluesky"),
        
        ("Extract kind field",
         "SELECT JSONExtractString(variantElement(data, 'JSON'), 'kind') as kind FROM bluesky_minimal_variant.bluesky_data LIMIT 1000",
         "SELECT data.kind FROM bluesky_sample.bluesky LIMIT 1000"),
        
        ("Filter by kind",
         "SELECT count() FROM bluesky_minimal_variant.bluesky_data WHERE JSONExtractString(variantElement(data, 'JSON'), 'kind') = 'commit'",
         "SELECT count() FROM bluesky_sample.bluesky WHERE data.kind = 'commit'"),
         
        ("Group by collection",
         "SELECT JSONExtractString(variantElement(data, 'JSON'), 'commit', 'collection') as collection, count() FROM bluesky_minimal_variant.bluesky_data WHERE collection != '' GROUP BY collection ORDER BY count() DESC LIMIT 5",
         "SELECT data.commit.collection as collection, count() FROM bluesky_sample.bluesky WHERE collection != '' GROUP BY collection ORDER BY count() DESC LIMIT 5"),
    ]
    
    for name, variant_query, json_query in test_queries:
        print(f"\n{name}:")
        
        # Test variant query
        variant_time, variant_result = run_clickhouse_query(variant_query)
        print(f"  Minimal Variant: {variant_time:.4f}s")
        
        # Test JSON query
        json_time, json_result = run_clickhouse_query(json_query)
        print(f"  Regular JSON:    {json_time:.4f}s")
        
        if variant_time > 0 and json_time > 0:
            ratio = variant_time / json_time
            print(f"  Ratio (V/J):     {ratio:.2f}x")
            if ratio > 1:
                print(f"  → JSON is {ratio:.1f}x faster")
            else:
                print(f"  → Variant is {1/ratio:.1f}x faster")

def show_storage_stats():
    """Show storage statistics."""
    print("\n" + "=" * 60)
    print("STORAGE STATISTICS")
    print("=" * 60)
    
    storage_queries = [
        ("Minimal Variant Table Size", "SELECT formatReadableSize(sum(bytes_on_disk)) as size_on_disk, count() as rows FROM system.parts WHERE database = 'bluesky_minimal_variant' AND table = 'bluesky_data' AND active = 1"),
        ("Regular JSON Table Size", "SELECT formatReadableSize(sum(bytes_on_disk)) as size_on_disk, count() as rows FROM system.parts WHERE database = 'bluesky_sample' AND table = 'bluesky' AND active = 1"),
        ("Column Details", "SELECT column, formatReadableSize(data_compressed_bytes) as compressed, formatReadableSize(data_uncompressed_bytes) as uncompressed FROM system.columns WHERE database = 'bluesky_minimal_variant' AND table = 'bluesky_data'"),
    ]
    
    for name, query in storage_queries:
        print(f"\n{name}:")
        avg_time, result = run_clickhouse_query(query)
        if avg_time > 0:
            print(f"  {result}")
        else:
            print(f"  Error: {result}")

def main():
    """Run all benchmarks."""
    print("MINIMAL VARIANT TABLE BENCHMARKS")
    print("=" * 60)
    print("Testing ultra-simple single Variant(JSON) column performance")
    print("")
    
    # Run all benchmark categories
    test_basic_queries()
    test_json_extraction()
    test_filtering_queries()
    test_aggregation_queries()
    compare_with_json_table()
    show_storage_stats()
    
    print("\n" + "=" * 60)
    print("BENCHMARK SUMMARY")
    print("=" * 60)
    print("✓ Minimal variant table uses only 1 column: data Variant(JSON)")
    print("✓ All field access requires JSONExtract functions")
    print("✓ Schema-on-read: can query any field without predefinition")
    print("✓ Compare results above to see performance vs regular JSON")
    print("")
    print("Key takeaways:")
    print("- Simpler schema, more complex queries")
    print("- True flexibility: query any JSON field")
    print("- Performance trade-off for simplicity")

if __name__ == '__main__':
    main() 