#!/usr/bin/env python3
"""
Fixed benchmark script for minimal variant table.
Uses correct syntax: toString(variantElement(data, 'JSON')) for JSONExtract functions.
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
        ("Sample Data", "SELECT toString(variantElement(data, 'JSON')) FROM bluesky_minimal_variant.bluesky_data LIMIT 1"),
    ]
    
    for name, query in queries:
        print(f"\n{name}:")
        avg_time, result = run_clickhouse_query(query)
        if avg_time > 0:
            print(f"  Time: {avg_time:.4f}s")
            if name == "Sample Data":
                print(f"  Result: {result[:200]}...")  # Truncate long JSON
            else:
                print(f"  Result: {result}")
        else:
            print(f"  Error: {result}")

def test_json_extraction():
    """Test JSON field extraction patterns."""
    print("\n" + "=" * 60)
    print("JSON FIELD EXTRACTION")
    print("=" * 60)
    
    # Test different JSON extraction methods using correct syntax
    extraction_queries = [
        ("Extract kind", "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') as kind, count() as cnt FROM bluesky_minimal_variant.bluesky_data GROUP BY kind ORDER BY cnt DESC"),
        
        ("Extract did", "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'did') as did FROM bluesky_minimal_variant.bluesky_data LIMIT 3"),
        
        ("Extract time_us", "SELECT JSONExtractUInt(toString(variantElement(data, 'JSON')), 'time_us') as time_us FROM bluesky_minimal_variant.bluesky_data WHERE JSONExtractUInt(toString(variantElement(data, 'JSON')), 'time_us') > 0 LIMIT 5"),
        
        ("Extract collection", "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') as collection, count() FROM bluesky_minimal_variant.bluesky_data WHERE JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') != '' GROUP BY collection ORDER BY count() DESC LIMIT 5"),
        
        ("Extract operation", "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'operation') as operation, count() FROM bluesky_minimal_variant.bluesky_data GROUP BY operation"),
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
        ("Filter by kind", "SELECT count() FROM bluesky_minimal_variant.bluesky_data WHERE JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') = 'commit'"),
        
        ("Filter by collection", "SELECT count() FROM bluesky_minimal_variant.bluesky_data WHERE JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') = 'app.bsky.feed.post'"),
        
        ("Filter by operation", "SELECT count() FROM bluesky_minimal_variant.bluesky_data WHERE JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'operation') = 'create'"),
        
        ("Complex filter", "SELECT count() FROM bluesky_minimal_variant.bluesky_data WHERE JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') = 'commit' AND JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') LIKE '%post%'"),
        
        ("Time range filter", "SELECT count() FROM bluesky_minimal_variant.bluesky_data WHERE JSONExtractUInt(toString(variantElement(data, 'JSON')), 'time_us') > 1700000000000000"),
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
        ("Count by kind", "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') as kind, count() FROM bluesky_minimal_variant.bluesky_data GROUP BY kind ORDER BY count() DESC"),
        
        ("Count by collection", "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') as collection, count() FROM bluesky_minimal_variant.bluesky_data WHERE collection != '' GROUP BY collection ORDER BY count() DESC LIMIT 10"),
        
        ("Count by operation", "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'operation') as operation, count() FROM bluesky_minimal_variant.bluesky_data GROUP BY operation"),
        
        ("Time stats", "SELECT min(JSONExtractUInt(toString(variantElement(data, 'JSON')), 'time_us')), max(JSONExtractUInt(toString(variantElement(data, 'JSON')), 'time_us')), avg(JSONExtractUInt(toString(variantElement(data, 'JSON')), 'time_us')) FROM bluesky_minimal_variant.bluesky_data WHERE JSONExtractUInt(toString(variantElement(data, 'JSON')), 'time_us') > 0"),
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
         "SELECT count() FROM bluesky_sample.bluesky_json"),
        
        ("Extract kind field",
         "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') as kind FROM bluesky_minimal_variant.bluesky_data LIMIT 1000",
         "SELECT data.kind FROM bluesky_sample.bluesky_json LIMIT 1000"),
        
        ("Filter by kind",
         "SELECT count() FROM bluesky_minimal_variant.bluesky_data WHERE JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') = 'commit'",
         "SELECT count() FROM bluesky_sample.bluesky_json WHERE data.kind = 'commit'"),
         
        ("Group by collection",
         "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') as collection, count() FROM bluesky_minimal_variant.bluesky_data WHERE collection != '' GROUP BY collection ORDER BY count() DESC LIMIT 5",
         "SELECT data.commit.collection as collection, count() FROM bluesky_sample.bluesky_json WHERE collection != '' GROUP BY collection ORDER BY count() DESC LIMIT 5"),
         
        ("Complex aggregation",
         "SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'operation') as op, JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection') as coll, count() FROM bluesky_minimal_variant.bluesky_data GROUP BY op, coll ORDER BY count() DESC LIMIT 3",
         "SELECT data.commit.operation as op, data.commit.collection as coll, count() FROM bluesky_sample.bluesky_json GROUP BY op, coll ORDER BY count() DESC LIMIT 3"),
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
        
        # Show first few results for verification
        if variant_time > 0:
            print(f"  Variant result: {variant_result[:100]}...")
        if json_time > 0:
            print(f"  JSON result:    {json_result[:100]}...")

def show_storage_stats():
    """Show storage statistics."""
    print("\n" + "=" * 60)
    print("STORAGE STATISTICS")
    print("=" * 60)
    
    storage_queries = [
        ("Minimal Variant Table Size", "SELECT formatReadableSize(sum(bytes_on_disk)) as size_on_disk, sum(rows) as rows FROM system.parts WHERE database = 'bluesky_minimal_variant' AND table = 'bluesky_data' AND active = 1"),
        
        ("Regular JSON Table Size", "SELECT formatReadableSize(sum(bytes_on_disk)) as size_on_disk, sum(rows) as rows FROM system.parts WHERE database = 'bluesky_sample' AND table = 'bluesky_json' AND active = 1"),
        
        ("Column Details - Variant", "SELECT name, formatReadableSize(data_compressed_bytes) as compressed, formatReadableSize(data_uncompressed_bytes) as uncompressed FROM system.columns WHERE database = 'bluesky_minimal_variant' AND table = 'bluesky_data'"),
        
        ("Column Details - JSON", "SELECT name, formatReadableSize(data_compressed_bytes) as compressed, formatReadableSize(data_uncompressed_bytes) as uncompressed FROM system.columns WHERE database = 'bluesky_sample' AND table = 'bluesky_json'"),
    ]
    
    for name, query in storage_queries:
        print(f"\n{name}:")
        avg_time, result = run_clickhouse_query(query)
        if avg_time > 0:
            print(f"  {result}")
        else:
            print(f"  Error: {result}")

def show_query_patterns():
    """Show the query patterns for reference."""
    print("\n" + "=" * 60)
    print("QUERY PATTERN REFERENCE")
    print("=" * 60)
    
    print("\nMinimal Variant Query Patterns:")
    print("1. Extract field:")
    print("   JSONExtractString(toString(variantElement(data, 'JSON')), 'field_name')")
    print("\n2. Extract nested field:")
    print("   JSONExtractString(toString(variantElement(data, 'JSON')), 'parent', 'child')")
    print("\n3. Filter by field:")
    print("   WHERE JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') = 'commit'")
    print("\n4. Group by field:")
    print("   GROUP BY JSONExtractString(toString(variantElement(data, 'JSON')), 'field')")
    
    print("\nRegular JSON Query Patterns:")
    print("1. Extract field:")
    print("   data.field_name")
    print("\n2. Extract nested field:")
    print("   data.parent.child")
    print("\n3. Filter by field:")
    print("   WHERE data.kind = 'commit'")
    print("\n4. Group by field:")
    print("   GROUP BY data.field")
    
    print("\nKey Differences:")
    print("- Variant: Requires toString(variantElement()) + JSONExtract functions")
    print("- JSON: Direct field access with dot notation")
    print("- Variant: More verbose but works with any JSON structure")
    print("- JSON: Simpler syntax but requires known schema")

def main():
    """Run all benchmarks."""
    print("MINIMAL VARIANT TABLE BENCHMARKS (FIXED)")
    print("=" * 60)
    print("Testing ultra-simple single Variant(JSON) column performance")
    print("Using correct syntax: toString(variantElement(data, 'JSON'))")
    print("")
    
    # Run all benchmark categories
    test_basic_queries()
    test_json_extraction()
    test_filtering_queries()
    test_aggregation_queries()
    compare_with_json_table()
    show_storage_stats()
    show_query_patterns()
    
    print("\n" + "=" * 60)
    print("BENCHMARK SUMMARY")
    print("=" * 60)
    print("✓ Minimal variant table: 1 column data Variant(JSON)")
    print("✓ Query syntax: JSONExtract(toString(variantElement(data, 'JSON')), ...)")
    print("✓ Schema-on-read: can query any field without predefinition")
    print("✓ Performance trade-off: simpler schema, more complex queries")
    print("")
    print("Key findings:")
    print("- Variant queries are more verbose but extremely flexible")
    print("- JSON queries are simpler and likely faster")
    print("- Variant approach enables true schema-on-read capabilities")
    print("- Choose based on: flexibility needs vs query performance")

if __name__ == '__main__':
    main() 