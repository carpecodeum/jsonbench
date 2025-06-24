# Minimal Variant Query Methods Benchmark Report

## Executive Summary

This report compares three different approaches to query JSON data stored in ClickHouse `Variant(JSON)` columns, testing performance across 5 representative queries with 1M records.

## Test Environment

- **Dataset**: 1M Bluesky social media records (180.03 MiB)
- **Table Structure**: `data Variant(JSON)` - single column storing JSON
- **Iterations**: 10 runs per query for statistical significance
- **Memory Optimization**: All queries use `max_threads=1, max_memory_usage=4GB`

## Query Methods Tested

### Method 1: toString() + JSONExtractString (Original)
```sql
JSONExtractString(toString(variantElement(data, 'JSON')), 'field')
```

### Method 2: CAST() + JSONExtractString (Alternative 1)  
```sql
JSONExtractString(CAST(variantElement(data, 'JSON') AS String), 'field')
```

### Method 3: JSON_VALUE with JSONPath (Alternative 2)
```sql
JSON_VALUE(toString(variantElement(data, 'JSON')), '$.field')
```

## Performance Results

| Method | Q1 | Q2 | Q3 | Q4 | Q5 | **Average** | Ranking |
|--------|----|----|----|----|----|-----------|----|
| **toString() + JSONExtractString** | 8.555s | 8.617s | 8.543s | 8.562s | 8.691s | **8.594s** | 🥇 **1st** |
| **JSON_VALUE with JSONPath** | 8.575s | 8.762s | 8.749s | 8.375s | 8.555s | **8.603s** | 🥈 **2nd** |
| **CAST() + JSONExtractString** | 8.426s | 10.477s | 8.830s | 8.648s | 8.885s | **9.053s** | 🥉 **3rd** |

### Performance Analysis

- **Winner**: `toString()` method is fastest at 8.594s average
- **Close Second**: `JSON_VALUE` is nearly identical at 8.603s (only 0.1% slower)  
- **Slowest**: `CAST()` method is 5.3% slower at 9.053s due to Q2 performance issues

## Detailed Query Breakdown

### Query Types Tested
1. **Q1**: Group by field (`kind`) - COUNT by category
2. **Q2**: Nested field grouping (`commit.collection`) - TOP 10 collections
3. **Q3**: Simple filter (`kind = 'commit'`) - COUNT with WHERE
4. **Q4**: Numeric filter (`time_us > threshold`) - Range query
5. **Q5**: Complex aggregation - Multi-field GROUP BY

### Method-Specific Performance

#### toString() + JSONExtractString (Original)
- ✅ **Best overall performance** (8.594s average)
- ✅ **Most consistent** timing across all queries
- ✅ **100% success rate** - no memory issues
- ✅ **Lowest variance** in execution times

#### JSON_VALUE with JSONPath (Alternative 2)  
- ✅ **Nearly identical performance** to toString() method
- ✅ **More readable syntax** with JSONPath expressions
- ✅ **Better for complex nested paths** (`$.commit.collection` vs `'commit', 'collection'`)
- ✅ **100% success rate** - reliable execution
- ⭐ **Recommended for new development**

#### CAST() + JSONExtractString (Alternative 1)
- ⚠️ **5.3% slower** overall due to Q2 performance issues
- ⚠️ **High variance** in Q2 (10.477s ± 3.742s)
- ✅ **Cleaner syntax** than toString() 
- ✅ **100% success rate** - works reliably

## Key Findings

### 1. Performance Similarity
All three methods show **similar performance** (~8.5-9s) because they all require the same fundamental operations:
- Extract JSON from Variant type
- Convert to String representation  
- Parse JSON string and extract fields

### 2. Memory Optimization Critical
**Without memory settings**, all methods fail with `MEMORY_LIMIT_EXCEEDED` errors. The optimization settings are essential:
```sql
SETTINGS max_threads = 1, max_memory_usage = 4000000000
```

### 3. Syntax Comparison

| Aspect | toString() | CAST() | JSON_VALUE |
|--------|------------|--------|------------|
| **Readability** | ⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Performance** | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| **Flexibility** | ⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ |
| **Nested Paths** | ⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ |

## Recommendations

### 🏆 **Primary Recommendation: JSON_VALUE**
```sql
JSON_VALUE(toString(variantElement(data, 'JSON')), '$.field')
```
**Why**: Best balance of performance, readability, and flexibility

### 🥈 **Alternative: toString() Method**  
```sql
JSONExtractString(toString(variantElement(data, 'JSON')), 'field')
```
**Why**: Marginally fastest, most established pattern

### 🥉 **Avoid: CAST() Method**
```sql 
JSONExtractString(CAST(variantElement(data, 'JSON') AS String), 'field')
```
**Why**: Slowest performance, especially on complex queries

## Comparison with JSON Baseline

⚠️ **Important Context**: All Variant approaches are **dramatically slower** than native JSON Object type:

| Approach | Performance | Storage |
|----------|-------------|---------|
| **JSON Object (native)** | **~0.12s** | 180.03 MiB |
| **Variant approaches** | **~8.6s** | 180.03 MiB |
| **Performance Ratio** | **70x slower** | Same storage |

## Conclusion

While all three Variant query methods work reliably with proper memory settings, **JSON_VALUE with JSONPath syntax** offers the best combination of performance and developer experience. However, for JSON-only use cases, **native JSON Object type remains 70x faster** with identical storage requirements.

The Variant approach is only beneficial when you need to store mixed data types in a single column - for pure JSON storage, stick with the native JSON Object type. 