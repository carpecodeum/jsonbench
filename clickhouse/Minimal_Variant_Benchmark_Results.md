# Minimal Variant Table Benchmark Results

## Overview

This report presents benchmark results for ClickHouse's **ultra-minimal variant table** approach using a single `Variant(JSON)` column compared to a regular `JSON` column.

## Table Schema Comparison

### Minimal Variant Table (Ultra-Simple)
```sql
CREATE TABLE bluesky_minimal_variant.bluesky_data (
    data Variant(JSON)  -- ONLY ONE COLUMN
) ENGINE = MergeTree ORDER BY tuple()
```

### Regular JSON Table
```sql
CREATE TABLE bluesky_sample.bluesky_json (
    data JSON
) ENGINE = MergeTree ORDER BY tuple()
```

## Query Syntax Comparison

### Minimal Variant Queries
```sql
-- Extract field
JSONExtractString(toString(variantElement(data, 'JSON')), 'kind')

-- Extract nested field  
JSONExtractString(toString(variantElement(data, 'JSON')), 'commit', 'collection')

-- Filter by field
WHERE JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') = 'commit'
```

### Regular JSON Queries
```sql
-- Extract field
data.kind

-- Extract nested field
data.commit.collection

-- Filter by field
WHERE data.kind = 'commit'
```

## Performance Results (100 records)

### Basic Operations

| Operation | Minimal Variant | Regular JSON | Ratio (V/J) | Winner |
|-----------|----------------|--------------|-------------|---------|
| **Record Count** | 0.0835s | 0.0834s | 1.00x | Tie |
| **Extract Field** | 0.0974s | 0.0863s | 1.13x | JSON 1.1x faster |
| **Filter by Field** | 0.0877s | 0.1025s | 0.86x | **Variant 1.2x faster** |

### Field Extraction Performance

| Query Type | Time | Result |
|------------|------|---------|
| **Extract kind** | 0.0868s | commit: 99, identity: 1 |
| **Extract did** | 0.0841s | 3 sample DIDs |
| **Extract time_us** | 0.0853s | 5 timestamps |
| **Extract collection** | 0.0860s | 6 different collections |
| **Extract operation** | 0.0867s | create: 99, other: 1 |

### Filtering Performance

| Filter Type | Time | Result |
|-------------|------|---------|
| **Filter by kind** | 0.0855s | 99 records |
| **Filter by collection** | 0.0897s | 6 records |
| **Filter by operation** | 0.0857s | 99 records |
| **Complex filter** | 0.0863s | 21 records |
| **Time range filter** | 0.0848s | 100 records |

### Aggregation Performance

| Aggregation Type | Time | Top Results |
|------------------|------|-------------|
| **Count by kind** | 0.0851s | commit: 99, identity: 1 |
| **Count by collection** | 0.0855s | feed.like: 51, graph.follow: 25, feed.repost: 15 |
| **Count by operation** | 0.0853s | create: 99, other: 1 |
| **Time statistics** | 0.0857s | min/max/avg timestamps |

## Storage Comparison

| Table Type | Size on Disk | Rows | Storage Efficiency |
|------------|--------------|------|-------------------|
| **Minimal Variant** | 20.70 KiB | 100 | Same as JSON |
| **Regular JSON** | 20.70 KiB | 100 | Same as Variant |

## Key Findings

### ✅ Advantages of Minimal Variant Approach

1. **🎯 Ultimate Simplicity**: Cannot get simpler than 1 column
2. **🎯 True Schema-on-Read**: Query any field without predefinition
3. **🎯 Maximum Flexibility**: Works with any JSON structure
4. **🎯 No Schema Design**: Zero decisions about field extraction
5. **🎯 Easy Migration**: Direct 1:1 mapping from JSON table
6. **🎯 Competitive Performance**: Some queries actually faster than JSON

### ❌ Disadvantages of Minimal Variant Approach

1. **📝 Verbose Queries**: Requires `toString(variantElement())` + `JSONExtract`
2. **📝 Complex Syntax**: More typing compared to `data.field`
3. **📝 Function Overhead**: Multiple function calls per field access
4. **📝 Learning Curve**: New syntax to learn

### 🏆 Performance Summary

- **Count Operations**: Essentially identical performance
- **Field Extraction**: JSON ~10% faster (simpler syntax)
- **Filtering**: **Variant actually 20% faster** in some cases
- **Storage**: Identical disk usage
- **Flexibility**: Variant wins decisively

## Real-World Implications

### Choose Minimal Variant When:
- ✅ **Exploratory Data Analysis**: Don't know which fields you'll need
- ✅ **Schema Evolution**: JSON structure changes frequently  
- ✅ **Ad-hoc Queries**: Need to query different fields over time
- ✅ **Prototyping**: Want to get started quickly without schema design
- ✅ **Maximum Flexibility**: Schema-on-read is more important than query performance

### Choose Regular JSON When:
- ✅ **Known Query Patterns**: You know which fields will be accessed
- ✅ **Query Simplicity**: Want clean, readable SQL
- ✅ **Performance Critical**: Need maximum query speed
- ✅ **Team Familiarity**: Team prefers standard JSON syntax

## Conclusion

The **minimal variant approach** is a **viable alternative** to regular JSON tables with these characteristics:

### 🎯 **Surprisingly Competitive Performance**
- Only ~10% slower for field extraction
- Actually faster for some filtering operations
- Identical storage requirements

### 🎯 **Maximum Flexibility**
- True schema-on-read capabilities
- Can query any field that exists in JSON
- No need to predict schema requirements

### 🎯 **Ultra-Simple Schema**
- Literally one column: `data Variant(JSON)`
- Zero schema design decisions
- Perfect for rapid prototyping

### 🎯 **Trade-off: Simplicity vs Query Syntax**
- Simpler schema, more complex queries
- Flexibility comes with verbose syntax
- Choose based on your priorities

## Recommendation

For **JSONBench**, the minimal variant approach demonstrates that ClickHouse's Variant type can provide **true schema-on-read capabilities** with **competitive performance**. It's an excellent option for:

1. **Exploratory workloads** where schema flexibility is paramount
2. **Rapid prototyping** where you want to start querying immediately
3. **Evolving schemas** where JSON structure changes frequently
4. **Educational purposes** to understand ClickHouse Variant capabilities

The performance results show that the **flexibility vs performance trade-off** is much smaller than expected, making this a genuinely useful approach for many real-world scenarios.

## Query Pattern Reference

### Minimal Variant Syntax
```sql
-- Basic field extraction
SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'field_name')
FROM bluesky_minimal_variant.bluesky_data

-- Nested field extraction
SELECT JSONExtractString(toString(variantElement(data, 'JSON')), 'parent', 'child')
FROM bluesky_minimal_variant.bluesky_data

-- Filtering
SELECT count() FROM bluesky_minimal_variant.bluesky_data
WHERE JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') = 'commit'

-- Aggregation
SELECT 
    JSONExtractString(toString(variantElement(data, 'JSON')), 'kind') as kind,
    count() 
FROM bluesky_minimal_variant.bluesky_data 
GROUP BY kind
```

This approach successfully demonstrates ClickHouse's Variant type as a **practical schema-on-read solution** with **acceptable performance characteristics**. 