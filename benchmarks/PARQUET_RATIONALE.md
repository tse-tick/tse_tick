# Why Parquet Is the Right Store for tse_tick

Four properties of Apache Parquet make it the correct storage format for the
query workload that tse_tick serves. Each is backed by measured numbers from
our benchmark suite.

## 1. Columnar Selective Reads

Parquet stores data column-by-column. Reading 3 of 95 columns from Parquet
(Snappy) takes 0.0166s versus 8.3091s for CSV
(500.5x faster). CSV must scan every byte of every row to extract a
subset of columns; Parquet skips column chunks that aren't requested.

## 2. Predicate Pushdown

DuckDB pushes filter predicates (e.g., ticker = '7203' AND time BETWEEN
'090000' AND '100000') into the Parquet reader, which uses min/max row-group
statistics to skip irrelevant row groups entirely. The query benchmark shows
this effect: a targeted query on the Hive Parquet store runs in 0.0205s,
versus 10.3823s for a pandas full-CSV scan
(506.5x faster).

## 3. Hive Partition Pruning

tse_tick writes Parquet files into a Hive-partitioned directory tree
(date=.../ticker=.../). When a query specifies a date and ticker, DuckDB
reads only the matching partition directories — it never opens files for
other dates or tickers. This is on top of intra-file predicate pushdown.

## 4. Cross-Tool Portability via Embedded Schema

Parquet files embed the full column schema (names, types, nullability).
DuckDB, Polars, pandas (via PyArrow), and Arrow read them with zero
conversion or configuration. CSV requires the user to know the column
count (95), types, and separator. This portability is not a performance
property but it eliminates a category of user errors when querying.

## Comparison Summary

| Format           | Size (MB) | Read All (s) | Read 3/95 (s) |
|------------------|-----------|--------------|----------------|
| CSV              | 2212.84     | 24.1905       | 8.3091          |
| Parquet (Snappy) | 99.33     | 0.2873       | 0.0166          |
| Feather (IPC)    | 795.6     | 1.9966       | 0.0671          |

Feather matches or beats Parquet on raw I/O speed, but lacks predicate
pushdown and Hive partition pruning — the two properties that make sub-second
queries on multi-year datasets possible.
