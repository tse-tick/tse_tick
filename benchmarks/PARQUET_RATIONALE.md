# Why Parquet Is the Right Store for tse_tick

Four properties of Apache Parquet make it the correct storage format for the
query workload that tse_tick serves. Each is backed by measured numbers from
our benchmark suite.

## 1. Columnar Selective Reads

Parquet stores data column-by-column. Reading 3 of 95 columns from Parquet
(Snappy) takes 0.0169s versus 11.42s for CSV
(676x faster). CSV must scan every byte of every row to extract a
subset of columns; Parquet skips column chunks that aren't requested.

## 2. Predicate Pushdown

DuckDB pushes filter predicates (e.g., ticker = '7203' AND time BETWEEN
'090000' AND '100000') into the Parquet reader, which uses min/max row-group
statistics to skip irrelevant row groups entirely. The query benchmark shows
this effect: a targeted query on the Hive Parquet store runs in 0.0135s,
versus 9.37s for a pandas full-CSV scan
(694.1x faster).

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
| CSV              | 2209.59   | 30.0052      | 11.4198         |
| Parquet (Snappy) | 99.68     | 0.9127       | 0.0169          |
| Feather (IPC)    | 797.61    | 2.1837       | 0.0862          |

Feather is uncompressed and the largest binary format, and here is no faster
than Parquet on raw I/O; more importantly it lacks predicate pushdown and Hive
partition pruning — the two properties that make sub-second queries on
multi-year datasets possible.
