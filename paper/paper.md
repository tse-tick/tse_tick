---
title: 'tse_tick: A Python library for parsing and querying Nikkei NEEDS tick-level market data'
tags:
  - Python
  - market microstructure
  - high-frequency data
  - tick data
  - Tokyo Stock Exchange
  - finance
authors:
  - name: Peter Romero
    affiliation: 1
  - name: Kazumi Li
    corresponding: true
    affiliation: 2
  - name: Masataka Hayashi
    affiliation: 3
affiliations:
  - name: Psychometrics Centre, University of Cambridge, Cambridge, United Kingdom
    index: 1
  - name: Graduate School of Economics, Keio University, Tokyo, Japan
    index: 2
  - name: Faculty of Economics, Keio University, Tokyo, Japan
    index: 3
date: 12 June 2026
bibliography: paper.bib
---

# Summary

Research in market microstructure --- the study of how prices form and orders
execute --- relies on tick-level data: records of every trade execution, quote
update, and order-book snapshot throughout the trading day
[@hasbrouck2007; @ohara1995]. For the Japanese equity market, the standard
academic source is the Nikkei NEEDS tick data service [@nikkei_needs], which
distributes data for all securities listed on the Tokyo Stock Exchange as
compressed, headerless CSV files inside daily ZIP archives.

`tse_tick` is an open-source Python library that automates the ingestion,
cleaning, and querying of these files. A single entry point, `create_df()`,
accepts a ZIP path, detects the data type and format era automatically, and
returns a cleaned Polars DataFrame [@polars2024] with English or Japanese
column names and categorical codes decoded to human-readable labels. For
large-scale work, a command-line tool (`tse-tick ingest`) converts entire date
ranges into Hive-partitioned Parquet stores [@apache_parquet] written via
Apache Arrow [@apache_arrow], which a query layer reads through DuckDB
[@raasveldt2019duckdb]. Built-in feature functions compute bid--ask spreads,
order-book depth, order-flow imbalance, and realized or Garman--Klass
volatility, and an event-window module extracts tick windows around corporate
disclosure events.

# Statement of need

Working with NEEDS tick data imposes a substantial fixed cost on every
research project. The raw files are headerless and split into up to 27 parts
per trading day; the format changed across years (2016 index data uses a
fixed-width text layout, later years use CSV with different column counts);
individual stock records span 95 columns encoding a 10-level order book as
interleaved price--volume--flag triples; and categorical fields hold raw
integer codes whose meanings are documented only in Japanese. Researchers
typically write ad-hoc parsing scripts that are rarely reusable across data
types, years, or research groups and, to our knowledge, no open-source
tooling for this data source existed.

`tse_tick` removes that cost. It supports all four NEEDS record types ---
individual stock ticks (TICST120, 95 output fields), daily stock summaries
(TICSS110, 82 fields from 83 raw), index ticks (TICIT110, 10 fields from 23
raw, 15 in 2016), and daily index summaries (TICIS110, 17 fields from 83
raw) --- with a 187-entry bilingual column mapping, automatic era detection,
and decoding of exchange, session, execution-type, and quote-condition codes.
Because archives come from an external distributor, ingestion enforces
decompression-bomb guards (entry count, size, and compression-ratio limits)
and the query layer validates identifiers and date/time filters before they
reach SQL.

# Performance

The processing pipeline was benchmarked on one trading day of TICST120 data
(4.8 million rows, 95 columns). Against the original pandas-based prototype
[@pandas2024] it is 85$\times$ faster; against a fair pandas C-engine baseline
with identical CSV configuration it is 28$\times$ faster on 16 threads and
7$\times$ faster single-threaded. For the query workload the library targets,
a single-ticker one-hour slice served from the Hive-partitioned Parquet store
via DuckDB completes 506$\times$ faster than a pandas scan of the equivalent
CSV, and Parquet storage is 22.3$\times$ smaller than raw CSV. Scripts,
environment details, and per-run results are included in the repository's
`benchmarks/` directory.

The test suite contains 181 tests, 133 of which run without any proprietary
data: a session-scoped fixture builds a small Parquet store at test time by
passing synthetic NEEDS-format archives through the production ingest
pipeline. The remaining tests run when an environment variable points to a
local NEEDS store.

`tse_tick` is distributed under the MIT license and developed openly on
GitHub; it will be available on PyPI as `tse-tick`.

# Acknowledgements

The library was developed at the Nakatsuma Seminar, Keio University. Access
to Nikkei NEEDS data requires an institutional subscription; no proprietary
data is redistributed with the software.

# References
