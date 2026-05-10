# NEEDS_tick

A Python package for processing Nikkei NEEDS high-frequency tick data from the Tokyo Stock Exchange, powered by **Polars** and **DuckDB**.

Handles automatic data type detection, bilingual column support (English/Japanese), and cleaning for four NEEDS data formats: TICST120 (individual stock ticks), TICSS110 (stock summary), TICIT110 (index ticks), and TICIS110 (index summary).

## Installation

```bash
git clone https://github.com/jevwithwind/NEEDS_tick.git
cd NEEDS_tick
pip install -e .
```

## Quick Start

### Python API

```python
import tse_tick

# Load individual stock tick data
df = tse_tick.create_df("path/to/HTICST120.20230104.1.zip", language='en')

# Load with Japanese column names
df_jp = tse_tick.create_df("path/to/HTICST120.20230104.1.zip", language='jp')

# Export to CSV
tse_tick.export_to_csv("path/to/HTICST120.20230104.1.zip", output_path="output.csv")

# Sample first 1000 rows
df_sample = tse_tick.create_df("path/to/HTICST120.20230104.1.zip", rows=1000)

# Explicit data type and year (skip auto-detection)
df = tse_tick.create_df(
    "path/to/file.zip",
    auto_detect=False,
    data_type="individual_stock",
    year=2023,
)
```

### CLI Ingest (Batch ZIP → Parquet)

Convert entire years, months, or specific date ranges of data into a partitioned Parquet store with one command:

```bash
# Ingest a specific date range (YYYYMMDD-YYYYMMDD)
tse-tick ingest \
    --data-type individual_stock \
    --period 20240201-20240205 \
    --input-root /Volumes/TSE_DATA \
    --output-root /Volumes/PARQUET_STORE

# Ingest a month range (YYYYMM-YYYYMM)
tse-tick ingest \
    --data-type individual_stock \
    --period 202401-202403 \
    --input-root /Volumes/TSE_DATA \
    --output-root /Volumes/PARQUET_STORE

# Ingest a full year (YYYY)
tse-tick ingest \
    --data-type individual_stock \
    --period 2024 \
    --input-root /Volumes/TSE_DATA \
    --output-root /Volumes/PARQUET_STORE

# Legacy: Ingest all TICST120 data for 2016-2023 using --years
tse-tick ingest \
    --data-type individual_stock \
    --years 2016-2023 \
    --input-root /Volumes/TSE_DATA \
    --output-root /Volumes/PARQUET_STORE

# Legacy: Ingest a single year using --year
tse-tick ingest \
    --data-type indices \
    --year 2022 \
    --input-root /Volumes/TSE_DATA \
    --output-root /Volumes/PARQUET_STORE

# Ingest a flat directory of ZIPs
tse-tick ingest \
    --data-type stock_summary \
    --year 2023 \
    --input-root /data/zips/ \
    --output-root /store/ \
    --flat

# Parallel ingest with 4 workers
tse-tick ingest \
    --data-type individual_stock \
    --years 2020-2023 \
    --input-root /data/ \
    --output-root /store/ \
    --parallel 4

# Skip resume (reprocess all files)
tse-tick ingest --no-resume ...

# Tick-specific ingest: keep only specified stocks
tse-tick ingest \
    --data-type individual_stock \
    --period 2024 \
    --input-root /Volumes/TSE_DATA \
    --output-root /Volumes/PARQUET_STORE \
    --tickers 7203,6758,9984

# Ticker filter from file (one ticker per line)
tse-tick ingest \
    --data-type individual_stock \
    --period 2024 \
    --input-root /Volumes/TSE_DATA \
    --output-root /Volumes/PARQUET_STORE \
    --tickers @ticker_list.txt

# Event-window filtered ingest (±120 min around each event)
tse-tick ingest \
    --data-type individual_stock \
    --period 20250106-20250131 \
    --input-root /Volumes/TSE_DATA \
    --output-root /Volumes/PARQUET_STORE \
    --filter-csv event_filter_list.csv \
    --window 120
```

| Flag | Description |
|------|-------------|
| `--period` | Date range: `YYYY` (year), `YYYYMM-YYYYMM` (month range), or `YYYYMMDD-YYYYMMDD` (day range). Takes precedence over `--years`/`--year`. |
| `--years` | Legacy: year range like `"2016-2023"` or `"2018,2019,2020"` |
| `--year` | Legacy: single year |
| `--flat` | Treat input-root as a flat folder of ZIPs (no year/month subdirectories) |
| `--no-resume` | Reprocess all files even if output exists |
| `--parallel` | Number of parallel worker processes (max 8) |
| `--tickers` | Comma-separated ticker codes, or `@file.txt` with one per line. Keeps only those tickers in output. |
| `--filter-csv` | Path to event filter CSV (columns: ticker, event_date, event_time, event_type, session_type, reaction_anchor_dt, zip_date). Enables event-window mode. Overrides `--tickers`. |
| `--window` | Minutes for ±window around each event's `reaction_anchor_dt` (default: 120). Only used with `--filter-csv`. |

### Query Parquet Store

```python
import tse_tick

# Query Nikkei 225 ticks for Jan 4, 2022
df = tse_tick.query_ticks(
    "/Volumes/PARQUET_STORE",
    data_type="indices",
    ticker=101,
    date="20220104",
    start_time="09:00:00",
    end_time="11:30:00",
)

# Get available dates
dates = tse_tick.get_available_dates("/Volumes/PARQUET_STORE")

# Extract event window around a disclosure
df_window = tse_tick.extract_event_window(
    "/Volumes/PARQUET_STORE",
    ticker=7203,
    event_date="20220728",
    event_time="15:00:00",
    before="120min",
    after="120min",
)
```

## Expected Folder Structure

The CLI expects NEEDS data organized as delivered by Nikkei:

```
{input_root}/
  2016/
    201601/                       # yearmonth
      HTICST120.20160104.1.zip
      HTICST120.20160104.2.zip
      HTICST120.20160105.1.zip
      ...
    201602/
    ...
  2017/
    201701/
    ...
```

Each trading day has 1–N ZIP files (N varies). The CLI auto-discovers all matching files.

## Supported Data Types

| Code | Type | Fields | Description |
|------|------|--------|-------------|
| TICST120 | `individual_stock` | 95 | Tick-level execution, bid/ask ×10, volume |
| TICSS110 | `stock_summary` | 82 | Daily OHLC, VWAP, session splits |
| TICIT110 | `indices` | 23 | Index tick updates |
| TICIS110 | `indices_summary` | 17 | Daily index summary |

Year range: 2016–2023.

## Parquet Output Layout

After ingest, data is stored as Hive-partitioned Parquet:

```
{output_root}/
  individual_stock/
    date=20230104/
      ticker=7203.parquet
      ticker=6758.parquet
      ...
    date=20230105/
    ...
  indices/
    date=20220104/
      ticker=101.parquet
      ticker=113.parquet
    ...
```

Event-window data uses a separate layout:

```
{output_root}/
  year=2017/
    month=03/
      20170315.parquet
      20170316.parquet
    ...
```

## Features

```python
import tse_tick

df = tse_tick.query_ticks("/store", ticker=7203, date="20220201")

# Bid-ask spread
spread = tse_tick.compute_spread(df)

# Order-book depth (10 levels per side)
depth = tse_tick.compute_depth(df, levels=5, side='both')

# Order flow imbalance over rolling window
ofi = tse_tick.compute_flow_imbalance(df, window='5min')

# Realized or Garman-Klass volatility
vol = tse_tick.compute_volatility(df, window='5min', method='realized')

# All features in one pass
features = tse_tick.compute_all_features(df)
```

## Security

Built-in protections for local data processing:

| Guard | Value |
|-------|-------|
| ZIP bomb detection (max decompressed) | 5 GB |
| ZIP compression ratio cap | 100:1 |
| Max ZIP entries | 5 |
| Max parallel workers | 8 |
| Query row limit | 10,000,000 |
| Path traversal prevention | Resolved path validation |
| SQL injection prevention | Identifier/date/time format validation |
| Traceback leakage | Errors logged, not printed to stdout |

## Test Status

```
33 passed, 66 skipped — verified against real TICST120 data (4.5M rows, 95 cols, 7.7s)
```

## Dependencies

- **polars** (≥0.20.0) — DataFrame engine
- **pyarrow** (≥12.0.0) — Parquet read/write and dataset API
- **duckdb** (≥0.9.0) — SQL query interface over Parquet store

## Authors

Originally developed as a collaborative project at Keio University, Nakatsuma Seminar:

- **Peter Romero** — initial architecture and package design
- **Masataka Hayashi** — schema definitions and data validation
- **Kazumi Li** ([@jevwithwind](https://github.com/jevwithwind)) — current maintainer

## License

[MIT](LICENSE)

## Contributing

Contributions are welcome. Please open an issue or submit a pull request.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -m 'Add your feature'`)
4. Push to the branch (`git push origin feature/your-feature`)
5. Open a Pull Request
