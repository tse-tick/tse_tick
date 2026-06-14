# tse_tick

A Python library for parsing, filtering, and querying Nikkei NEEDS tick data from the Tokyo Stock Exchange.

**Who it's for:** Researchers working with NEEDS tick data who need to convert thousands of zipped CSVs into queryable Parquet stores, filter by ticker or event windows, and handle format changes across historical eras.

**What it solves:** NEEDS data is delivered as daily ZIP files (1–27 parts per day) with era-dependent schemas — 2016 used fixed-width records for indices, 2017+ switched to CSV, and individual stocks have 95 columns with complex quote-book nesting. This library detects the format automatically, validates for security, parses everything into clean DataFrames, and writes Hive-partitioned Parquet.

**Data access required:** This tool does NOT provide NEEDS data itself. You must have an institutional subscription (Nikkei NEEDS) and access to the raw TICST120/TICSS110/TICIT110/TICIS110 ZIP files.

---

## Features

- **4 data types** — TICST120 (individual stock ticks, 95 cols), TICSS110 (daily stock summary, 82 cols), TICIT110 (index ticks, 10 cols), TICIS110 (daily index summary, 17 cols)
- **Multi-era format support** — 2016 fixed-width (TICIT010), 2017-2019, and 2020-2025 CSV formats, auto-detected from the ZIP filename
- **Polars backend** — fast CSV parsing, vectorized cleaning, memory-efficient
- **CLI batch ingestion** — `tse-tick ingest` converts entire years/months/date ranges to partitioned Parquet
- **Ticker filtering** (`--tickers`) — keep only specific stock codes at read time
- **Event-window extraction** (`--filter-csv`) — extract ±N minute windows around corporate events with automatic after-hours reaction-anchor shifting
- **Bilingual columns** — English and Japanese column names via `--language en|jp`
- **Security guards** — ZIP bomb detection (5 GB max decompressed, 100:1 compression ratio cap, max 5 entries), path traversal prevention, query row limits (10M)

---

## Installation

```bash
pip install tse-tick
```

For DuckDB-powered Parquet query support:

```bash
pip install tse-tick[query]
```

Development install from source:

```bash
git clone https://github.com/jevwithwind/tse_tick.git
cd tse_tick
pip install -e ".[dev]"
```

Requires Python ≥3.9 and the dependencies listed in `pyproject.toml` (polars, pyarrow). Optional: `pip install tse-tick[query]` for DuckDB-powered Parquet queries.

---

## Quick Start

### Python API — load a single ZIP

```python
import tse_tick

# Load individual stock tick data (auto-detects data type and year)
df = tse_tick.create_df("path/to/HTICST120.20230104.1.zip", language="en")

# Load with Japanese column names
df_jp = tse_tick.create_df("path/to/HTICST120.20230104.1.zip", language="jp")

# Sample first 1000 rows only
df_sample = tse_tick.create_df("path/to/HTICST120.20230104.1.zip", rows=1000)

# Explicit data type and year (skip auto-detection)
df = tse_tick.create_df(
    "path/to/file.zip",
    auto_detect=False,
    data_type="individual_stock",
    year=2023,
)
```

### CLI — batch ingest to Parquet

```bash
# Ingest a date range
tse-tick ingest \
    --data-type individual_stock \
    --period 20240201-20240205 \
    --input-root /path/to/TSE_DATA \
    --output-root /path/to/PARQUET_STORE

# Ingest a full year
tse-tick ingest \
    --data-type individual_stock \
    --period 2024 \
    --input-root /path/to/TSE_DATA \
    --output-root /path/to/PARQUET_STORE

# Ticker-filtered ingest (keep only specified stocks)
tse-tick ingest \
    --data-type individual_stock \
    --period 2024 \
    --input-root /path/to/TSE_DATA \
    --output-root /path/to/PARQUET_STORE \
    --tickers 7203,6758,9984

# Ticker filter from file (one ticker per line)
tse-tick ingest \
    --data-type individual_stock \
    --period 2024 \
    --input-root /path/to/TSE_DATA \
    --output-root /path/to/PARQUET_STORE \
    --tickers @ticker_list.txt

# Event-window filtered ingest (±120 min around each event)
tse-tick ingest \
    --data-type individual_stock \
    --period 20250106-20250131 \
    --input-root /path/to/TSE_DATA \
    --output-root /path/to/PARQUET_STORE \
    --filter-csv event_filter_list.csv \
    --window 120
```

### Query the Parquet store

```python
import tse_tick

# Query specific ticker and date
df = tse_tick.query_ticks(
    "/path/to/PARQUET_STORE",
    data_type="individual_stock",
    ticker=7203,
    date="20240201",
    start_time="09:00:00",
    end_time="11:30:00",
)

# Get available dates and tickers
dates = tse_tick.get_available_dates("/path/to/PARQUET_STORE")
tickers = tse_tick.get_available_tickers("/path/to/PARQUET_STORE", date="20240201")
```

### Feature extraction

```python
import tse_tick

df = tse_tick.query_ticks("/store", ticker=7203, date="20220201")

# Bid-ask spread
spread = tse_tick.compute_spread(df)

# Order-book depth (10 levels per side)
depth = tse_tick.compute_depth(df, levels=5, side="both")

# Order flow imbalance over rolling window
ofi = tse_tick.compute_flow_imbalance(df, window="5min")

# All features in one pass
features = tse_tick.compute_all_features(df)
```

---

## Data Types

| Code | Internal Name | Output Fields | Description |
|------|--------------|---------------|-------------|
| TICST120 | `individual_stock` | 95 | Tick-level executions, 10-level bid/ask quotes, volume |
| TICSS110 | `stock_summary` | 82 (83 raw) | Daily OHLC, VWAP, session splits, quote statistics |
| TICIT110 | `indices` | 10 (23 raw, 15 in 2016) | Index tick updates (Nikkei 225, TOPIX, etc.) |
| TICIS110 | `indices_summary` | 17 (83 raw) | Daily index summary prices |

---

## Multi-Era Format Support

The format changed only once, after 2016, and only for the index types (fixed-width to CSV); individual stock and stock summary files were CSV throughout. The library detects the era automatically from the ZIP filename (the year) and applies the correct parser.

| Era | Individual Stocks | Stock Summary | Index Ticks | Index Summary |
|-----|-------------------|---------------|-------------|---------------|
| **2016** | CSV, 95 cols | CSV, 83 cols | **Fixed-width (69 bytes)** | **Fixed-width (hybrid)** |
| **2017-2025** | CSV, 95 cols | CSV, 83 cols | CSV, 23 cols | CSV, 83 cols |

No user action needed — if your ZIP filename contains `2016`, the fixed-width parser is used automatically for index data.

---

## Performance

`tse_tick` is built on Polars (CSV parsing, vectorized cleaning) and DuckDB over Hive-partitioned Parquet (queries). Measured on one day of HTICST120 (4.78 M rows, 95 columns, 2.16 GB raw CSV) on an Intel Core i5-14400F (10-core / 16-thread) with 32 GB RAM, Python 3.11, Polars 1.40, pandas 2.2.

| Comparison | Speedup | Source |
|------------|---------|--------|
| Polars (16T) vs pandas (Python engine) | **55.5×** | `benchmarks/results_engine_summary.csv` |
| Polars (16T) vs pandas (C engine, fair baseline) | **22.8×** | `benchmarks/results_engine_summary.csv` |
| Polars (1 thread) vs pandas (C engine) | **6.2×** | `benchmarks/results_engine_summary.csv` |
| DuckDB + Hive Parquet vs pandas CSV scan (single-ticker hour slice) | **694.1×** | `benchmarks/results_query.csv` |
| Parquet (Snappy) storage size vs raw CSV | **22× smaller** (100 MB vs 2.2 GB) | `benchmarks/results_format.csv` |

The three Polars speedup numbers are deliberately reported together: against the original pandas Python-engine prototype, against a fair C-engine baseline (all-string dtypes, forced column count), and at single-thread parity to isolate the contribution of threading from the engine itself. Polars wins on all three.

`tse_tick` defaults to Polars because the ingest workload (multi-GB daily CSVs, mostly columnar transformations) hits exactly the case where lazy expression planning and parallel CSV parsing dominate; pandas-on-DataFrame's row-oriented model leaves throughput on the table even with the C engine. For querying, the Parquet store + DuckDB combination converts repeated single-ticker / single-date filters from full file scans into partition pruning, which is the source of the ~700× query speedup.

To reproduce: `python benchmarks/run_all.py` (see `benchmarks/ENVIRONMENT.md`).

---

## Expected Input Layout

The CLI expects NEEDS data organized as delivered by Nikkei:

```
{input_root}/
  2016/
    201601/
      HTICST120.20160104.1.zip
      HTICST120.20160104.2.zip
      ...
    201602/
    ...
  2017/
    201701/
    ...
```

---

## Parquet Output Layout

Standard ingest produces Hive-partitioned Parquet per ticker per date:

```
{output_root}/
  individual_stock/
    date=20230104/
      ticker=7203.parquet
      ticker=6758.parquet
      ...
```

Event-window filtered ingest writes per-date files:

```
{output_root}/
  year=2025/
    month=01/
      20250106.parquet
      20250107.parquet
      ...
```

---

## CLI Reference

| Flag | Description |
|------|-------------|
| `--data-type` (required) | `individual_stock`, `stock_summary`, `indices`, or `indices_summary` |
| `--input-root` (required) | Root directory with NEEDS ZIPs in `{year}/{yearmonth}/` layout |
| `--output-root` (required) | Root directory for Parquet output |
| `--period` | Date range: `YYYY`, `YYYYMM-YYYYMM`, or `YYYYMMDD-YYYYMMDD` |
| `--language` | Column name language: `en` (default) or `jp` |
| `--parallel` | Number of parallel workers (default 1, max 8) |
| `--no-resume` | Disable resume (reprocess dates even if output exists) |
| `--tickers` | Comma-separated codes or `@file.txt` with one per line. Keeps only these stocks. |
| `--filter-csv` | Path to event filter CSV. Enables event-window mode. Overrides `--tickers`. |
| `--window` | Window minutes around each event's reaction anchor (default 120). Only with `--filter-csv`. |
| `--flat` | Treat input-root as a flat directory (no year/month subdirectories) |
| `--years` / `--year` | Legacy flags for specifying year(s) directly |

### Event Filter CSV Format

When using `--filter-csv`, the file must have these columns:

| Column | Description |
|--------|-------------|
| `ticker` | 4-digit stock code (string) |
| `event_date` | Original event date `YYYY-MM-DD` |
| `event_time` | Original event time `HH:MM` (JST) |
| `event_type` | Category (`earnings`, `buyback`, `dividend`, etc.) |
| `session_type` | `intraday` or `after_hours` |
| `reaction_anchor_dt` | Datetime to center the window on `YYYY-MM-DD HH:MM` (JST) |
| `zip_date` | TICST120 date `YYYYMMDD` whose ZIP contains the relevant ticks |

For after-hours events, `reaction_anchor_dt` shifts to the next trading day's 09:00 open, and `zip_date` points to that next day's ZIP file. This is critical: centering on the event time (e.g., 15:30) would produce empty windows because the market is closed.

---

## Python API Reference

### `create_df(folder_path, language="en", rows=None, auto_detect=True, data_type=None, year=None, ticker_filter=None)`

Load and clean tick data from a ZIP file or directory of ZIP files.

- `folder_path` — path to a `.zip` file or directory of `.zip` files
- `language` — `"en"` or `"jp"` for column names
- `rows` — max rows to return
- `auto_detect` — if `True`, detect data type and year from path. If `False`, must provide `data_type` and `year`
- `data_type` — `"individual_stock"`, `"stock_summary"`, `"indices"`, or `"indices_summary"`
- `year` — data year (e.g., 2023)
- `ticker_filter` — optional `set` of 4-digit stock codes to pre-filter at line level

Returns a Polars DataFrame with English or Japanese column names.

### `export_to_csv(folder_path, output_path=None, language="en", rows=None)`

Load and export to CSV. If `output_path` is `None`, generates a filename.

---

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

---

## Contributing

Contributions are welcome. Please open an issue or submit a pull request.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -m 'Add your feature'`)
4. Push to the branch (`git push origin feature/your-feature`)
5. Open a Pull Request

Development setup:
```bash
pip install -e ".[dev]"
pytest tests/ -v
```

---

## Testing

```bash
pytest tests/ -v
```

The suite collects **181 tests**: **133 pass** and **48 are skipped**. Stage-1
(ingestion) and Stage-2 (query, order-book features, and
event-window-from-Parquet) both run with no proprietary data — a session-scoped
pytest fixture builds a tiny Hive-partitioned Parquet store at test time by
feeding synthetic, obviously-fake `individual_stock` (TICST120) ZIPs through the
real ingest pipeline (`tests/synthetic_data.py`, `tests/conftest.py`).

The 48 skips load **real NEEDS files** from local paths
(`test_real_data.py` and the real-ZIP cases in `test_ingest.py`), plus a handful
of fixtures outside the synthetic store's scope. They run automatically once a
local NEEDS store is present.

---

## Citation

If you use this software in your research, please cite it. A `CITATION.cff` file is included in the repository.

---

## License

[MIT](LICENSE)

---

## Authors

- **Kazumi Li** — Schema definitions, package architecture, current maintainer
- **Masataka Hayashi** — Initial pandas-based prototype
- **Peter Romero** — Original concept and initial project design

Developed at Keio University, Nakatsuma Seminar.
