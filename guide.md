# NEEDS_tick Guide

## Section 1: Project Structure

### Root
| File | Description |
|------|-------------|
| `pyproject.toml` | Package metadata, dependencies (`polars`, `pyarrow`, `duckdb`), tool configs (black, pytest, mypy, flake8). |
| `CHANGELOG.md` | Version history. |
| `LICENSE` | MIT license text. |
| `CONTRIBUTING.md` | Development setup and contribution guidelines. |
| `.gitignore` | Git ignore rules (excludes `data/`, `descriptions/`, build artifacts). |
| `REFORM_PLAN.md` | Architecture plan for the v0.2.0 polars migration and CLI (implemented). |

### `tse_tick/` — main package
| File | Description |
|------|-------------|
| `tse_tick/__init__.py` | Public API surface: re-exports `create_df`, `export_to_csv`, `discover_zips`, ingest, query, event_window, features, and parquet functions. |
| `tse_tick/enhanced.py` | Core parsing pipeline: `create_df()`, `export_to_csv()`, `detect_data_type_and_year()`, `get_1y_dataframe()`, `set_columns()`, `get_final_columns()`, `discover_zips()`. ZIP bomb protection via `_MAX_DECOMPRESSED_BYTES` and `_MAX_ZIP_ENTRIES`. |
| `tse_tick/core.py` | Low-level data cleaning (`clean_data()`) and a fixed-width/`+`-delimited line parser for 2016-format files (`parse_line()`). Type casting, categorical decoding via `get_schemas_categorical()`. |
| `tse_tick/schemas.py` | Column name schemas for all four data types (95/82/23/17 fields) and a full English-to-Japanese column name mapping dict. |
| `tse_tick/ingest.py` | Batch ZIP-to-Parquet ingestion: `ingest_single_zip()`, `ingest_directory()`, `ingest_year()`, `ingest_year_from_root()`, `ingest_event_windows()`. Worker cap at `_MAX_WORKERS=8`. |
| `tse_tick/cli.py` | CLI entry point: `tse-tick ingest` with `--data-type`, `--years`, `--input-root`, `--output-root`, `--parallel`, `--no-resume`, `--flat`. |
| `tse_tick/io/parquet.py` | Parquet read/write utilities. Two layouts: general store (`date=YYYYMMDD/ticker=NNNN.parquet`) and event window store (`year=YYYY/month=MM/YYYYMMDD.parquet`). Time columns coerced to strings for Parquet compatibility. |
| `tse_tick/query.py` | DuckDB-powered query interface: `query_ticks()` (structured queries with partition pruning), `query_sql()` (raw SQL escape hatch — **privileged**), `get_available_dates()`, `get_available_tickers()`. Includes path traversal protection and query row limit (`_MAX_QUERY_ROWS=10M`). |
| `tse_tick/event_window.py` | Tick extraction ± N minutes around corporate disclosure events: `extract_event_window()`, `extract_batch_event_windows()`, and `_filter_ticks_for_events()` (streaming, used by `ingest_event_windows()`). |
| `tse_tick/features.py` | Order-book feature engineering: `compute_spread()`, `compute_depth()`, `compute_flow_imbalance()`, `compute_volatility()`, `compute_all_features()`. |
| `tse_tick/py.typed` | PEP 561 marker that declares the package ships inline type hints. |

### `tests/`
| File | Description |
|------|-------------|
| `tests/__init__.py` | Empty init to make `tests/` a package. |
| `tests/test_parquet.py` | Parquet I/O tests with synthetic polars DataFrames (12 passing). |
| `tests/test_ingest.py` | Batch ingestion pipeline tests (12 passing). Uses real TICST120 ZIP when available. |
| `tests/test_event_window.py` | Event window filter tests with synthetic TICST120 data (8 passing). |
| `tests/test_features.py` | Feature engineering tests (19 skipped — need Parquet store). |
| `tests/test_query.py` | DuckDB query tests (13 skipped — need populated Parquet store). |
| `tests/test_parquet_io.py` | Legacy duplicate of test_parquet.py (14 skipped). |
| `tests/test_schemas.py` | Schema test stub. |
| `tests/test_core.py` | Core module test stub. |

### `descriptions/`
Untracked reference materials: PDF manuals (`TICST1@@.pdf`, `TICIT110.pdf`, `TIC@S@10.pdf`), exploration notebooks, prototype scripts, schema reference CSVs.

### `scripts/`
| File | Description |
|------|-------------|
| `scripts/ingest_event_windows.py` | Standalone CLI for event-window extraction pipeline. |

---

## Section 2: Usage Workflow

### Installation

```bash
git clone https://github.com/jevwithwind/tse_tick.git
cd tse_tick
pip install -e .
```

### Loading data with `create_df()`

`create_df()` accepts a path to a single ZIP file, a directory containing ZIPs, or a nested NEEDS folder structure. The data type and year are auto-detected from the path/filename, or can be specified manually.

```python
import tse_tick

# Auto-detect from filename (H TICST120.20230104.1.zip -> individual_stock, 2023)
df = tse_tick.create_df("path/to/HTICST120.20230104.1.zip", language='en')

# Manual specification (skip auto-detection)
df = tse_tick.create_df(
    "path/to/file.zip",
    auto_detect=False,
    data_type="individual_stock",
    year=2023,
    language='en',
)

# Ticker filter (pre-filters at CSV read level for memory efficiency)
df = tse_tick.create_df("path/to/HTICST120.20230104.1.zip", ticker_filter={'7203', '1301'})

# Sample first N rows
df_sample = tse_tick.create_df("path/to/HTICST120.20230104.1.zip", language='en', rows=1000)
```

### Switching between English and Japanese columns

```python
df_en = tse_tick.create_df("path/to/HTICST120.20230104.1.zip", language='en')
df_jp = tse_tick.create_df("path/to/HTICST120.20230104.1.zip", language='jp')
```

### Exporting to CSV

```python
tse_tick.export_to_csv("path/to/HTICST120.20230104.1.zip", output_path="output.csv")
```

### CLI: Batch ZIP → Parquet

```bash
# Ingest a date range (recursive discovery of year/month folders)
tse-tick ingest \
    --data-type individual_stock \
    --years 2016-2023 \
    --input-root /Volumes/TSE_DATA \
    --output-root /Volumes/PARQUET_STORE

# Ingest a single year with 4 parallel workers
tse-tick ingest \
    --data-type indices \
    --year 2022 \
    --input-root /Volumes/TSE_DATA \
    --output-root /Volumes/PARQUET_STORE \
    --parallel 4

# Ingest a flat directory of ZIPs
tse-tick ingest \
    --data-type stock_summary \
    --year 2023 \
    --input-root /data/zips/ \
    --output-root /store/ \
    --flat

# Reprocess everything (no resume)
tse-tick ingest --no-resume ...
```

### Querying the Parquet store

```python
# Structured query with partition pruning
df = tse_tick.query_ticks(
    "/Volumes/PARQUET_STORE",
    data_type="indices",
    ticker=101,
    date="20220104",
    start_time="09:00:00",
    end_time="11:30:00",
)

# Raw SQL (privileged — only with trusted input)
df = tse_tick.query_sql("/Volumes/PARQUET_STORE", "SELECT * FROM ticks LIMIT 100")

# List available dates and tickers
dates = tse_tick.get_available_dates("/Volumes/PARQUET_STORE")
tickers = tse_tick.get_available_tickers("/Volumes/PARQUET_STORE", date="20220201")
```

### Event window extraction

```python
# Single event
df_window = tse_tick.extract_event_window(
    "/Volumes/PARQUET_STORE",
    ticker=7203,
    event_date="20220728",
    event_time="15:00:00",
    before="120min",
    after="120min",
)
```

### Feature engineering

```python
df = tse_tick.query_ticks("/Volumes/PARQUET_STORE", ticker=7203, date="20220201")

spread = tse_tick.compute_spread(df)
depth = tse_tick.compute_depth(df, levels=5, side='both')
ofi = tse_tick.compute_flow_imbalance(df, window='5min')
vol = tse_tick.compute_volatility(df, window='5min', method='realized')
features = tse_tick.compute_all_features(df)
```

---

## Section 3: How the Code Works

This section traces the execution path of:

```python
tse_tick.create_df("HTICST120.20220201.1.zip", language='en')
```

### Step 1 — `create_df()` entry point (`enhanced.py`)

`create_df` is the top-level function. With `auto_detect=True`, it delegates to `detect_data_type_and_year()`. When `auto_detect=False`, it uses the explicitly provided `data_type` and `year`.

### Step 2 — `detect_data_type_and_year(folder_path)` (`enhanced.py`)

**Year extraction:** Iterates each part of the path, applying `r'(20\d{2})'`. From `HTICST120.20220201.1.zip`, the part `20220201` matches → `year = 2022`.

**Data type detection:** Lowercases the full path string and checks for keyword substrings:
- `'ticst'` is found in `hticst120.20220201.1.zip` → `data_type = 'individual_stock'`

Returns `('individual_stock', 2022)`.

### Step 3 — `get_1y_dataframe(folder_path, year, kind, ...)` (`enhanced.py`)

**Path handling:** Single ZIP file → `zip_files = [Path("HTICST120.20220201.1.zip")]`.

**ZIP bomb protection:** Checks `file_size`, `compress_size`, and compression ratio before decompressing. Maximum decompressed size: 5 GB. Maximum ZIP entries: 5.

**Reading:** All 95 columns are read as `pl.String` via `pl.read_csv(f, has_header=False, schema_overrides=...)`. Polars does no type inference, avoiding mixed-type parse errors. Type casting happens later in `clean_data()`.

**2016 special case:** For `indices_summary` or `indices` data from 2016, files use fixed-width / `+`-delimited format. `parse_line()` from `core.py` handles these, building a list of dicts passed to `pl.DataFrame()`.

**Ticker filter:** When `ticker_filter` is provided for `individual_stock`, lines are filtered at the byte level by scanning for the 6th CSV field (Stock Code), avoiding full decompression overhead.

Returns a raw polars DataFrame with string columns named `"column_1"` through `"column_95"`.

### Step 4 — `set_columns(df, kind, language)` (`enhanced.py`)

Assigns human-readable column names via `df.rename()`. For 95 columns, the schema from `get_schema_individual_stock_95()` is applied. For Japanese, each English name is mapped via `get_japanese_column_mapping()`.

### Step 5 — `clean_data(df, kind, language)` (`core.py`)

This is the most complex step.

**Type casting:** Column indices target specific columns:
- `int_list` — volume, counter fields → `.cast(pl.Int64)` with `.fill_null(0)`
- `float_list` — price, quote fields → `.fill_null(0.0)`
- `time_list` — time columns → `.fill_null(None)`

Categorical columns (Execution Type, Ayumi Flag, Close Quote Flag) are excluded from `int_list` to prevent type conflicts during decoding.

**Date/time parsing:**
- `"Data Date"` → `pl.col().str.to_datetime("%Y%m%d")`
- Time columns → kept as strings in `"HHMMSS"` format (Polars-native `pl.Time` is avoided for Parquet compatibility)

**Whitespace stripping:** `pl.col(pl.String).str.strip_chars()` vectorized on all string columns.

**Categorical decoding:** Iterates columns, building replacement mappings from `get_schemas_categorical()`. Uses `pl.col(name).replace(mapping_dict)` for efficient batched replacement. Unknown values are mapped to `f"Unknown ({var})"`.

Returns a fully typed and decoded polars DataFrame.

### Step 6 — Column filtering (`enhanced.py`)

For `individual_stock`, all 95 columns are kept. For other types, `get_final_columns()` returns curated subsets (10 for indices, 17 for indices_summary, 82 for stock_summary).

### The `language='jp'` special case

When Japanese is requested, `set_columns()` assigns Japanese names. But `clean_data()` relies on English column names. To handle this:
1. A reverse mapping (`en_to_jp`) is built
2. Columns are temporarily renamed to English
3. `clean_data()` runs with English names
4. Columns are renamed back to Japanese
5. If final column filtering is needed, column names are translated to Japanese first

---

## Section 4: Architecture Overview

### Two-Stage Architecture

```
STAGE 1: INGEST (ZIP -> Parquet Store)

  Raw NEEDS ZIPs
  (HTICST120.YYYYMMDD.N.zip)
         |
         v
  create_df()          (enhanced.py -> core.py -> schemas.py)
         |
         v
  write_partitioned_parquet()
  [ io/parquet.py ]
         |
         v
  Partitioned Parquet Store
  data_dir/individual_stock/date=YYYYMMDD/ticker=NNNN.parquet

STAGE 2: QUERY & ANALYZE (Parquet Store -> Research)

  Partitioned Parquet Store
         |
         +-->  query_ticks() / query_sql()     -> pl.DataFrame
         |     [ query.py - DuckDB ]
         |
         +-->  extract_event_window()           -> pl.DataFrame
         |     [ event_window.py ]
         |
         +-->  compute_spread() / compute_depth()
         |     compute_flow_imbalance()          -> pl.Series/DataFrame
         |     [ features.py ]
```

### Data Flow

```
CSV in ZIP   --->   pl.read_csv(string)   --->   column_1...95
                                                  (all String)
                          |
                          v
              set_columns() [enhanced.py]
                          |
                          v
                Record Type...Buy Quote Flag UNDER
                          |
                          v
              clean_data() [core.py]
              - cast to Int/Float
              - parse dates/times
              - strip whitespace
              - decode categorical values
                          |
                          v
              get_final_columns() [enhanced.py]
              (subset columns for non-stock types)
                          |
                          v
              write_partitioned_parquet() [io/parquet.py]
```

### Engine: Polars

The package migrated from pandas to polars in v0.2.0. Key benefits:

- **CSV I/O**: `pl.read_csv()` is 20-50x faster than `pd.read_csv()`, especially with `engine="python"` removed
- **Vectorized operations**: `str.strip_chars()`, `str.to_datetime()`, all type casting
- **Batch replacement**: `pl.col(name).replace(mapping_dict)` for categorical decoding
- **Memory efficiency**: No copy-on-write overhead for column operations
- **Parquet**: `write_parquet()` / `read_parquet()` with snappy compression

### Constraints

| ID | Constraint | Location |
|----|-----------|----------|
| C0 | Only zip_dates with events are opened | `ingest.py::ingest_event_windows` |
| C1 | ZIPS read in-memory via create_df(), never extracted to disk | `enhanced.py::get_1y_dataframe` |
| C2 | One raw DataFrame in memory at a time | `ingest.py` loops |
| C3 | `del raw_df, gc.collect()` after every ZIP | `ingest.py` finally blocks |
| C4 | Output written to partitioned Parquet, never single file | `io/parquet.py` |
| C5 | Corrupt ZIPS logged to corrupt_zips.txt and skipped | `ingest.py::ingest_event_windows` |
| C6 | JST timezone applied before timestamp comparisons | `event_window.py` |
| C7 | zip_date forced to str on CSV load | `ingest.py::ingest_event_windows` |
| C8 | ZIP bomb protection (max 5 GB decompressed, max 5 entries) | `enhanced.py` |
| C9 | Max parallel workers capped at 8 | `ingest.py` |
| C10 | Query row limit at 10M | `query.py` |

### Security

| Protection | Location |
|-----------|----------|
| ZIP bomb guard (5 GB max, 100:1 compression ratio cap) | `enhanced.py` |
| SQL injection prevention (identifier/date/time validation) | `query.py` |
| Path traversal prevention (`data_dir` resolved and validated) | `query.py::_resolve_type_dir` |
| Worker cap (max 8 parallel processes) | `ingest.py` |
| Query row limit (10M default) | `query.py` |
| No traceback leaks to stdout (logged via logger, not print) | `ingest.py` |
| `query_sql()` documented as privileged API | `query.py` |
