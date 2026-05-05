# NEEDS_tick — Project Reference

For installation, quick start, and usage examples, see [README.md](README.md).

---

## Project Structure

### Root
| File | Description |
|------|-------------|
| `pyproject.toml` | Package metadata, dependencies (`polars`, `pyarrow`, `duckdb`), tool configs |
| `CHANGELOG.md` | Version history |
| `LICENSE` | MIT license |
| `CONTRIBUTING.md` | Dev setup and contribution guidelines |
| `.gitignore` | Git ignore rules |

### `tse_tick/` — main package
| File | Job | Key Functions |
|------|-----|---------------|
| `__init__.py` | Public API surface | Re-exports all public functions |
| `enhanced.py` | CSV→DataFrame pipeline | `create_df()`, `export_to_csv()`, `detect_data_type_and_year()`, `discover_zips()` |
| `core.py` | Cleaning + 2016 parser | `clean_data()` (type cast, decode), `parse_line()` (fixed-width format) |
| `schemas.py` | Column names (EN/JP) | Schema lists for 95/82/23/17 fields, English↔Japanese mapping |
| `ingest.py` | Batch ZIP→Parquet | `ingest_single_zip()`, `ingest_directory()`, `ingest_year_from_root()` |
| `io/parquet.py` | Parquet I/O | `write_partitioned_parquet()`, `read_parquet_partition()`, event-window variants |
| `query.py` | DuckDB queries | `query_ticks()`, `query_sql()` (privileged), `get_available_dates()` |
| `event_window.py` | Event study windows | `extract_event_window()`, `_filter_ticks_for_events()` |
| `features.py` | Order-book features | `compute_spread()`, `compute_depth()`, `compute_flow_imbalance()`, `compute_volatility()` |
| `cli.py` | CLI entry point | `tse-tick ingest` |

### `tests/`
| File | What it tests | Status |
|------|--------------|--------|
| `test_parquet.py` | Parquet I/O with synthetic data | 12 ✓ |
| `test_ingest.py` | Batch ingestion pipeline | 12 ✓ (uses real ZIP when available) |
| `test_event_window.py` | Event window filters | 8 ✓ |
| `test_features.py` | Feature engineering | 19 skipped (needs Parquet store) |
| `test_query.py` | DuckDB queries | 13 skipped (needs populated Parquet store) |
| `test_parquet_io.py` | Legacy duplicate | 14 skipped |
| `test_schemas.py` | Schema stubs | stub |
| `test_core.py` | Core module stubs | stub |

### Other directories
| Directory | Contents |
|-----------|----------|
| `descriptions/` | Reference PDFs (TICST1@@.pdf, TICIT110.pdf, TIC@S@10.pdf), exploration notebooks, prototypes |
| `scripts/` | `ingest_event_windows.py` — standalone event-window extraction CLI |
| `examples/` | Jupyter notebook and Python script for basic usage demos |

---

## How `create_df()` Works

Trace of `create_df("HTICST120.20220201.1.zip", language='en')`:

```
Step 1: detect_data_type_and_year()
   ├── Year: regex r'(20\d{2})' on path parts → 2022
   └── Type: lowercase path, match 'ticst' → individual_stock

Step 2: get_1y_dataframe()
   ├── ZIP bomb check (max 5 GB, 100:1 ratio, max 5 entries)
   ├── pl.read_csv(has_header=False, schema_overrides={all→String})
   │   Returns: column_1 through column_95 (all String)
   └── pl.concat() multiple ZIP parts if present

Step 3: set_columns()
   └── df.rename(column_N → English name from schema)

Step 4: clean_data()
   ├── Cast int columns (volumes, counters) → pl.Int64, fill null→0
   ├── Cast float columns (prices, quotes) → pl.Float64, fill null→0.0
   ├── Time columns → fill null→None, keep as String "HHMMSS"
   ├── Data Date → str.to_datetime("%Y%m%d")
   ├── str.strip_chars() on all String columns
   └── Categorical decode:
       ├── Record Type ("1200" → "Stocks - Multiple Quote")
       ├── Exchange Code ("11" → "Tokyo Stock Exchange (TSE)")
       ├── Security Type, Session, Stock Code suffix
       ├── Execution Type, Ayumi Flag — stock vs index variants
       └── Quote Flag columns

Step 5: get_final_columns()
   └── individual_stock: keep all 95 cols
       Others: subset to 10 (indices) / 17 (indices_summary) / 82 (stock_summary)
```

**Japanese language mode**: set_columns assigns JP names first, then the pipeline temporarily renames to English for clean_data (which uses English columns), then renames back to Japanese. Clean data must always see English names.

---

## Architecture

```
 STAGE 1: INGEST                           STAGE 2: QUERY
 ┌─────────────────────┐                   ┌─────────────────────┐
 │ ZIP → create_df()   │                   │ query_ticks()       │
 │   ↓                 │                   │ query_sql()         │
 │ clean_data()        │   write to    →   │ extract_event_...() │
 │   ↓                 │   Parquet         │ compute_spread()    │
 │ pl.DataFrame (95c)  │                   │ ...                 │
 └─────────────────────┘                   └─────────────────────┘
```

Output layout:
- General: `{output}/individual_stock/date=YYYYMMDD/ticker=NNNN.parquet`
- Event windows: `{output}/year=YYYY/month=MM/YYYYMMDD.parquet`

---

## Constraints

| ID | Rule | Enforced in |
|----|------|-------------|
| C1 | ZIPS read in-memory, never extracted to disk | `enhanced.py` |
| C2 | One raw DataFrame in memory at a time | `ingest.py` loops |
| C4 | Partitioned Parquet output, never single file | `io/parquet.py` |
| C5 | Corrupt ZIPS logged and skipped | `ingest.py` |
| C6 | JST timezone on all timestamp comparisons | `event_window.py` |
| C8 | ZIP bomb guard: 5 GB, 100:1 ratio, 5 entries | `enhanced.py` |
| C9 | Max parallel workers: 8 | `ingest.py` |
| C10 | Query row limit: 10M | `query.py` |

---

## Security

| Protection | Mechanism |
|------------|-----------|
| ZIP bomb | Size/ratio/entry-count checks before decompression |
| SQL injection | Regex validation on identifiers, dates, times |
| Path traversal | Path resolve + prefix validation |
| Worker cap | `max_workers` clamped to 8 |
| Query overflow | Default LIMIT 10M on structured queries |
| Traceback leak | Errors go to logger, not stdout |
| `query_sql()` | Documented as privileged; read-only by DuckDB in-memory design |
