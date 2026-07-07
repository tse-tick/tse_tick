# tse_tick

A Python library for parsing, filtering, and querying Nikkei NEEDS tick data from the Tokyo Stock Exchange.

**Who it's for:** Researchers working with NEEDS tick data who need to convert thousands of zipped CSVs into queryable Parquet stores, filter by ticker or event windows, and handle format changes across historical eras.

**What it solves:** NEEDS data is delivered as daily ZIP files (1–27 parts per day) with era-dependent schemas — 2016 used fixed-width records for indices, 2017+ switched to CSV, and individual stocks have 95 columns with complex quote-book nesting. This library detects the format automatically, validates for security, parses everything into clean DataFrames, and writes Hive-partitioned Parquet.

**Data access required:** This tool does NOT provide NEEDS data itself. You must have an institutional subscription (Nikkei NEEDS) and access to the raw TICST120/TICSS110/TICIT110/TICIS110 ZIP files. If your data is shared via Google Drive, see [the rclone download guide](https://github.com/tse-tick/tse_tick/blob/main/rclone_guide.md) for mirroring it to local disk.

---

## Features

- **4 data types** — TICST120 (individual stock ticks, 95 cols), TICSS110 (daily stock summary, 82 cols), TICIT110 (index ticks, 10 cols), TICIS110 (daily index summary, 17 cols)
- **Multi-era format support** — 2016 fixed-width (TICIT010/TICIS010) and 2017-2025 CSV, auto-detected from the ZIP filename
- **Polars backend** — fast CSV parsing, vectorized cleaning, memory-efficient
- **CLI batch ingestion** — `tse-tick ingest` converts entire years/months/date ranges to partitioned Parquet
- **Ticker filtering** (`--tickers`) — keep only specific stock codes at read time
- **Event-window extraction** (`--filter-csv`) — extract ±N minute windows around corporate events with automatic after-hours reaction-anchor shifting
- **Bilingual columns** — English and Japanese column names via `--language en|jp`
- **One-shot reader** (`read_ticks`) — raw ZIPs → a ticker/time-filtered DataFrame with no Parquet store to build first; **part-pruned** for `individual_stock` (opens only the ticker's parts, not the whole day)
- **One-call two-stage** (`extract_to_store`) — ingest one or more tickers for a period into a reusable store and get the DataFrame back in one call (the recommended path for repeated reads / large multi-ticker extractions; no 10M-row cap)
- **Name translation** (`translate`) — look up the `tse_tick` equivalent of a yfinance / Polygon / ccxt call (tables in `tse_tick/data/translations.json`; override with `TSE_TICK_TRANSLATIONS`)
- **Typed enums** (`DataType`, `Language`) — autocomplete-friendly and accepted anywhere the magic strings are
- **Security guards** — ZIP bomb detection (5 GB max decompressed, 100:1 compression ratio cap, max 5 entries), path traversal prevention, query row limits (10M)

---

## Installation

```bash
pip install tse-tick               # from PyPI: core (polars, pyarrow)
pip install "tse-tick[query]"      # + DuckDB-powered Parquet queries
```

To work from the latest (unreleased) source instead, install in editable mode:

```bash
git clone https://github.com/tse-tick/tse_tick.git
cd tse_tick

pip install -e .             # core: polars, pyarrow
pip install -e ".[query]"    # + DuckDB-powered Parquet queries
pip install -e ".[dev]"      # + everything for development (tests, linters, jupyter)
```

Requires Python ≥3.9. Core dependencies are polars and pyarrow; the `query` extra adds DuckDB (see `pyproject.toml`).

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

### CLI — export one ticker to CSV or Parquet

For a quick slice straight from the raw ZIPs — ideal if you don't write Python. For
`individual_stock` + a ticker it is **part-pruned** (opens only the ticker's parts), so it's fast
and the result is complete:

```bash
tse-tick export \
    --data-type individual_stock \
    --tickers 7203 \
    --period 20240201-20240205 \
    --input-root /path/to/TSE_DATA \
    --output toyota.csv            # .csv or .parquet, chosen by extension
```

Add `--store /path/to/store` to build a **reusable Parquet store** as it exports (the two-stage
path — best when you'll read the data again; requires the `[query]` extra):

```bash
tse-tick export --data-type individual_stock --tickers 7203 \
    --period 20240201-20240205 --input-root /path/to/TSE_DATA \
    --output toyota.csv --store /path/to/toyota_store
```

### Query the Parquet store

> **Note:** the query functions (`query_ticks`, `query_sql`, `get_available_*`) require the
> **`[query]` extra** — `pip install "tse-tick[query]"` (DuckDB). On the core install, use the
> DuckDB-free `read_parquet_partition(store, "individual_stock", date=..., ticker=...)` instead.

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

# Order-book depth (levels per side; default 10)
depth = tse_tick.compute_depth(df, levels=5, side="both")

# Order flow imbalance over rolling window
ofi = tse_tick.compute_flow_imbalance(df, window="5min")

# All features in one pass
features = tse_tick.compute_all_features(df)
```

### Two access patterns

`tse_tick` gives you a filtered DataFrame two ways. **For anything you'll read more than once, prefer the two-stage store** — you pay the raw scan once and every later query is sub-second.

1. **Two-stage (recommended — scale / repeated work).** `ingest` the raw ZIPs into a Hive-partitioned Parquet store once, then `query_ticks` it repeatedly (~694× faster than a pandas CSV scan; see [Performance](#performance)). `extract_to_store(...)` does both in one call:

```python
import tse_tick

# Build a reusable store for one or several tickers and get the DataFrame back —
# in one call. No 10M-row cap, so a whole month of active tickers comes back complete.
df = tse_tick.extract_to_store(
    "/path/to/TSE_DATA",          # a .zip, flat folder, or ANY folder above the data
    "/path/to/PARQUET_STORE",     # reusable store (built once; resume-safe, part-pruned)
    "202402",                     # a day, month, year, or range
    ["7203", "9984"],             # one code, or a list — Toyota + SoftBank
)
# ...every later read of the store is sub-second:
df = tse_tick.query_ticks("/path/to/PARQUET_STORE", data_type="individual_stock",
                          ticker=7203, date="20240201")
```

2. **One-shot (quick, targeted exploration).** `read_ticks(...)` reads straight from raw ZIPs to a ticker/time-filtered DataFrame with no store to build first. For `individual_stock` + a ticker filter it is **part-pruned** — it opens only the small run of numbered parts that hold the ticker (not every part of the day), so a single-ticker read is several times faster while returning **identical** rows (pass `prune_parts=False` to force a full scan). Accepts a **date range** (`date="20240201-20240205"`); best for one or a few tickers over a bounded window. The `tse-tick export` CLI wraps it to CSV/Parquet for non-coders.

```python
import tse_tick

# Toyota (7203) over a date range — straight from the raw ZIPs, no store.
# Part-pruned: opens only 7203's parts; the result is complete and exact.
df = tse_tick.read_ticks(
    "/path/to/TSE_DATA",          # a .zip, a flat folder, or ANY folder above the data (located by type+date)
    ticker_filter={"7203"},
    date="20240201-20240205",     # single day "20240201", a month "202402", a year "2024", or a range
    start_time="09:00:00",
    end_time="11:30:00",
)
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

**Real NEEDS deliveries are often nested** — e.g. `個別株式{year}/TICST120/{yyyymm}/HTICST120.*.zip`
(a Japanese-named year folder, then the data-type code, then the month). You don't have to match the
strict layout above: **point `--input-root` (or `read_ticks(...)` / `tse-tick export`) at _any_ folder
that contains the data** — files are located by **type + date**, regardless of folder names or depth.
Tip: aim at the common parent (e.g. `G:\NEEDS`) to cover several years at once.

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
| `--parallel` | Parallel worker processes for **flat** ingest (default 1; the `--period`/`--year` path runs sequentially). Values above 8 are clamped to 8. |
| `--no-resume` | Disable resume (reprocess dates even if output exists) |
| `--tickers` | Comma-separated codes or `@file.txt` with one per line. Keeps only these stocks. |
| `--filter-csv` | Path to event filter CSV. Enables event-window mode. Overrides `--tickers`. |
| `--window` | Window minutes around each event's reaction anchor (default 120). Only with `--filter-csv`. |
| `--flat` | Treat input-root as a flat directory (no year/month subdirectories) |
| `--years` / `--year` | Legacy flags for specifying year(s) directly |

### Event Filter CSV Format

When using `--filter-csv`, the ingest path builds each window from `zip_date`, `ticker`, and `reaction_anchor_dt`. The CSV may also carry these descriptive columns:

| Column | Description |
|--------|-------------|
| `ticker` | 4-digit stock code (string) |
| `event_date` | Original event date `YYYY-MM-DD` — metadata; not used by ingest |
| `event_time` | Original event time `HH:MM` (JST) — metadata; not used by ingest |
| `event_type` | Category (`earnings`, `buyback`, `dividend`, etc.) |
| `session_type` | `intraday` or `after_hours` |
| `reaction_anchor_dt` | Datetime to center the window on `YYYY-MM-DD HH:MM` (JST) |
| `zip_date` | TICST120 date `YYYYMMDD` whose ZIP contains the relevant ticks |

For after-hours events, `reaction_anchor_dt` shifts to the next trading day's 09:00 open, and `zip_date` points to that next day's ZIP file. This is critical: centering on the event time (e.g., 15:30) would produce empty windows because the market is closed.

---

## Python API Reference

### `create_df(folder_path, language="en", rows=None, auto_detect=True, data_type=None, year=None, ticker_filter=None, max_oneshot_bytes=<5 GB>)`

Load and clean tick data from a ZIP file or directory of ZIP files.

- `folder_path` — path to a `.zip` file or directory of `.zip` files
- `language` — `"en"` or `"jp"` for column names
- `rows` — max rows to return
- `auto_detect` — if `True`, detect data type and year from path. If `False`, must provide `data_type` and `year`
- `data_type` — `"individual_stock"`, `"stock_summary"`, `"indices"`, or `"indices_summary"`
- `year` — data year (e.g., 2023)
- `ticker_filter` — optional `set` of 4-digit stock codes to pre-filter at line level
- `max_oneshot_bytes` — cumulative decompressed-size ceiling for the one-shot read (default 5 GB; `None` disables). Crossing it raises a catchable `OneShotMemoryError`; use the two-stage `ingest_* → query_ticks` path instead.

Returns a Polars DataFrame with English or Japanese column names.

### `export_to_csv(folder_path, output_path=None, language="en", rows=None)`

Load and export to CSV. If `output_path` is `None`, generates a filename.

### `read_ticks(source, *, data_type="individual_stock", ticker_filter=None, date=None, start_time=None, end_time=None, columns=None, rows=10_000_000, language="en", prune_parts=True, max_oneshot_bytes=...)`

One-shot read: raw NEEDS ZIPs → a ticker/time-filtered DataFrame, no store.

- `source` — a `.zip`, a flat folder, or any folder above the data (located by type + date)
- `ticker_filter` — a `set` of codes (e.g. `{"7203"}`); a bare `"7203"`/`7203` also works
- `date` — a day `"YYYYMMDD"`, month `"YYYYMM"`, year `"YYYY"`, or a `"start-end"` range
- `prune_parts` — for `individual_stock` + a `ticker_filter`, open only the contiguous run of numbered parts that hold the ticker (plus the day's trailing appendix part) instead of every part. Falls back to a full scan if the ascending-code layout can't be confirmed, so results are identical — only faster. Default `True`; set `False` to force a full scan.

### `extract_to_store(input_root, output_dir, period, ticker, *, data_type="individual_stock", start_time=None, end_time=None, language="en", resume=True)`

Two-stage in one call: ingest `ticker` for `period` into a reusable, part-pruned Parquet store (`output_dir`), then return the queried DataFrame. `ticker` accepts **one code or an iterable** (`"7203"` or `["7203", "9984"]`); several tickers are ingested in one pass and returned concatenated. Prefer it over `read_ticks` when the data will be read more than once — the raw scan is paid once, later `query_ticks` reads are sub-second, and there's no 10M-row cap. Requires the `[query]` extra (DuckDB).

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

## What's New in 0.11.6

`tse_tick` 0.11.6 — `pip install -U tse-tick`. Faster single-ticker reads + a one-call two-stage helper:

- **Part-pruning for `individual_stock` single-ticker reads.** `read_ticks(...)` (and the ticker-filtered `ingest_*` / `tse-tick export`) now open only the contiguous run of numbered parts that hold the ticker — plus the day's trailing off-auction appendix part — instead of every part of the day. Validated **row-for-row identical** to a full scan across a 3-year Toyota (7203) sample (18/18 days), ~5-7× faster typically. Controlled by `read_ticks(..., prune_parts=True)` (default; set `False` to force a full scan); falls back to a full scan automatically if the ascending stock-code layout can't be confirmed.
- **`extract_to_store(input_root, store, period, ticker, ...)`** — two-stage in one call: build a reusable, part-pruned Parquet store then return the queried DataFrame. The recommended path when you'll read the data more than once.
- **`tse-tick export --store <dir>`** — build a reusable store while exporting (single `individual_stock` ticker).

## What's New in 0.11.5

`tse_tick` 0.11.5 — `pip install -U tse-tick`. Bug fixes from an alpha-test report:

- **Large one-shot reads fail safely.** `create_df` / `read_ticks` now raise a catchable `OneShotMemoryError` (a `MemoryError`) — proactively when a multi-part day's decompressed size crosses a ceiling, or by converting a Polars OOM panic — instead of an uncatchable crash, and point you at the two-stage `ingest_*` → `query_ticks` path. New `max_oneshot_bytes=` tunes the ceiling (default 5 GB; `None` disables). The `ingest_*` functions re-raise it, so it can never silently write a partial day.
- **`create_df` honors an explicit `year=` / `data_type=`** under the default `auto_detect=True` — a correctly-named ZIP in a folder whose path has no year now reads when you pass `year=`.
- **`query_ticks` warns on truncation** (a capturable `TruncationWarning`) instead of silently returning exactly `limit` rows — and no longer false-warns on a result that exactly fills `limit`.

Earlier highlights (0.11.4): single-sourced data-type classification (internal, no API change). (0.11.3): flat-folder day→month discovery fix; `get_info` / `get_supported_years` consistency. (0.11.2): `indices` column-subset projection no longer inflates rows. (0.11.1): a bare `ticker_filter` code is treated as a single code.

See [`CHANGELOG.md`](https://github.com/tse-tick/tse_tick/blob/main/CHANGELOG.md) for the full list.

---

## Notes for library users

- **Quiet by default.** `create_df`, `read_ticks`, and the `ingest_*` functions emit diagnostics via
  `logging`, not `print`, so they never write to stdout (or crash on non-ASCII paths) unless you opt
  in with `logging.basicConfig(level=logging.INFO)`. The `tse-tick` CLI still prints progress.
- **Windows-friendly `print`.** On Windows, importing `tse_tick` switches Polars to ASCII table borders
  **and** reconfigures `stdout`/`stderr` to UTF-8, so a bare `print(df)` no longer raises
  `UnicodeEncodeError` on a cp1252 console — neither the box-drawing borders nor the content glyphs
  (`datetime[μs]`, `≤` in column names, `—` in exchange values). Opt out of both with
  `TSE_TICK_ASCII_TABLES=0`; `tse_tick.display(df)` prints any DataFrame as UTF-8 on any platform
  regardless.
- **Discovery round-trips.** `get_available_tickers(...)` returns **string** codes (e.g. `["6758",
  "7203"]`) you can pass straight to `read_ticks(ticker_filter=...)`; alphanumeric codes (e.g. `"130A"`)
  are preserved rather than dropped. (`read_ticks` / `query_ticks` also accept `int` codes.)
- **Flexible discovery.** Structured-root `read_ticks` / `discover_zips` find ZIPs under the documented
  `{year}/{yearmonth}/` layout, a `{yearmonth}/` folder directly under the root (e.g. a `…/TICST120`
  type folder), and — as a fallback — recursively under nested delivery trees such as
  `個別株式{year}/TICST120/{yyyymm}/`.
- **One numbered ZIP is part of a day.** NEEDS splits each day across parts by ascending code, so
  filtering a lone `HTICST120.<date>.N.zip` by ticker can return 0 rows (Toyota 7203 is in a later
  part) — pass the day's directory or a structured root for complete coverage.
- **Numeric dtypes.** Price/quote columns (`Execution Price`, `Sell Quote 1 Best`, …) are `Float64`, and
  **all `stock_summary` measures** (OHLC, VWAP, volumes, amounts, counts) are `Float64` too — so `.mean()`
  and arithmetic work without manual casting. (Stores ingested before the relevant change held these as
  `String` — re-ingest to refresh.)
- **Time filters keep the whole order book.** For `individual_stock`, quote-only book updates have a blank
  `Execution Time` but a real `Update Time`; `read_ticks` / `query_ticks` time windows fall back to
  `Update Time` for those rows, so a session filter retains in-window quote updates (not just
  trade-coincident snapshots) — what `compute_depth` / `compute_spread` / `compute_flow_imbalance` need.
- **Index codes are raw codes.** `indices` and `indices_summary` both return `Index Code` as the raw
  numeric code (e.g. `"101"`), matching what you pass to `ticker_filter` and the `ticker=` partition;
  `ticker_filter` also accepts the display name (`"Nikkei 225"`). (Stores written before 0.7.0 held
  decoded names for `indices` — re-ingest to refresh.)
- **Empty results keep their schema and warn.** A read that matches nothing — a date with no ZIPs (e.g.
  a market holiday), an unknown ticker/index code, or an over-tight filter — returns an empty *but
  fully-typed* DataFrame (all columns present), so chained access like `df["Exchange Code"]` won't raise.
  `read_ticks` also emits a capturable `tse_tick.NoDataWarning` (a `UserWarning`) for **every** zero-row
  result across all four types, so "no data" is never silent — trap it with `warnings.catch_warnings()`
  or silence it with `warnings.filterwarnings("ignore", category=tse_tick.NoDataWarning)`. The `rows=`
  cap likewise emits a capturable `tse_tick.TruncationWarning` when it truncates a result (the signal to
  build a store and use `query_ticks`).
- **Summary stores are compact (date-partitioned).** The two daily-aggregate types (`stock_summary`,
  `indices_summary`) write **one Parquet file per date** with the code kept as a column — not one tiny
  file per (date × ticker), which previously blew a 15 MB month up to a 2.4 GB store of ~87k files.
  `query_ticks` / `get_available_tickers` prune/read that column; the tick types keep per-ticker files.
  Re-ingest summary stores built before this change.
- **Ingestion entry points** are the functions `ingest_period`, `ingest_single_zip`,
  `ingest_year_from_root`, … — `tse_tick.ingest` itself is the submodule.
- **Event windows.** `extract_event_window(store, ticker, event_date, event_time, before, after,
  data_type=...)` returns the ticks around one event from a store, adding a `seconds_from_event` column
  (quote-only rows are timed via `Update Time`, like `query_ticks`). It works for the tick types
  (`individual_stock`, `indices`); `before`/`after` apply only when `event_time` is given (omit it for the
  whole day). `extract_batch_event_windows` runs many events and returns `None` for any that fail.

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

The suite collects **336 tests**. Without a local NEEDS store, **288 pass** and **48 skip**; with a complete NEEDS store, **all 336 pass**. Stage-1
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

If you use this software in your research, please cite it using the `CITATION.cff` file in the repository. A technical paper describing the library is in preparation.

---

## License

[MIT](https://github.com/tse-tick/tse_tick/blob/main/LICENSE)

---

## Authors

- **Kazumi Li** — Schema definitions, package architecture, current maintainer
- **Masataka Hayashi** — Initial pandas-based prototype
- **Peter Romero** — Original concept and initial project design

Developed at Keio University, Nakatsuma Seminar.
