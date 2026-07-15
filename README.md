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
- **CLI batch ingestion** — `tse-tick ingest` converts entire years/months/date ranges to partitioned Parquet; independent trading days spread across a RAM-aware process pool by default (`--parallel auto`)
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

**On Linux**, three prerequisites bite before tse_tick itself does: Python **≥3.9** (Ubuntu 20.04's
system 3.8 won't resolve the package); a fresh Debian/Ubuntu needs `apt install python3-venv`
(matching your minor version, e.g. `python3.12-venv`) before `python3 -m venv` works; and the polars
wheel needs **AVX2** and a reasonably modern glibc — on an older CPU, install `polars-lts-cpu`
instead. See **[Linux and WSL notes](#linux-and-wsl-notes)** for runtime behavior.

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

Novice mistakes read in plain language rather than as a Python traceback: a malformed
`--period`, a missing `--input-root`, or a time filter on a daily-summary type prints a
one-line `Error: …` (exit code 1), and a no-data day (e.g. an exchange holiday) prints a
`Warning: …` note and still writes the empty file (exit 0). Pass `--log-level DEBUG` to
see the full traceback.

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

> `query_ticks` / `get_available_tickers` accept a flexible `date=` — a day `"YYYYMMDD"`, month
> `"YYYYMM"`, year `"YYYY"`, or a `"start-end"` range (the same forms `read_ticks` / `ingest_period`
> take) — so a store you built with a month can be queried with that same month string.

#### Export a large slice to one Parquet file (memory-safe)

When a slice is too big to hold in RAM as one DataFrame (a multi-year active ticker), stream it
straight to a single Parquet file instead of calling `query_ticks(..., limit=None)`:

```python
import tse_tick

manifest = tse_tick.export_query(
    "/path/to/PARQUET_STORE",
    "toyota_2017_2019.parquet",       # single output file
    data_type="individual_stock",
    ticker=7203,
    date="2017-2019",
    overwrite=False,                  # refuses to clobber an existing file unless True
)
# {'path': 'toyota_2017_2019.parquet', 'rows': 136436016, 'dates': 733, ...}
```

`export_query` walks the store's days in order and appends each as a Parquet row group, so peak
memory stays bounded regardless of period length (it does *not* return the data — just a manifest).
Its output is row-identical to concatenating `query_ticks(..., limit=None)` over the same slice.
Requires the `[query]` extra.

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

# The if __name__ == "__main__": guard is REQUIRED for parallel ingest when this
# runs as a .py script: workers start with the 'spawn' method, which re-imports
# the script — an unguarded top-level call would re-run itself in every worker
# (tse_tick raises an explanatory error if you forget). In Jupyter/the REPL there
# is nothing to re-import, so parallel ingest is on by default there.
if __name__ == "__main__":
    # Build a reusable store for one or several tickers and get the DataFrame back —
    # in one call. No 10M-row cap, so a whole month of active tickers comes back
    # complete (past ~10M rows a capturable LargeResultWarning fires — for multi-year
    # periods, ignore the returned frame and read the store in query_ticks slices).
    df = tse_tick.extract_to_store(
        "/path/to/TSE_DATA",          # a .zip, flat folder, or ANY folder above the data
        "/path/to/PARQUET_STORE",     # reusable store (built once; resume-safe, part-pruned)
        "202402",                     # a day, month, year, or range (e.g. "2021-2023")
        ["7203", "9984"],             # one code, or a list — Toyota + SoftBank
        max_workers="auto",           # parallel per-date ingest (capped by cores + RAM)
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

> **Share-class families.** A 4-char `individual_stock` code selects its whole
> share-class **family** everywhere: `"7203"` (and equally `"72031"`) reads Toyota
> plus its suffixed classes (New Shares `72031`, …) in `read_ticks`,
> `extract_to_store`, and `query_ticks` — matching what a filtered ingest has
> always stored. On a built store, `query_ticks(ticker="72031")` (the 5-char
> form) reads exactly that class.

> **⚠️ Reading a lot at once? Mind the 10M-row cap.** `read_ticks` **and** `query_ticks`
> both return at most **10,000,000 rows** per call by default — on hitting it, the
> result is truncated and a capturable `tse_tick.TruncationWarning` is emitted. A whole
> **month** of a couple of *active* tickers can exceed this: SoftBank (9984) alone runs
> **>10M rows/month**, so a single `read_ticks(..., date="YYYYMM")` over 9984 plus another
> active name truncates partway through the month rather than erroring. To get
> **everything**, pick one:
>
> - **Two-stage (recommended for scale)** — `ingest_period(...)` → `query_ticks(...)`
>   in bounded slices (per day / per month; pass `limit=None` per slice for the full
>   rows). The store is reusable and every slice read is sub-second.
> - **`extract_to_store(...)`** — the only call with **no row cap**: it builds the
>   store and returns the whole period as ONE in-memory DataFrame. Past ~10M rows a
>   capturable `tse_tick.LargeResultWarning` fires first — for multi-year periods of
>   an active ticker, ignore the returned frame and read the store in `query_ticks`
>   slices instead (the frame can be tens of GB).
> - **`export_query(store, out.parquet, ...)`** — stream the whole slice straight to a
>   **single Parquet file** without ever holding it in memory. The memory-safe way to
>   get a multi-year active ticker in *one* place: it walks the store's days in order
>   and appends each as a row group, so peak RAM stays bounded regardless of period
>   length (a 3-month 7203 export plateaus ~3.6 GB vs the ~100 GB a whole-frame read
>   would need). Returns a small manifest, not the data. Requires the `[query]` extra.
> - **Loop per day** — call `read_ticks(..., date=day)` for each trading day and
>   `pl.concat(...)`; each day stays well under the cap.
> - **Lift the cap** — `read_ticks(..., rows=None)` / `query_ticks(..., limit=None)`
>   reads it all in one shot, bounded only by memory. If the result is too large to
>   assemble as one frame — a multi-year range of an *active* name is enormous (Toyota
>   7203 for 2017–2019 is ~136M rows × 95 cols ≈ 100 GB in RAM) — the read path raises
>   a catchable `tse_tick.OneShotMemoryError` and the query path a catchable
>   `tse_tick.QueryMemoryError` (both `MemoryError` subclasses), pointing you back to
>   the bounded slices above rather than a raw DuckDB out-of-memory traceback.
>
> If a one-shot read might be large, check for a `TruncationWarning` (it's the signal
> to switch to the two-stage store):
>
> ```python
> import warnings
> with warnings.catch_warnings(record=True) as w:
>     warnings.simplefilter("always")
>     df = tse_tick.read_ticks(root, ticker_filter={"7203", "9984"}, date="202201")
> if any(issubclass(x.category, tse_tick.TruncationWarning) for x in w):
>     ...  # truncated — build a store instead
> ```

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

`tse_tick` is built on Polars (CSV parsing, vectorized cleaning) and DuckDB over Hive-partitioned Parquet (queries). Hardware for every row below: an Intel Core i5-14400F (10-core / 16-thread) with 32 GB RAM, Python 3.11, Polars 1.40, pandas 2.2. The engine and storage rows are measured on **one ZIP part** of HTICST120 (`HTICST120.20170104.1.zip` — 4.78 M rows, 95 columns, 2.16 GB raw CSV); a full trading day is 1–27 such parts. The query row uses a different, smaller input — see its note.

| Comparison | Speedup | Source |
|------------|---------|--------|
| Polars (16T) vs pandas (Python engine) | **55.5×** | `benchmarks/results_engine_summary.csv` |
| Polars (16T) vs pandas (C engine, fair baseline) | **22.8×** | `benchmarks/results_engine_summary.csv` |
| Polars (1 thread) vs pandas (C engine) | **6.2×** | `benchmarks/results_engine_summary.csv` |
| DuckDB + Hive Parquet vs pandas CSV scan (single-ticker hour slice; measured on 1.5 M rows) | **694.1×** | `benchmarks/results_query.csv` |
| Parquet (zstd, the default) storage size vs raw CSV | **31× smaller** (70.8 MB vs 2.2 GB) | `benchmarks/results_format.csv` |
| Parquet (snappy, pre-0.14 stores) storage size vs raw CSV | **22× smaller** (99.7 MB vs 2.2 GB) | `benchmarks/results_format.csv` |

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
| `--period` | Date range: `YYYY`, `YYYY-YYYY`, `YYYYMM-YYYYMM`, or `YYYYMMDD-YYYYMMDD` |
| `--language` | Column name language: `en` (default) or `jp` |
| `--parallel` | Parallel worker processes for per-date ingest: a positive int or `auto` (**default**: the machine's logical cores). Applies to `--period`, `--year`, and `--flat`; not to `--filter-csv` event windows. Capped by the machine's logical cores **and** available RAM. A ticker-filtered ingest (≤64 codes) streams, so each worker is bounded at ~3 GB whatever the day's size; a full-frame ingest holds the whole day per worker and is what the RAM cap mainly binds on. Pass `1` to force serial. |
| `--no-resume` | Disable resume (reprocess dates even if output exists) |
| `--compression` | Parquet codec: `zstd` (default — smaller, faster reads) or `snappy` (matches pre-0.14 stores). Mixed-codec stores read fine, so no re-ingest is needed. |
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

### `create_df(folder_path, language="en", rows=None, auto_detect=True, data_type=None, year=None, ticker_filter=None, max_oneshot_bytes=<5 GB>, on_morsel=None)`

Load and clean tick data from a ZIP file or directory of ZIP files.

- `folder_path` — path to a `.zip` file or directory of `.zip` files
- `language` — `"en"` or `"jp"` for column names
- `rows` — max rows to return
- `auto_detect` — if `True`, detect data type and year from path. If `False`, must provide `data_type` and `year`
- `data_type` — `"individual_stock"`, `"stock_summary"`, `"indices"`, or `"indices_summary"`
- `year` — data year (e.g., 2023)
- `ticker_filter` — optional `set` of 4-digit stock codes to pre-filter at line level
- `max_oneshot_bytes` — cumulative decompressed-size ceiling for the one-shot read (default 5 GB; `None` disables). Crossing it raises a catchable `OneShotMemoryError`; use the two-stage `ingest_* → query_ticks` path instead.
- `on_morsel` — advanced/internal. A callable invoked with each cleaned ~64 MB morsel as it is parsed, instead of accumulating the whole frame; `create_df` then returns an **empty** frame and the morsels are the output. This is what the ingest engine uses to bound memory (see `--parallel`). **Only honoured on the bounded read** — `data_type="individual_stock"` *with* a `ticker_filter`. On any other path it is silently ignored and you get the whole frame back, so do not rely on it alone to cap memory. Leave as `None` for normal reads.

Returns a Polars DataFrame with English or Japanese column names.

### `export_to_csv(folder_path, output_path=None, language="en", rows=None)`

Load and export to CSV. If `output_path` is `None`, generates a filename. `language="jp"`
is written as UTF-8 **with a BOM** (`utf-8-sig`) so Excel on a Japanese Windows locale opens it
without mojibake; `language="en"` is ASCII and BOM-free.

### `read_ticks(source, *, data_type="individual_stock", ticker_filter=None, date=None, start_time=None, end_time=None, columns=None, rows=10_000_000, language="en", prune_parts=True, max_oneshot_bytes=...)`

One-shot read: raw NEEDS ZIPs → a ticker/time-filtered DataFrame, no store.

- `source` — a `.zip`, a flat folder, or any folder above the data (located by type + date)
- `ticker_filter` — a `set` of codes (e.g. `{"7203"}`); a bare `"7203"`/`7203` also works
- `date` — a day `"YYYYMMDD"`, month `"YYYYMM"`, year `"YYYY"`, or a `"start-end"` range
- `prune_parts` — for `individual_stock` + a `ticker_filter`, open only the contiguous run of numbered parts that hold the ticker (plus the day's trailing appendix part) instead of every part. Falls back to a full scan if the ascending-code layout can't be confirmed, so results are identical — only faster. Default `True`; set `False` to force a full scan.

### `extract_to_store(input_root, output_dir, period, ticker, *, data_type="individual_stock", start_time=None, end_time=None, language="en", resume=True, max_workers=None, compression="zstd")`

Two-stage in one call: ingest `ticker` for `period` into a reusable, part-pruned Parquet store (`output_dir`), then return the queried DataFrame. `ticker` accepts **one code or an iterable** (`"7203"` or `["7203", "9984"]`); several tickers are ingested in one pass and returned concatenated, and a 4-char code selects its whole share-class family (`"7203"` ⇒ 7203 + 72031 …). Prefer it over `read_ticks` when the data will be read more than once — the raw scan is paid once, later `query_ticks` reads are sub-second, and there's no 10M-row cap. The result is scoped to `period` even on a reused store holding other days. If Stage 1 lost data (a corrupt part), a capturable `PartialIngestWarning` names the affected dates. `max_workers` — an int, `"auto"` (logical cores, RAM-capped), or `None` (default: the `TSE_TICK_MAX_WORKERS` env var if set; auto in Jupyter/REPL; serial from a script — parallel from a script requires the `if __name__ == "__main__":` guard, since spawn workers re-import your script). Everything is returned as ONE in-memory DataFrame — past ~10M rows a capturable `LargeResultWarning` fires; for big periods read the built store in `query_ticks` slices instead. Requires the `[query]` extra (DuckDB).

---

## Security

Built-in protections for local data processing:

| Guard | Value |
|-------|-------|
| ZIP bomb detection (max decompressed) | 5 GB |
| ZIP compression ratio cap | 100:1 |
| Max ZIP entries | 5 |
| Parallel worker cap | RAM-aware: ≤ logical cores, and N × per-worker estimate ≤ 70% of available RAM (~3 GB/worker when streaming a ticker-filtered ingest; the whole day's frame when full-frame) |
| Query row limit | 10,000,000 |
| Path traversal prevention | Resolved path validation |
| SQL injection prevention | Identifier/date/time format validation |

---

## Release History

See [`CHANGELOG.md`](https://github.com/tse-tick/tse_tick/blob/main/CHANGELOG.md) for what changed in each release.

---

## Notes for library users

- **Quiet by default.** `create_df`, `read_ticks`, and the `ingest_*` functions emit diagnostics via
  `logging`, not `print`, so they never write to stdout (or crash on non-ASCII paths) unless you opt
  in with `logging.basicConfig(level=logging.INFO)` — worth doing for a long ingest: the per-date
  progress lines carry an `[i/N]` counter and resumed runs log how many dates they skipped. The
  `tse-tick` CLI still prints progress.
- **Parallel ingest defaults.** `max_workers` accepts `"auto"` (logical cores, RAM-capped). The
  default (`None`) resolves to the `TSE_TICK_MAX_WORKERS` env var (an int or `auto`) when set, to
  auto in Jupyter/the REPL (spawn-safe there), and to serial from a `.py` script — where parallel
  runs need the `if __name__ == "__main__":` guard because spawn workers re-import your script.
  The CLI defaults to `--parallel auto`.
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
  Stores now persist that effective time as an internal `Effective Time` key so a time window can skip
  Parquet row groups instead of scanning them (a 1-minute slice measured **7.64x** faster on real data,
  a 09:00–11:30 session window **1.27x**, for +0.52% store bytes). It is an index, not data: it is
  excluded from every *documented output* — `query_ticks`, `export_query`, `read_parquet_partition`
  and the typed-empty frames — so `individual_stock` still returns its **95** NEEDS columns (plus the
  Hive `date` partition column = 96 from the store). The one exception is the `query_sql` escape
  hatch, which by design exposes the store as-is: its `ticks` view shows the key, so `SELECT *` there
  returns **97** (95 + `Effective Time` + `date`). That is what lets you write your own fast time
  predicates against it. Purely additive —
  **older stores keep working unchanged, with no re-ingest**; they simply fall back to the old
  (unpruned) expression, so re-ingest only if you want the speedup.
- **Index codes are raw codes.** `indices` and `indices_summary` both return `Index Code` as the raw
  numeric code (e.g. `"101"`), matching what you pass to `ticker_filter` and the `ticker=` partition;
  `ticker_filter` also accepts the display name (`"Nikkei 225"`). (Stores written before 0.7.0 held
  decoded names for `indices` — re-ingest to refresh.)
- **Empty results keep their schema and warn.** A read that matches nothing — a date with no ZIPs (e.g.
  a market holiday), an unknown ticker/index code, or an over-tight filter — returns an empty *but
  fully-typed* DataFrame (all columns present), so chained access like `df["Exchange Code"]` won't raise.
  Both `read_ticks` and `query_ticks` emit a capturable `tse_tick.NoDataWarning` (a `UserWarning`) on
  **every** zero-row result across all four types, so "no data" is never silent on either read path — trap
  it with `warnings.catch_warnings()` or silence it with
  `warnings.filterwarnings("ignore", category=tse_tick.NoDataWarning)`. The `rows=`
  cap likewise emits a capturable `tse_tick.TruncationWarning` when it truncates a result (the signal to
  build a store and use `query_ticks`).
- **Big queries spill to system temp.** DuckDB-backed queries (`query_ticks`, `query_sql`,
  `extract_to_store`'s Stage-2) spill large sorts to `<system temp>/tse_tick_duckdb_spill`
  rather than a `.tmp/` folder in your working directory. An interrupted query can leave
  files there; they are safe to delete.
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

## Linux and WSL notes

tse_tick is developed on Windows and verified on Linux (WSL2 Ubuntu 24.04, Python 3.12, polars
1.42, DuckDB 1.5). Stores are **fully portable across the two** — all four data types written on
either OS read correctly on the other, including over `\\wsl$`. The points below are the ones that
actually differ, or that surprise people coming from a fork-based mental model.

- **The `if __name__ == "__main__":` guard is required on Linux too.** tse_tick forces the `spawn`
  start method on every platform (fork deadlocks Polars: it copies the mutex state of the rayon
  thread pool but not the threads holding it, so the child hangs on its first Polars call). Linux
  users used to fork semantics don't expect this. Spawn re-imports your script in each worker, so an
  unguarded top-level parallel ingest would re-run itself — tse_tick raises a designed error rather
  than hanging. From a `.py` script the default is serial (with a hint); Jupyter and the REPL have
  nothing to re-import, so they parallelize automatically.
- **Worker sizing reads `MemAvailable`.** `_cap_workers` fits `N × per-worker estimate` inside 70% of
  available RAM, and on Linux "available" means `/proc/meminfo`'s `MemAvailable` — *not* `MemFree`.
  This matters on a long-running research box, where the page cache holds most of idle RAM: `MemFree`
  can read ~1 GB on a 128 GB machine and would throttle a parallel ingest toward serial for no
  reason. A `Limiting workers N -> M` notice is normal and expected; it means the cap engaged.
- **Filenames match case-insensitively on every platform.** A `HTICST120.20230104.1.ZIP` delivery is
  discovered on Linux exactly as it always was on Windows. Two files whose names differ **only** by
  case (possible on Linux, not on Windows) resolve to one file, with a logged warning naming the one
  ignored — reading both would double-count that trading day.
- **Cross-machine exports are multiset-equal, not byte-identical.** Same-timestamp tick order is
  non-deterministic (accepted; see the note above and PR #45), so a Linux export and a Windows export
  of the same slice can order same-second ties differently. Compare as sets/sorted frames, not with
  `diff` or a file hash. This is not a Linux bug.
- **WSL specifics.** The WSL2 VM gets ~half the host's RAM by default, so a low worker cap there
  (e.g. `Limiting workers 16 -> 3` at 3.0 GB/worker on a 15.5 GB VM) is normal — raise it in
  `.wslconfig` if you want more. Reading raw ZIPs over `/mnt/g` (9p) works and was only mildly slower
  than native in our runs. Put virtualenvs on ext4 (`~`), not under `/mnt/*`.

---

## Contributing

Contributions are welcome. Please open an issue or submit a pull request.

See **[CONTRIBUTING.md](CONTRIBUTING.md)** for the development setup, code style (`black` /
`flake8` / `mypy`), PR guidelines, and how to add a name-translation mapping.

```bash
pip install -e ".[query,dev]"   # [query] is required to run the tests, not optional
pytest tests/ -v
```

---

## Testing

```bash
pip install -e ".[query,dev]"                   # [query] is required by the tests, not optional
export TSE_TICK_DATA_ROOT=/path/to/needs_root   # optional; enables the data-gated tests
pytest --no-cov
```

The suite collects **634 tests**. Stage-1 (ingestion) and Stage-2 (query, order-book features, and
event-window-from-Parquet) both run with no proprietary data — a session-scoped pytest fixture
builds a tiny Hive-partitioned Parquet store at test time by feeding synthetic, obviously-fake
`individual_stock` (TICST120) ZIPs through the real ingest pipeline (`tests/synthetic_data.py`,
`tests/conftest.py`).

| Profile | Result | Skips are |
|---------|--------|-----------|
| Linux, no data root | 584 pass / 50 skip / **0 fail** | 48 data-gated + 2 platform-gated |
| Linux, `TSE_TICK_DATA_ROOT` set | 633 pass / 1 skip / **0 fail** | 1 platform-gated |
| Windows, no data root | 584 pass / 50 skip / **0 fail** | 48 data-gated + 2 platform-gated |
| Windows, `TSE_TICK_DATA_ROOT` set | 632 pass / 2 skip / **0 fail** | 2 platform-gated |

**Nothing fails on either OS.** Every skip is deliberate, and its reason names what would run it:

- **48 data-gated tests** load **real NEEDS files** (`test_real_data.py` and the real-ZIP cases in
  `test_ingest.py`). They run automatically once `TSE_TICK_DATA_ROOT` points at a local NEEDS store.
  The default root is a **Windows** path, so off Windows they skip until you set the variable — and
  because a suite of skips still looks green, the reasons say so explicitly rather than leaving you
  to conclude the real-data half ran.
- **A few platform-gated tests** skip on the OS they don't apply to, which is why the two
  `TSE_TICK_DATA_ROOT` rows differ by one: the cp1252-console fix is Windows-only (skips on Linux),
  while the case-variant-filename tests need a case-sensitive filesystem (skip on Windows).

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
