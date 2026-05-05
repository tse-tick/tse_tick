# tse_tick Reform Plan: End-to-End Tick Data Pipeline

## 1. Current State Assessment

### What Works
- **Stage 0 (Parsing):** `create_df()` in `enhanced.py` is fully implemented. It reads a single ZIP file or a flat directory of ZIPs, auto-detects data type and year, parses CSV/2016-fixed-width formats, assigns bilingual column names, and decodes categorical values.
- **Schemas:** All four data type schemas are defined in `schemas.py` (TICST120=95 cols, TICSS110=82 cols, TICIT110=23 cols, TICIS110=17 cols).
- **2016 Fixed-width Parsing:** `core.py::parse_line()` handles the `+`-delimited format used in 2016 for indices data.
- **Ingest Parquet I/O Modules:** `ingest.py`, `io/parquet.py`, `quPery.py`, `event_window.py`, `features.py` have full signatures and docstrings but raise `NotImplementedError`.

### What Does NOT Work
- `ingest.py` (batch ZIP → Parquet): `NotImplementedError("Not yet implemented — waiting for NEEDS data access")`
- `io/parquet.py` (write/read partitioned Parquet): Same `NotImplementedError`
- `query.py` (DuckDB queries): Same
- `event_window.py` (post-ingestion): Same
- `features.py` (post-ingestion): Same
- `enhanced.py::create_df()` only handles **flat directories** of ZIPs and **single ZIP files**. It does NOT recursively traverse the hierarchical NEEDS folder structure.

### Data Format Details (from Manuals + Code)

| Code | Type | Fields | Manual PDF |
|------|------|--------|------------|
| TICST120 | `individual_stock` | 95 | `TICST1@@.pdf` |
| TICSS110 | `stock_summary` | 82 | `TIC@S@10.pdf` |
| TICIT110 | `indices` | 23 | `TICIT110.pdf` |
| TICIS110 | `indices_summary` | 17 | Renamed (TIC@S@10) |

### NEEDS Folder Structure (User's Actual Zips)

```
個別株式2016/                    ← root, one per year per data type
├── TICIS010/                   ← type-specific subdirectory
├── TICIT010/                   （whichever type was downloaded）
├── TICSS110/
└── TICST120/
    ├── 201601/                 ← yearmonth
    │   ├── HTICST120.20160104.1.zip
    │   ├── HTICST120.20160104.2.zip
    │   ├── HTICST120.20160104.3.zip
    │   ├── HTICST120.20160105.1.zip
    │   └── ...
    ├── 201602/
    └── ...
```

Key facts:
- One file type per download (e.g., the `TICST120/` folder contains ONLY TICST120 zips).
- Each trading day has **1–N ZIP files** (N not constant).
- File naming: `H{CodeType}.{YYYYMMDD}.{SequenceNumber}.zip`
- Zips contain a **single text file** (CSV) internally, read in-memory via `zipfile.ZipFile`.

---

## 2. Target Architecture

### Two-Stage Pipeline (Reusing Existing Architecture)

```
┌──────────────────────────────────────────────────────────────────────┐
│ STAGE 1: INGEST (ZIP → Parquet Store)                                │
│                                                                       │
│  User's NEEDS folder                                                  │
│  {data_type}20XX/{CodeType}/{YYYYMM}/H{Code}.{date}.N.zip            │
│       │                                                               │
│       ├──▶ ingest_single_zip()           ← activate existing stub     │
│       │    [enhanced.py::create_df()]                                  │
│       │    ├── auto-detect type/year from path                        │
│       │    ├── pd.read_csv() raw                                       │
│       │    ├── set_columns() + clean_data()                            │
│       │    └── return cleaned DataFrame                                │
│       │                                                               │
│       ├──▶ write_partitioned_parquet()   ← activate existing stub     │
│       │    [io/parquet.py]                                             │
│       │    Layout: output_dir/{data_type}/                             │
│       │            date=YYYYMMDD/ticker=NNNN.parquet                   │
│       │                                                               │
│       └── free memory (del + gc.collect())                             │
│                                                                       │
│  CLI: tse_tick ingest --data-type individual_stock \                  │
│                        --years 2016-2023 \                             │
│                        --input-root /path/to/data \                    │
│                        --output-root /path/to/parquet_store            │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│ STAGE 2: QUERY & ANALYZE (Parquet Store → Research)                   │
│                                                                       │
│  Partitioned Parquet Store                                            │
│       │                                                               │
│       ├──▶ query_ticks()              ← DuckDB (activate stub)        │
│       ├──▶ extract_event_window()     ← event study                   │
│       ├──▶ compute_spread()           ← features                      │
│       └──▶ compute_all_features()     ← combined features             │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 3. User Workflow (Ideal UX)

### Step 0: Clone and Install

```bash
git clone https://github.com/jevwithwind/tse_tick.git
cd tse_tick
pip install -e "."
```

### Step 1: One-Line Ingest (Simplest Path)

```bash
# Convert all TICST120 data for 2016-2023 into Parquet
tse_tick ingest \
    --data-type individual_stock \
    --years 2016-2023 \
    --input-root /Volumes/TSE_DATA/個別株式 \
    --output-root /Volumes/PARQUET_STORE
```

The CLI auto-discovers the folder structure by scanning for `{year}/{yearmonth}/H*.zip` patterns under the input root.

### Step 2: Ingest a Single Year (Selective)

```bash
tse_tick ingest \
    --data-type indices \
    --years 2022 \
    --input-root /Volumes/TSE_DATA/指数 \\
    --output-root /Volumes/PARQUET_STORE
```

### Step 3: Query via Python API

```python
import tse_tick

# Query Nikkei 225 ticks for Jan 2022
df = tse_tick.query_ticks(
    "/Volumes/PARQUET_STORE",
    data_type="indices",
    index_code=101,       # Nikkei 225
    date="20220104",
    start_time="09:00:00",
    end_time="11:30:00"
)

# Get available trading dates
dates = tse_tick.get_available_dates("/Volumes/PARQUET_STORE")
```

### Step 4: Event-Study Extraction

```python
df_window = tse_tick.extract_event_window(
    "/Volumes/PARQUET_STORE",
    ticker=7203,
    event_date="20220728",
    event_time="15:00:00",
    before="120min",
    after="120min",
)
```

---

## 4. Implementation Plan

### Phase 1: Core Ingest Activation (MUST DO)

Files to modify: `tse_tick/ingest.py`, `tse_tick/io/parquet.py`, `tse_tick/enhanced.py`

#### 4.1.1 Fix `io/parquet.py` — Remove NotImplementedError

The module has full working logic inside but is blocked by `NotImplementedError` at the top. **Unblock the module** by removing the guard, then fix/adjust:

- `write_partitioned_parquet()` — Write DataFrame to Hive-partitioned Parquet:
  ```
  output_dir/{data_type}/date=YYYYMMDD/ticker=NNNN.parquet
  ```
  - `_coerce_time_cols()` converts `datetime.time` objects to "HHMMSS" strings for Parquet compatibility (Parquet doesn't support `datetime.time`).
  - Group by `Data Date` (converted to YYYYMMDD string) → write per-date, per-ticker.
  - Use snappy compression.

- `write_event_window_parquet()` — For event-window output:
  ```
  output_dir/year=YYYY/month=MM/YYYYMMDD.parquet
  ```

- `read_parquet_partition()` — PyArrow dataset read with partition pruning.
- `read_partitioned_parquet()` — Read from event-window store.

#### 4.1.2 Fix `ingest.py` — Remove NotImplementedError

The module has full working logic but is blocked. Unblock and adjust:

- `ingest_single_zip()` — process one ZIP to Parquet. Calls `create_df()` → `write_partitioned_parquet()`.
- `ingest_directory()` — process all ZIPs in a flat directory.
- `ingest_year()` — filter ZIPs by year substring.
- `ingest_event_windows()` — for event studies (already has working tests).

#### 4.1.3 Extend `enhanced.py::get_1y_dataframe()` — Recursive Discovery

Currently only works for flat directories of ZIPs or single ZIPs. Add a new discovery mode that:

1. Given a year, data type, and root path, constructs the expected path pattern:
   ```
   {root}/{year}/{yearmonth}/H{CodeType}.{YYYYMMDD}.*.zip
   ```
2. Collects all matching ZIP files sorted by date and sequence number.
3. Processes each ZIP one at a time (C2 constraint: one raw DataFrame in memory).

Add a function `discover_zips(input_root, data_type, year, start_month, end_month)` that:

| Data Type | Code Type Prefix | Chromed |
|-----------|-----------------|---------|
| `individual_stock` | `HTICST120` | Individual stock tick |
| `stock_summary` | `HTICSS110` | Stock daily summary |
| `indices` | `HTICIT110` | Index tick |
| `indices_summary` | `HTICIS110` | Index daily summary |

Scans `{input_root}/{year}/{yearmonth}/` and collects `H{CodeType}.{date}.*.zip`.

### Phase 2: CLI Entry Point (MUST DO)

Create `tse_tick/cli.py` with a single entry point supporting:

```
tse_tick ingest \
    --data-type {individual_stock|stock_summary|indices|indices_summary} \
    --years 2016-2023 | --years 2018,2019,2020 | --year 2023 \
    --months 1-12 | --months 1,2,3 \
    --input-root /path/to/data \
    --output-root /path/to/parquet_store \
    --parallel N \
    --language en
```

Register as a setuptools console_scripts entry point in `pyproject.toml`:

```toml
[project.scripts]
tse-tick = "tse_tick.cli:main"
```

### Phase 3: Query Activation (SHOULD DO)

Activate `query.py` by removing `NotImplementedError` guard:

- `query_ticks()` — already has full DuckDB SQL generation logic. Just unblock.
- `query_sql()` — allows raw SQL via DuckDB view.
- `get_available_dates()` — scans partition directories.
- `get_available_tickers()` — scans ticker partition directories.

### Phase 4: Features & Event Window Activation (NICE TO HAVE)

Activate `features.py` and `event_window.py` by removing NotImplementedError guards.
These already have full implementations and extensive test coverage.

### Phase 5: Tests (SHOULD DO)

Most tests are `pytest.skip("Waiting for NEEDS data access")`. After activation:

1. Tests that use synthetic DataFrames (test_parquet.py, test_ingest.py for corrupt ZIPs, test_event_window.py for `_filter_ticks_for_events`) — already work.
2. Tests marked `pytest.skip` for operations requiring real ZIP data — keep skipped but remove the `NotImplementedError`-related skip message. Replace with more specific test data requirements.

---

## 5. Input Contract: What the User MUST Provide

| Input | Required | Description |
|-------|----------|-------------|
| `--data-type` | YES | One of `individual_stock`, `stock_summary`, `indices`, `indices_summary` |
| `--years` | YES | Year(s) to process (2016–2023 range) |
| `--input-root` | YES | Root directory where the year/month/zip structure lives |
| `--output-root` | YES | Where to write the Parquet store |
| `--months` | NO | Override default 1–12 if partial year needed |
| `--parallel` | NO | Number of worker processes (default 1) |

The user does NOT need to:
- Specify column names or schemas (encoded in `schemas.py`)
- Specify file format details (handled by `enhanced.py` logic)
- Manually list ZIP files (discovery is automatic)
- Know about 2016 special format (handled by `parse_line()`)
- Manually handle corrupt ZIPs (logged and skipped)

---

## 6. Data Type / Code Type Mapping

| `--data-type` Value | NEEDS File ID | File Prefix | Fields | Manual |
|---------------------|---------------|-------------|--------|--------|
| `individual_stock` | TICST120 | `HTICST120` | 95 | `TICST1@@.pdf` |
| `stock_summary` | TICSS110 | `HTICSS110` | 82 | `TIC@S@10.pdf` |
| `indices` | TICIT110 | `HTICIT110` | 23 | `TICIT110.pdf` |
| `indices_summary` | TICIS110 | `HTICIS110` | 17 | See TIC@S@10.pdf |

---

## 7. MIME Type Information

- Each ZIP contains exactly **one text file**.
- Format: CSV with comma-separated values, `"`-quoted string fields (for 2017+).
- 2016 special cases: Fixed-width or `+`-delimited format for indices types (handled by `parse_line()`).
- Encoding: ASCII (numeric + English chars) — no Japanese in raw data; Japanese only in decoded column names.
- Line endings: CRLF (`\r\n`).

---

## 8. Task Checklist (Ordered by Priority)

### MUST DO (P0)

- [ ] **Unblock `io/parquet.py`**: Remove `NotImplementedError` guard; verify `write_partitioned_parquet()`, `read_parquet_partition()`, `write_event_window_parquet()`, `read_partitioned_parquet()` work.
- [ ] **Unblock `ingest.py`**: Remove `NotImplementedError` guard; verify `ingest_single_zip()`, `ingest_directory()`, `ingest_year()` work.
- [ ] **Add recursive ZIP discovery** to `enhanced.py` or a new `discovery.py`: Given input_root, data_type, year(s), month(s) → return sorted list of ZIP paths.
- [ ] **Add `tse_tick/cli.py`**: CLI with argparse, `ingest` subcommand.
- [ ] **Register `tse-tick` console script** in `pyproject.toml`.
- [ ] **Update `enhanced.py::create_df()`**: Accept `auto_detect=False` with explicit `data_type` and `year` parameters (currently raises `ValueError` for manual).
- [ ] **Resume-support**: Check if output Parquet already exists for a given date before processing; skip if present.

### SHOULD DO (P1)

- [ ] **Unblock `query.py`**: Remove `NotImplementedError` guard.
- [ ] **Unblock `event_window.py`**: Remove `NotImplementedError` guard.
- [ ] **Unblock `features.py`**: Remove `NotImplementedError` guard.
- [ ] **Add progress bar** (tqdm) to ingest pipeline.

### NICE TO HAVE (P2)

- [ ] **Parallel ingest** via `concurrent.futures.ProcessPoolExecutor` (signatures already exist in `ingest.py`).
- [ ] **Corrupt ZIP handling**: Already in `ingest_event_windows()`, extend to general ingest.
- [ ] **Incremental ingest**: Only process ZIPs newer than last Parquet modification.
- [ ] **Config file** (`config.yml`) for default paths to avoid long CLI arguments.
- [ ] **Logging** to file alongside progress output.

---

## 9. Estimated File Changes Summary

| File | Change |
|------|--------|
| `tse_tick/io/parquet.py` | Remove `NotImplementedError` guard at top; minor fixes |
| `tse_tick/ingest.py` | Remove `NotImplementedError` guard at top; minor fixes |
| `tse_tick/query.py` | Remove `NotImplementedError` guard at top |
| `tse_tick/event_window.py` | Remove `NotImplementedError` guard at top |
| `tse_tick/features.py` | Remove `NotImplementedError` guard at top |
| `tse_tick/enhanced.py` | Add manual `data_type`/`year` parameter support; add recursive ZIP discovery function |
| `tse_tick/cli.py` | **NEW**: CLI entry point with argparse |
| `tse_tick/__init__.py` | Minor: ensure all unblocked functions are importable |
| `pyproject.toml` | Add `[project.scripts]` entry for `tse-tick` command |
| `tests/` | Update skip messages; add integration test for discovery |
| `REFORM_PLAN.md` | **NEW**: This document |

---

## 10. Constraints (Preserved from Existing Architecture)

1. **C1** — ZIPs are always read in-memory via `create_df()`, never extracted to disk.
2. **C2** — Only one raw DataFrame lives in memory at a time.
3. **C3** — `del raw_df, gc.collect()` after every ZIP iteration.
4. **C4** — Output is written to partitioned Parquet, never a single file.
5. **C5** — Corrupt ZIPs are logged and skipped.
6. **C6** — JST timezone applied before timestamp comparisons.
7. **C7** — `zip_date` forced to `str` on CSV load.
