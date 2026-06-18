# tse_tick — LLM Reference Guide

## 1. Project Identity

| Property | Value |
|----------|-------|
| Package | `tse_tick` |
| Version | 0.4.0 (Beta) — on PyPI (`pip install tse-tick`) |
| Language | Python 3.9+ (tested on 3.9 / 3.11 / 3.13) |
| Engine | **Polars** (migrated from pandas in v0.2.0) |
| Dependencies | core: `polars>=0.20.0`, `pyarrow>=12.0.0`; optional `query` extra: `duckdb>=0.9.0` |
| Repository | `https://github.com/tse-tick/tse_tick.git` |
| Data | Nikkei NEEDS high-frequency tick data (Tokyo Stock Exchange, proprietary) |
| Year range | 2016–2025 |
| Architecture | Two-stage: **INGEST** (ZIP→Parquet) → **QUERY** (DuckDB + features), plus a one-shot ZIP→DataFrame reader (`read_ticks`) shipped in 0.3.0 (see §13) |

---

## 2. Directory Structure

```
tse_tick/                          # Project root
├── pyproject.toml                   # Package metadata, deps, black/pytest/coverage/mypy/flake8 configs
├── CHANGELOG.md                     # version history (current: 0.4.0)
├── README.md                        # User-facing docs (installation, quick start, usage)
├── CONTRIBUTING.md                  # Dev setup & PR guidelines
├── ARCHITECTURE.md              # THIS FILE — package architecture reference
├── LICENSE                          # MIT
├── .gitignore                       # Ignores: *.zip, *.parquet, *.csv, data/, output/, descriptions/, technical_paper/
├── .github/workflows/tests.yml      # CI: pytest on Python 3.9 / 3.11 / 3.13
├── .github/workflows/publish.yml    # Release: build + twine check + PyPI trusted publishing (OIDC)
│
├── tse_tick/                        # *** Main package (pip-installable) ***
│   ├── __init__.py                  # Public API: re-exports all functions, enums, translate, get_info()
│   ├── cli.py                       # CLI: tse-tick ingest with --period, --years/--year, --flat
│   ├── constants.py                 # DataType / Language enums (str-subclassing; new in 0.3.0)
│   ├── translate.py                 # yfinance/Polygon/ccxt → tse_tick name map; loads data/translations.json (0.3.0)
│   ├── schemas.py                   # Column name definitions (EN/JP) for all 4 data types
│   ├── enhanced.py                  # Core ETL: create_df(), read_ticks(), discover_zips(), parse_period()
│   ├── core.py                      # Data cleaning + 2016 fixed-width parser + categorical schemas + _tick_datetime()
│   ├── ingest.py                    # Batch ZIP→Parquet: ingest_period(), ingest_year_from_root(), _process_zips()
│   ├── query.py                     # DuckDB SQL interface over partitioned Parquet stores
│   ├── event_window.py              # ±N-minute tick extraction around corporate disclosure events
│   ├── features.py                  # Order-book feature engineering (spread, depth, OFI, volatility)
│   ├── io/
│   │   ├── __init__.py              # (empty)
│   │   └── parquet.py              # Hive-partitioned Parquet read/write + event-window I/O
│   ├── data/
│   │   └── translations.json        # translate() mapping tables (override: TSE_TICK_TRANSLATIONS)
│   └── py.typed                     # PEP 561 marker
│
├── tests/                           # Test suite (pytest, 227 tests; 179 pass / 48 skip w/o data, 227/0 with a NEEDS store)
│   ├── __init__.py
│   ├── conftest.py                  # Session-scoped synthetic Parquet fixtures (stock_store, indices_store, events_df)
│   ├── synthetic_data.py            # Generates obviously-fake NEEDS-format ZIPs feeding those fixtures
│   ├── test_parquet.py              # 12 — Parquet I/O (synthetic)
│   ├── test_parquet_io.py           # 14 — Parquet partition read/write (synthetic)
│   ├── test_ingest.py               # 16 — batch ingestion (synthetic + real-ZIP cases, data-gated)
│   ├── test_query.py                # 15 — DuckDB query layer (synthetic store)
│   ├── test_features.py             # 20 — order-book features (synthetic store)
│   ├── test_event_window.py         # 22 — event-window extraction (synthetic + real-data-gated)
│   ├── test_cli.py                  # 13 — CLI end-to-end (synthetic)
│   ├── test_api_additions.py        # 13 — translate / DataType / Language / query_ticks str-int (synthetic)
│   ├── test_read_ticks.py           # 14 — one-shot read_ticks: single ZIP / flat dir / structured root (synthetic)
│   ├── test_paper_examples.py       # 5  — locks the paper's API listings
│   ├── test_real_data.py            # 64 — real NEEDS files, all 4 types/eras (data-gated)
│   ├── test_schemas.py              # STUB (1 line; schema coverage in test_real_data.py)
│   └── test_core.py                 # STUB (1 line; cleaning coverage via synthetic-fixture tests)
│
├── scripts/
│   └── ingest_event_windows.py      # Standalone CLI: extract ±N-minute event windows from raw ZIPs
│
├── benchmarks/                      # Reproducible Polars-vs-pandas + storage/query benchmarks (see §9)
│   ├── run_all.py                   # Orchestrator → run_engine / run_format / run_query_fix / run_correctness
│   ├── worker_engine.py             # Subprocess engine worker (isolated timing)
│   ├── generate_assets.py           # Result CSVs → paper tables + benchmark_figure.pdf
│   └── results_*.csv                # Aggregate timings (tracked; *_prev.csv = prior run)
│
├── examples/
│   ├── notebooks/
│   │   └── 01_basic_usage.ipynb
│   └── scripts/
│       └── example_basic_usage.py    # Demo script (NOTE: still imports pandas — legacy)
│
└── descriptions/                    # *** GITIGNORED — reference materials only ***
    ├── README.md
    ├── TICST1@@.pdf                 # PDF manual for stock tick data (TICST120)
    ├── TICIT110.pdf                 # PDF manual for index tick data (TICIT110)
    ├── TIC@S@10.pdf                # PDF manual for stock summary data (TICSS110)
    ├── manual_text.txt              # Text copy of manual
    ├── prototypes/                  # Original standalone scripts before refactoring
    ├── notebooks_exploration/       # 32 Jupyter notebooks (4 data types × 8 years)
    ├── notebooks_misc/              # Additional exploration notebooks
    └── schema_references/           # CSV files (source-of-truth for column mappings)
```

---

## 3. Four Data Types

| API Name | Code | Output Columns (raw) | Description |
|----------|------|---------------------|-------------|
| `individual_stock` | TICST120 | 95 | Tick-level executions, bid/ask quotes (10 levels), volume |
| `stock_summary` | TICSS110 | 82 (83 raw) | Daily OHLC, VWAP, session splits, execution-size buckets per stock |
| `indices` | TICIT110 | 10 (23 raw, 15 in 2016) | Index tick updates (Nikkei 225, TOPIX, etc.) |
| `indices_summary` | TICIS110 | 17 (83 raw from 2017) | Daily index summary (AM/PM OHLC per index) |

---

## 4. Key Changes (CHANGELOG Summary)

### v0.2.0 — Polars Migration (2026-05-05)

| Change | Detail |
|--------|--------|
| **Pandas → Polars** | All data processing uses polars (20-50x CSV I/O speedup) |
| Time columns | Now stored as plain strings (HHMMSS) internally for Parquet compatibility |
| DuckDB output | `.df()` → `.pl()` for native polars DataFrame returns |
| Type casting | `pl.Int64` / `pl.Float64` instead of numpy dtypes |
| String stripping | Vectorized `str.strip_chars()` instead of row-wise `map()` |
| Categorical decode | Batched `pl.col().replace()` dicts instead of iterative `.map()` |
| CLI added | `tse-tick ingest` with `--data-type`, `--years`, `--input-root`, `--output-root` |
| ZIP discovery | `discover_zips()` auto-traverses `{year}/{yearmonth}/` layout |
| Resume support | `--no-resume` flag; skips dates with existing parquet output |
| Added functions | `ingest_year_from_root()`, `discover_zips()` |
| Removed | pandas, numpy from core deps; `enhanced_backup.py`; `pd.NaT` complexity |

### v0.2.1 — Security Hardening + Period-Based Ingestion (2026-05-05/06)

| Change | Detail |
|--------|--------|
| ZIP bomb protection | Max 5 GB decompressed, max 5 entries, 100:1 compression ratio cap |
| Path traversal prevention | `_resolve_type_dir()` validates resolved paths stay within data root |
| Worker cap | `max_workers` clamped to 8 |
| Query limit | 10M default LIMIT on `query_ticks()` |
| Traceback leak fix | `traceback.print_exc()` → `logger.error(exc_info=True)` |
| SQL injection guard | Regex validation on identifiers, dates, time strings in `query_ticks()` |
| **Period-based ingestion** | `parse_period()` + `discover_zips(dates=...)` + `ingest_period()` + `--period` CLI flag |
| Day/month/year levels | `YYYYMMDD-YYYYMMDD` (day range), `YYYYMM-YYYYMM` (month range), `YYYY` (year) |
| Year range removed | No hard restriction on supported years; any year accepted |
| Removed files | `debug_regex.py`, `validate.py`, `validate_final.py`, `enhanced_backup.py` |
| PDFs moved | Manual PDFs + `manual_text.txt` moved to `descriptions/` (gitignored) |

### v0.2.2 → 0.2.3 (see `CHANGELOG.md` for full detail)

| Change | Detail |
|--------|--------|
| Event-window mode | `--filter-csv` / `--window` / `--tickers` flags; `extract_event_window`, `extract_batch_event_windows` |
| Stage-2 test fixture | Synthetic Hive-Parquet store built via the real ingest pipeline — unblocks query/feature/event-window tests with no proprietary data |
| Index Code fix | `indices_summary` (TICIS110) col 5 named `Index Code` (was silently dropped); raw 2017+ summary layout is 83 cols, output 17 |
| Field-count convention | All surfaces report **output** field counts (95 / 82 / 10 / 17) with raw counts in parentheses |
| CI + real-data tests | GitHub Actions (3.9/3.11/3.13); real-data tests for all 4 types across eras |
| Benchmarks | Tracked, reproducible engine/format/query suite + a Polars==pandas correctness gate (see §9) |

### v0.3.0 — PyPI release + import API polish (2026-06-16)

| Change | Detail |
|--------|--------|
| **Published to PyPI** | `pip install tse-tick` (0.3.0, Beta); a release-triggered `publish.yml` builds sdist+wheel, runs `twine check`, and uploads via **OIDC trusted publishing** (no stored token) |
| `read_ticks()` (one-shot) | Raw ZIPs → ticker/time-filtered DataFrame with no Parquet store (see §13) |
| `translate` / `mapping` | yfinance/Polygon/ccxt → `tse_tick` name lookup; tables in `tse_tick/data/translations.json` (override: `TSE_TICK_TRANSLATIONS`) |
| `DataType` / `Language` enums | `tse_tick/constants.py`; `get_supported_data_types()` now derives from `DataType` |
| `query_ticks(ticker=…)` | Now accepts `str` or `int` (normalized); PEP 257 docstrings added across the public API |
| Packaging | `setuptools>=77` + `license-files` (PEP 639), `name = "tse-tick"`, Development Status **Beta** |
| Tests | Suite grew to **208** (`test_api_additions` 13, `test_read_ticks` 14): 160 pass / 48 skip without data, all 208 pass with a NEEDS store |

### v0.4.0 — file-driven translations + clean-room reliability fixes (2026-06-18)

| Change | Detail |
|--------|--------|
| Translation override | Mapping tables load from `tse_tick/data/translations.json`; optional `TSE_TICK_TRANSLATIONS` env var merges a user file over the built-in (public API unchanged) |
| No-crash, quiet I/O | Library diagnostics moved from `print` to `logging` — fixes `UnicodeEncodeError` on non-ASCII paths and the unsuppressible stdout |
| NEEDS-layout discovery | `discover_zips` falls back to a recursive search, so nested delivery trees (`個別株式{year}/TICST120/{yyyymm}/`) work |
| Dtype consistency | All price/quote columns cast to `Float64` (**store-schema change**: was `String`; re-ingest to refresh) |
| Empty-but-typed reads | No-match `read_ticks` / `create_df` / `query_ticks` return the full schema with 0 rows |
| Ordering | `read_ticks` parts sort naturally by `(date, part#)`; row-cap truncation logs a warning |
| Tests | Suite **227** (`+12` regression tests across 4 new files) |

---

## 5. Polars Data Pipeline — How It Works

### 5.1 Entry Point: `create_df()`

```
create_df(folder_path, language, rows, auto_detect, data_type, year, ticker_filter)
    │
    ├─ [1] detect_data_type_and_year(folder_path)
    │      ├─ Year: regex r'(20\d{2})' on path parts
    │      └─ Type: keyword matching in lowercase path ("ticst"→individual_stock, etc.)
    │              Fallback: inspect actual ZIP filenames in directory
    │
    ├─ [2] get_1y_dataframe(folder_path, year, kind, rows, ticker_filter)
    │      ├─ ZIP bomb checks (size, ratio, entry count)
    │      ├─ pl.read_csv(has_header=False, schema_overrides={column_1..95: String})
    │      │   All columns read as Strings to avoid inference errors
    │      ├─ Special cases:
    │      │   ├─ 2016 indices_summary → parse_line() fixed-width parser
    │      │   ├─ 2016 indices → parse_line(kind="indices") fixed-width parser
    │      │   └─ ticker_filter active → line-level pre-filter before CSV parse
    │      └─ pl.concat() multiple ZIP parts vertically
    │
    ├─ [3] set_columns(df, kind, language)
    │      ├─ Maps column_N → English schema names based on column count
    │      ├─ Handles 23-col (indices/old stock) and 95-col (extended stock) variants
    │      └─ If language="jp": renames English→Japanese via mapping dict
    │
    ├─ [4] clean_data(df, kind, language)
    │      ├─ Japanese mode: temporarily rename JP→EN for cleaning, then back to JP
    │      ├─ Type casting (by positional index):
    │      │   ├─ Int64: volume / quote-volume / quote-flag columns (fill_null→0)
    │      │   ├─ price/quote-price columns: fill_null→0.0 but mostly kept as
    │      │   │   String (documented limitation; only one quote-price cast Float64)
    │      │   └─ Time columns: fill_null→None, keep as String
    │      ├─ Data Date → str.to_datetime("%Y%m%d")
    │      ├─ Time string slicing: Execution Time→6 chars, Update Time→12 chars
    │      ├─ str.strip_chars() on all String columns (vectorized)
    │      └─ Categorical decoding via pl.col().replace():
    │          ├─ Record Type ("1200"→"Stocks - Multiple Quote")
    │          ├─ Exchange Code ("11"→"Tokyo Stock Exchange (TSE)")
    │          ├─ Security Type, Session, Stock Code suffix
    │          ├─ Execution Type / Ayumi Flag (stock vs index variants)
    │          ├─ Volume Flag, Quote Flag columns
    │          └─ Unknown values → "Unknown (code)" in-place
    │
    └─ [5] get_final_columns(data_type)
           └─ individual_stock: all 95 columns
              indices: subset to 10 columns (Execution-focused)
              indices_summary: subset to 17 columns (AM/PM prices)
              stock_summary: subset to 82 columns (execution stats + quote stats)
```

### 5.2 Period-Based Ingestion (`parse_period` + `ingest_period`)

`parse_period(period_str)` accepts three formats and expands them:

| Format | Example | Expands to |
|--------|---------|------------|
| `YYYY` | `2024` | Entire year (12 months) |
| `YYYYMM-YYYYMM` | `202401-202403` | All months in range → `months_by_year` dict |
| `YYYYMMDD-YYYYMMDD` | `20240201-20240205` | All calendar dates in range → `dates` list |

Under the hood, `_expand_date_range` uses `datetime.timedelta(days=1)` iteration, and `_expand_month_range` iterates (year, month) tuples.

`discover_zips` was extended with a `dates` parameter. When `dates` is provided, it globs for specific date patterns like `{prefix}.{YYYYMMDD}.*.zip` rather than the broader `{prefix}.*.zip`:

```
discover_zips(input_root, data_type, years=[2024], months=[2], dates=['20240201'])
    → glob: ./2024/202402/HTICST120.20240201.*.zip
```

`_process_zips()` is a shared helper that iterates discovered ZIPs, calls `ingest_single_zip()` per file, and respects the resume flag (skips dates with existing parquet output).

### 5.3 Batch Ingestion: `ingest_year_from_root()` (legacy)

```
ingest_year_from_root(input_root, output_dir, year, data_type)
    │
    ├─ discover_zips(input_root, data_type, [year])
    │   └─ glob: {root}/{year}/{yearmonth}/{CODEMAP[data_type]}.*.zip
    │      (e.g., {root}/2022/202201/HTICST120.20220104.1.zip)
    │
    ├─ For each ZIP:
    │   ├─ Resume check: if output/type/date=YYYYMMDD/ticker=*.parquet exists → skip
    │   └─ ingest_single_zip() → create_df() → write_partitioned_parquet()
    │
    └─ Output layout:
        {output}/individual_stock/date=YYYYMMDD/ticker=NNNN.parquet
        (Hive-partitioned: date level, then ticker level, snappy compression)
```

### 5.4 Japanese Language Flow

Japanese mode requires special handling because `clean_data()` references columns by English name:

```
create_df(..., language="jp")
    │
    ├─ set_columns() → assigns JP column names via get_japanese_column_mapping()
    ├─ Temporarily renames JP→EN using inverse mapping
    ├─ clean_data() runs on English-named columns (type casts, time slicing, categorical decode)
    ├─ Renames back EN→JP
    └─ get_final_columns() → column subset also mapped to JP
```

### 5.5 Key Polars Patterns Used

| Pattern | Where | Why |
|---------|-------|-----|
| `pl.read_csv(has_header=False, schema_overrides=...)` | `enhanced.py` | Read NEEDS CSVs with no header, force all cols to String |
| `pl.col().str.strip_chars()` | `core.py` | Vectorized trailing-space removal |
| `pl.col().replace(dict)` | `core.py` | Batched categorical decoding (replaces row-wise map) |
| `pl.when().then().otherwise()` | `core.py` | Conditional column transformations |
| `pl.col().fill_null(value).cast(Int64)` | `core.py` | Safe null→zero→int conversion |
| `str.to_datetime("%Y%m%d")` | `core.py` | String-to-date parsing |
| `df.group_by("_date_str").agg_groups()` | `parquet.py` | Partition write grouping |
| `rolling(index_column="time", period=...)` | `features.py` | Time-based rolling window aggregations |
| `pl.from_arrow(table)` | `parquet.py`, `query.py` | Arrow→Polars conversion for PyArrow dataset reads |

---

## 6. Stage 2 — Query Architecture

### 6.1 `query.py` — DuckDB SQL Interface

```
query_ticks(data_dir, ticker, date, start_time, end_time, columns, limit)
    │
    ├─ Input validation (regex on identifiers, dates YYYYMMDD, times HH:MM:SS)
    ├─ Path traversal check via _resolve_type_dir()
    ├─ Builds SQL: SELECT col_select FROM read_parquet(glob, hive_partitioning=true)
    │              WHERE date=... AND ticker=... AND "Execution Time" >= ...
    │              ORDER BY "Data Date", "Execution Time" LIMIT 10M
    ├─ DuckDB in-memory connection → con.execute(sql).pl() → polars DataFrame
    └─ Connection closed in finally block

query_sql(data_dir, sql) — PRIVILEGED ESCAPE HATCH
    ├─ Same path validation
    ├─ Creates DuckDB VIEW "ticks" backed by glob *.parquet
    └─ Passes user SQL through with NO sanitization (documented warning)
```

### 6.2 `event_window.py` — Event Study Windows

Two modes of operation:

**A. Event list from Parquet stores:**
```
extract_event_window(data_dir, ticker, event_date, event_time, before, after)
    ├─ Calls query_ticks() for the target ticker+date+time range
    └─ Adds "seconds_from_event" column relative to event_time
```

**B. Event list from raw ZIPs (filter-based):**
```
ingest_event_windows_period(input_root, output_dir, period, filter_csv, window_minutes)
    ├─ Reads event filter CSV (ticker, event_date, reaction_anchor_dt, zip_date)
    ├─ For each date with events:
    │   ├─ Discovers ZIPs by glob pattern
    │   ├─ create_df(zip, ticker_filter=needed_tickers) — pre-filters at line level
    │   ├─ _filter_ticks_for_events(raw_df, events, window_minutes)
    │   │   ├─ Builds tick datetime from "Data Date" + "Execution Time" → JST timezone
    │   │   ├─ For each event: filter by ticker (first 4 chars) + time window (±N mins)
    │   │   └─ Attaches event metadata columns (event_ticker, event_type, session_type, reaction_anchor)
    │   └─ write_event_window_parquet() → {output}/year=YYYY/month=MM/YYYYMMDD.parquet
    └─ Corrupt ZIPs logged to corrupt_zips.txt
```

### 6.3 `features.py` — Order-Book Feature Engineering

All functions operate on a single tick DataFrame (one ticker, one day):

| Function | Output | Method |
|----------|--------|--------|
| `compute_spread()` | Series | `Sell Quote 1 Best - Buy Quote 1 Best` (NULL if either is 0/missing) |
| `compute_depth(levels, side)` | DataFrame | Extracts Sell/Buy Quote Vol columns up to N levels |
| `compute_flow_imbalance(window)` | Series | OFI = (buy_vol - sell_vol) / (buy_vol + sell_vol) over rolling window |
| `compute_volatility(window, method)` | Series | "realized": sqrt(sum(log_returns²)); "garman_klass": sqrt(0.5*ln(H/L)² - (2ln2-1)*ln(C/O)²) |
| `compute_all_features(levels, windows)` | DataFrame | Wraps all four above into one augmented DataFrame |

---

## 7. Parquet I/O (`io/parquet.py`)

### Output Layouts

**General ingest:**
```
{output_root}/{data_type}/date=YYYYMMDD/ticker=NNNN.parquet    # Hive-partitioned
```

**Event windows:**
```
{output_root}/year=YYYY/month=MM/YYYYMMDD.parquet
```

### Key Functions

| Function | Purpose |
|----------|---------|
| `write_partitioned_parquet(df, output_dir, data_type)` | Groups by date→ticker, writes partitioned snappy parquet |
| `read_parquet_partition(data_dir, data_type, date, ticker, columns)` | PyArrow dataset filter read |
| `write_event_window_parquet(df, output_dir)` | Event-window format; appends to existing files |
| `read_partitioned_parquet(data_dir, year, month)` | PyArrow dataset read with year/month filters |
| `_coerce_time_cols(df)` | Converts datetime.time objects to HHMMSS strings (Parquet compat) |

---

## 8. Configuration (`pyproject.toml`)

| Tool | Config |
|------|--------|
| **Build** | setuptools>=77 + wheel; static `version = "0.4.0"`; `license-files = ["LICENSE"]` (PEP 639); `packages.find` include=`tse_tick*` |
| **CLI** | `tse-tick = "tse_tick.cli:main"` |
| **Extras** | `query` (duckdb), `test` (pandas/pytest/pytest-cov), `dev` (test + black/flake8/mypy/jupyter), `docs` |
| **Black** | line-length=100, target Python 3.9–3.12 |
| **Pytest** | `--cov=tse_tick`, testpaths=["tests"] |
| **Coverage** | source=["tse_tick"], omit tests + pycache |
| **Mypy** | Python 3.10, warn_return_any, strict_equality, `tests.*` ignored |
| **Flake8** | max-line-length=100, ignore E203/E266/E501/W503 |

---

## 9. Benchmarks (`benchmarks/`)

Reproducible benchmarks back the performance numbers in the README and the
technical paper. Run everything with `python benchmarks/run_all.py` (a thin
orchestrator); the canonical scripts are:

| Script | Produces | Measures |
|--------|----------|----------|
| `run_engine.py` | `results_engine.csv`, `results_engine_summary.csv` | parse+clean: Polars vs pandas (Python-engine prototype + fair C-engine baseline), all 4 types, 16T / 1T |
| `run_format.py` | `results_format.csv` | storage formats — CSV / CSV.gz / Parquet (Snappy, Zstd) / Feather / Pickle: size, read, write, selective read |
| `run_query_fix.py` | `results_query.csv` | single-ticker hour slice: DuckDB + Hive-Parquet vs pandas CSV scan |
| `run_correctness.py` | (gate) | asserts Polars output is byte-identical to the pandas-fair pipeline for all 4 types |

`worker_engine.py` runs each engine condition in an isolated subprocess (7 reps,
1 warm-up, median); `generate_assets.py` turns the result CSVs into the paper's
tables and `benchmark_figure.pdf`. See `benchmarks/ENVIRONMENT.md` for the
reference machine and package versions.

---

## 10. Test Status

**227 tests.** Without proprietary data (the CI profile): **179 pass / 48 skip**.
With a complete local NEEDS store (`TSE_TICK_DATA_ROOT`): **all 227 pass**.

| Area | Coverage |
|------|----------|
| Stage-1 (ingest) | `test_ingest` (16), `test_parquet` (12), `test_parquet_io` (14) — synthetic + real-ZIP cases |
| Stage-2 (query / features / event-window from Parquet) | `test_query` (15), `test_features` (20), `test_event_window` (22) — run against a **synthetic Hive-Parquet store** built by the real ingest pipeline (`conftest.py` + `synthetic_data.py`), so they need no proprietary data |
| CLI | `test_cli` (13) — end-to-end on synthetic data |
| Additive API | `test_api_additions` (13), `test_read_ticks` (16), `test_translate_data` (7) — `translate` / enums / `query_ticks` str-int ticker, the one-shot `read_ticks`, and the file-driven translation tables + `TSE_TICK_TRANSLATIONS` override, all on synthetic data |
| Clean-room fixes | `test_quiet_and_unicode` (3), `test_discovery` (3), `test_dtypes` (1), `test_empty_schema` (3) — logging/no-crash on CJK paths, nested-layout discovery, Float64 dtypes, empty-but-typed frames |
| Paper examples | `test_paper_examples` (5) — locks the technical paper's API listings |
| Real data | `test_real_data` (64) + real-ZIP cases in `test_ingest` — all 4 types across the 2016 fixed-width and 2017+ CSV eras; **gated on local NEEDS files** (these are the 48 no-data skips) |

`test_core.py` and `test_schemas.py` are 1-line stubs — cleaning and schema
correctness are exercised by `test_real_data.py` and the synthetic-fixture
tests. The earlier "Stage 2 has zero coverage" gap is **resolved**.

---

## 11. Security Architecture

| Protection | Location | Mechanism |
|------------|----------|-----------|
| ZIP bomb | `enhanced.py` | Max 5 GB decompressed, 5 entries, 100:1 ratio |
| Path traversal | `query.py` | Resolved path prefix validation in `_resolve_type_dir()` |
| Parallel cap | `ingest.py` | `_MAX_WORKERS = 8`, enforced in `ingest_directory()` |
| Query overflow | `query.py` | Default `LIMIT 10_000_000` on `query_ticks()` |
| SQL injection | `query.py` | Column identifiers screened by a character **blocklist** (rejects `"` `\` `;` backticks, CR/LF/TAB, NUL — but allows spaces in NEEDS column names); dates `^\d{8}$`, times `^\d{2}:\d{2}:\d{2}$`; `ticker` normalized to an alphanumeric token |
| Traceback leak | `ingest.py` | `traceback.print_exc()` → `logger.error(exc_info=True)` |
| Privileged SQL | `query.py` | `query_sql()` documented with WARNING docstring; read-only by DuckDB in-memory design |

---

## 12. Constraints (Design Invariants)

| ID | Rule | Enforced in |
|----|------|-------------|
| C1 | ZIPs read in-memory, never extracted to disk | `enhanced.py` (io.BytesIO) |
| C2 | One raw DataFrame in memory at a time | `ingest.py` loops + `del` + `gc.collect()` |
| C4 | Partitioned Parquet output, never single monolithic file | `io/parquet.py` |
| C5 | Corrupt ZIPs logged and skipped (not fatal) | `ingest.py`, `scripts/ingest_event_windows.py` |
| C6 | JST timezone on all timestamp comparisons | `event_window.py` |
| C8 | ZIP bomb guard | `enhanced.py` (checked before decompression) |
| C9 | Max parallel workers: 8 | `ingest.py` |
| C10 | Query row limit: 10M | `query.py` |

---

## 13. Shipped in 0.3.0

The public API names are **stable** — no renames (an earlier rename-to-yfinance/Polygon/ccxt proposal
was reversed). These additions shipped in 0.3.0 (live on PyPI) and are purely additive:

| Item | What |
|------|------|
| `read_ticks` (one-shot) | Raw ZIPs → ticker/time-filtered DataFrame with **no** Parquet store — composes `discover_zips` + `create_df`'s byte-level ticker fast-path + the `event_window` time filter (factored into a shared `_tick_datetime` helper). For quick, targeted exploration; the two-stage store path remains the tool for wide/repeated work. |
| `translate` module | Static, dependency-free mapping from yfinance / Polygon / ccxt names → `tse_tick` names, plus a `translate(source, name)` lookup. Lets users of those libraries find our equivalents **without** coupling us to their APIs. |
| `DataType` / `Language` enums | `tse_tick/constants.py` — `str`-subclassing enums for the four data types and the two languages (autocomplete; no magic strings). |

See [`CHANGELOG.md`](CHANGELOG.md) for the full list.
