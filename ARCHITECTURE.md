# tse_tick — LLM Reference Guide

## 1. Project Identity

| Property | Value |
|----------|-------|
| Package | `tse_tick` |
| Version | 0.11.5 (Beta) — on PyPI (`pip install tse-tick`) |
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
├── CHANGELOG.md                     # version history (current: 0.11.5)
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
│   ├── enhanced.py                  # Core ETL: create_df(), read_ticks(), discover_zips(), parse_period(), _prune_parts_by_ticker()
│   ├── partscan.py                  # Part-pruning: probe part start codes, select the ticker's contiguous run ∪ last part (0.11.6)
│   ├── core.py                      # Data cleaning + 2016 fixed-width parser + categorical schemas + _tick_datetime()
│   ├── ingest.py                    # Batch ZIP→Parquet: ingest_period(), extract_to_store(), ingest_year_from_root(), _process_zips()
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
├── tests/                           # Test suite (pytest, 336 tests; 288 pass / 48 skip w/o data, 336/0 with a NEEDS store)
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

### v0.5.0 — complete multi-part ingest + CLI export (2026-06-18)

| Change | Detail |
|--------|--------|
| Multi-part-day ingest | `ingest_*` group all ZIP parts of a date, concat, and write each ticker once (atomic per-date resume) — fixes the silent loss of all-but-the-first part (e.g. Toyota 7203 absent) |
| `tse-tick export` | New CLI verb: raw ZIPs → CSV/Parquet for a ticker/time slice via `read_ticks`, no store needed |
| Auto-location | `--input-root` / `read_ticks` / `export` accept any folder containing the data (located by type + date), at any nesting |
| Smaller fixes | `--parallel` flagged `--flat`-only; CLI progress → stdout; README query example notes the `[query]` extra |
| Tests | Suite **237** (`+10` regression tests: `test_ingest_multipart`, `test_locate`, export CLI) |

### v0.6.0 — empty-result + Windows-console robustness (2026-06-18)

| Change | Detail |
|--------|--------|
| Missing-date reads | `read_ticks` on a date with no ZIPs (e.g. a holiday) now warns and returns a **typed empty** frame (full schema), not a silent `(0, 0)` — identical schema to the no-match path. `create_df`'s finalize tail is factored into `_finalize_raw`, shared with the new `_empty_typed_frame` so the two empty schemas can't drift |
| Windows `print(df)` | Import enables Polars ASCII tables on Windows (cp1252 can't encode Polars' Unicode borders); env opt-out `TSE_TICK_ASCII_TABLES=0`. New cross-platform `tse_tick.display(df)` writes UTF-8 to the stream buffer |
| Discovery fast path | `discover_zips` adds a `{yearmonth}/`-directly-under-root fast path (e.g. `…/TICST120`) before the recursive fallback; docstring corrected to match |
| Docs | A single numbered ZIP holds only part of a day (filtering a lone part → 0 rows); pass the directory/root |
| Tests | Suite **243** (`+6`: no-ZIPs empty+warn, `display`/Windows print, discovery fast-path) |

### v0.11.6 — part-pruning for single-ticker reads + one-call two-stage (2026-07-07)

| Change | Detail |
|--------|--------|
| Part-pruning | `read_ticks` (individual_stock + ticker_filter) opens only the ticker's contiguous **run** of numbered parts **∪ {last part}** (the day's trailing off-auction appendix), via `partscan.py` (probe each part's first stock code → backward run-scan). Falls back to a full scan if the ascending-code layout isn't monotonic, so results are **row-for-row identical** (validated 18/18 days, 3-year Toyota 7203). `prune_parts=True` default. Ticker-filtered `ingest`/`_ingest_grouped` and `tse-tick export` use the same path |
| `extract_to_store()` | Two-stage in one call: ingest a ticker for a period into a reusable, part-pruned store → return the queried DataFrame. `tse-tick export --store <dir>` exposes it on the CLI. Requires `[query]` |
| DRY | The field-5 stock-code parse is now the single shared `partscan.extract_stock_code` (used by the raw-byte fast path and the probes) |
| Tests | Suite **373** (`+18`: `test_partscan.py` 10, `test_part_pruning.py` 5, `test_extract_to_store.py` 2, `test_cli.py` +1) |

### v0.11.5 — alpha-test fixes: one-shot OOM guard, query truncation warning, explicit year (2026-06-28)

| Change | Detail |
|--------|--------|
| One-shot OOM | `create_df`/`read_ticks` raise a catchable `OneShotMemoryError` (a `MemoryError`) — proactively when the cumulative decompressed size of the parts crosses `max_oneshot_bytes` (default 5 GB; `None` disables), or by converting a Polars `PanicException` (a `BaseException`) during the load. Bounded `ticker_filter` fast path exempt; `ingest_*` re-raise it (no silent partial-day writes) |
| Explicit year/type | `create_df` auto-detects only the `None` of `year`/`data_type`, so an explicit `year=` is honored under the default `auto_detect=True` (a year-less folder path no longer raises) |
| Truncation warning | `query_ticks` emits a capturable `TruncationWarning` on truncation, probing `limit+1` so an exact-fit result doesn't false-warn |
| Tests | Suite **355** (`+19`: `test_alpha_fixes.py`) |

### v0.11.4 — internal consolidation: single-sourced data-type classification (2026-06-19)

| Change | Detail |
|--------|--------|
| SSOT classification | the valid / summary / tick / index data-type checks (duplicated as literals in ~20 places across 8 modules) now derive from one source in `constants.py` (`DATA_TYPES`/`VALID_DATA_TYPES`, `SUMMARY_TYPES`, `TICK_TYPES`, `INDEX_TYPES`, `validate_data_type()`), tied to the `DataType` enum. No API/behavior change; validation messages byte-identical |
| Drift guard | `test_consolidation.py` invariants — groupings partition the four types; modules share one validator; partition-cols keys == valid set; `get_info()` field counts derivable from the schemas |
| Notebook | `02_evaluation.ipynb` reloads a freshly-installed release without a kernel restart |
| Tests | Suite **336** (`+5`: `test_consolidation`) |

### v0.11.3 — flat-folder day→month discovery; get_info/get_supported_years consistency; eval notebook (2026-06-19)

| Change | Detail |
|--------|--------|
| Flat-folder discovery | `read_ticks` on a flat folder of a monthly type + a single-day date now resolves via `discover_zips` (the same day→month logic as the structured root) instead of a filename-substring match that missed monthly files → false-empty. Removes the divergent `_date_prefixes` path |
| Consistency | `get_info()` returns without printing (no double-print); `get_supported_years()` + the banner share one `_SUPPORTED_YEARS=(2016,2025)` constant; `get_supported_years`/`get_version` documented |
| Added | `examples/notebooks/02_evaluation.ipynb` — standalone README-conformant acceptance test (all 4 types × both eras, pass/fail verdict) |
| Tests | Suite **331** (`+8`: `test_run12_fixes`) |

### v0.11.2 — indices columns= projection day-prune fix + API/message papercuts (2026-06-19)

| Change | Detail |
|--------|--------|
| Indices projection | `read_ticks` applies a `columns=` projection **after** all filtering (code, time, monthly day-prune) instead of per-part before it — a subset that dropped `Data Date` was skipping the day-prune and returning the whole month (~20× rows). Latent for all monthly types; `indices` surfaced it |
| API surface | `tse_tick.ingest` (submodule) hidden from `dir()` via `__dir__` (still importable, now documented); `get_info(path)` raises a guiding `ValueError` not a raw `TypeError` |
| Messages/metadata | empty `ticker_filter=set()` named in the no-data warning; `__author__` + package metadata list all three authors consistently |
| Tests | Suite **323** (`+12`: `test_run11_fixes`) |

### v0.11.1 — ticker_filter footgun guard (2026-06-19)

| Change | Detail |
|--------|--------|
| Bare ticker_filter | `_normalize_ticker_filter` treats a bare `str`/`int` code as a one-element filter (was: a `str` iterated into characters → silent empty (F1); an `int` → `TypeError` (F2)). Applied in `read_ticks` + `create_df`; a set/list/iterable is unchanged |
| Tests | Suite **311** (`+7`: `test_run10_fixes`) |

### v0.11.0 — event-window fix (blank Execution Time), data_type param, full docstrings, clearer messages (2026-06-19)

| Change | Detail |
|--------|--------|
| Event-window crash | `extract_event_window` parsed `Execution Time` from every row; quote-only rows have a blank one (kept by `query_ticks` via its `Update Time` fallback) → `ValueError "… ::"`. Now uses the same `Execution Time`→`Update Time` effective-time fallback (vectorized), so every in-window row is timed |
| Event-window data_type | `extract_event_window` / `extract_batch_event_windows` gained a `data_type` param (tick types only; `*_summary` rejected) — index event windows now work; was hardcoded to `individual_stock` |
| Docstrings | Every exported callable now has a docstring (event-window family, `ingest_year`/`ingest_year_from_root`/`ingest_event_windows_period`, `write_partitioned_parquet`/`write_event_window_parquet`, `get_supported_data_types`) |
| Messages | `parse_period` errors list the complete accepted set (one shared help string); `NoDataWarning` flags a `data_type`/folder mismatch (no-ZIPs) and an inverted `start`/`end` window (empty result) |
| Tests | Suite **304** (`+9`: `test_run8_fixes`) |

### v0.10.0 — compact summary stores; capturable rows-cap warning; summary time normalization (2026-06-19)

| Change | Detail |
|--------|--------|
| Summary store layout | `stock_summary`/`indices_summary` partition by **date only** (one file/date, code kept as a column) instead of one file per (date × ticker) — was ~87k 1-row files / 2.4 GB / ~3 min per month (160× amplification). `query_ticks`/`get_available_tickers` prune/read the code column for these types; tick types unchanged. **Re-ingest summary stores** |
| rows-cap warning | `read_ticks` `rows` cap emits a capturable `TruncationWarning` (a `UserWarning`) via `warnings`, not `logging` — the same channel as `NoDataWarning` |
| Store-helper errors | `get_available_dates`/`get_available_tickers`/`query_ticks` point a raw-NEEDS-path caller to `ingest_*` (or raw discovery via the code column) |
| Summary time width | `*_summary` `*Time` columns normalized to fixed-width 6-char `HHMMSS` across eras (2016 `HHMM` / 2017+ `HHMMSSffffff`) |
| Tests | Suite **295** (`+13`: `test_run7_fixes`, incl. a 2016 legacy-`…010` discovery regression) |

### v0.9.0 — real-data defect fixes: numeric stock_summary, time filter keeps quote rows (2026-06-18)

| Change | Detail |
|--------|--------|
| stock_summary dtypes | `clean_data` casts all stock_summary measures (OHLC, VWAP, volumes, amounts, counts) to `Float64` (were `String`, so `.mean()` returned null); id/code + `HHMMSS` time columns stay string. **Re-ingest stock_summary stores** |
| Time-filter fallback | `individual_stock` quote-only rows (blank `Execution Time`, real `Update Time`) are retained in a time window via an `Update Time` fallback in `read_ticks` (`_filter_time_window`) and `query_ticks` (SQL `WHERE`/`ORDER BY`), individual_stock-only; the filter was dropping ~94% of a liquid day |
| Docs | `read_ticks` notes typical one-shot timing (opens every ZIP part; prefer `ingest_*` + `query_ticks`) |
| Tests | Suite **282** (`+5`: `test_run6_fixes`; + `stock_summary_csv` / quote-row synthetic generators) |

### v0.8.0 — cross-type consistency & DX polish: capturable no-data warning, fixed-width index time, string ticker codes, UTF-8 Windows stdout (2026-06-18)

| Change | Detail |
|--------|--------|
| No-data signaling | `read_ticks` emits a capturable `NoDataWarning` (a `UserWarning`) for every zero-row result, uniformly across all four types (was: only `individual_stock` no-ZIPs, via logging) |
| Index time width | index-tick `Execution Time` normalized to a fixed-width 6-char `HHMMSS` across eras (`_exec_time_6char`; 2016 `HHMM` → `HHMMSS`) |
| Ticker discovery | `get_available_tickers` returns `list[str]` (round-trips into `ticker_filter`; preserves alphanumeric codes like `"130A"`, was silently dropped by `int()`) |
| Windows print | import reconfigures `stdout`/`stderr` to UTF-8 (guarded, opt-out `TSE_TICK_ASCII_TABLES=0`) so `print(df)` survives content glyphs `μ`/`≤`/`—` |
| Docs | `parse_period`/`ingest_period` single forms, `ingest_directory` docstring, `int` ticker tolerance, documented the extra `date` column `query_ticks` returns |
| Tests | Suite **277** (`+20`: `test_run5_fixes` across F1–F7, incl. a 2016 fixed-width index fixture) |

### v0.7.0 — all-four-types correctness: summary store, jp/ingest filters, 2016 index era, raw Index Code (2026-06-18)

| Change | Detail |
|--------|--------|
| Summary store path | `query_ticks` orders by `Execution Time` only for tick types, so it no longer crashes for `stock_summary` / `indices_summary` |
| ticker_filter coverage | applied on ingest for the non-stock types, and resolved en-or-jp in `read_ticks` (`_resolve_col`) so it is honored under `language="jp"`; time filtering is jp-aware too |
| 2016 index era | `clean_data` guards the 15-field schema (no more `Update Time` crash on the empty frame); `discover_zips` also searches the legacy `…010` record code; `_tick_datetime_expr` parses 4-digit `HHMM` times |
| Monthly day-pruning | `read_ticks` prunes monthly results to the requested day(s); `parse_period` accepts a bare `YYYYMM` / `YYYYMMDD` |
| Index Code domain | raw numeric code for **both** index types (was: `indices` decoded to names) — matches `ticker_filter` + partition key, language-independent, joinable |
| API hygiene | `get_info()` returns its banner string; `os`/`sys` no longer leak; `HTICIS*` auto-detects as `indices_summary` |
| Tests | Suite **257** (`+14`: `test_run4_fixes` across F1–F12 + auto-detect) |

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
| **Build** | setuptools>=77 + wheel; static `version = "0.11.5"`; `license-files = ["LICENSE"]` (PEP 639); `packages.find` include=`tse_tick*` |
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

**336 tests.** Without proprietary data (the CI profile): **288 pass / 48 skip**.
With a complete local NEEDS store (`TSE_TICK_DATA_ROOT`): **all 336 pass**.

| Area | Coverage |
|------|----------|
| Stage-1 (ingest) | `test_ingest` (16), `test_parquet` (12), `test_parquet_io` (14) — synthetic + real-ZIP cases |
| Stage-2 (query / features / event-window from Parquet) | `test_query` (15), `test_features` (20), `test_event_window` (22) — run against a **synthetic Hive-Parquet store** built by the real ingest pipeline (`conftest.py` + `synthetic_data.py`), so they need no proprietary data |
| CLI | `test_cli` (14) — end-to-end on synthetic data, incl. the `export` verb |
| Additive API | `test_api_additions` (13), `test_read_ticks` (16), `test_translate_data` (7) — `translate` / enums / `query_ticks` str-int ticker, the one-shot `read_ticks`, and the file-driven translation tables + `TSE_TICK_TRANSLATIONS` override, all on synthetic data |
| Clean-room fixes | `test_quiet_and_unicode` (3), `test_discovery` (3), `test_dtypes` (1), `test_empty_schema` (3) — logging/no-crash on CJK paths, nested-layout discovery, Float64 dtypes, empty-but-typed frames |
| Multi-part ingest + locate | `test_ingest_multipart` (2), `test_locate` (7) — every ZIP part of a day is collected and written once (BUG 1+2), and data is located at any NEEDS tree level |
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
