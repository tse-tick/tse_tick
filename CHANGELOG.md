# Changelog — tse_tick

## [Unreleased]

### Fixed
- **indices_summary output was missing the Index Code column** (16 columns instead of the documented 17): the 83-column raw layout names column 5 "Stock Code" while the final column selection expects "Index Code", so the identifier was silently dropped — leaving index-summary rows unidentifiable. `set_columns()` now renames the field for `indices_summary`, which also routes it through the Index Code decoder (e.g. `101` → "Nikkei 225") instead of the stock-suffix decoder. Found by the new 2017 real-data smoke test.
- `pyproject.toml` version aligned to 0.2.3 (was 0.2.2), so build artifacts match `__version__` and this changelog.
- Example notebook (`examples/notebooks/01_basic_usage.ipynb`) stripped of all saved outputs, which contained non-redistributable NEEDS records, and of personal local paths; stale branding and dead documentation links corrected.
- Real-data test paths repointed from stale machine-specific locations to a `TSE_TICK_DATA_ROOT` environment variable (default `G:\flash_crash`) with per-class skip gates, so partially available data still gets tested; `detect_data_type_and_year` tests no longer require data files at all.
- README era table corrected: the 2016 index summary format is fixed-width (hybrid `+`-delimited), not CSV.
- `get_info()` year range updated to 2016-2025.

### Added
- `tests/test_cli.py`: CLI coverage (argument parsing, validation errors, and end-to-end synthetic-data ingestion), previously 0% — now 82%. Package coverage 61% → 76%.
- Real-data smoke tests for the 2017 stock-summary and index files (`raw_other/`). Suite grows 165 → 181 tests: without proprietary data 133 pass / 48 skip; with `TSE_TICK_DATA_ROOT` pointing at a local NEEDS store, 155 pass / 26 skip.
- GitHub Actions test workflow (`.github/workflows/tests.yml`).
- Benchmarks suite tracked in-repo (scripts, environment documentation, aggregate results CSVs).

### Changed
- Author order standardized to Peter Romero, Kazumi Li, Masataka Hayashi across CITATION.cff, LICENSE, pyproject.toml, `__init__.py`, and README.

## [0.2.3] - 2026-05-29

### Added
- **Synthetic Stage-2 test fixture** (`tests/synthetic_data.py`, `tests/conftest.py`): a session-scoped pytest fixture builds a tiny Hive-partitioned Parquet store at test time by running synthetic, obviously-fake NEEDS-format ZIPs (correct 95-field TICST120 positional layout, three tickers across two trading dates with a real lunch gap) through the **real** ingest pipeline (`ingest_single_zip`). No proprietary NEEDS data is used. This unblocks the previously-skipped Stage-2 tests (query, features, event-window-from-Parquet, Parquet I/O) so they execute in CI: passing tests went from **42 to 104**, skips from **118 to 56** (remaining skips need real NEEDS files or are out of fixture scope).

### Fixed
- **`query_ticks` ticker filter broken against the real store layout** (`query.py`): the ticker is encoded in the Parquet *filename* (`ticker=NNNN.parquet`), which DuckDB Hive partitioning does not expose as a column, so `ticker=`/`extract_event_window` queries raised `BinderException`. Now prunes by selecting the matching per-ticker files directly (robust to the in-file code column being categorically decoded).
- **`query_ticks` time-range filter returned wrong rows** (`query.py`): `Execution Time` is stored as 6-digit `"HHMMSS"`, but the filter compared against `"HH:MM:SS"`, so lexicographic comparison silently mismatched (e.g. `14:00–15:00` returned nothing). Colons are now stripped from the validated `start_time`/`end_time` before comparison.
- **`query_ticks` column pruning rejected all real columns** (`query.py`): the SQL-injection identifier guard's word-only regex rejected the spaces present in every TICST120 column name (`"Execution Time"`, …). Replaced with a blocklist that still rejects the double-quote breakout character, backslash, semicolons, backticks and control characters, while allowing spaces inside the double-quoted identifiers.
- **`read_parquet_partition` date/ticker filters raised** (`io/parquet.py`): the Hive `date` column is inferred as an integer, so comparing it to a `"YYYYMMDD"` string raised an Arrow kernel error; and the filename-encoded ticker was queried as a (non-existent) partition field. The date field is now cast to string for comparison and the ticker is matched on the in-file code column.
- **Rolling features broke with the documented default window** (`features.py`): `compute_flow_imbalance` / `compute_volatility` / `compute_all_features` passed `window="5min"` straight to Polars `rolling`, whose duration grammar only accepts `m` for minutes, raising `InvalidOperationError`. A small normalizer now maps `"5min"` → `"5m"` while still accepting native Polars units.
- **Volume Flag decode unreachable** (`core.py`): the categorical-decode loop's `if "Vol" in col: continue` skipped the `Volume Flag` column before it could reach its `elif col == "Volume Flag":` branch, leaving raw `"0"` / `"128"` codes in the output. Added an exception for `Volume Flag`, and removed index 15 from the `individual_stock` `int_list` so the column stays as `String` for the decode. Output now reads `"Final"` / `"Estimated"`.
- **TICIS110 column 5 mislabel** (`schemas.py`, `enhanced.py`, `io/parquet.py`): column 5 of the indices-summary schema stores an index identifier but was labeled `Stock Code`. Renamed to `Index Code` everywhere (schema, output mapping, default Parquet partition key, paper schema table). Aligns with `TICIT110.Index Code` and removes the cross-schema inconsistency.
- **Field-count documentation drift** (`README.md`, `tse_tick/__init__.py:get_info`): README "Features" line said TICSS110 had `83 cols`; `get_info()` said TICIT110 had `23 fields`. Both were raw-CSV counts. Standardized all surfaces on **output** counts with raw counts in parentheses where they differ: TICSS110 = 82 (83 raw), TICIT110 = 10 (23 raw, 15 in 2016).

### Changed
- **Technical paper** (`technical_paper/main.tex`): Section 5.3 "Categorical decoding" list now includes Volume Flag with its decoded labels; Appendix Table 12 row 16 type updated `int → string`; TICIS110 schema table + surrounding prose use `Index Code` / `指数コード`; `%TODO: KEVIN` removed (stock-summary numeric casting scoped honestly as a documented limitation).
- **Benchmark asset** (`benchmarks/paper_assets/engine_benchmark.tex`): Index Summary row corrected from `(83 cols)` → `(17 cols)` (copy-paste bug); 7-column table tightened (`\footnotesize`, `\tabcolsep=4pt`, shortened backend labels) to eliminate a 139 pt overfull `\hbox` that pushed text past the right margin.

## [0.2.2] - 2026-05-18

### Added
- `--tickers` flag: filter at read time by stock code (comma list or `@file.txt`)
- `--filter-csv` flag: extract ±N minute windows around corporate events from an external CSV
- `--window` flag: configurable window size in minutes (default 120, used with `--filter-csv`)
- Event-window mode handles after-hours reaction-anchor shifting via `zip_date` column
- Event-window output tags ticks with `event_ticker`, `event_type`, `session_type`, `reaction_anchor`
- `CITATION.cff` for academic citation (BibTeX-compatible)
- `ARCHITECTURE.md` — package architecture reference (renamed from `structure_guide.md`)

### Fixed
- **Multi-era format audit**: verified all 4 data types across 2016/2017-2019/2020-2025 eras against 9 PDF manuals
- **`parse_line()` byte offset bug**: fixed off-by-1 for `price` and `volume` fields in 2016 TICIT010 fixed-width parser (`core.py:42,45`). Was reading 1 byte too late, silently truncating the most significant digit for large values.
- **Internal column leak**: `_tick_dt` and `_stock_4` internal filter columns are now dropped before writing Parquet output (`ingest.py:425-428`)
- **`corrupt_zips.txt` relocated** to `_ingest_logs/` subdirectory to prevent PyArrow from trying to read it as a Parquet file
- `get_supported_years()` now returns `(2016, datetime.now().year)` dynamically instead of hardcoded `(2016, 2024)`

### Changed
- **README rewritten** — publication-quality with full CLI reference, Python API docs, data type table, multi-era format support, security table, and contributing guide
- Project renamed from `NEEDS_tick` to `tse_tick` (author list reordered, email removed from `pyproject.toml`)
- `scripts/ingest_event_windows.py` deprecated with runtime `DeprecationWarning` — use `tse-tick ingest --filter-csv` instead
- Author section unified across `__init__.py`, `pyproject.toml`, `README.md`

### Removed
- Docker files (`Dockerfile`, `docker-compose.yml`) — not needed for a pip-installable Python package
- `setuptools_scm` from build dependencies (version is hardcoded)

## [0.2.1] - 2026-05-05
### Security
- ZIP bomb protection: max 5 GB decompressed, max 5 entries, 100:1 compression ratio cap (`enhanced.py`)
- Path traversal prevention: `_resolve_type_dir()` validates resolved paths (`query.py`)
- Parallel worker cap: max 8 processes (`ingest.py`)
- Query row limit: 10M default LIMIT on `query_ticks()` (`query.py`)
- Traceback leakage fix: `traceback.print_exc()` replaced with `logger.error(exc_info=True)` (`ingest.py`)
- `query_sql()` documented as privileged API with warning docstring (`query.py`)

### Removed
- `debug_regex.py` — one-off benchmarking script with hardcoded Windows paths
- `validate.py`, `validate_final.py` — one-off validation scripts (pandas-based, hardcoded paths)
- `tse_tick/enhanced_backup.py` — pre-migration pandas duplicate
- `tests/test_enhanced.py` — empty test stub
- PDF manuals (`TICST1@@.pdf`, `TICIT110.pdf`, `TIC@S@10.pdf`) moved to `descriptions/`
- `manual_text.txt` moved to `descriptions/`

### Fixed
- Hardcoded Windows paths (`F:/`) replaced with generic examples in `scripts/ingest_event_windows.py` docstring
- `schema_overrides` key format fixed: `column_1`-style keys for polars `read_csv(has_header=False)`

### Docs
- `GUIDE.md` rewritten for v0.2.0 polars architecture with dataflow diagrams, security constraints, and CLI reference
- Test count updated: 33 passed, 66 skipped (verified against real TICST120 4.5M-row ZIP)

## [0.2.0] - 2026-05-05
### Changed
- **Migrated from pandas to polars** for all data processing (20-50x speedup for CSV I/O)
- Time columns now stored as strings (HHMMSS format) internally for Parquet compatibility
- DuckDB query interface switched from `.df()` to `.pl()` for native polars returns
- Column type casting uses `pl.Int64`/`pl.Float64` instead of numpy dtypes
- Stripped trailing spaces via vectorized `str.strip_chars()` instead of `map()`
- Categorical decoding now batches replacements via `pl.col().replace()` dicts

### Added
- **CLI entry point**: `tse-tick ingest` with `--data-type`, `--years`, `--input-root`, `--output-root`
- **Recursive ZIP discovery**: `discover_zips()` auto-traverses `{year}/{yearmonth}/` structure
- **Resume support**: `--no-resume` flag; skips dates with existing parquet output
- **Manual mode**: `create_df(auto_detect=False, data_type=..., year=...)` for explicit control
- **`ingest_year_from_root()`**: Ingests a full year from the NEEDS folder hierarchy

### Fixed
- SQL injection vulnerability in `query_ticks()` — added input validation for identifiers, dates, and time strings
- Categorical decode bug: columns no longer cast to int before string replacement
- Parquet write: `str.replace("-", "")` replaced with `str.replace_all()` to handle full date strings
- `_filter_ticks_for_events()` rewritten in polars with proper datetime/time parsing
- Test fixtures updated to use polars DataFrames (23 passing, 29 skipped for NEEDS data)

### Removed
- pandas and numpy from core dependencies (moved to dev)
- `enhanced_backup.py` (duplicate of enhanced.py)
- `pd.NaT` / `datetime.time` interop complexity

## [0.1.0] - 2024
### Added
- Core data processing for Nikkei NEEDS tick data
- Bilingual column support (English/Japanese)
- Support for 4 data types (TICST120, TICSS110, TICIT110, TICIS110)
- Automatic data type and year detection
- ZIP file streaming
- Data cleaning and validation pipeline
