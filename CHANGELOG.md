# Changelog — tse_tick

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
