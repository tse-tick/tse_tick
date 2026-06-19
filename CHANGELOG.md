# Changelog — tse_tick

## [Unreleased]

## [0.11.0] - 2026-06-19

Fixes from an eighth real-data run: the event-window analytics path crashed on the primary data type,
plus error-message and docstring papercuts. Read / ingest / query paths were already correct.

### Fixed
- **`extract_event_window` no longer crashes on `individual_stock`.** It computed `seconds_from_event` by
  parsing `Execution Time` from every row, but quote-only book updates have a blank `Execution Time` (the
  rows `query_ticks` keeps via its `Update Time` fallback) — so any real window raised
  `ValueError: time data '… ::' does not match format …`, and the batch variant silently returned `None`
  for every event. It now uses the same `Execution Time` → `Update Time` effective-time fallback, so every
  in-window row is timed (and the computation is vectorized in Polars).
- **`extract_event_window` supports `indices`, not just `individual_stock`.** It gained a `data_type`
  parameter (a tick type — `individual_stock` or `indices`; the daily-aggregate `*_summary` types are
  rejected with a clear message) instead of being hardcoded to `individual_stock`.
- **`parse_period` error messages list the complete set of accepted forms** — including the bare single
  `YYYYMM` / `YYYYMMDD` the code accepts and `read_ticks` documents (some messages previously omitted
  them, contradicting the accepted inputs for a natural mistake like `date="2023-05-08"`).

### Changed
- **Docstrings added across the public API** — `extract_event_window`, `extract_batch_event_windows`,
  `ingest_year`, `ingest_year_from_root`, `get_supported_data_types`, `write_partitioned_parquet`,
  `write_event_window_parquet`, and `ingest_event_windows_period` (every exported callable now has one).
  The event-window docs note that `before` / `after` apply only with `event_time` (omit it for the full
  day).
- **`NoDataWarning` hints are less holiday-centric** — the no-ZIPs message now also flags a possible
  `data_type` / folder mismatch, and the empty-result message notes an inverted `start_time` / `end_time`.

## [0.10.0] - 2026-06-19

Fixes from a seventh real-data run: a Major store-build scalability defect for the summary types, plus
warning/discovery/time-format papercuts. Read paths were already correct on all four types (the 2016
legacy `…010` era probe did not reproduce).

### Fixed
- **`stock_summary` / `indices_summary` ingest no longer explodes into tens of thousands of tiny Parquet
  files.** The store partitioned summary types by `(date, code)` — one file per (date × ticker) — but each
  summary (date, code) is ~1 row, so one 15 MB month became a 2.4 GB store of ~87k one-row files (~160×
  size amplification, ~3 min; a multi-year build was impractical). Summary stores now partition by **date
  only** (one file per date, the code kept as a column), ~20 files/month at ~1× size; `query_ticks` and
  `get_available_tickers` prune/read the code column for these types. Tick types
  (`individual_stock`/`indices`) are unchanged. **Re-ingest summary stores** to adopt the compact layout.
- **`read_ticks` `rows` cap now warns through `warnings`, not `logging`.** Hitting the cap emits a
  capturable `tse_tick.TruncationWarning` (a `UserWarning`) — the same channel as `NoDataWarning`, so
  `warnings.catch_warnings()` / `simplefilter("error")` catch it — and the docstring no longer claims the
  cap "silently truncates".
- **`*_summary` intraday `*Time` columns are normalized to a fixed-width 6-char `HHMMSS` across eras**
  (2016 `…010` emitted 4-char `HHMM`, 2017+ `…110` emitted 12-char `HHMMSSffffff`), matching the
  `Data Date` normalization and the index `Execution Time` treatment.

### Changed
- **Store-only discovery helpers give an actionable error on a raw NEEDS path.** `get_available_dates` /
  `get_available_tickers` / `query_ticks` now explain, when no store exists, that they read a *built*
  Parquet store — run `ingest_*` first, or discover codes from raw data via `read_ticks` (no
  `ticker_filter`) and the `Stock Code` / `Index Code` column.

## [0.9.0] - 2026-06-18

Fixes from a sixth real-data run: a silent wrong result (`stock_summary` numbers typed as `String`) and a
silent data loss (time-filtering `individual_stock` dropped ~94% of the day).

### Fixed
- **`stock_summary` measures are numeric again.** Every measure column (OHLC, VWAP, volumes, amounts,
  counts) came back as `String`, so `.mean()` / arithmetic silently produced `null` — contradicting the
  README's `Float64` guarantee that the other three types honor. The `stock_summary` cleaning path now
  casts all measure columns to `Float64` (id/code columns and the `HHMMSS` time columns stay string).
  **Re-ingest `stock_summary` stores** to refresh the column dtypes.
- **Time-filtering `individual_stock` no longer silently drops quote-only rows.** Pure order-book updates
  (no trade) carry a blank `Execution Time` but a real `Update Time`; the time window keyed only on
  `Execution Time`, so a 09:00–15:00 filter kept ~6% of a liquid day (trade-coincident snapshots only)
  and silently discarded ~94% of in-session quote updates. The window now falls back to `Update Time` for
  those rows in both `read_ticks` and `query_ticks` (the `Execution Time` column itself is unchanged in
  the output), so the advertised order-book features see the whole in-window book.

### Changed
- **`read_ticks` docstring** now notes typical one-shot timing — every ZIP part of each requested day is
  opened, so a single ticker-day can take tens of seconds; use `ingest_*` + `query_ticks` for faster
  repeated/narrow work.

## [0.8.0] - 2026-06-18

Polish from a fifth real-data run that exercised all four data types and found **no crashes or wrong
results** — only cross-type consistency and developer-experience gaps.

### Fixed
- **No-data signaling is now consistent and capturable.** `read_ticks` already returned a typed-empty
  frame for every "no data" case, but only `individual_stock`'s no-ZIPs path *said* anything — and via
  `logging`, which `warnings.catch_warnings(record=True)` can't trap. Every zero-row result (no ZIPs, a
  holiday inside a monthly file, an unknown ticker/index code, an over-tight filter) now emits a
  capturable `tse_tick.NoDataWarning` (a `UserWarning`), uniformly across all four types.
- **`Execution Time` is a fixed-width 6-char `HHMMSS` for index ticks across eras.** 2016 index ticks
  stored `HHMM` (`"0900"`) while 2017+ stored `HHMMSS` (`"090005"`), so raw string/number math or
  cross-year comparison on the column was inconsistent. 2016 values are now padded to `HHMMSS`. (The time
  *filter* already handled both widths.)
- **Docstrings match the implementation.** `parse_period` / `ingest_period` now document the single
  `YYYYMM` and `YYYYMMDD` forms the code already accepts; `ingest_directory` gained a docstring;
  `ticker_filter` is documented as accepting `int` codes too; and `read_ticks` / `query_ticks` now note
  that the store path returns one extra `date` partition column (the two access paths' schemas differ by
  exactly that column).

### Changed
- **`get_available_tickers()` returns string codes** (e.g. `["6758", "7203"]`) instead of `int`s, so its
  output feeds straight into `read_ticks(ticker_filter=...)` with no conversion, and modern
  **alphanumeric** TSE codes (e.g. `"130A"`) are preserved rather than silently dropped by an `int()`
  parse. Pure-digit codes still sort numerically. *(Return-type change — warrants a minor version bump.)*
- **On Windows, importing `tse_tick` now also reconfigures `stdout`/`stderr` to UTF-8** (in addition to
  the ASCII table borders from 0.6.0), so a naive `print(df)` no longer raises `UnicodeEncodeError` on the
  non-ASCII *content* a DataFrame carries (the `datetime[μs]` dtype header, `≤` in column names, `—` in
  exchange values). Windows-only, opt out with `TSE_TICK_ASCII_TABLES=0`; `tse_tick.display(df)` remains
  the explicit cross-platform UTF-8 alternative.

## [0.7.0] - 2026-06-18

Fixes from a fourth real-data run that exercised **all four** NEEDS data types (the prior runs were
individual_stock-centric): the store→`query_ticks` path for summaries, ticker/time filtering for the
non-stock types and under `language="jp"`, the 2016 index era, and a unified raw-code `Index Code`
across both index types.

### Fixed
- **`query_ticks` crashed for both summary types** (`stock_summary`, `indices_summary`): a hard-coded
  `ORDER BY "Execution Time"` referenced a column those daily-aggregate schemas don't have. The
  order-by now adds `Execution Time` only for the tick types.
- **`ingest_period(ticker_filter=…)` was silently ignored for `stock_summary` / `indices` /
  `indices_summary`** — the store kept *every* code (large silent disk/time blow-up). Ingest now
  prunes these types by ticker too (the filter previously only drove the `individual_stock` fast path).
- **`read_ticks(ticker_filter=…)` was silently ignored under `language="jp"`** for the
  non-`individual_stock` types (returned the whole month, ~19× too much). Ticker- and time-filters now
  resolve their column in either language.
- **`read_ticks(start_time/end_time, language="jp")` always raised** ("require an 'Execution Time'
  column") because the column had been renamed to Japanese — fixed by the same language-aware lookup.
- **2016 index reads crashed on the normal path** (`ColumnNotFoundError: "Update Time"`): the
  typed-empty-frame builder assumed the 2017+ 23-field schema. `clean_data` now guards columns absent
  from the 2016 15-field schema, and `discover_zips` also searches the legacy `…010` index record code
  (`HTICIT010` / `HTICIS010`), so 2016 index data is reachable via the documented workflow.
- **2016 index time filtering silently returned empty**: 2016 `Execution Time` is `HHMM` (no seconds)
  vs 2017+ `HHMMSS`; the shared timestamp parser now defaults missing seconds to `00`.
- **Monthly types over-returned**: a single-day or day-range `read_ticks` request returned the whole
  month (the ZIP is monthly). Results are now pruned to the requested day(s), consistent with the daily
  `individual_stock` files and with `query_ticks`.
- **`parse_period` (and `ingest_period`) rejected a bare single day/month**: now accept `YYYYMM` and
  `YYYYMMDD`, matching the forms `read_ticks(date=…)` already takes.
- **`create_df(auto_detect=True)` misdetected `indices_summary` files as `stock_summary`**: the filename
  probe matched `HTICIS` (the indices_summary prefix) for stock_summary. `HTICIS*` files now correctly
  auto-detect as `indices_summary`.

### Changed
- **`Index Code` is now the raw numeric code for both index types.** `indices` previously decoded it to
  a display name (e.g. "Nikkei 225") while `indices_summary` already showed the code. The in-file value
  now equals the `ticker_filter` input and the partition filename (`ticker=101`), is language-independent,
  and lets the two index types be joined; `ticker_filter` still accepts a display name. Codes missing
  from the name table (e.g. 108) show as the code itself rather than "Unknown (108)". **Re-ingest index
  stores** to refresh the column.
- **`get_info()` now returns the banner string** (in addition to printing it) and has a docstring.
- **Stdlib modules `os` / `sys` no longer leak** into the public `tse_tick` namespace.

## [0.6.0] - 2026-06-18

Closes the gaps a third real-data run surfaced: missing-date reads now warn and keep their schema,
`print(df)` no longer crashes on a Windows console, and structured-root discovery has a real fast path.

### Added
- **`tse_tick.display(df)`** — print a DataFrame as UTF-8 regardless of console encoding; a
  cross-platform alternative to `print(df)` that never raises `UnicodeEncodeError`.
- On **Windows**, importing `tse_tick` now switches Polars to ASCII table borders so a bare
  `print(df)` works out of the box (legacy cp1252 consoles cannot encode Polars' Unicode box-drawing
  characters). No effect off Windows; opt out with `TSE_TICK_ASCII_TABLES=0`.

### Fixed
- **Silent empty result for a date with no ZIPs** (e.g. an exchange holiday such as Golden Week):
  `read_ticks` returned a schemaless `(0, 0)` frame with no explanation. It now logs a warning
  ("no ZIP files found … verify these are trading days") and returns a **typed empty** frame with the
  full column set — the same schema a no-match read returns, so `df["Exchange Code"]` and `df.schema`
  behave identically however a read comes back empty.

### Changed
- **`discover_zips` gained a `{yearmonth}/`-directly-under-root fast path** alongside the documented
  `{year}/{yearmonth}/` one, so pointing at a `…/TICST120` type folder (the common case) resolves
  without a full recursive tree walk. The recursive fallback still covers deeper nested deliveries, and
  the docstring now describes the layouts actually supported.

### Documentation
- Noted that a **single numbered ZIP holds only part of a day** (NEEDS splits each day across parts by
  ascending code; Toyota 7203 sits in a later part), so filtering a lone part can yield 0 rows — pass
  the day's directory or a structured root for complete coverage (`read_ticks` / `create_df` docstrings
  and README).
- README: the Windows `print(df)` / `tse_tick.display` note and the missing-date warning behaviour.

## [0.5.0] - 2026-06-18

Complete multi-part-day ingest (fixes the silent CLI data loss), a new `tse-tick export` CLI verb,
and robust data auto-location — from a second clean-room run.

### Added
- **`tse-tick export` CLI verb** — read raw ZIPs and write a ticker/time slice straight to CSV or
  Parquet (`--tickers` / `--period` / `--start-time` / `--end-time` / `--output`), no Parquet store
  required. The no-code path to one ticker over a date range.

### Fixed
- **CLI/period ingest dropped all but the first ZIP part of each day** (silent, catastrophic). NEEDS
  splits a trading day across multiple parts by ticker range (plus a closing tail), but resume keyed
  on the *date*, so once part 1 wrote output every later part was skipped (e.g. Toyota 7203 absent),
  and the per-ticker writer overwrote rather than merged (so `--no-resume` kept only the last part).
  Ingest now groups all parts of a date, reads + concatenates them, and writes each ticker once — the
  complete day. Resume and `--no-resume` are both correct and idempotent.
- **`--parallel` is now flagged as `--flat`-only**: it was silently ignored on the `--period` path;
  the CLI warns instead of implying it parallelized.

### Changed
- **CLI progress now logs to stdout** (was stderr), so a successful run no longer surfaces as red
  `NativeCommandError` lines under PowerShell.

### Documentation
- `read_ticks` / `tse-tick export` / `--input-root` accept **any folder** that contains the data —
  files are located by type + date regardless of nesting (`個別株式{year}/TICST120/{yyyymm}/`), so
  pointing at a common parent (e.g. `G:\NEEDS`) works. Regression tests cover every tree level.
- README: `read_ticks` examples show a **date range** and note it reads every part of a day; the
  `query_ticks` example flags the `[query]` (DuckDB) extra and the DuckDB-free `read_parquet_partition`.

## [0.4.0] - 2026-06-18

Optional file-driven translation overrides (`TSE_TICK_TRANSLATIONS`) plus a batch of clean-room
reliability fixes (Windows non-ASCII paths, real-world NEEDS layouts, dtypes, empty results).

### Changed
- **Translation tables externalized to data** (`tse_tick/data/translations.json`): the yfinance /
  Polygon / ccxt → `tse_tick` name maps now load from a shipped JSON file at import instead of inline
  Python dicts, so contributors can amend them with no code change. Power users can merge their own
  entries by pointing the optional `TSE_TICK_TRANSLATIONS` env var at a JSON file of the same shape.
  The public API (`translate` / `mapping` / `SUPPORTED_SOURCES`) and default behaviour are unchanged.
- Documented that ingestion uses the `ingest_period` / `ingest_single_zip` / `ingest_year_from_root` …
  functions; the bare `tse_tick.ingest` is the submodule (so `inspect.signature(tse_tick.ingest)` is
  not meaningful).

### Fixed
- **`UnicodeEncodeError` crash on non-ASCII paths** (Windows): library functions printed raw paths
  (e.g. `個別株式…`), which aborted `create_df`/`read_ticks` on a legacy-codepage console. All library
  diagnostics now go through `logging` (silent by default; the CLI still shows them), so they can no
  longer crash callers or spam stdout.
- **`discover_zips` couldn't see the real NEEDS delivery tree** (`個別株式{year}/TICST120/{yyyymm}/`):
  it now falls back to a recursive search when the documented `{year}/{yearmonth}/` layout matches
  nothing, so structured-root `read_ticks` works against the real data.
- **Inconsistent price/quote dtypes**: `Execution Price` and most quote levels came back as `str`
  while `Buy Quote 1 Best` was `Float64`. `clean_data` now casts all price/quote columns to `Float64`.
  *Store note:* newly-ingested Parquet stores hold these columns as `Float64` (were `String`);
  re-ingest to refresh older stores.
- **Empty reads lost their schema**: a no-match `read_ticks` / `create_df` / `query_ticks` returned a
  `(0, 0)` frame, so `df["Exchange Code"]` raised `ColumnNotFoundError`. They now return an
  empty-but-typed frame with the full column set.
- **`read_ticks` row-cap truncation was non-chronological and silent**: daily parts are now sorted
  naturally by `(date, part-number)` so truncation is chronological, and hitting the `rows` cap logs a
  warning.

## [0.3.0] - 2026-06-16

First release published to **PyPI**: `pip install tse-tick`.

### Fixed
- **indices_summary output was missing the Index Code column** (16 columns instead of the documented 17): the 83-column raw layout names column 5 "Stock Code" while the final column selection expects "Index Code", so the identifier was silently dropped — leaving index-summary rows unidentifiable. `set_columns()` now renames the field for `indices_summary`, which also routes it through the Index Code decoder (e.g. `101` → "Nikkei 225") instead of the stock-suffix decoder. Found by the new 2017 real-data smoke test.
- `pyproject.toml` version aligned to 0.2.3 (was 0.2.2), so build artifacts match `__version__` and this changelog.
- Example notebook (`examples/notebooks/01_basic_usage.ipynb`) stripped of all saved outputs, which contained non-redistributable NEEDS records, and of personal local paths; stale branding and dead documentation links corrected.
- Real-data test paths repointed from stale machine-specific locations to a `TSE_TICK_DATA_ROOT` environment variable (default `G:\flash_crash`) with per-class skip gates, so partially available data still gets tested; `detect_data_type_and_year` tests no longer require data files at all.
- README era table corrected: the 2016 index summary format is fixed-width (hybrid `+`-delimited), not CSV.
- `get_info()` year range updated to 2016-2025.
- CI test workflow installs pandas via a new `test` extra (`pip install -e .[query,test]`); the previous `.[query]`-only install lacked pandas (imported by `tests/test_event_window.py`), which aborted pytest collection on all Python versions. Added `from __future__ import annotations` to `tests/test_event_window.py` and `tests/synthetic_data.py` so their PEP 604 `X | None` annotations remain importable under Python 3.9.

### Added
- **Optional name-translation layer** (`tse_tick/translate.py`): `translate(source, name)` maps yfinance / Polygon / ccxt function and argument names to the `tse_tick` equivalent (e.g. `translate("polygon", "get_aggs") == "query_ticks"`, `translate("yfinance", "tickers") == "ticker_filter"`); `mapping(source=None)` dumps the tables for docs / `help()`. Static and dependency-free — the package does not import those libraries. **No public name was renamed** (an earlier rename-everywhere proposal was reversed).
- **`read_ticks()` one-shot reader** (`enhanced.py`): reads raw NEEDS ZIPs straight to a ticker/time-filtered Polars DataFrame with **no Parquet store** to build first — tuned for exploration (e.g. "ticker 7203 on 2024-02-01, 09:00–11:30" in one call). Accepts a single ZIP, a flat folder, or a structured `{year}/{yearmonth}/` root; composes `create_df`'s `individual_stock` raw-byte ticker fast path, `discover_zips` / `parse_period`, and the new shared `_tick_datetime` helper. Complements the two-stage `ingest_*` → `query_ticks` scale path.
- **`DataType` / `Language` enums** (`tse_tick/constants.py`): `str`-subclassing enums for the four data types and two languages, accepted anywhere the magic strings are; `get_supported_data_types()` now derives from `DataType`.
- **PEP 257 docstrings** across the public API — `query_ticks` / `get_available_dates` / `get_available_tickers`, `create_df` / `export_to_csv` / `discover_zips`, the `compute_*` features, `ingest_single_zip` / `ingest_period`, and the two Parquet store readers (`read_parquet_partition` vs `read_partitioned_parquet`, now clearly disambiguated).
- Tests for the new additive API (`tests/test_api_additions.py`, `tests/test_read_ticks.py`).
- `tests/test_cli.py`: CLI coverage (argument parsing, validation errors, and end-to-end synthetic-data ingestion), previously 0% — now 82%. Package coverage 61% → 76%.
- Real-data tests covering all four NEEDS types across the 2016 fixed-width and 2017+ CSV eras (`test_real_data.py`; `test_ingest.py` ingest auto-detection for stock_summary / indices / indices_summary). With the 0.3.0 additive-API tests, the suite now totals **208 tests**: without proprietary data **160 pass / 48 skip**; with a complete local NEEDS store, **all 208 pass / 0 skip**.
- GitHub Actions test workflow (`.github/workflows/tests.yml`).
- Benchmarks suite tracked in-repo (scripts, environment documentation, aggregate results CSVs).
- `rclone_guide.md`: step-by-step guide for downloading the Nikkei NEEDS dataset from a Shared-with-me Google Drive folder to local disk via rclone (remote setup, the required `--drive-shared-with-me` flag, structure mapping and sizing, a one-slice smoke test, PowerShell/bash transfer loops, and `rclone check` MD5 verification).

### Changed
- **`query_ticks` `ticker` now accepts `str` or `int`** (e.g. `7203` or `"7203"`) and is normalized to the stored code; the parameter was previously typed `Optional[int]`. Backward-compatible; invalid or unsafe values (glob/path metacharacters, wrong types) now raise a clear `ValueError`.
- Shared `_tick_datetime` / `_tick_datetime_expr` helper (`core.py`) consolidates the `HHMMSS`/colon timestamp construction previously duplicated in `event_window.py` (and mirrored in `query.py` / `features.py`); `_filter_ticks_for_events` now uses it (behaviour unchanged).
- `pyproject.toml`: `setuptools>=77` and `license-files = ["LICENSE"]` for a clean PEP 639 build; the project `name` is normalized to `tse-tick`; Development Status moved to `4 - Beta`.
- Author order set to Kazumi Li, Masataka Hayashi, Peter Romero across CITATION.cff, LICENSE, pyproject.toml, `__init__.py`, and README.
- Benchmarks re-run on the reference machine (Intel i5-14400F, 10c/16t, 32 GB; Python 3.11, Polars 1.40, pandas 2.2); all `results_*.csv` refreshed (previous run preserved as `results_*_prev.csv`). The Polars↔pandas correctness gate passes for all four data types. Updated headline figures: engine (HTICST120) 55.5× vs the Python-engine prototype, 22.8× vs the fair C-engine baseline (16 threads), 6.2× single-threaded; query (DuckDB + Hive Parquet vs pandas CSV scan) 694×; Parquet 22.2× smaller than CSV with 676× faster 3-column selective reads. Fixed the stale `G:\flash_crash_pilot` data path in `benchmarks/run_format.py`.

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
