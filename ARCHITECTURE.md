# tse_tick — LLM Reference Guide

## 1. Project Identity

| Property | Value |
|----------|-------|
| Package | `tse_tick` |
| Version | 0.15.0 (Beta) — on PyPI (`pip install tse-tick`) |
| Language | Python 3.9+ (tested on 3.9 / 3.11 / 3.13) |
| Engine | **Polars** (migrated from pandas in v0.2.0) |
| Dependencies | core: `polars>=1.0.0`, `pyarrow>=12.0.0`; optional `query` extra: `duckdb>=1.1.0` (both are hard floors — see v0.14.2 below) |
| Repository | `https://github.com/tse-tick/tse_tick.git` |
| Data | Nikkei NEEDS high-frequency tick data (Tokyo Stock Exchange, proprietary) |
| Year range | 2016–2025 |
| Architecture | Two-stage: **INGEST** (ZIP→Parquet) → **QUERY** (DuckDB + features), plus a one-shot ZIP→DataFrame reader (`read_ticks`) shipped in 0.3.0 (see §13) |

---

## 2. Directory Structure

```
tse_tick/                          # Project root
├── pyproject.toml                   # Package metadata, deps, black/pytest/coverage/mypy/flake8 configs
├── CHANGELOG.md                     # version history (current: 0.15.0)
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
│   ├── cli.py                       # CLI: tse-tick ingest (--period, --years/--year, --flat, --parallel, --compression) + tse-tick export
│   ├── constants.py                 # SSOT: DataType / Language enums + VALID_DATA_TYPES / SUMMARY_TYPES / TICK_TYPES / INDEX_TYPES / validate_data_type()
│   ├── translate.py                 # yfinance/Polygon/ccxt → tse_tick name map; loads data/translations.json (0.3.0)
│   ├── schemas.py                   # Column name definitions (EN/JP) for all 4 data types
│   ├── enhanced.py                  # Core ETL: create_df(), read_ticks(), discover_zips(), parse_period(), _prune_parts_by_ticker()
│   ├── partscan.py                  # Part-pruning: probe part start codes, bound the ticker's run arithmetically ∪ last part (0.14.0)
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
├── tests/                           # Test suite (pytest, 603 tests; 555 pass / 48 skip w/o data — the skips are real-data-gated)
│   ├── __init__.py
│   ├── conftest.py                  # Session-scoped synthetic Parquet fixtures (stock_store, indices_store, events_df)
│   ├── synthetic_data.py            # Generates obviously-fake NEEDS-format ZIPs feeding those fixtures
│   ├── test_parquet.py              # 12 — Parquet I/O (synthetic)
│   ├── test_parquet_io.py           # 15 — Parquet partition read/write (synthetic)
│   ├── test_ingest.py               # 16 — batch ingestion (synthetic + real-ZIP cases, data-gated)
│   ├── test_ingest_multipart.py     # 2  — every part of a multi-part day ingested, written once
│   ├── test_ingest_parallel.py      # 13 — parallel per-date ingest: spawn pool, RAM-aware cap (0.13.0)
│   ├── test_ingest_per_date_prune.py # 7 — per-date part-prune after the resume-skip (0.12.2)
│   ├── test_extract_to_store.py     # 8  — one-call two-stage, single + multi ticker (0.11.6/0.12.0)
│   ├── test_extract_batched_query.py # 12 — single-scan extract query ≡ per-ticker loop (0.13.0)
│   ├── test_export_query.py         # 11 — streamed single-file export (0.14.3)
│   ├── test_field5_filter.py        # 12 — vectorized field-5 ticker filter byte-identical (0.12.2)
│   ├── test_partscan.py             # 15 — part probing / contiguous-run selection (0.11.6)
│   ├── test_part_pruning.py         # 5  — pruned read ≡ full scan (0.11.6)
│   ├── test_query.py                # 15 — DuckDB query layer (synthetic store)
│   ├── test_features.py             # 20 — order-book features (synthetic store)
│   ├── test_event_window.py         # 22 — event-window extraction (synthetic + real-data-gated)
│   ├── test_cli.py                  # 20 — CLI end-to-end (synthetic), incl. the export verb
│   ├── test_api_additions.py        # 13 — translate / DataType / Language / query_ticks str-int (synthetic)
│   ├── test_read_ticks.py           # 16 — one-shot read_ticks: single ZIP / flat dir / structured root (synthetic)
│   ├── test_translate_data.py       # 7  — file-driven translation tables + TSE_TICK_TRANSLATIONS override
│   ├── test_alpha_fixes.py          # 19 — 0.11.5 alpha-report fixes (OOM guard, truncation warn, explicit year=)
│   ├── test_consolidation.py        # 5  — single-sourced data-type classification invariants (0.11.4)
│   ├── test_discovery.py            # 4  — nested NEEDS-layout discovery
│   ├── test_locate.py               # 7  — data located at any NEEDS tree level
│   ├── test_dtypes.py               # 1  — Float64 price/quote dtypes
│   ├── test_empty_schema.py         # 6  — empty-but-typed frames
│   ├── test_quiet_and_unicode.py    # 5  — logging-not-print, CJK-safe console
│   ├── test_input_validation.py     # 6  — input-root validation / friendly CLI errors
│   ├── test_family_codes.py         # 10 — 4-char family vs 5-char exact ticker semantics
│   ├── test_compression.py          # 5  — zstd default, mixed-codec stores (0.14.0)
│   ├── test_ingest_audit2_fixes.py  # 19 — ingest audit 2 H1–M4 (0.13.2)
│   ├── test_zero_row_resume.py      # 5  — a 0-row date must not resume-skip
│   ├── test_audit_fixes.py          # 16 — 0.13.0 two-stage audit B1–B11
│   ├── test_run4_fixes.py … test_run12_fixes.py  # 88 — regression suites from real-data QA runs 4-12 (no run 9 file)
│   ├── test_run14_fixes.py          # 7  — run-14 real-data QA regressions
│   ├── test_round16_fixes.py … test_round21_fixes.py  # 87 across 6 files — one per review round
│   │                                #   (round20 = morsel-bounded ingest; round21 = Effective Time key)
│   ├── test_paper_examples.py       # 5  — locks the paper's API listings
│   ├── test_real_data.py            # 64 — real NEEDS files, all 4 types/eras (data-gated)
│   ├── test_schemas.py              # STUB (1 line; schema coverage in test_real_data.py)
│   └── test_core.py                 # 3 tests (most cleaning coverage lives in the synthetic-fixture tests)
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
│   │   ├── 01_basic_usage.ipynb
│   │   └── 02_evaluation.ipynb      # release acceptance test
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

### v0.15.0 — materialized effective-time key: time windows prune row groups (issue #65) (2026-07-15)

| Change | Detail |
|--------|--------|
| Stored `Effective Time` (`io/parquet.py`) | `query_ticks` / `_query_extract_batch` filtered and ordered `individual_stock` on a CASE over two columns (`Execution Time`, falling back to `substr("Update Time", 1, 6)` for the quote-only book rows that are ~94% of a liquid day — the 0.9.0 fix). Parquet row-group min/max statistics can only be matched against a **stored column**, so a scalar expression over two columns never pruned: every selected file was read in full and filtered row-by-row. New `_add_effective_time` materializes the value as an `Int32` `HHMMSS` column, called by **both** writers (`write_partitioned_parquet` and 0.14.6's streaming `PartitionedParquetAppender`). Scoped to `individual_stock` — the other three types already filter a stored `Execution Time`, so they prune today and a duplicate would only cost bytes |
| Why the layout was already ready | 0.14.6's morsel writer emits ~148k-row row groups whose effective-time ranges are already tight and disjoint (measured on 7203 `20250409`: rg[0] `080000–091250`, rg[1] `091250–092822`, …, 19 groups) because ingest writes each day in raw NEEDS order, which is **already time-ordered** — measured **1 inversion in 2,564,238 rows** (the file is 2 sorted runs: the main block plus the 554-row off-auction appendix). So the statistics needed no ingest-time sort; only the CASE was blocking the predicate. #68 enabled this fix rather than conflicting with it |
| `Int32` over `String` | Both preserve time order (fixed-width `HHMMSS` digits compare identically either way). `Int32` measured faster on the canonical session window (1.41x vs 1.25x in isolation), narrows the key 6 bytes → 4, and delta-encodes well over the already-ordered data. This is issue #66's integer-time direction applied to the one column where it is **additive** — the rest of #66 changes existing column dtypes and needs its own schema-versioned release |
| Stored, never returned | The key is an internal index: `_select_clause` emits `SELECT * EXCLUDE ("Effective Time")`, and `read_parquet_partition` / the typed-empty paths drop it, so `individual_stock` keeps its locked **95** columns everywhere. This also means an unfiltered whole-day query never reads it — hence no regression on the no-time-filter shape. `query_sql` intentionally still exposes the raw store |
| Backward compatibility | **Additive — no re-ingest.** `_store_has_effective_time` reads one Parquet footer (schema only); a pre-#65 store answers "no" and keeps the CASE. An unreadable footer also answers "no", so the fallback is never *less* correct — the same degradation contract `partscan` uses when it cannot confirm the ascending layout |
| Mixed stores (the sharp edge) | Resume ingests new dates into an existing store, so a pre-#65 store gains keyed dates while older dates lack the column — and DuckDB rejects a file list whose **first bound file** has a column a later one lacks (`InvalidInputException: schema mismatch in glob`). Only one ordering trips it (old-file-first silently ignores the extra column), which is why the tests parametrize **both**. Both builders wrap the fast path and retry once on `duckdb.InvalidInputException` with the CASE + `union_by_name=true` + EXCLUDE: the key is simply null on the older files and the CASE never reads it, so results are correct and unaccelerated. Optimistic-with-fallback rather than an up-front uniformity scan because probing every file costs ~2.4 ms each (~1.8 s on a 750-file ticker-year, ~9 s on a 3,800-file full-market day) — it would cost more than the optimization saves |
| Measured (real NEEDS, interleaved A/B) | 7203 + 9984, `20250409` (7203 day = 2,564,238 rows / 19 row groups), pre-#65 store vs #65 store: 1-min slice **7.64x** (36.7σ), 09:00–09:05 **5.65x** (27.2σ), README session 09:00–11:30 **1.27x** (5.4σ), unfiltered day **unchanged** (0.9σ — noise), store **+0.52%** bytes. The win scales with window selectivity: pruning skips row groups, so a window keeping most of the day has little left to skip. All six real-data filter shapes returned frames identical to the pre-#65 store |
| Tests | `test_round21_fixes.py` (22): key is `Int32` and equals the CASE on every row (fixture injects quote-only rows — the synthetic generator emits none); scoping/idempotence/malformed-time→null; no leak via `query_ticks`/`_query_extract_batch`/`export_query`/`read_parquet_partition`/typed-empty; **7 parametrized legacy-vs-stored identity shapes**; the 0.9.0 quote-only-rows-kept guarantee; streaming-appender vs concat-writer identity + schema stability across morsels; indices untouched |

### v0.14.6 — morsel-bounded parse & ingest: peak independent of the day's size (2026-07-15)

| Change | Detail |
|--------|--------|
| Morsel parse (`enhanced.py`) | `create_df` handed a whole filtered part to one `pl.read_csv`, so the all-String frame (~243M string cells) stayed alive while `clean_data` cast it — a measured **4.6x** transient (9.03 GB peak for a 1.95 GB / 2,563,684-row result). The already-filtered bytes (`_read_individual_stock_matches`) are now sliced by `_iter_raw_morsels` into newline-aligned `_MORSEL_BYTES` (64 MB ≈ 180k rows) chunks; each is read + `_finalize_raw`d and only the cleaned (4.6x smaller) frame is kept. Threaded as a `finalize` callback through `create_df` → `get_1y_dataframe` → `_read_zip_member`; `_done()` makes the "finalize ⇒ cleaned output" contract unconditional rather than coupled to which branch ran. Part peak **9.03 → 4.54 GB** |
| Streaming date write (`ingest.py`, `io/parquet.py`) | The real bound. `_ingest_date_group` held every cleaned part, `pl.concat`-ed the day and copied it per ticker — 4.67M rows (7203+9984, `20250409`) = **24.52 GB**, un-fixable by any `max_workers`. New `PartitionedParquetAppender` opens one `pq.ParquetWriter` per `(date, ticker)` and appends each morsel as a row group; `create_df(..., on_morsel=appender.write)` streams cleaned morsels straight to it, so neither the part's frame nor the day's ever exists. Peak **24.52 → 2.40 GB (10x)**, independent of day size (NEEDS size-splits parts at ~55 MB, so growth adds parts, not part size). Reuses `_partition_value`/`_index_code_lookup` for keys and the same hidden-`.tmp` + `os.replace` two-phase atomicity (`commit()`/`abort()`, all-or-nothing), so a failed day publishes nothing. Gated to `individual_stock` + ≤ `_MAX_STREAM_TICKERS` (64) codes — a full-frame day would need thousands of concurrent writers, so it keeps the concat path |
| Guard stops guessing | 0.14.5's per-code `_TICKER_WORKER_GB` scaling is superseded by a constant `_STREAM_WORKER_GB` on the streaming path, because a streamed day's peak no longer depends on the data. This retires a heuristic that **could not** be made correct: file bytes do not predict filtered rows (an extreme day keeps ~100% of a pruned part, a normal one ~15% — `20250409` opens *fewer* pruned bytes than `20240403` yet yields 4.7x the rows). Exactly the "plan-driven parallelism sized from statistics" failure mode named in the morsel-driven paper. Wider-than-stream filters still scale per code |
| Why identity holds | `clean_data`/`set_columns` are purely element-wise (no `group_by`/`over`/`sort`/`join`/window/aggregation), so `concat(finalize(morsel_i)) == finalize(concat(morsel_i))`; morsels are newline-aligned (the byte filter already assumes no embedded newlines) and processed in file order. Proved on real data: streamed vs concat stores `frames_equal` for both codes on `20250409` |
| Prior art | Design shaped by Leis et al., *Morsel-Driven Parallelism* (SIGMOD 2014) — constant-size work units taken at run time, ~100k tuples, because sizing work from statistics fails under skew; Boncz et al., *MonetDB/X100* (CIDR 2005) — cache-resident vectors over full materialisation; Mühlbauer et al., *Instant Loading* (PVLDB 2013) — chunk-parallel CSV bulk loading; Palkar et al., *Sparser* (PVLDB 2018) — filter the raw bytestream before parsing, which validates keeping the existing field-5 byte filter. Full citations in `plans/round20-morsel-bounded-ingest-plan.md` |
| Tests | `test_round20_fixes.py` (13): morsel reassembly/boundary/unterminated-tail; parse identity vs unbatched (en, jp, multi-ticker, rows-cap, typed-empty); streaming-vs-concat **store** identity; no temp files + coverage marker; abort publishes nothing when a part dies; wide filter keeps the concat path. `test_round19_fixes.py` updated: per-code scaling now asserted only for wider-than-stream filters |

### v0.14.5 — TSE alphanumeric codes break part-pruning; filtered ingest RAM cap (2026-07-14)

| Change | Detail |
|--------|--------|
| Alphanumeric codes (the root cause) | TSE issues 4-char codes ending in a letter from **2024** (e.g. `162A`). `part_start_code` parsed a part's first record with `int()` → `None`, and ONE unprobeable part made `select_parts_for_day` open **every** part of that day. Measured on real data: **0/8 sampled days affected in 2017-2019 and 2023, 4/8 in 2024, 4/8 in 2025** — which is exactly why the 7203/2017-2019 extraction worked and a 2023-2025 one did not. Codes are now compared as the fixed-width **4-char tokens** NEEDS writes, not ints: token order == NEEDS' ordering, and for all-digit codes it is identical to numeric order, so their selected parts are **unchanged**. A non-4-char token falls back to a full scan (fixed width is what makes lexicographic order sound — `"999"` would sort above `"1301"`), preserving the locked "never less correct than a full scan" contract. Real `20240403` (27 parts): prunes to **2 parts**, read **472s → 13s (35×)**, frame **identical** `(530472, 95)`; per-date ingest **204s/5.14 GB → 73s/2.21 GB**. Alphanumeric `ticker_filter` (`{"130A"}`) now prunes too (the `isdigit()` gate had disabled it) |
| Filtered ingest RAM cap (the amplifier) | `_estimate_worker_gb` returned a flat `_FILTERED_WORKER_GB` (0.5 GB) for ANY `ticker_filter`, so `_cap_workers`'s RAM ceiling never bound and a filtered ingest ran **one worker per core**. Reality: ~1.1 GB **per kept code** (measured `{"7203","9984"}`/`20240403` = 990,975 rows → **2.21 GB**), so Jupyter's 16-worker default needed ~35 GB on a 34 GB box (~24.6 GB avail) → a worker was killed → `BrokenProcessPool`. New `_filtered_worker_gb(full_gb, n)` = `min(full_gb, _TICKER_WORKER_GB × n)` (1.5 GB/code, headroom over the measured 1.1): scales with filter breadth, never exceeds the day's full frame, keeps small/synthetic days at the floor. Unfiltered + summary/index estimates unchanged. **Note:** `max_workers=None` resolves to one-per-core in Jupyter (`_interactive_main()`: `__main__` has no `__file__`) but to serial in a script — the same notebook is far more parallel than the same `.py` |
| Killed-worker error | A worker killed by the OS never raises, so the pool surfaced only `BrokenProcessPool` ("terminated abruptly") — no cause, no remedy — while aborting a multi-hour ingest at `future.result()`. Both pool sites now convert it to the new `IngestWorkerError` (a catchable `RuntimeError`, core-install importable) naming the likely cause, the resume-safety of completed dates, and `max_workers`. Same pattern as `QueryMemoryError` replacing DuckDB's raw `OutOfMemoryException`. Per-date Python exceptions still return `{"date","error"}` dicts and do not abort |
| Known follow-up | `_filtered_worker_gb` scales linearly per code; it cannot know a day will fall back to a full scan (probing every day up front costs more than it saves), so it assumes pruning succeeds. With the alphanumeric fix that is now true for the observed 2017-2025 range, but a genuinely corrupt/non-ascending day still costs a full frame under a filtered estimate |
| Tests | `test_round19_fixes.py` (8): pruned-vs-full row identity on an alphanumeric day (incl. reading `162A` itself), pruning no longer forced to all parts, `IngestWorkerError` type/importability + the actionable message via a fake dead pool, `_filtered_worker_gb` scaling + full-frame clamp, and a 16-core/24.6 GB box no longer running one worker per core. `test_partscan.py` (+4): alphanumeric probe, day-with-alphanumeric-part pruning, alphanumeric `ticker_filter`, and the still-required non-ascending fallback |

### v0.14.4 — CLI presentation: friendly errors & no-data notes (normal-user QA) (2026-07-14)

| Change | Detail |
|--------|--------|
| CLI error rendering | `main()` wraps the `ingest`/`export` dispatch and converts the library's deliberate user-facing errors (`ValueError` from `validate_time_filter_support` / `parse_period`, `FileNotFoundError` from a missing `--input-root` or `@tickers` file) into a one-line `Error: <message>` on stderr + exit 1, instead of letting a full traceback (internal module paths) reach a non-coder. Scope is intentionally the *input-mistake* families only — an unexpected exception still raises (a real bug should be loud); the caught traceback is logged at `--log-level DEBUG` (`logger.debug(exc_info=True)`) |
| CLI warning rendering | A CLI-only `warnings.showwarning` (`_clean_showwarning`, installed inside a `catch_warnings()` block so global state is restored) prints tse_tick's own warnings — detected via `category.__module__.startswith("tse_tick")` — as a clean `Warning: <message>` note on **stdout** (consistent with the existing stdout-progress / PowerShell-red-avoidance choice), dropping Python's `…\cli.py:NNN: NoDataWarning:` prefix + echoed source line. Third-party warnings keep the stock formatting. `simplefilter("always")` so a single CLI run always shows its notices. **Library-only warn contract is untouched** (catchable `NoDataWarning`/`TruncationWarning`/… unchanged for API users) |
| Tests | `test_cli.py` (+4): time-filter-on-summary and missing-`--input-root` → clean `Error:` (no traceback, exit 1); bad `--period` → clean `Error:` for `ingest`; no-data day → clean `Warning:` note on stdout with no warning chrome, empty file still written |

### v0.14.3 — memory-safe query path: streaming export + catchable OOM guard (2026-07-14)

| Change | Detail |
|--------|--------|
| `export_query` (issue #59) | New public `export_query(store, output_path, …)` streams a store slice to a **single Parquet file** without materializing it: it walks the store's `date=` partitions in order and appends each stored day as a Parquet row group (via `pyarrow.parquet.ParquetWriter`), reusing `query_ticks` per day so the output is row-identical to concatenating `query_ticks(..., limit=None)` over the slice (same tie-order caveat, PR #45). Peak memory is bounded regardless of period length — the way to get a multi-year active ticker to disk where `query_ticks(limit=None)` OOMs (measured: 3-month 7203 export plateaus ~3.6 GB; a 4.98M-row real-data month matched `query_ticks` exactly). Returns a manifest `{path, rows, dates, …}`; `overwrite=False` refuses to clobber; a no-data export writes a typed-empty file + `NoDataWarning`. Requires `[query]`. Known follow-up: it re-globs the store per day (query_ticks's N+1), fine for a single-ticker store, optimizable to one walk later |
| Query OOM (round-18) | `query_ticks` / `_query_extract_batch` (the `extract_to_store` Stage-2) now raise a catchable `QueryMemoryError` (a `MemoryError`, sibling to `OneShotMemoryError`) instead of leaking DuckDB's raw `OutOfMemoryException`. A `limit=None` scan of a multi-year active ticker assembles as one frame that overflows RAM at the Arrow conversion (7203 / 2017–2019 ≈ 136M rows × 95 cols ≈ 100 GB); the new error carries tse_tick's own remedy (read the built store in bounded slices) rather than DuckDB's un-reachable `SET threads=…` hint. Converted at every high-level `.pl()` site via `_execute_to_polars`; the `query_sql` escape hatch is intentionally left raw. Symmetric with the read path's `OneShotMemoryError` |
| Memory hardening | DuckDB query connections set `preserve_insertion_order=false` to lower peak memory on large ordered scans. Safe: both structured builders always impose an explicit `ORDER BY`, and the within-same-timestamp tie order was already non-deterministic (PR #45), so output is unchanged |
| Tests | `test_export_query.py` (11): output identity vs `query_ticks(limit=None)` across all four types + family/date-range/time-window/columns, per-day row-group streaming, overwrite guard, typed-empty no-data. `test_round18_fixes.py` (7): catchable/`MemoryError` type + import-without-`[query]`, `_execute_to_polars` conversion + pass-through, end-to-end, insertion-order, ordering preserved |

### v0.14.2 — package-integrity + real-data bug-hunt: dep floors, empty-filter fix, flexible query date (2026-07-13)

| Change | Detail |
|--------|--------|
| Dependency floors | `polars>=1.0.0` (was `0.20.0`) and `duckdb>=1.1.0` (was `0.9.0`, `[query]` extra). The code uses `pl.String`, `list.get(…, null_on_oob=)`, `read_csv(schema_overrides=)`, and the partitioned-parquet writer (all polars ≥1.0.0), and `query_ticks` relies on DuckDB hive partitioning **not** deriving a column from the `ticker=NNNN.parquet` filename (≥1.1.0). Old floors imported but crashed on the first ticker-filtered read → broke both `examples/notebooks`. Pinned with in-line comments in `pyproject.toml` |
| Empty `ticker_filter` (B1) | `read_ticks(individual_stock, ticker_filter=set())` returned the **whole unfiltered market** — the fast path gated on filter *truthiness*, so an empty (falsy) set fell through to the no-filter branch. Now matches nothing (typed-empty + `NoDataWarning`), mirroring the `indices` sibling and `extract_to_store`'s "≥1 ticker" rule. Three gates in `enhanced.py` fixed to `is not None`: the field-5 filter branch, the one-shot size-guard exemption, and `get_1y_dataframe`'s no-rows typed-empty return |
| Flexible query date (B3) | `query_ticks` / `get_available_tickers` accept `YYYY` / `YYYYMM` / `YYYYMMDD` / `start-end` (via `parse_period`), matched as an inclusive range over the Hive `date` partition — not just an exact day. Same forms and errors as `read_ticks` / `ingest_period`; the internal `_query_extract_batch` date bounds stay strict `YYYYMMDD` |
| Stray `ticker` column (A2) | `query_ticks` and `_query_extract_batch` defensively drop a `ticker` column if a (past or future) DuckDB derives one from the filename. Safe — no output schema has a literal `ticker` column — and robust across DuckDB versions regardless of the floor |
| `OneShotMemoryError` message (B2) | A sub-GB `max_oneshot_bytes` override no longer renders as `"0 GB"`; both the estimated size and the limit are shown in the largest fitting unit to 3 significant figures (`1000 B`, `150 MB`, `5 GB`). Default 5 GB message unchanged |

### v0.14.1 — round-16 fixes: summary time-filter guard, query_ticks no-data warning, language validation (2026-07-13)

| Change | Detail |
|--------|--------|
| Summary time-filter guard | `start_time`/`end_time` on a `*_summary` type now raises `ValueError` at **every** entry point via one shared `constants.validate_time_filter_support`. Only `read_ticks` guarded before; `query_ticks`, `_query_extract_batch` and `extract_to_store` passed the filter to DuckDB → raw binder error (`"Execution Time" not found`), and `extract_to_store` did so only **after** a full Stage-1 ingest (partial store left on disk). `extract_to_store` now validates up front — fails in ~0 s, no wasted ingest |
| `query_ticks` no-data warning | `query_ticks` emits the capturable `NoDataWarning` on a zero-row result (unknown code, absent date, over-tight time window), matching `read_ticks`. Was silent. No `filterwarnings=error` in the suite, so existing empty-result tests still pass |
| `language` validation | `read_ticks`/`create_df` reject an unrecognized `language` via `constants.validate_language` (covers `export_to_csv` + `ingest_*`/`extract_to_store` transitively). Was: `"ja"`/`"fr"` fell through to an empty decode map → raw NEEDS codes with English headers, silently. Only `"en"`/`"jp"` are valid; the message points at `"jp"` (which has always produced full Japanese headers **and** values) |
| `discover_zips` docstring | Now states the two index types search both the current `…110` and legacy 2016 `…010` record-code prefixes (the recursive fallback already did; wording implied one prefix) |

### v0.14.0 — two-stage audit: family semantics, zero-row resume, batched clean, zstd, auto workers (2026-07-12)

| Change | Detail |
|--------|--------|
| Share-class **family semantics** | A 4-char `individual_stock` code selects parent + suffixed classes end-to-end (`"7203"` ⇒ 7203 + 72031); a longer code is family-rooted by the raw/two-stage entry points. Fixes Stage 2 silently dropping `ticker=72031.parquet` rows that Stage 1 had always ingested (extract_to_store < read_ticks), and a raw `"72031"` request matching nothing. `query_ticks` 5-char form stays exact (single-class escape hatch). `_query_extract_batch` orders by the 4-char family root |
| Zero-row resume | A cleanly-read filtered day with no matching rows writes its coverage marker into an otherwise-empty `date=` dir; resume skips on the marker alone. Was: re-pruned + re-scanned on EVERY resumed run. `get_available_dates` skips marker-only dirs |
| `PartialIngestWarning` | `extract_to_store` no longer discards Stage-1 results — lost parts/dates warn capturably (days stay resume-eligible). Missing DuckDB now raises a guided `pip install tse-tick[query]` error before Stage 1 |
| `clean_data` batched | ~80 one-expression `with_columns` calls collapsed into per-family batches (Polars parallelizes within a call); categorical decode is ONE expression per column (was: full-column `unique()` round-trip + a when/then pass per unknown value). Byte-identical output (hash-verified, all 4 types × en/jp); 2.15× on 4 cores |
| Partscan arithmetic | `select_parts_for_day` bounds the run from the probed starts (two bisects/ticker); `_part_contains` full-part Python scans removed (also fixes its EOFError run-truncation edge). Over-selects ≤ 1 boundary part only on exact start==code equality |
| zstd default | Store writes default to `compression="zstd"` (−30% size, ~3× read vs snappy per `results_format.csv`); `compression=` plumbed through every ingest entry point + CLI `--compression`. Mixed-codec stores read fine |
| `max_workers="auto"` | Accepts `"auto"` (logical cores, RAM-capped); default `None` = env `TSE_TICK_MAX_WORKERS`, auto in Jupyter/REPL (spawn-safe), serial+hint from scripts. CLI `--parallel` defaults to auto (ingest + export) |
| Threaded ticker writes | A date's ≥16 per-ticker files write across a bounded 8-thread pool (−36% zstd / −40% snappy on the write step); sequential commit loop + all-or-nothing cleanup unchanged |
| First-run guardrails | Nonexistent `input_root` raises `FileNotFoundError` on the structured path (was: "Done: 0 succeeded, 0 failed"); zero-ZIP discovery warns capturably (`NoDataWarning`); `[i/N]` progress + resume-skip summary; CLI `export --store` accepts multi-ticker; strict Buy-Quote casts removed (one malformed value no longer aborts a day) |

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
| Worker cap | `max_workers` clamped to 8 *(historical — replaced in 0.13.0 by the RAM-aware cores+RAM cap, see §11)* |
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

### v0.13.0 — parallel per-date ingest (RAM-aware) + single-scan extract query (2026-07-09)

| Change | Detail |
|--------|--------|
| Parallel per-date ingest (#43) | `ingest_period` / `ingest_year_from_root` (⇒ `extract_to_store`, CLI `--period`/`--year`) dispatch their independent per-date units across a **`spawn`-started** process pool when `max_workers > 1` (default 1 = serial; `spawn` avoids a `fork`-after-Polars deadlock). `max_workers` / CLI `--parallel` was previously a **silent no-op** on the structured-root path. Store byte-identical to serial; results sorted by date. Measured 2.3× (4 workers) / 3.6× (8 workers) on a ticker-filtered multi-day ingest |
| RAM-aware worker cap (#43) | `_cap_workers`: never more workers than logical cores, AND N × per-worker-frame ≤ **70% of available RAM** (`_RAM_SAFETY_FRACTION`); per-worker estimated from the largest day's part bytes (× `_FULLFRAME_EXPANSION` = 8) for full-frame `individual_stock`, ~0.5 GB for ticker-filtered / summary / index. Each worker's Polars thread pool bounded to `cores // concurrency`. Remaining `max_workers` no-ops (flat `ingest_year`, event-window builder) now log a warning. Per-date `gc.collect()` kept (full-frame ingest peaks near the RAM ceiling on a 34 GB box) |
| Single-scan extract query (#44) | `extract_to_store` Stage-2 no longer issues an N+1 per-ticker `query_ticks` loop (fresh DuckDB connection + full store re-glob per ticker; the `*_summary` types re-scanned the whole store N times). Now one connection, one store walk, one scan for all tickers (`query._query_extract_batch`); same row multiset in the same `(code, Data Date, effective-time)` order — tick-type order *within* a same-timestamp tie is arbitrary, as it already was (DuckDB parallel sort) |
| `--flat --parallel` pickling fix | `ingest_directory(..., max_workers>1)` crashed (`Can't pickle local object`) under `spawn`; the pool task is now the module-level `_ingest_single_zip_safe` |

### v0.12.2 — vectorized ticker filter + per-date pruning (2026-07-08)

| Change | Detail |
|--------|--------|
| Vectorized field-5 filter (#38) | The `individual_stock` ticker fast path filtered raw lines with a pure-Python per-line loop; it now streams each part in bounded 16 MB blocks and extracts field 5 with a vectorized Polars filter — ~2× faster per opened part (183.5 s → 91.2 s on a real 13-part day), **byte-identical** kept-line set, bounded memory |
| Per-date pruning (#39) | `ingest_period(..., ticker_filter=...)` pruned every day's parts up front (~80 min for a year before the first write; resume re-pruned everything). Pruning now runs per date inside the loop, **after** the resume-skip — a partition lands per day, resume prunes only what it ingests |

### v0.12.1 — extract_to_store row-cap fix (2026-07-07)

| Change | Detail |
|--------|--------|
| No 10M cap | `extract_to_store` queried via `query_ticks` with the default `limit=10_000_000`, capping a high-volume ticker-month (e.g. SoftBank 9984) at 10M rows. It now queries `limit=None` — all rows, matching the two-stage promise |
| Notebooks | `01_basic_usage` / `02_evaluation` refreshed for 0.12.0 (part-pruning, multi-ticker `extract_to_store`) |

### v0.12.0 — multi-ticker `extract_to_store` (2026-07-07)

| Change | Detail |
|--------|--------|
| Multi-ticker `extract_to_store` | `ticker` accepts a `str` **or an iterable** (`"7203"` or `["7203","9984"]`). Tickers are ingested in one part-pruned `ingest_period` pass, then each is queried and the frames concatenated (sorted-code order). No 10M-row cap (unlike one-shot `read_ticks`). Single-ticker calls unchanged. Absent-ticker empty frames (which `query_ticks` returns without the `date` partition column) are dropped before concat |
| Tests | `test_extract_to_store.py` +3 (multi-ticker, one-absent, empty-raises) |

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
create_df(folder_path, language, rows, auto_detect, data_type, year, ticker_filter,
          max_oneshot_bytes=<5 GB>, on_morsel=None)
    │
    │   TWO SHAPES, one core. Steps [3]-[5] are the same work either way. The selector
    │   is `bounded = resolved_filter is not None` — i.e. data_type=="individual_stock"
    │   AND a ticker_filter. NOT on_morsel (see the trap below):
    │     • UNBOUNDED (no ticker_filter, or any other data_type) — the linear
    │       [2]→[3]→[4]→[5] below. Materialises the all-String frame, then casts it.
    │     • MORSEL-BOUNDED (individual_stock + ticker_filter, 0.14.6) — [3]-[5] are
    │       bundled into _finalize_raw and threaded INTO [2] as a `finalize` callback
    │       applied per 64 MB newline-aligned morsel (_MORSEL_BYTES). The all-String
    │       member frame never exists; only the cleaned (~4.6x smaller) morsel is kept.
    │       Sound because clean_data/set_columns are purely element-wise:
    │       concat(finalize(morsel_i)) == finalize(concat(morsel_i)).
    │         └─ on_morsel (optional, on TOP of bounded): hand each cleaned morsel to a
    │            sink and drop it, so not even the member's cleaned frame is kept —
    │            create_df then returns an EMPTY frame (the sink owns the rows). This
    │            is what §5.3's streaming ingest passes appender.write into.
    │            TRAP: `on_morsel=on_morsel if bounded else None` — on an unbounded
    │            read the callback is SILENTLY DISCARDED and the full frame returned.
    │
    ├─ [1] detect_data_type_and_year(folder_path)
    │      ├─ Year: regex r'(20\d{2})' on path parts
    │      └─ Type: keyword matching in lowercase path ("ticst"→individual_stock, etc.)
    │              Fallback: inspect actual ZIP filenames in directory
    │
    ├─ [2] get_1y_dataframe(folder_path, year, kind, rows, ticker_filter,
    │                       max_oneshot_bytes, finalize, on_morsel)
    │      ├─ ZIP bomb checks (size, ratio, entry count)
    │      ├─ pl.read_csv(has_header=False, schema_overrides={column_1..95: String})
    │      │   All columns read as Strings to avoid inference errors
    │      ├─ Special cases:
    │      │   ├─ 2016 indices_summary → parse_line() fixed-width parser
    │      │   ├─ 2016 indices → parse_line(kind="indices") fixed-width parser
    │      │   └─ ticker_filter active → line-level pre-filter before CSV parse
    │      └─ pl.concat() multiple ZIP parts vertically (unbounded path only)
    │
    ├─ [3] set_columns(df, kind, language)
    │      ├─ Maps column_N → English schema names based on column count
    │      ├─ Handles 23-col (indices/old stock) and 95-col (extended stock) variants
    │      └─ If language="jp": renames English→Japanese via mapping dict
    │
    ├─ [4] clean_data(df, kind, language)
    │      ├─ Japanese mode: temporarily rename JP→EN for cleaning, then back to JP
    │      ├─ Type casting (by positional index, batched into single expression passes):
    │      │   ├─ Int64: volume / quote-volume / quote-flag columns (fill_null→0)
    │      │   ├─ Float64: all 27 price / quote-price columns (`float_list`),
    │      │   │   cast strict=False then fill_null→0.0 (they were String pre-0.4.0)
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
    └─ [5] Subset to the type's output columns — SKIPPED for individual_stock
           ├─ individual_stock: step skipped (`if data_type != "individual_stock"`),
           │   so the cleaned frame passes through with all 95 columns. Note
           │   get_final_columns() has NO individual_stock branch — calling it with
           │   "individual_stock" falls to the else and returns stock_summary's 82
           │   names. The gate, not the function, is what makes 95 correct.
           └─ get_final_columns(data_type) for the other three:
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

`_process_zips()` is a thin wrapper over `_ingest_grouped()`, the shared engine behind every structured-root ingest (all three `--period` granularities and `--year`): it groups the discovered ZIPs **by date** and ingests each date as an atomic unit — read all of the day's parts → concat → clean → write one `date=` partition — with resume keyed per date (the cheap skip check stays on the parent; the expensive per-date part-prune runs inside the unit, so a partition lands after each day). With `max_workers > 1` the independent per-date units are dispatched across a **`spawn`-started** `ProcessPoolExecutor` (`fork` would deadlock Polars); `_cap_workers()` clamps the worker count to the machine's logical cores AND available RAM (see §11), and each worker's Polars thread pool is bounded to `cores // concurrency` so N processes don't oversubscribe. The store is byte-identical to a serial run; the results list is sorted by date.

### 5.3 Batch Ingestion: `ingest_year_from_root()` → `_ingest_grouped()`

```
ingest_year_from_root(input_root, output_dir, year, data_type, max_workers=1)
    │
    ├─ discover_zips(input_root, data_type, [year])
    │   └─ glob: {root}/{year}/{yearmonth}/{_CODE_TYPE_MAP[data_type]}.*.zip
    │      (e.g., {root}/2022/202201/HTICST120.20220104.1.zip)
    │
    ├─ _ingest_grouped(): group ZIPs by date → one task per date
    │   ├─ Resume check (parent, cheap): _coverage_satisfied() reads the date's coverage
    │   │   marker — NOT mere file existence (a store built for ticker A must not
    │   │   resume-skip a later request for ticker B; audit finding H2)
    │   ├─ Worker count: _cap_workers(max_workers) — ≤ logical cores AND RAM-fitted (§11)
    │   └─ Per date (serial, or spawn-pool worker when workers > 1):
    │       ├─ part-prune to ticker_filter (individual_stock; inside the unit, #39)
    │       └─ _ingest_date_group() picks ONE of two write paths:
    │           ├─ STREAMING (0.14.6) — individual_stock + ticker_filter ≤ 64 codes:
    │           │   create_df(on_morsel=appender.write) sends each cleaned 64 MB morsel
    │           │   straight to a PartitionedParquetAppender as a row group, then
    │           │   commit()/abort(). Neither the part's frame nor the day's is ever
    │           │   materialised — peak ≈ one morsel, independent of the day's size.
    │           └─ CONCAT (full-frame days, summary/index types):
    │               create_df() per part → pl.concat → write_partitioned_parquet()
    │               + del/gc.collect() between concat and write (full-frame RAM headroom)
    │
    └─ Output layout:
        tick types:    {output}/individual_stock/date=YYYYMMDD/ticker=NNNN.parquet
        summary types: {output}/stock_summary/date=YYYYMMDD/YYYYMMDD.parquet
                       (date only; the code stays a column — see §7)
        (Hive-partitioned, zstd compression by default)
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
| `df.group_by("_date_str", maintain_order=True)` | `parquet.py` | Partition write grouping |
| `rolling(index_column="time", period=...)` | `features.py` | Time-based rolling window aggregations |
| `pl.from_arrow(table)` | `parquet.py` | Arrow→Polars conversion for PyArrow dataset reads (`query.py` goes through DuckDB's `.pl()` instead) |

---

## 6. Stage 2 — Query Architecture

### 6.1 `query.py` — DuckDB SQL Interface

```
query_ticks(data_dir, ticker, date, start_time, end_time, columns, limit)
    │
    ├─ Input validation (identifiers; times HH:MM:SS; date via parse_period →
    │                     YYYY / YYYYMM / YYYYMMDD / start-end, as a date range)
    ├─ Path traversal check via _resolve_type_dir()
    ├─ Resolve the effective-time expression ONCE (0.15.0):
    │   _store_has_effective_time(sample_file) — reads ONE Parquet footer (schema only);
    │   an unreadable footer answers "no" and takes the fallback, so degradation is never
    │   less correct (the same contract partscan uses).
    │     ├─ stored  → '"Effective Time"'  (Int32 HHMMSS; hits row-group min/max stats,
    │     │             so a time window SKIPS row groups instead of scanning them)
    │     └─ absent  → _EFFECTIVE_TIME_CASE: a CASE over ("Execution Time",
    │                   substr("Update Time",1,6)) — correct, but a scalar expression
    │                   cannot match row-group statistics, so nothing prunes
    ├─ Builds SQL: SELECT _select_clause(col_select, stored)   -- '* EXCLUDE ("Effective
    │              Time")' when stored: the key is an index, never returned
    │              FROM read_parquet(glob, hive_partitioning=true)
    │              WHERE date>=lo AND date<=hi AND ticker=... AND <time_expr> >= ...
    │              ORDER BY <order_cols>   -- '"Data Date"' alone for the summary types;
    │                                      -- individual_stock appends <time_expr> (so
    │                                      -- quote rows interleave chronologically
    │                                      -- rather than sorting on a blank Execution
    │                                      -- Time); indices appends "Execution Time"
    │              LIMIT {limit + 1}       -- limit+1 is the truncation probe
    ├─ _execute_to_polars() → polars DataFrame (maps DuckDB OOM → QueryMemoryError)
    ├─ MIXED-STORE FALLBACK: a resumed store can hold keyed and un-keyed dates, and
    │   DuckDB rejects a file list whose first file carries a column a later one lacks.
    │   On duckdb.InvalidInputException → rebuild with the CASE + union_by_name=true
    │   (correct, just unaccelerated for those dates; re-ingest to accelerate)
    └─ Connection closed in finally block

export_query(data_dir, output_path, ...) — streamed single-file export (0.14.3)
    ├─ Loops query_ticks(limit=None) one day at a time, in date order
    ├─ Appends each day's frame to ONE pyarrow ParquetWriter as a row group
    │   → peak RAM ≈ one trading day, independent of the period's length
    ├─ Per-day NoDataWarnings suppressed; warns once if the WHOLE export is empty
    └─ Returns a manifest {"path", "rows", "dates"} — NOT the data
       (extract_to_store's Stage 2 uses _query_extract_batch instead: one scan for
        all tickers, returning frames rather than streaming to a file)

query_sql(data_dir, sql) — PRIVILEGED ESCAPE HATCH
    ├─ Same path validation
    ├─ Creates DuckDB VIEW "ticks" backed by glob *.parquet, as a bare
    │   SELECT * FROM read_parquet(glob, hive_partitioning=true)
    │   → by design this exposes the store AS-IS. It neither EXCLUDEs the key (no
    │     _select_clause) nor drops the hive-derived partition column (no
    │     _drop_partition_ticker_column), so on a 0.15.0 store SELECT * returns 97:
    │       95 NEEDS + "Effective Time" + "date"
    │     vs query_ticks's 96 (95 NEEDS + "date"; key excluded). That is the point —
    │     it lets a caller write their own fast time predicate against the key.
    └─ Passes user SQL through with NO sanitization (documented warning)
```

### 6.2 `event_window.py` — Event Study Windows

Two modes of operation:

**A. Event list from Parquet stores:**
```
extract_event_window(data_dir, ticker, event_date, event_time, before, after, columns, data_type)
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
| `compute_volatility(window, method)` | Series | "realized": sqrt(sum(log_returns²)); "garman_klass": sqrt(0.5*ln(H/L)² - (2ln2-1)*ln(C/O)²). Trade rows only (Execution Price > 0); NULL for non-trade and warm-up rows, aligned to input rows |
| `compute_all_features(levels, windows)` | DataFrame | Wraps all four above into one augmented DataFrame |

---

## 7. Parquet I/O (`io/parquet.py`)

### Output Layouts

Layout is per data type — driven by `_DEFAULT_PARTITION_COLS`, NOT one universal shape.

**Tick types** (`individual_stock`, `indices`) — partition by `(Data Date, code)`, so each
per-ticker-day file is large and prunes well:
```
{output_root}/{data_type}/date=YYYYMMDD/ticker=NNNN.parquet    # Hive-partitioned
```

**Daily-aggregate summary types** (`stock_summary`, `indices_summary`) — partition by **date
only**, the code kept as a **column**. These hold ~1 row per (date, code), so a per-ticker file
there is one tiny row: the per-ticker layout meant tens of thousands of files and ~160x size
amplification (it blew a 15 MB month up to a 2.4 GB store of ~87k files — the 0.10.0 fix).
`query_ticks` / `get_available_tickers` prune that column via row-group statistics:
```
{output_root}/{data_type}/date=YYYYMMDD/YYYYMMDD.parquet
```

**Event windows:**
```
{output_root}/year=YYYY/month=MM/YYYYMMDD.parquet
```

### Key Functions

| Function | Purpose |
|----------|---------|
| `write_partitioned_parquet(df, output_dir, data_type, compression="zstd")` | Whole-frame path: groups by date→ticker, writes partitioned parquet (zstd default; ≥16 ticker files per date write across a small thread pool). Calls `_add_effective_time`. |
| `PartitionedParquetAppender(output_dir, data_type, compression="zstd")` | **0.14.6 streaming writer.** One `pq.ParquetWriter` per `(date, ticker)`; `.write(morsel)` appends a cleaned morsel as a row group, so the day's frame is never materialised. Same two-phase atomicity as the batch writer (hidden pid-suffixed `.tmp` + `os.replace`, all-or-nothing `commit()`/`abort()`), so a failed day publishes nothing and stays re-ingestable. Also calls `_add_effective_time`. |
| `_add_effective_time(df, data_type)` | **0.15.0.** Materialises the internal `Effective Time` (Int32 `HHMMSS`) key on `individual_stock` frames. Element-wise, so appending it per morsel is identical to computing it on a whole concatenated day — which is what lets both writers share it. |
| `read_parquet_partition(data_dir, data_type, date, ticker, columns)` | PyArrow dataset filter read (excludes the internal key) |
| `write_event_window_parquet(df, output_dir)` | Event-window format; appends to existing files |
| `read_partitioned_parquet(data_dir, year, month)` | PyArrow dataset read with year/month filters |
| `_coerce_time_cols(df)` | Converts datetime.time objects to HHMMSS strings (Parquet compat) |

---

## 8. Configuration (`pyproject.toml`)

| Tool | Config |
|------|--------|
| **Build** | setuptools>=77 + wheel; static `version = "0.15.0"`; `license-files = ["LICENSE"]` (PEP 639); `packages.find` include=`tse_tick*` |
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

**603 tests.** Without proprietary data (the CI profile): **555 pass / 48 skip**.
With a complete local NEEDS store (`TSE_TICK_DATA_ROOT`): **all 603 pass**.

| Area | Coverage |
|------|----------|
| Stage-1 (ingest) | `test_ingest` (16), `test_parquet` (12), `test_parquet_io` (15), `test_ingest_multipart` (2) — synthetic + real-ZIP cases; every ZIP part of a day collected and written once |
| Ingest performance (0.11.6–0.13.0) | `test_ingest_parallel` (13: spawn pool ≡ serial store, RAM-aware cap), `test_ingest_per_date_prune` (7: prune after resume-skip), `test_partscan` (15), `test_part_pruning` (5: pruned ≡ full scan), `test_field5_filter` (12: vectorized filter byte-identical) |
| Stage-2 (query / features / event-window from Parquet) | `test_query` (15), `test_features` (20), `test_event_window` (22) — run against a **synthetic Hive-Parquet store** built by the real ingest pipeline (`conftest.py` + `synthetic_data.py`), so they need no proprietary data |
| Two-stage extraction | `test_extract_to_store` (8: single + multi ticker), `test_extract_batched_query` (12: single-scan query ≡ per-ticker loop), `test_export_query` (11: streamed single-file export) |
| 0.13.0 two-stage audit fixes (B1–B11) | `test_audit_fixes` (16) — atomic partition write + footer-validated resume (B11), spawn-bootstrap guard error (B1), `extract_to_store` max_workers / period-scoped query / `LargeResultWarning` (B4/B5/B2), DuckDB `temp_directory` (B3), `YYYY-YYYY` periods (B8) |
| Ingest audit 2 (0.13.2, H1–M4) | `test_ingest_audit2_fixes` (19), `test_zero_row_resume` (5: a 0-row date must not resume-skip) |
| CLI | `test_cli` (20) — end-to-end on synthetic data, incl. the `export` verb |
| Additive API | `test_api_additions` (13), `test_read_ticks` (16), `test_translate_data` (7) — `translate` / enums / `query_ticks` str-int ticker, the one-shot `read_ticks`, and the file-driven translation tables + `TSE_TICK_TRANSLATIONS` override, all on synthetic data |
| Robustness fixes | `test_alpha_fixes` (19: OOM guard, truncation warn, explicit `year=`), `test_consolidation` (5: single-sourced type classification), `test_quiet_and_unicode` (5), `test_discovery` (4), `test_dtypes` (1), `test_empty_schema` (6), `test_locate` (7), `test_input_validation` (6), `test_family_codes` (10: 4-char family vs 5-char exact), `test_compression` (5: zstd default, mixed-codec stores) |
| Real-data QA regression suites | `test_run4_fixes` … `test_run12_fixes` (88 across 8 files; no run-9 file) + `test_run14_fixes` (7) — each locks the fixes from one real-data QA run |
| Round-N regression suites | `test_round16_fixes` (18), `test_round17_fixes` (14), `test_round18_fixes` (7), `test_round19_fixes` (8), `test_round20_fixes` (13: morsel-bounded parse/ingest), `test_round21_fixes` (27: the materialized `Effective Time` key) — each locks one review round |
| Paper examples | `test_paper_examples` (5) — locks the technical paper's API listings |
| Real data | `test_real_data` (64) + real-ZIP cases in `test_ingest` — all 4 types across the 2016 fixed-width and 2017+ CSV eras; **gated on local NEEDS files** (these are the 48 no-data skips: 40 + 8) |

`test_schemas.py` is a 1-line stub and `test_core.py` holds only 3 tests — cleaning and
schema correctness are exercised mainly by `test_real_data.py` and the synthetic-fixture
tests. The earlier "Stage 2 has zero coverage" gap is **resolved**.

---

## 11. Security Architecture

| Protection | Location | Mechanism |
|------------|----------|-----------|
| ZIP bomb | `enhanced.py` | Max 5 GB decompressed, 5 entries, 100:1 ratio |
| Path traversal | `query.py` | Resolved path prefix validation in `_resolve_type_dir()` |
| Parallel cap | `ingest.py` | `_cap_workers()`: ≤ logical cores AND N × per-worker frame ≤ 70% of available RAM (`_RAM_SAFETY_FRACTION`); per-worker sized from the largest day's part bytes for full-frame `individual_stock`; pools are `spawn`-started, per-worker Polars threads bounded |
| Query overflow | `query.py` | Default `LIMIT 10_000_000` on `query_ticks()` |
| SQL injection | `query.py` | Column identifiers screened by a character **blocklist** (rejects `"` `\` `;` backticks, CR/LF/TAB, NUL — but allows spaces in NEEDS column names); dates `^\d{8}$`, times `^\d{2}:\d{2}:\d{2}$`; `ticker` normalized to an alphanumeric token |
| Traceback leak | `ingest.py` | `traceback.print_exc()` → `logger.error(exc_info=True)` |
| Privileged SQL | `query.py` | `query_sql()` documented with WARNING docstring; read-only by DuckDB in-memory design |

---

## 12. Constraints (Design Invariants)

| ID | Rule | Enforced in |
|----|------|-------------|
| C1 | ZIPs read in-memory, never extracted to disk | `enhanced.py` (io.BytesIO) |
| C2 | Bounded memory per worker — but the bound differs by path | **Streaming** (`individual_stock` + ≤`_MAX_STREAM_TICKERS` codes): peak ≈ one 64 MB morsel, independent of the day's size; estimated flat at `_STREAM_WORKER_GB` (3.0). **Full-frame**: one whole date's frame per worker, held via `ingest.py` loops + `del` + `gc.collect()`. Either way the worker count is RAM-capped (C9) |
| C4 | Partitioned Parquet output, never single monolithic file | `io/parquet.py` |
| C5 | Corrupt ZIPs logged and skipped (not fatal) | `ingest.py`, `scripts/ingest_event_windows.py` |
| C6 | JST timezone on all timestamp comparisons | `event_window.py` |
| C8 | ZIP bomb guard | `enhanced.py` (checked before decompression) |
| C9 | Parallel workers RAM-aware: ≤ logical cores, N × per-worker frame ≤ 70% of available RAM | `ingest.py` (`_cap_workers`) |
| C10 | Query row limit: 10M | `query.py` |
| C11 | Partition writes are atomic (hidden temp file + `os.replace`); resume validates Parquet footer magic before skipping a date | `io/parquet.py`, `ingest.py` |
| C12 | Parallel ingest pools use the `spawn` start method (never `fork`); a user's `max_workers>1` script call must sit under `if __name__ == "__main__":` (an unguarded top-level call gets an actionable `RuntimeError`) | `ingest.py` |

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
