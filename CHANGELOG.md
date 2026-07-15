# Changelog — tse_tick

## [Unreleased]

## [0.15.1] - 2026-07-15

**Linux portability.** The package core already ran correctly on Linux at 0.15.0 — `spawn` is
forced explicitly, coverage markers are portable JSON, DuckDB globs are normalized, the Windows
console shim no-ops elsewhere, and Windows-built stores of all four data types read correctly from
Linux (and back). What did not hold up was everything around it: the test suite showed two
failures that looked like package breakage, parallel ingest under-parallelized on any box with a
warm page cache, an oddly-cased NEEDS delivery was **silently invisible**, and a Japanese ticker
file decoded differently per OS. Verified on WSL2 Ubuntu 24.04 / Python 3.12 / polars 1.42 /
DuckDB 1.5 against real NEEDS data, and re-verified on Windows. Closes #72.

No re-ingest, no API change, no new dependencies (`/proc/meminfo` is stdlib).

### Fixed
- **Discovery now matches filenames case-insensitively on every platform.** Python's `glob` is
  case-insensitive on Windows but case-sensitive everywhere else, so a `HTICST120.20230104.1.ZIP`
  delivery — from a re-zip, a backup tool, or a one-off NEEDS drop — was invisible on Linux while
  Windows ingested it. **The dangerous part was that a *partial* miss is silent:** the
  zero-discovery `NoDataWarning` only fires when *nothing* matches, so a single odd-cased file
  meant Linux quietly dropped days that Windows kept, and two machines disagreed about the same
  data with no error on either. Measured on a synthetic mixed-case directory, Linux found
  `['…20230105.1.zip']` where Windows found both days; it now finds both everywhere. Applied at
  all nine discovery sites (both `discover_zips` fast paths, the recursive fallback, the flat-dir
  read/`create_df`/`read_ticks` paths, `ingest_directory`, `ingest_year`, and the event-window
  glob). Paths differing **only** by case — which can coexist on Linux but not Windows — now
  resolve to one deterministically chosen file with a logged warning, because reading both would
  concatenate one trading day's data twice. Windows behavior is unchanged. The single-file
  argument path already lowercased its suffix; the codebase is now consistent.
- **`--tickers @file` is read as UTF-8 on every platform.** It used the locale default, so the same
  file parsed differently per OS — and `ticker_filter` legitimately accepts Japanese index display
  names. Measured on Windows (cp1252): a UTF-8 file of Japanese names raised `UnicodeDecodeError`,
  and a BOM-prefixed file yielded the ticker `'ï»¿7203'`, which silently matches nothing. Now
  `utf-8-sig` — plain UTF-8 reads unchanged, and the BOM Windows editors prepend is stripped.

### Changed
- **Parallel ingest sizes workers from `MemAvailable` on Linux, not `MemFree`.**
  `_available_ram_gb()` read `sysconf(SC_AVPHYS_PAGES)`, which tracks **MemFree** and excludes the
  reclaimable page cache, while the Windows branch (`GlobalMemoryStatusEx.ullAvailPhys`) includes
  standby memory — i.e. the two platforms disagreed, and Linux under-reported. On a long-running
  research box the page cache holds most of idle RAM, so `MemFree` can sit near ~1 GB on a 128 GB
  machine; `_cap_workers()` then silently degraded a parallel ingest toward serial and warned about
  an absurdly small "available RAM". Measured on WSL with 6 GB of warm page cache: `MemFree` 8.94 GB
  vs `MemAvailable` 15.93 GB, and the cap for a 3.0 GB/worker streaming ingest went **2 → 3
  workers** — 3 being what the same box already gave with a cold cache. Falls back to the old
  arithmetic when the field or file is missing, which also keeps macOS on its existing path; the
  Windows branch is untouched. Failure direction was always safe (under-parallelize, never OOM), so
  this is performance/UX, not correctness.
- **The parallel-ingest RAM messages no longer claim every worker holds a whole trading
  day.** 0.14.6 made a ticker-filtered ingest (≤ `_MAX_STREAM_TICKERS`, 64 codes) stream,
  bounding each worker at `_STREAM_WORKER_GB` (3.0 GB) *independent of the day's size* —
  but the docs and messages never caught up. `_cap_workers`'s "Limiting workers N -> M …"
  warning still told users "Each worker holds a whole trading day" and advised them to
  "ticker-filter" when they already had — the exact case where the claim is false. Fixed
  everywhere it was asserted: the `--parallel` **CLI help**, the `IngestWorkerError` class
  docstring, the four public `ingest_*` `max_workers:` docstrings, `_worker_died_error`'s
  OOM message, and the `_cap_workers` / `_estimate_worker_gb` docstrings. All now
  attribute the whole-day hold to the **full-frame** path (no ticker filter, or >64
  codes), which is where it is still true. The 70% RAM cap itself
  (`_RAM_SAFETY_FRACTION = 0.7`) is unchanged — only the wording. No behaviour change; no
  re-ingest.
- **`read_ticks`'s docstring no longer cites an unsourced row count.** It claimed "7203 +
  9984 for one January is ~25M rows" — a number backed by no `results_*.csv` and no
  CHANGELOG, reaching users through `help(read_ticks)`. Replaced with the sourced
  ">10M rows/month" fact, matching the README.

### Docs
- **README documents Linux/WSL behavior and the `TSE_TICK_DATA_ROOT` test knob.** A new "Linux and
  WSL notes" section covers the things that cost a verification session real time: the
  `if __name__ == "__main__":` guard applies on Linux too (`spawn` is forced there — fork deadlocks
  Polars — which nobody coming from fork semantics expects); worker sizing reads `MemAvailable`, so
  a `Limiting workers N -> M` notice is normal; a Linux and a Windows export of the same slice are
  multiset-equal but **not** byte-identical, because same-timestamp tie order is non-deterministic
  (accepted — PR #45 — so compare as sorted frames, not with `diff`); and WSL's ~half-host-RAM VM
  makes a low worker cap expected. Installation gains the Linux prerequisites (Python ≥3.9,
  `python3-venv`, and AVX2 → `polars-lts-cpu` on older CPUs). Testing now documents all four
  profiles and states that **nothing fails on either OS** — every skip is deliberate.

### Testing
- **The CLI tests no longer spawn a literal `"python"`.** `tests/test_real_data.py::TestCLI` ran
  `subprocess.run(["python", …])`; stock Debian/Ubuntu ships only `python3`, so both tests died
  with `FileNotFoundError` on Linux unless a venv happened to be activated — two failures that read
  as package breakage to anyone running `venv/bin/python -m pytest`. Now `sys.executable`, which is
  portable **and** pins the subprocess to the interpreter running pytest. A source-scanning guard
  keeps it from coming back, in the style of `test_consolidation.py`.
- **Data-gated skip reasons name `TSE_TICK_DATA_ROOT`.** The default root is a Windows path, so off
  Windows all 48 data-gated tests skipped and the suite looked green while the real-data half never
  ran — and nothing on screen named the knob that runs it. The reasons now say so, in
  `test_real_data.py`, `test_ingest.py`, and `test_field5_filter.py`. No behavior change when the
  variable is set.
- **`tests/test_linux_portability.py`** — 31 synthetic-first regression tests covering all of the
  above: `MemAvailable` parsing and the worker cap it feeds, case-insensitive discovery across the
  fast paths / recursive fallback / flat-dir reads / `ingest_directory`, case-variant dedupe, and
  UTF-8 `@file` decoding.
- **Fixed the documented contributor setup, which could not run the test suite.**
  `CONTRIBUTING.md` and `README.md` both said `pip install -e ".[dev]"`; `[dev]` carries no
  DuckDB, `query.py` imports it at module level, and the query tests import
  `tse_tick.query` directly — 9 collection errors. Both now say `.[query,dev]` (CI uses
  `.[query,test]`). The package itself was never affected: `__init__.py` guards the import
  and raises an actionable "install tse-tick[query]" instead.
- **`Effective Time` is excluded from every *documented* output — not from `query_sql`.**
  The README claimed it was "excluded from every result, so `individual_stock` still
  returns its 95 columns". `query_sql` registers its `ticks` view as a bare `SELECT * FROM
  read_parquet(..., hive_partitioning=true)`, so it neither excludes the key nor drops the
  Hive partition column: on a 0.15.0 store `SELECT *` returns **97** (95 NEEDS +
  `Effective Time` + `date`), against `query_ticks`'s 96 (95 + `date`). That is by design —
  it is what lets a caller write a fast hand-written time predicate against the key — and
  the 0.15.0 entry below already scoped it correctly; the README now matches.
- **Test counts corrected to a measured 603 / 555 pass / 48 skip.** README said 430/382/48
  and ARCHITECTURE said both 414/366/48 and 430/382/48 in the same file. Each was true when
  written. The 48 skips and their 40 + 8 attribution were already right and are unchanged.
- **ARCHITECTURE's reference sections no longer describe superseded designs as current.**
  §5.1 (morsel-bounded parse; all 27 price columns cast `Float64`, not "mostly String";
  the `individual_stock` output-subset step is skipped, not applied), §5.3 (the streaming
  vs concat write paths; resume reads a coverage marker, not mere file existence), §6.1
  (the resolved effective-time expression, the `limit + 1` probe, the mixed-store
  `union_by_name` fallback, `export_query`), §7 (`PartitionedParquetAppender`,
  `_add_effective_time`, and the summary types' date-only layout), and §12's C2.
- **Version stamps.** `CITATION.cff` 0.11.4 → 0.15.0 (+ `date-released`); ARCHITECTURE's
  three disagreeing stamps (0.14.3 / 0.14.0 / 0.13.0) → 0.15.0.
- **Perf claims re-scoped to what was actually measured.** The Performance preamble
  attributed every row to "one day … 4.78 M rows", but the 694.1x query row was measured on
  1.5 M rows and the engine/storage rows use one ZIP *part*, not a whole day. The storage
  headline advertised Snappy (22x) when the default since 0.14 is zstd — now both are
  listed, with zstd's **31x** first. The unsourced "~25M rows" was replaced with the
  sourced ">10M rows/month" fact (see Changed — it shipped in `read_ticks`'s docstring too).
- Audit plan and the full finding list: `plans/doc-accuracy-audit-plan.md`.

## [0.15.0] - 2026-07-15

Time-window queries on `individual_stock` now prune Parquet row groups instead of
scanning every one: a 1-minute slice is **7.64x** faster, a 5-minute window **5.65x**,
the README's 09:00–11:30 session window **1.27x**, for **+0.52%** store bytes. The
change is **additive — no re-ingest**: stores written before it keep working unchanged
on the old expression. Closes #65.

Minor bump rather than a patch because the **store gains a column**: partitions written
by 0.15.0 carry an internal `Effective Time` key. Nothing a caller sees changes —
`individual_stock` still returns its locked **95** columns — and old stores keep
reading, so the bump signals the on-disk addition, not a break.

Three sibling proposals from the same pipeline review were **closed on measured
evidence** rather than implemented — see #64 (the per-file ingest sort is a no-op: real
data is already time-ordered, 1 inversion in 2,564,238 rows, and sorting would undo
0.14.6's memory bound), #67 (the "~10³ rows per ticker-day file" premise is false —
the real median is 14,260), and #66 → #70 (Float32 quote columns perturb 737,074 rows
on round-trip; the safe Int32/Enum narrowing is deferred to its own schema-versioned
release).

### Added
- **A stored `Effective Time` key (`Int32` `HHMMSS`) on `individual_stock` partitions.**
  `query_ticks` / `_query_extract_batch` filtered and ordered on a CASE over two columns
  (`Execution Time`, falling back to `substr("Update Time", 1, 6)` for the quote-only
  book rows that are ~94% of a liquid day). A scalar expression cannot be matched against
  Parquet row-group min/max statistics, so a time predicate could never skip a row group —
  every selected file was read in full and filtered row-by-row. The value is now
  materialized at write time by both writers (`write_partitioned_parquet` and 0.14.6's
  streaming `PartitionedParquetAppender`), so the predicate hits row-group statistics.
  It is computed element-wise, so appending it per morsel is identical to computing it on
  a whole concatenated day.

  Measured against a pre-#65 store on real NEEDS data (7203 + 9984, `20250409`; the 7203
  day is 2,564,238 rows in 19 row groups), interleaved A/B, median of 9–11:

  | query | before | after | speedup |
  |---|---|---|---|
  | 1-minute slice (09:30–09:31) | 915.2 ms | 119.8 ms | **7.64x** (36.7σ) |
  | narrow window (09:00–09:05) | 956.4 ms | 169.3 ms | **5.65x** (27.2σ) |
  | session window (09:00–11:30) | 2645.4 ms | 2088.7 ms | **1.27x** (5.4σ) |
  | whole day, no time filter | 3817.2 ms | 4130.5 ms | unchanged (0.9σ — noise) |
  | store bytes | 55,193,678 | 55,479,881 | **+0.52%** |

  The win scales with the window's selectivity — it skips row groups, so a window that
  keeps most of the day has little left to skip. All six real-data filter shapes returned
  frames identical to the pre-#65 store.

### Changed
- **Both SQL builders lose the duplicated CASE.** `query_ticks` and `_query_extract_batch`
  resolve the effective-time expression once, from the store: the materialized column when
  present, the CASE otherwise. Detection reads one Parquet footer (schema only); an
  unreadable footer answers "no" and takes the fallback, so the degradation is never less
  correct — the same contract `partscan` uses when it cannot confirm the ascending layout.
- **The key is stored, never returned.** It is an internal index, so it is excluded from
  every documented output: `individual_stock` keeps its locked **95** columns from
  `query_ticks`, `_query_extract_batch`, `export_query`, `read_parquet_partition`, and the
  typed-empty frames. An unfiltered whole-day query therefore never pays to read it (hence
  "unchanged" above). The `query_sql` escape hatch still exposes the raw store, so its
  `ticks` view shows the column — use it for fast hand-written time predicates.
- **Mixed stores keep working.** Resume ingests new dates into an existing store, so a
  pre-#65 store gains keyed dates while its older dates lack the column — and DuckDB
  rejects a file list whose first file carries a column a later one does not. Both
  builders now fall back to the CASE with `union_by_name=true` when that happens, so a
  half-upgraded store reads correctly (just unaccelerated for those dates; re-ingest the
  older dates to accelerate them). Detection reads a single Parquet footer per query —
  probing every file would cost ~2.4 ms each (~1.8 s on a 750-file ticker-year), which is
  why the fast path is optimistic with a correct fallback rather than an up-front scan.

## [0.14.6] - 2026-07-15

A ticker-filtered ingest's peak memory no longer scales with the trading day's size.
The worst real day measured drops from **24.52 GB to 2.40 GB — 10x** — and the store it
writes is **byte-identical** (verified on real data: `20250409` / 7203+9984 / 4,673,760
rows, both `ticker=` files `frames_equal`). Rows/day keep growing (raw TICST120: 2017 =
101.9 GB → 2025 = 351.9 GB; largest month 202504 = 42.0 GB), so this is what stops the
ingest OOMing again as volume rises. Design + prior art:
`plans/round20-morsel-bounded-ingest-plan.md`. No re-ingest needed.

### Fixed
- **The parse no longer materialises a whole part as one all-String frame.** `create_df`
  handed the entire filtered part to a single `pl.read_csv`, so the all-String frame
  (~243M string cells) stayed alive while `clean_data` cast it — a measured **4.6x**
  transient over the result (9.03 GB peak for a 1.95 GB / 2,563,684-row frame). The
  already-filtered bytes are now read and cleaned in newline-aligned **morsels**
  (`_MORSEL_BYTES`, 64 MB ≈ 180k rows), and only the cleaned (4.6x smaller) frames are
  kept. Part-level peak **9.03 → 4.54 GB**.
- **A filtered ingest no longer holds the day.** `_ingest_date_group` accumulated every
  cleaned part, `pl.concat`-ed the day, then copied it again per ticker — so one April-2025
  day (4.67M rows for two codes) needed **24.5 GB**, and no `max_workers` value fit it.
  Each cleaned morsel is now appended straight to its `(date, ticker)` Parquet writer as a
  row group via the new `PartitionedParquetAppender`, so neither the part's frame nor the
  day's is ever materialised: peak is **~one morsel**, independent of the day's size.
  NEEDS size-splits parts at ~55 MB, so a busier day means *more* parts, not bigger ones,
  and the bound holds as volume grows. Day-level peak **24.52 → 2.40 GB (10x)**; 8 workers
  now fit on a 34 GB box where 6 previously OOM'd.
  Same layout and the same two-phase atomicity as `write_partitioned_parquet` (hidden
  pid-suffixed `.tmp` + `os.replace`, all-or-nothing cleanup), so a failed day publishes
  nothing and stays fully re-ingestable. Applies to `individual_stock` with a filter of up
  to `_MAX_STREAM_TICKERS` (64) codes; full-frame days (thousands of ticker files) and the
  summary/index types keep the proven concat path.
- **The worker-RAM guard stops guessing.** With a streamed day bounded, the per-code
  scaling added in 0.14.5 is superseded by a constant (`_STREAM_WORKER_GB`) for the
  streaming path. This retires a heuristic that could never be made correct — file bytes do
  not predict filtered rows (an extreme day keeps ~100% of a pruned part, a normal one
  ~15%), which is why it mis-sized workers twice. A filter too wide to stream still scales
  per code, clamped by the day's full frame.

### Notes
Output identity rests on `clean_data`/`set_columns` being purely element-wise (no
`group_by`/`over`/`sort`/`join`/window/aggregation), so
`concat(finalize(morsel_i)) == finalize(concat(morsel_i))`; morsels are newline-aligned and
processed in file order. Asserted directly by tests, and proved on real data against the
0.14.5 output rather than assumed.

## [0.14.5] - 2026-07-14

Two defects that together killed a parallel `ingest_period` over 2023–2025 with a bare
`BrokenProcessPool` after 13 minutes: TSE's **alphanumeric stock codes** (issued from
2024) silently disabled part-pruning for ~half of all 2024/2025 trading days, and the
RAM-aware worker cap never bound on a ticker-filtered ingest. Parse/clean output is
unchanged and part-pruning stays **row-for-row identical to a full scan** (re-verified
on real data: the 27-part `20240403` returned an identical `(530472, 95)` frame pruned
and unpruned). No re-ingest needed.

### Fixed
- **Alphanumeric stock codes no longer disable part-pruning.** TSE issues 4-char codes
  ending in a letter from 2024 (e.g. `162A`); `part_start_code` parsed a part's first
  record with `int()` and returned `None` for those, and a single unprobeable part made
  `select_parts_for_day` fall back to opening **every** part of that day. Measured on
  `G:\NEEDS`: **0/8 sampled days affected in 2017–2019 and 2023, but 4/8 in 2024 and 4/8
  in 2025**. Codes are now compared as the fixed-width **4-char tokens** NEEDS writes
  rather than as ints — token order equals NEEDS' ordering, and for the all-digit codes
  that predate 2024 it is identical to their numeric order, so the parts selected for
  them are unchanged. Real-data measurement on `20240403` (27 parts): pruning now selects
  **2 parts, and the read went 472s → 13s (35×)** with a byte-identical result; the
  per-date ingest went **204s / 5.14 GB → 73s / 2.21 GB**. A non-4-char token still falls
  back to a full scan, so an unconfirmed layout is never pruned wrongly.
  An alphanumeric `ticker_filter` (e.g. `{"130A"}`) now prunes too — the old `isdigit()`
  gate disabled pruning for it entirely.
- **The RAM-aware worker cap now binds on a ticker-filtered ingest.**
  `_estimate_worker_gb` returned a flat 0.5 GB for *any* `ticker_filter`, so
  `_cap_workers` computed a RAM ceiling that never clamped and a filtered ingest ran one
  worker per core. A filtered worker-day actually costs ~1.1 GB **per code** (measured:
  `{"7203","9984"}` on `20240403` = 990,975 rows → **2.21 GB** peak), so the Jupyter
  default of 16 workers needed ~35 GB on a 34 GB box (~24.6 GB available) and a worker
  was killed. The estimate now scales per kept code (`_TICKER_WORKER_GB`, 1.5 GB with
  headroom), clamped by the whole day's frame so it can never exceed a full-frame
  estimate and a small day stays at the floor. Unfiltered and summary/index estimates
  are unchanged.
- **A killed ingest worker now raises `tse_tick.IngestWorkerError`** instead of a bare
  `BrokenProcessPool: A process in the process pool was terminated abruptly`. A worker
  killed by the OS (usually for memory) never gets to raise, so the pool reported
  neither cause nor remedy while aborting a multi-hour ingest. The new error names the
  likely cause, states that completed dates are already written and **resume-safe**, and
  points at `max_workers` — mirroring how `QueryMemoryError` replaces DuckDB's raw
  `OutOfMemoryException`. Per-date Python exceptions are unaffected (still captured per
  date as `{"date", "error"}` result dicts).

### Added
- **`tse_tick.IngestWorkerError`** — a catchable `RuntimeError` subclass, importable on a
  core (no `[query]`) install.

## [0.14.4] - 2026-07-14

A CLI presentation pass from a normal-user (non-coder) QA acceptance test: `tse-tick
export`/`ingest` now show plain-language errors and no-data notices instead of raw
Python tracebacks and warning chrome. **CLI-only** — no change to parsed/cleaned output
or the library's `warnings`-based API contract; the full synthetic + real-data suite is
green (551 passed) and `flake8`/`mypy` show no new findings on `cli.py`.

### Fixed
- **CLI errors are one-liners, not Python tracebacks.** `tse-tick export`/`ingest`
  let the library's deliberate user-facing errors — an unsupported
  `--start-time`/`--end-time` on a daily-summary type, a malformed `--period`, a
  missing `--input-root` or `@tickers` file (`ValueError` / `FileNotFoundError`, each
  already carrying a complete message) — propagate as a full traceback exposing
  internal module paths (`…\tse_tick\constants.py:112`). They now surface as a single
  `Error: <message>` line on stderr with exit code 1. The full traceback stays
  available with `--log-level DEBUG`.
- **CLI no-data / truncation notices are clean.** The CLI rendered `tse_tick`'s own
  warnings (e.g. `NoDataWarning` on an exchange holiday) with Python's default
  `…\cli.py:NNN: NoDataWarning:` prefix and an echoed source line on stderr (red under
  PowerShell). They now print as a clean `Warning: <message>` note on stdout (exit 0,
  the typed-empty output file still written). CLI-only — the library's `warnings`-based
  API contract (catchable `NoDataWarning` / `TruncationWarning` / …) is unchanged.

## [0.14.3] - 2026-07-14

A memory-safe query path: a catchable guard when a result won't fit in RAM
(round-18), plus a streaming `export_query` that writes an arbitrarily large slice to
one Parquet file without ever holding it in memory. No change to parsed/cleaned row
output; the full synthetic + real-data suite is green and `flake8`/`mypy` show no new
findings.

### Added
- **`tse_tick.export_query(store, output_path, …)`** — stream a store slice to a
  **single Parquet file** without materializing it. Where `query_ticks(..., limit=None)`
  over a multi-year active ticker raises `QueryMemoryError` (~100 GB for Toyota 7203 /
  2017–2019), `export_query` walks the store's `date=` partitions in order and appends
  each stored day as a Parquet row group, so peak memory stays bounded regardless of
  period length (measured: a 3-month 7203 export plateaued ~3.6 GB, vs 2.8 GB for one
  month — sub-linear). It reuses `query_ticks` per day, so the written rows are
  **identical** to concatenating `query_ticks(..., limit=None)` over the same slice
  (verified on real data: a 4.98M-row month matched exactly), for all four data types,
  with family/`date=`/time-window/column filters. Returns a small manifest
  (`{path, rows, dates, …}`), not the data; refuses to overwrite an existing file
  unless `overwrite=True`; a no-data export writes a typed-empty file and warns
  `NoDataWarning`. Requires the `[query]` extra. (Issue #59.)
- **`tse_tick.QueryMemoryError`** — a catchable `MemoryError` subclass raised when a
  store query would exhaust memory materializing its result as one in-memory
  DataFrame. It is importable on a core (no-`[query]`) install (defined alongside
  `OneShotMemoryError` in `enhanced.py`, not in the DuckDB-gated `query.py`), and
  being a `MemoryError` it lets one `except MemoryError` cover an over-large read
  **and** an over-large query.

### Fixed
- **`query_ticks(..., limit=None)` over a large range now fails with an actionable,
  catchable error instead of a raw DuckDB `OutOfMemoryException`.** A `limit=None`
  query for a multi-year active ticker asks DuckDB to sort and return the whole
  result as one Polars frame — Toyota `7203` for 2017–2019 is ~136M rows × 95 cols
  ≈ **100 GB in RAM**, which overflows a typical machine at the Arrow conversion
  (`ArrowBuffer: failed to allocate …`). The underlying `duckdb.OutOfMemoryException`
  — whose `SET threads=…` / `SET memory_limit=…` advice the caller cannot reach
  through this API — is now caught at every high-level `.pl()` site (`query_ticks`
  and the batched `_query_extract_batch` behind `extract_to_store`) via a new
  `_execute_to_polars` helper and re-raised as `QueryMemoryError` carrying tse_tick's
  own remedy: read the built store back in bounded slices (narrow `date=`, a smaller
  `limit=`, or loop per day / per month). The privileged `query_sql` escape hatch is
  intentionally left raw. (Reported via the run-16 extraction notebook.)

### Changed
- **DuckDB query connections disable insertion-order preservation**
  (`preserve_insertion_order=false`) to lower peak memory on large `limit=None`
  scans. Safe and output-preserving: `query_ticks` and `_query_extract_batch` always
  impose an explicit `ORDER BY`, and the within-same-timestamp tick tie order was
  already non-deterministic (PR #45).

## [0.14.2] - 2026-07-13

Package-integrity + real-data bug-hunt fixes: correct dependency floors, an
empty-`ticker_filter` correctness fix, and a flexible query-side `date=`. No
changes to parsed/cleaned row output — every fix is a filter-gating, formatting,
packaging, or query-date change; the full synthetic + real-data suite is green and
`flake8`/`mypy` show no new findings.

### Fixed
- **`read_ticks(data_type="individual_stock", ticker_filter=set())` now matches
  nothing instead of silently returning the whole unfiltered market.** The
  `individual_stock` raw-byte fast path gated on the *truthiness* of the filter, so
  an empty (falsy) `set()` fell through to the "no filter" branch and returned every
  code for the window — with no warning — while the same call on `indices` correctly
  returned empty and `extract_to_store(ticker=set())` correctly raised. It now
  returns a typed-empty frame and the capturable `NoDataWarning`, mirroring the
  `indices` sibling. Three truthiness gates were corrected to `is not None`: the
  field-5 filter branch, the one-shot size-guard exemption, and the no-rows
  typed-empty return in `get_1y_dataframe` (the last was the one that would
  otherwise have turned an empty set into a raised `ValueError`). (Report B1.)
- **`OneShotMemoryError` no longer renders a sub-GB limit as `"0 GB"`.** A small
  `max_oneshot_bytes` override (e.g. `1000`) printed `"… exceeds the 0 GB one-shot
  limit"`; both the estimated size and the limit are now formatted in the largest
  fitting unit to three significant figures (`"1000 B"`, `"150 MB"`, `"5 GB"`). The
  default 5 GB message is unchanged. (Report B2.)

### Changed
- **Dependency floors raised to what the code actually requires: `polars>=1.0.0`**
  (was `>=0.20.0`) **and `duckdb>=1.1.0`** (was `>=0.9.0`, in the `[query]` extra).
  The code uses `pl.String`, `list.get(…, null_on_oob=)`, `read_csv(schema_overrides=)`,
  and the partitioned-parquet writer — none present in polars 0.20.x — and
  `query_ticks` relies on DuckDB hive partitioning not deriving a column from the
  `ticker=NNNN.parquet` filename (leaked as a spurious `ticker` column on duckdb
  ≤1.0.0). An install resolving to the old floors imported but crashed on the first
  ticker-filtered read, which broke both `examples/notebooks`. The floors are now
  pinned with in-line comments so they are not lowered again. (Report A1.)
- **`query_ticks` and `get_available_tickers` accept the same flexible `date=` forms
  as `read_ticks` / `ingest_period`** — a day `"YYYYMMDD"`, month `"YYYYMM"`, year
  `"YYYY"`, or a `"start-end"` range — matched against the store's Hive `date`
  partition, instead of only a single exact `YYYYMMDD` day. Building a store with a
  month and then querying it with that same month string now works. Delegates to
  `parse_period`, so the accepted syntax and error messages are identical across the
  read and query paths. (Report B3.)
- **`query_ticks` and the batched Stage-2 query defensively drop a stray Hive
  `ticker` column** if a past or future DuckDB derives one from the
  `ticker=NNNN.parquet` filename. No NEEDS output schema has a literal `ticker`
  column (codes are `Stock Code` / `Index Code`), so the drop is always safe and
  makes the store path resilient across DuckDB versions regardless of the floor.
  (Report A2.)

## [0.14.1] - 2026-07-13

Round-16 post-deployment bug-hunt fixes: consistent no-data signalling and
argument validation across the one-shot (`read_ticks`) and store (`query_ticks`,
`extract_to_store`) read paths. No parse/clean output changes — every fix is a
guard, a validation, or a warning; `black`/`flake8`/`mypy` clean and the full
synthetic suite is green.

### Fixed
- **`start_time`/`end_time` on a `*_summary` type now raises `ValueError` at every
  entry point, before any work.** The two summary types are daily aggregates with
  no `Execution Time` column, so only `read_ticks` rejected a time filter;
  `query_ticks`, `_query_extract_batch` and `extract_to_store` passed it through to
  DuckDB, which failed with a raw binder error (`Referenced column "Execution
  Time" not found`) — and `extract_to_store` did so only **after** running the
  full Stage-1 ingest and leaving a partial store on disk. All four paths now
  share one `validate_time_filter_support` guard; `extract_to_store` validates up
  front, so the call fails in ~0 s with no wasted ingest.
- **`query_ticks` now emits the capturable `NoDataWarning` on a zero-row result,
  matching `read_ticks`.** A store query that resolves to nothing — a date not in
  the store, a code never ingested, or a time window that excludes every row —
  previously returned a typed-empty frame silently; the two documented read paths
  now signal "no data" the same way (silenceable via
  `warnings.filterwarnings("ignore", category=tse_tick.NoDataWarning)`).
- **An unrecognized `language` now raises `ValueError` instead of silently
  returning raw undecoded codes.** Only `"en"` and `"jp"` are valid; a value such
  as `"ja"` fell through to an empty categorical-decode map and returned raw NEEDS
  codes (`"11"`, `"1"`) with English headers and no warning. `read_ticks` and
  `create_df` now validate via `validate_language` — covering `export_to_csv` and
  the `ingest_*`/`extract_to_store` paths transitively — with a message pointing
  at `"jp"`. (`language="jp"` has always produced full Japanese headers **and**
  values; it is the correct value for Japanese output.)

### Docs
- **`discover_zips` docstring** now notes that the two index types search both the
  current `…110` and the legacy 2016 `…010` record-code prefixes (the recursive
  fallback already did this; the wording implied a single prefix).

## [0.14.0] - 2026-07-12

Two-stage extraction audit for `individual_stock`: one high-severity Stage-2
data-loss fix, resume/robustness fixes, large multi-core speedups, and
first-run UX. Behavior changes: share-class family semantics, zstd store
default, parallel-by-default where spawn is provably safe.

> **Before releasing from a workstation with real NEEDS data:** run
> `benchmarks/run_correctness.py` and `pytest tests/test_real_data.py` — the
> `clean_data` rewrite is hash-verified byte-identical on synthetic data for
> all four types (en+jp), but the real-data byte-identity gates skip in CI.

### Fixed
- **Suffixed share-class rows are no longer silently dropped by Stage 2
  (HIGH).** Stage 1's field-5 filter has always kept a code's whole family
  (a `{"7203"}` request also ingests New Shares `72031` into its own
  `ticker=72031.parquet`), but Stage 2 selected files by exact stem — those
  rows were paid for and unreachable, so `extract_to_store("7203")` returned
  fewer rows than `read_ticks("7203")` on real data, and a raw `"72031"`
  request matched **nothing** anywhere. A 4-char code now selects its whole
  family end-to-end (`read_ticks`, ingest + resume coverage, `query_ticks`,
  `extract_to_store`); a longer code is rooted to its family by the raw-read /
  two-stage entry points, while `query_ticks(ticker="72031")` (5-char form)
  still reads exactly that class off a built store.
- **Zero-row days no longer re-scan on every resume.** A filtered day whose
  ticker never traded wrote nothing — no partition, no coverage marker — so
  every resumed run re-probed and re-read its parts forever. A cleanly-read
  zero-row day now records its coverage marker (in an otherwise-empty
  `date=` dir) and resume skips it; a day that lost parts still writes no
  marker and stays fully re-ingestable; `get_available_dates` skips
  marker-only dirs.
- **One malformed quote value no longer aborts a whole day.** The strict
  Float64 pre-casts of `Buy Quote 1 Best` / `Buy Quote Vol 1` are gone; both
  follow the same non-strict path as their 51 sibling quote columns
  (malformed → 0).
- **`extract_to_store` no longer hides Stage-1 losses.** Lost parts / failed
  dates now raise a capturable `PartialIngestWarning` naming the affected
  dates (they stay resume-eligible) instead of silently returning a frame
  missing those rows.
- **Missing DuckDB fails fast and guided.** `extract_to_store` without the
  `[query]` extra raises `ImportError` pointing at `pip install
  tse-tick[query]` *before* Stage 1 runs; the top-level query shims name the
  extra too.
- Partscan's `_part_contains` EOFError edge (a truncated part silently cut
  the backward run walk, dropping earlier parts of the run) is gone with the
  containment scan itself (see Performance).

### Performance
- **`clean_data` batched (2.15× on 4 cores; scales with cores).** ~80
  one-expression `with_columns` calls (int/float casts, time slices, ~90
  strips) collapsed into per-family batches — Polars parallelizes expressions
  within a call. The categorical decode is ONE expression per column, all in
  one batch (was: a full-column `unique()` Python round-trip plus one
  when/then pass PER unknown value, across six duplicated branches). Output
  hash-verified byte-identical for all four types × en/jp.
- **Part selection is arithmetic.** `select_parts_for_day` bounds a ticker's
  run with two bisects over the probed first-line start codes — no part is
  decompressed beyond its first line during selection (was: a full per-line
  Python scan of the run-terminating part per ticker per day, plus re-opened
  holding parts). Over-selects at most one boundary part, only on exact
  start==code equality.
- **zstd store default.** ~30% smaller and ~3× faster to read than snappy on
  this data (`results_format.csv`). `compression=` (`"zstd"`/`"snappy"`) is
  plumbed through every ingest entry point and the CLI (`--compression`);
  codecs are per-file, so existing snappy stores read fine and resume can
  extend them — no re-ingest.
- **Threaded per-ticker writes.** A date fanning out into ≥16 `ticker=` files
  writes them across a bounded 8-thread pool (−36% zstd / −40% snappy on the
  write step; Polars' Rust writer releases the GIL). The sequential
  temp→`os.replace` commit loop and all-or-nothing cleanup are unchanged.
- `_coerce_time_cols` no longer materializes a `drop_nulls()` copy of ~90
  String columns per partition write (its String-dtype branch could never
  fire); it now only considers `pl.Time` / `pl.Object` columns.

### Added
- `max_workers="auto"` (logical cores, still RAM-capped by `_cap_workers`) on
  `ingest_period`, `ingest_year_from_root`, `ingest_directory`,
  `extract_to_store`. Their default is now `None`: the `TSE_TICK_MAX_WORKERS`
  env var (int or `auto`) when set; auto in an interactive session
  (Jupyter/REPL — spawn has nothing to re-import there, so no `__main__`
  guard is needed); serial from a script, with a one-time hint. CLI
  `--parallel` defaults to `auto` on `ingest` and (new flag) `export`.
- `PartialIngestWarning` (exported), `compression=` parameters,
  `--compression` CLI flag.
- First-run guardrails: a nonexistent `input_root` raises `FileNotFoundError`
  on the structured/period path (was: `"Done: 0 succeeded, 0 failed"`);
  zero-ZIP discovery emits a capturable `NoDataWarning` naming root/type/
  scope; structured-root progress lines carry `[i/N]` and resumed runs log a
  skipped-dates summary.
- CLI `export --store` accepts **multiple** `--tickers` (was: silently fell
  back to a capped one-shot read without building the store).
- `examples/scripts/example_basic_usage.py` demonstrates the two-stage
  pipeline (and no longer imports pandas).

### Changed
- Share-class family semantics (see Fixed) — a behavior change for callers
  who relied on `extract_to_store`/`query_ticks` returning only the exact
  4-char code's file.
- Store compression default snappy → zstd (see Performance).
- Tests: suite **447 passed / 50 data-gated skips** (new:
  `test_family_codes.py`, `test_zero_row_resume.py`, `test_compression.py`,
  `test_input_validation.py`; partscan exact-selection pins updated for the
  boundary-equality supersets).

### Deferred (future work)
- Int64→Int32/Float32 dtype narrowing (halves the full-frame day and doubles
  the RAM-capped worker count, but breaks the store schema — needs a
  re-ingest and a migration note).
- Lazy/streaming rewrite (`scan_csv` → `sink_parquet`) and the Polars GPU
  engine: the pipeline is I/O+parse bound and DuckDB Stage 2 has no GPU path;
  revisit if profiles change.

## [0.13.3] - 2026-07-12

Fixes for the run14 real-data acceptance-test bug report — the analytics/export
layer (the core read/ingest/query pipeline verified production-solid across all
four types and both eras). No public API signatures change.

### Fixed
- **`compute_volatility` no longer returns NaN/inf on a standard `individual_stock`
  frame (Finding 1, silent wrong results).** A liquid day is ~94% quote-only book
  rows, which carry `Execution Price = 0`; the estimator took log-returns over
  those zeros (`log(0)` → `-inf`/`NaN`) and the rolling window propagated the
  poison, so a real 09:00–11:30 TYO:7203 frame (120,192 rows, 112,793 quote-only)
  came back 112,793 NaN + 1,035 inf with only ~6,363 corrupted "finite" values.
  `compute_volatility` (both `realized` and `garman_klass`) now excludes non-trade
  rows (`Execution Price > 0` and a parseable time) before any log-return/OHLC; on
  that same real frame it now yields all 7,399 trade-row values finite (0 NaN, 0
  inf), identical to the trades-only reference. Propagates through
  `compute_all_features`.
- **Feature "undefined" values are `null`, never `NaN` (Finding 4).**
  `compute_volatility` marked warm-up / undefined positions with `NaN`, so
  `df.drop_nulls()` silently kept them (and they broke null-aware aggregations);
  it now emits `null` for non-trade rows and windows with no realised return,
  matching sibling `compute_spread` / `compute_flow_imbalance`. Its result is now
  aligned to the input frame's rows (the documented `compute_spread` convention)
  rather than an internal time-sorted order.
- **`extract_event_window` no longer leaks a Polars `String → Date` deprecation
  (Finding 2, forward-compat).** The `seconds_from_event` path fed a `"YYYY-MM-DD"`
  string to the shared `_tick_datetime_expr`, whose `cast(pl.Date)` on a string is
  deprecated (a hard error in Polars 2.0) and printed an un-suppressable warning
  from Polars' worker threads to stderr. It now passes the event day as a `Date`
  literal, so the cast is a no-op.
- **`export_to_csv(language="jp")` writes UTF-8 with a BOM (Finding 3).** The JP
  CSV was BOM-less UTF-8, which Excel on a Japanese Windows locale renders as
  mojibake (`レコード種別` / `東証` → garbage); it is now written `utf-8-sig`. The
  `en` export is ASCII and stays BOM-free; Polars/pandas readers strip the BOM
  transparently.

## [0.13.2] - 2026-07-12

Fixes for the ingest / raw-parse audit findings (H1–H2 high, M1–M4 medium) on the
`individual_stock` ingest and raw-ZIP→DataFrame paths.

### Fixed
- **Flat-path ingest no longer overwrites multi-part days (H1, data loss).**
  `ingest_directory` and `ingest_year` now group a flat folder's ZIPs by filename
  date token and ingest each trading day as ONE unit (all parts read and
  concatenated before the write), exactly like `ingest_period`. Previously each
  ZIP wrote independently, so NEEDS' closing-appendix part (which repeats tickers
  from earlier parts) clobbered each affected `ticker=` file down to its ~4 tail
  rows — silently. Their result dicts are now per **day** (`{"date", "parts",
  "rows", "output_path", ...}`); ZIPs with no date token keep the per-ZIP shape.
  `ingest_single_zip`'s docstring now warns it is a single-part primitive.
- **Resume is coverage-aware (H2, silent wrong results).** Each written date
  partition now carries a `_ingest_coverage.json` marker recording whether it was
  a full or ticker-filtered ingest (coverage accumulates across runs). Resume
  skips a date only when its recorded coverage includes the current request — a
  store built for ticker A no longer resume-skips (and silently returns nothing
  for) a later `extract_to_store` / `ingest_period` request for ticker B, and a
  full ingest over a previously filtered store now completes it. Legacy stores
  without markers keep the old skip semantics for full requests; filtered
  requests skip only when every requested `ticker=` file already exists.
- **Per-part read errors are recorded and the day stays resume-eligible (M1).**
  A date group that loses parts (corrupt ZIP, parse failure) now returns
  `"errors": [...]` in its result dict and its coverage marker is flagged
  incomplete, so `resume=True` re-ingests the day instead of trusting a
  permanently partial one. The zip-bomb guards (entry count / compression ratio
  / member size) now raise `SuspiciousZipError`, which propagates instead of
  being logged-and-skipped by the generic per-ZIP handler two lines below.
- **`ingest_year` no longer ingests wrong-year files (M2).** The year filter now
  matches the filename date token's year; the old substring match let
  `year=2012` pick up `HTICST120.20201207.*.zip` ("20201207" contains "2012").
- **Suffixed stock codes no longer collide with their parent (M3).**
  `clean_data` keeps `Stock Code` raw (no more `"72031"` → `"72031New Shares"`),
  stock partition filenames use the full code (`ticker=72031.parquet` instead of
  truncating to `ticker=7203.parquet`), and the partition writer merges groups
  that resolve to the same target file — a parent + new-shares day used to crash
  the whole ingest (double `os.replace` of one temp file) or silently mislabel
  the suffixed rows as the parent.
- **Multi-member ZIPs are read in full (M4).** `create_df` (and everything on
  top of it) parses every file member of a ZIP; previously only
  `namelist()[0]` was read even though up to 5 members pass the entry guard, so
  members 2–5 were silently dropped.
- **`read_ticks` warns on an exact-fit row cap with data left unread (L1).**
  The ZIP loop broke at `total >= rows` but the `TruncationWarning` fired only
  when the result exceeded the cap, so a total landing exactly on `rows` with
  ZIPs still unread returned silently incomplete data. The loop now reads one
  row past the cap (the same overflow-by-one detection `query_ticks` uses) and
  warns whenever it stopped early; a genuine exact fit still does not warn.
- **Malformed dates no longer create an invisible `date=None` partition (L2).**
  A raw `Data Date` that `clean_data`'s non-strict parse nulled used to be
  filed under `date=None/` — unreachable by every date-scoped query and treated
  as a real date by resume. Both Parquet writers now drop such rows before
  partitioning and log a warning with the dropped-row count.

## [0.13.1] - 2026-07-11

Fixes for the 11 findings of the 0.13.0 two-stage extraction audit (TYO:7203,
2021–2023, `benchmark_extraction_7203/run_7203_2021-2023_twostage_v0.13.0/`):
data-corruption resume (B11), the unguarded-spawn crash (B1), `extract_to_store`
ergonomics (B2/B4/B5), DuckDB temp spill (B3), `YYYY-YYYY` periods (B8), and doc
drift (B3/B6/B9/B10 + stale test counts).

### Added
- **`extract_to_store(..., max_workers=N)` (B4).** The recommended two-stage one-liner
  can now use 0.13.0's parallel per-date ingest; it was pinned serial because the
  parameter was never passed through (~9.3 h serial vs ~79 min at 8 workers for a
  3-year single-ticker ingest, per the audit's measurements).
- **`parse_period` accepts `YYYY-YYYY` year ranges (B8).** `"2021-2023"` now works
  everywhere a period is accepted (`ingest_period`, `extract_to_store`, the CLI
  `--period`); previously the intuitive 3-year form raised `ValueError`.
- **`tse_tick.LargeResultWarning` (B2).** `extract_to_store` deliberately returns all
  rows (no cap — that stays); past ~10M rows it now emits this capturable warning
  before materializing, pointing at bounded `query_ticks` slices of the just-built
  store (a 3-year active-ticker frame is tens of GB and can OOM the machine).

### Fixed
- **Interrupted ingest can no longer corrupt a partition that `resume=True` then
  trusts forever (B11 — observed live as an unreadable `date=20220511` partition).**
  Partition writes are now atomic: every file is written to a hidden temp name and
  `os.replace()`d into place, per-date as a unit (a multi-file date installs all
  files or none). The resume check additionally validates the Parquet footer magic
  of existing partition files and deletes + re-ingests a truncated one instead of
  skipping it. The event-window writer (which rewrites a date file to append) is
  atomic too, so an interruption no longer destroys previously accumulated rows.
- **Unguarded top-level parallel ingest now fails with an actionable error (B1).**
  `max_workers > 1` uses `spawn` workers (deliberate — `fork` deadlocks Polars),
  which re-import the calling script; an `ingest_*` / `extract_to_store` call at
  module top level re-ran itself in every worker and died with the cryptic stdlib
  `freeze_support` RuntimeError. The re-execution is now detected and raises a
  RuntimeError that shows the required `if __name__ == "__main__":` guard, which is
  also documented in the `max_workers` docstrings, README, ARCHITECTURE, and the
  evaluation notebook.
- **`extract_to_store` on a reused store returns exactly `period` (B5).** Its Stage-2
  query ran with `date=None` for any multi-day period, silently returning every day
  the store held (e.g. 2021+2022 after two consecutive yearly extracts). The query is
  now scoped to the period's inclusive date bounds.
- **Summary-type dates now resume-skip.** The resume check only globbed
  `ticker=*.parquet`, which the date-partitioned summary stores don't contain, so
  their (daily-token) dates re-ingested on every resumed run (found while fixing B11).
- **DuckDB spill moved out of the working directory (B3).** Query connections set
  `temp_directory` to `<system temp>/tse_tick_duckdb_spill`; a whole-store
  `query_ticks` was observed spilling 31 GB into an orphaned `./.tmp/` in the
  caller's cwd when interrupted.
- **Notebook `02_evaluation` no longer aborts on a stale editable install (B7).** The
  SETUP cell warns (RuntimeWarning) on a `__version__` vs dist-metadata mismatch
  instead of raising before any check runs.

### Changed
- **README/ARCHITECTURE corrections (B3/B6/B9/B10).** The "no row cap" claim is scoped
  to `extract_to_store` only (`query_ticks` defaults to the 10M cap and warns — the
  cap warning box now names both `read_ticks` and `query_ticks`); the two-stage README
  example shows guarded `max_workers`; the CLI `--parallel` row and the security
  tables reflect the 0.13.0 cores+RAM worker cap (the flat "8" is gone); test counts
  updated (430 collected; 382 pass / 48 data-gated skips without data).

### Tests
- `tests/test_audit_fixes.py` (+16): planted-truncation resume recovery, atomic /
  all-or-nothing writes, event-window write safety, spawn-bootstrap detection (unit +
  end-to-end subprocess), `extract_to_store` passthrough / period scoping / warning,
  `YYYY-YYYY` parsing and end-to-end multi-year ingest, DuckDB temp-directory config,
  summary resume-skip. Full suite: 430 with-data / 382+48-skip without.

## [0.13.0] - 2026-07-09

Parallel per-date ingest with a RAM-aware worker cap (#43) and a single-scan
`extract_to_store` query (#44); output-preserving (same Parquet store, same queried rows).

### Performance
- **Parallelized the per-date ingest loop and honored `max_workers` on the structured-root
  path (#43).** `ingest_period` / `ingest_year_from_root` (used by `extract_to_store` and
  the CLI `--period` / `--year`) processed trading days one at a time on one core; the
  `max_workers` argument and the CLI `--parallel` flag were a **silent no-op** there. Each
  date is an independent unit (read its parts → concat → clean → write one `date=`
  partition), so they now dispatch across a **`spawn`-started** process pool when
  `max_workers > 1` (default `1`, serial; `spawn` avoids a `fork`-after-Polars deadlock —
  `fork` copies Polars' thread-pool lock state but not its threads, hanging the worker). The store is **byte-identical** to the serial path and the results list is
  sorted by date for determinism; the per-date part-prune stays inside the worker so it
  remains interleaved with each day's write (#39 incremental progress preserved). Measured
  **2.3× on 4 workers and 3.6× on 8 workers** on a 16-core box for a ticker-filtered
  multi-day ingest (388 s → 169 s → 107 s).

### Changed
- **RAM-aware parallel-ingest worker cap (#43).** Each worker process holds a whole trading
  day's frame, so the cap is by the machine's logical cores **and its available RAM** — not
  a flat 8, and *not* a naive `os.cpu_count()`, which would OOM a full-frame parallel ingest
  (one busy `individual_stock` day is many GB). The cap estimates per-worker memory (small
  for ticker-filtered / summary / index; sized from the largest day's part bytes for
  full-frame `individual_stock`) and clamps workers so `N × per-worker` stays within 70 %
  of available RAM, with a warning; ticker-filtered ingests parallelize freely. Each
  worker's Polars thread pool is also bounded (`cores // concurrency`) so N processes don't
  oversubscribe. The remaining `max_workers` no-ops (the flat `ingest_year`, the event-window
  builder) now log a warning instead of silently ignoring the flag, and the per-date
  `gc.collect()` calls were kept (full-frame ingest is memory-critical).

### Fixed
- **`extract_to_store` no longer issues an N+1 per-ticker query (#44).** Its Stage-2 step
  opened a fresh DuckDB connection and re-globbed the whole store for **every** ticker, and
  for the two `*_summary` types each per-ticker call re-scanned the **entire** store. It now
  runs **one connection and one scan** for all tickers, building every ticker's file list
  from a single store walk (the summary types scan the store once with an `IN`-list instead
  of N times). It returns the **same multiset of rows in the same `(code, Data Date,
  effective-time)` order** as the old loop; the `*_summary` types (one row per (code, date))
  are fully deterministic and byte-identical. For the tick types the order *within* a
  same-`(date, time)` tie is arbitrary — but it already is in the current code:
  `query_ticks` orders via DuckDB's parallel sort, which does not fix a tie order, so two
  runs of the existing per-ticker path differ only in the position of same-timestamp rows
  (measured: 16 of ~912k rows for one ticker-month). Verified on real data *with*
  same-second ties across single / multiple / absent tickers and tick / summary types.
  `query_ticks` is unchanged; scoped to the extract path's fixed `limit=None`.
- **`ingest_directory(..., max_workers>1)` no longer crashes.** Its process-pool task was a
  local closure, which cannot be pickled under the `spawn` start method (Windows/macOS), so
  `--flat --parallel N` raised `Can't pickle local object`. The task is now a module-level
  function; the parallel store is byte-identical to serial.

## [0.12.2] - 2026-07-08

Faster ticker-filtered `individual_stock` reads and ingests (performance only; output unchanged).

### Performance
- **Vectorized the field-5 ticker filter in `get_1y_dataframe`.** The
  `individual_stock` ticker fast path filtered raw lines with a pure-Python per-line
  loop (byte-scanning field 5 via `extract_stock_code`), which roughly doubled each
  opened part's read time and was the dominant CPU cost of a ticker-filtered
  read/ingest once part-pruning had narrowed which parts open. It now streams each
  part in bounded blocks and extracts field 5 with a vectorized Polars filter —
  **~2× faster per opened part** (measured 183.5 s → 91.2 s, 2.01×, opening all 13
  parts of a real Toyota `7203` day) with a **byte-identical** kept-line set (verified
  line-for-line on real multi-part days, including the off-auction appendix part).
  Memory stays bounded: only matching lines are handed to Polars, so peak RAM tracks
  the matched rows plus one 16 MB block, never the whole decompressed part.

### Fixed
- **Ticker-filtered ingest now prunes per day, not the whole period upfront.**
  `ingest_period(..., ticker_filter=...)` for `individual_stock` pruned every day's
  parts before writing any partition — for a year that was ~80 min of probe/boundary
  scans (~20 s/day) with no partition and no checkpoint written first, and, because
  pruning ran before the per-date resume check, a resumed run re-pruned the whole
  period before skipping already-written dates. Pruning now runs per date inside the
  ingest loop, after the resume-skip check: a partition lands after each day
  (incremental progress + per-day checkpoint), and a resumed run prunes only the dates
  it actually ingests. Store contents are unchanged.

## [0.12.1] - 2026-07-07

Bug fix: `extract_to_store` returns all rows (no 10M query cap), plus refreshed example notebooks.

### Fixed
- **`extract_to_store` no longer truncates a very active ticker at 10M rows.** It
  queried the store via `query_ticks` with the default `limit=10_000_000`, so a
  whole month of a high-volume ticker (e.g. SoftBank `9984` — >10M rows/month) came
  back capped at 10M (partial days), with a `TruncationWarning`. It now queries with
  `limit=None`, returning **all** of the ticker's rows for the period — matching the
  "no 10M cap" the two-stage path promises. Found by running the example notebooks
  against a full month of `["7203", "9984"]`.

### Documentation
- **Example notebooks refreshed for 0.12.0** (`01_basic_usage`, `02_evaluation`):
  `read_ticks` is part-pruned (not "every part"); the two-stage section leads with
  `extract_to_store` taking one *or many* tickers with no row cap.

## [0.12.0] - 2026-07-07

Multi-ticker `extract_to_store` and clearer guidance for large / multi-ticker reads.

### Changed
- **`extract_to_store` accepts one *or many* tickers.** Its `ticker` argument now
  takes a string **or an iterable** (`"7203"` or `["7203", "9984"]`) — the tickers are
  ingested into the store in one part-pruned pass and returned concatenated. This makes
  a whole month of several active tickers a single call with no 10M-row cap (the
  one-shot `read_ticks` limit that truncated such reads). Single-ticker calls are
  unchanged. An absent ticker contributes no rows (its empty per-ticker frame — which
  `query_ticks` returns without the `date` partition column — is dropped before concat).

### Documentation
- **Large / multi-ticker reads and the 10M-row cap.** README and the `read_ticks`
  docstring now spell out that a one-shot read caps at 10,000,000 rows (with a
  `TruncationWarning`) — a whole month of a couple of active tickers exceeds it —
  and give the three ways to read everything: two-stage `ingest_period` →
  `query_ticks` (or `extract_to_store`), a per-day loop, or `rows=None`.

## [0.11.6] - 2026-07-07

Faster single-ticker `individual_stock` reads via part-pruning, plus a one-call two-stage helper.

### Added
- **Part-pruning for `individual_stock` single-ticker reads.** NEEDS numbers each day's TICST120 parts
  in ascending stock-code order, so a ticker's rows sit in a short contiguous **run** of parts (plus a
  trailing off-auction/special-records **appendix** in the day's last part). `read_ticks(...)` now
  probes each part's first record, opens only that run **∪ {last part}**, and falls back to opening all
  parts if the ascending-code layout can't be confirmed — so results are **row-for-row identical** to a
  full scan, only faster (~5-7× typical; validated 18/18 days on a 3-year Toyota 7203 sample). New
  `read_ticks(..., prune_parts=True)` (default; `False` forces a full scan). New module
  `tse_tick/partscan.py`.
- **`extract_to_store(input_root, output_dir, period, ticker, ...)`** — two-stage in one call: ingest a
  ticker for a period into a reusable, part-pruned Parquet store, then return the queried DataFrame.
  The recommended path when the data will be read more than once. Requires the `[query]` extra.
- **`tse-tick export --store <dir>`** — build a reusable store while exporting a single
  `individual_stock` ticker (two-stage).

### Changed
- **Ticker-filtered `individual_stock` ingest is part-pruned** (`ingest_period` / `_ingest_grouped`), so
  the two-stage store build opens only the ticker's parts. Store contents unchanged.
- The field-5 stock-code parse used by the raw-byte fast path is now the single shared
  `partscan.extract_stock_code`.

## [0.11.5] - 2026-06-28

Fixes from an alpha-test report — three findings, hardened after a full code review that caught a
data-loss regression in the first pass.

### Fixed
- **Large one-shot reads no longer crash uncatchably.** A normal multi-part `individual_stock` day
  could exhaust memory and raise an **uncatchable** Polars `PanicException` (it subclasses
  `BaseException`, so `except Exception` can't catch it). `create_df` / `read_ticks` now raise a
  catchable `OneShotMemoryError` (a `MemoryError`) — either proactively, when the cumulative
  decompressed size of the parts they would load crosses a ceiling (default 5 GB), or by converting a
  Polars panic during the load — with guidance to use the two-stage `ingest_single_zip()` →
  `query_ticks()` path. The bounded `individual_stock` `ticker_filter` fast path is exempt (it keeps
  only matching lines), and the `ingest_*` functions **re-raise** the error rather than swallowing it,
  so a too-large read aborts loudly instead of silently writing a partial day.
- **`create_df` honors an explicit `year=` (and `data_type=`) under the default `auto_detect=True`.**
  It now auto-detects only whichever you leave as `None`, so a correctly-named ZIP in a folder whose
  path has no year reads when you pass `year=` (it previously raised `Could not detect year from path`).
- **`query_ticks` no longer silently truncates at `limit`.** Hitting the cap now emits the same
  capturable `TruncationWarning` that `read_ticks` uses; a result that *exactly* fills `limit` with
  nothing dropped does not warn (it probes one row beyond the cap to tell the two apart).

### Added
- **`max_oneshot_bytes=`** on `create_df` / `read_ticks` — the one-shot decompressed-size ceiling
  (default 5 GB; pass a larger value for a high-RAM machine, or `None` to disable the guard).
- **`OneShotMemoryError`** (exported) — the catchable signal for an over-large one-shot read.

## [0.11.4] - 2026-06-19

Internal consolidation (**no API or behavior change**) — the durable follow-up to the run9/10/11 pattern
of fixes drifting into new inconsistencies.

### Changed
- **Data-type classification is now single-sourced.** The "which types are X" checks were duplicated as
  literal tuples/sets in ~20 places across 8 modules (`valid_types`, plus the summary / tick / index
  classifications), free to drift — the root cause behind several past inconsistencies. They now derive
  from one source in `tse_tick.constants` (`DATA_TYPES` / `VALID_DATA_TYPES`, `SUMMARY_TYPES`,
  `TICK_TYPES`, `INDEX_TYPES`, and a `validate_data_type()` helper, all tied to the `DataType` enum).
  Validation messages are byte-identical and no public name changed. A new invariant test suite guards
  against the drift returning (including that `get_info()`'s field counts stay derivable from the schemas).

### Fixed
- **The evaluation notebook (`examples/notebooks/02_evaluation.ipynb`) reloads a freshly-installed release
  without a kernel restart** — its install cell now purges `tse_tick` from `sys.modules` and re-imports
  after `pip install -U`, and SETUP errors if the imported version ≠ the installed one.

## [0.11.3] - 2026-06-19

Fixes from an eleventh real-data run: a Major flat-folder discovery bug plus two consistency papercuts.
Fixed consistency-first — each change removes a divergent code path rather than adding another. Also ships
a standalone, README-conformant evaluation notebook.

### Fixed
- **A flat folder of a monthly-packaged type + a single-day date no longer returns a false-empty result.**
  `read_ticks(r"…\TICIT110\202305", data_type="indices", date="20230508")` returned `(0, N)` with a
  misleading `NoDataWarning` even though the day is inside that month's ZIP: the flat-folder branch matched
  the date token as a filename *substring*, so a day (`20230508`) never matched a monthly file
  (`…202305.zip`). The flat path now resolves dates via `discover_zips` — the same day→month logic the
  structured-root path already used — so a single day maps onto its monthly file (then the existing
  day-prune trims it). Affects `indices` / `indices_summary` / `stock_summary` (both eras); daily-packaged
  `individual_stock` was unaffected and still matches by day.
- **`get_info()` no longer prints and returns** (so `print(tse_tick.get_info())` showed the banner twice).
  It now returns the string only — print it to display.

### Changed
- **`get_supported_years()` is consistent and documented.** It returned a dynamic `(2016, current_year)`
  (e.g. `(2016, 2026)`) that disagreed with the `get_info()` banner (`2016-2025`) and had no docstring.
  Both now derive from one `_SUPPORTED_YEARS = (2016, 2025)` constant; `get_supported_years()` and
  `get_version()` are documented.

### Added
- **A standalone evaluation notebook** (`examples/notebooks/02_evaluation.ipynb`) — a README-conformant
  acceptance test that exercises every documented access pattern (one-shot `read_ticks`, two-stage
  `ingest_period` → `query_ticks`) for all four data types across both eras, with per-case pass/fail/skip
  checks (column counts, dtypes, ticker/time filtering, one-shot↔store consistency, no-data handling) and
  an overall verdict. Point it at a NEEDS root via `TSE_TICK_DATA_ROOT` (or edit `DATA_ROOT`) and *Run All*
  to validate a release; standardizes QA runs on the documented usage so they don't drift into edge paths.

## [0.11.2] - 2026-06-19

Fixes from a tenth real-data run: a Major projection-correctness bug on `indices`, plus API-surface and
message papercuts. (Otherwise clean — 0 blockers, and the 2016 legacy `…010` era path works.)

### Fixed
- **`read_ticks(data_type="indices", columns=<subset>)` no longer returns the whole month (~20× inflated)
  when the projection omits `Data Date`.** The monthly day-prune runs at the end and needs `Data Date`,
  but the column projection was applied per-part **before** it — so dropping `Data Date` silently skipped
  the prune and returned every day in the file. Projection now happens **after** all filtering (code,
  time, and the day-prune), so a `columns=` subset returns exactly the requested day's rows. (Latent for
  all monthly types; `indices` surfaced it because the report combined a single-day + time + projection.)
- **`get_info(path)` raises a guiding `ValueError` instead of a raw `TypeError`.** `get_info()` describes
  the package and takes no dataset path; passing one now explains how to inspect data (`read_ticks` /
  `get_available_*`) rather than failing with "takes 0 positional arguments".

### Changed
- **`tse_tick.ingest` (the submodule) no longer clutters `dir(tse_tick)`.** `dir()` now lists the curated
  public API (via `__dir__`), so the bare `ingest` module no longer sits next to `ingest_period` etc. (a
  novice trying `tse_tick.ingest(...)` got "'module' object is not callable"). The submodule is still
  importable (`tse_tick.ingest.…`) and now carries a docstring pointing to the real entry points.
- **An empty `ticker_filter=set()` is named in the no-data warning** (`… ticker_filter=[]`) instead of
  being dropped — an accidentally-empty filter matches nothing, and omitting it wrongly implied no filter
  was applied.
- **Author metadata is consistent.** `get_info()` / `__author__` and the package metadata now both list
  all three authors (Kazumi Li, Masataka Hayashi, Peter Romero); the maintainer email is unchanged.

## [0.11.1] - 2026-06-19

### Fixed
- **A bare `ticker_filter` code no longer misbehaves.** Passing a single code as a `str` (e.g.
  `ticker_filter="101"`) was iterated character-by-character into `{'1', '0', '1'}` — matching nothing and
  returning a silent typed-empty result — and passing an `int` (e.g. `ticker_filter=101`) raised a raw
  `TypeError`. A bare `str`/`int` is now treated as a one-element filter on both read entry points
  (`read_ticks` and `create_df`); a `set`/list/iterable of codes is unchanged.

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
