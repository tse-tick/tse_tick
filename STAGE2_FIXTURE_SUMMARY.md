# Stage-2 Synthetic Fixture — Implementation Summary

Carry-back note for the tse_tick Claude project. This pass unblocked the
previously-skipped Stage-2 tests by building a **synthetic Parquet fixture**, so
the suite runs with no proprietary NEEDS data. Package version bumped
`0.2.2 → 0.2.3` (the unreleased `[0.2.3]` CHANGELOG section was already open).

## What the fixture is

Synthetic, obviously-fake **`individual_stock` (TICST120)** tick data, shaped
exactly like the headerless NEEDS CSV (95 quoted, comma-separated positional
fields). Three tickers (`7203`, `6758`, `9984`) across two trading dates
(`20230703`, `20230704`), 40 rows each per date, with timestamps split between
the morning (09:00–11:30) and afternoon (12:30–15:00) sessions around a real
lunch gap, and prices that vary row-to-row so order-book features are non-trivial.

## Where it lives / how it is built

- `tests/synthetic_data.py` — generators: `individual_stock_csv(...)` builds the
  raw CSV bytes from `get_schema_individual_stock_95()` (positional layout is
  guaranteed correct because values are emitted in schema order); `write_zip(...)`
  packs a NEEDS-style ZIP.
- `tests/conftest.py` — session-scoped fixtures:
  - `stock_store` — writes the synthetic ZIPs to a `tmp_path` and runs each
    through the **real** `ingest_single_zip(...)`, returning the store root.
  - `feature_ticks` — a clean 95-column TICST120 frame loaded from the store.
  - `events_csv` / `events_df` — a tiny synthetic event filter CSV/DataFrame for
    the event-window path.

**Run-through-real-ingest principle:** the Parquet is never hand-written. It is
produced by the same `create_df → write_partitioned_parquet` code path as
production; nothing large is committed and the store is always pipeline-fresh.

## Partition layout mirrored (at tiny scale)

```
{store}/individual_stock/
  date=20230703/
    ticker=6758.parquet
    ticker=7203.parquet
    ticker=9984.parquet
  date=20230704/
    ticker=6758.parquet
    ...
```

i.e. Hive `date=YYYYMMDD/` directories with one `ticker=NNNN.parquet` file per
ticker — identical in structure to the real ~5,186-ticker, 2016–2025 store, just
tiny. (The ticker is encoded in the **filename**, not a directory — this drove
two of the fixes below.)

## Before → after test counts

| | Total collected | Passed | Skipped |
|---|---:|---:|---:|
| Original (pre-fixture) | 160 | 42 | 118 |
| First pass (fixture)   | 160 | 104 | 56 |
| **Second pass (indices fix + paper examples)** | **165** | **110** | **55** |

Second pass added 5 new paper-example tests (160 + 5 = 165 collected) and moved
the 1 indices query test from skip to pass (so passes +6, skips −1).

Stage-2 modules (final passing):

| Module | Final |
|---|---:|
| `test_query.py` | 15 (incl. indices) |
| `test_features.py` | 20 |
| `test_parquet_io.py` | 14 |
| `test_event_window.py` | 22 |
| `test_paper_examples.py` (new, 2nd pass) | 5 |

## Remaining skips (55) and why

- **`test_real_data.py` (47)** — gated on real NEEDS files at hardcoded
  `G:\flash_crash_pilot\…` / `G:\HTIC*…` paths. Legitimately real-data only.
- **`test_ingest.py` (8)** — 5 need the real `2022/202202/HTICST120.20220201.1.zip`;
  3 are `stock_summary` / `indices` / `indices_summary` auto-detect stubs, out of
  the individual_stock fixture's scope (skip reasons updated to say so).

(The previously-skipped `test_query_ticks_indices_data_type` is now ENABLED — see
the indices partition fix below.)

## Product bugs found by the fixture and fixed (approved)

Running the tests for the first time exposed latent query/feature bugs (real
NEEDS data would have failed identically — these are not fixture artifacts):

1. **`query.py` — ticker filter** (`BinderException`): ticker is in the filename,
   which Hive partitioning does not expose. Now prunes by selecting matching
   `ticker=NNNN.parquet` files directly. *(Unblocked all event-window tests.)*
2. **`query.py` — time-range filter** returned wrong rows: `Execution Time` is
   `"HHMMSS"` but the filter compared `"HH:MM:SS"`. Colons are now stripped before
   comparison.
3. **`query.py` — column pruning** rejected every real column: the SQL-injection
   identifier guard's word-only regex rejected spaces. Replaced with a blocklist
   that still rejects `"` (breakout), `\`, `;`, backtick, and control characters.
   *(Security-relevant change — protection preserved.)*
4. **`io/parquet.py` — `read_parquet_partition`** date/ticker filters raised: the
   Hive `date` is an integer (cast to string for comparison) and the
   filename-encoded ticker isn't a partition field (matched on the in-file code
   column).
5. **`features.py` — rolling window default**: `window="5min"` is invalid in this
   Polars version (`m` = minutes). Added a normalizer mapping `"5min" → "5m"`
   while still accepting native Polars units.
6. **`io/parquet.py` — indices/indices_summary garbled partition filename**
   (2nd pass): `clean_data` decodes `Index Code` (`"101" → "Nikkei 225"`) for
   display *before* partitioning, so the store wrote `ticker=Nikk.parquet`,
   breaking ticker queries for 2 of the 4 data types. `write_partitioned_parquet`
   now reverse-maps the decoded display name (EN/JP) back to the raw code for the
   `ticker=` filename (and parses `Unknown (NNN)` codes); the in-file `Index Code`
   column keeps its decoded display value. The decode itself is unchanged
   (resolved decision). individual_stock is untouched (its `Stock Code` is not
   decoded). Verified via a synthetic TICIT110 ZIP through the real pipeline:
   filenames are now `ticker=101.parquet` / `ticker=113.parquet`, and
   `query_ticks(data_type="indices", ticker=101)` returns rows showing "Nikkei 225".

### Indices fork — decision

Chosen: **FIX** (reverse-map partition key) + enable the indices query test. The
fix touches only `io/parquet.py` (+ a lazy import of `core.get_schemas_categorical`);
`query_ticks` was already correct once filenames are right. **Re-ingest:** any
indices/indices_summary store built by the old code has garbled filenames and
must be re-ingested — none exists in the repo, and individual_stock stores are
unaffected, so this is effectively moot. **Coverage:** `test_query_ticks_indices_data_type`
is now enabled (10-field routing + raw-code ticker filter, via a minimal synthetic
TICIT110 `indices_store` fixture). `indices_summary`'s separate 83-col `set_columns`
routing remains untested (no fixture); the filename fix applies to it too but is
unexercised.

### Paper examples (2nd pass)

Verified Listings 1–4 (Section 6) exactly as printed: **all 8 top-level names**
(`create_df`, `export_to_csv`, `query_ticks`, `compute_spread/depth/flow_imbalance/
volatility/all_features`) are exposed at the top level (no missing exports), and
Listing 4 runs on **raw** `query_ticks` output (96 cols incl. the Hive `date`
column) — no paper-text or product fix was needed. Locked in by
`tests/test_paper_examples.py` (5 tests).

### Figure 2 (2nd pass)

`benchmarks/paper_assets/benchmark_figure.pdf` was stale (prototype bar 622.6s vs
Table 7's 631.405s). Regenerated **only the figure** from `results_engine_summary.csv`
via `generate_assets.generate_figure(...)` (it reads the CSV; no hardcoded value).
Did NOT run the full `generate_assets.main()` — its `FILE_CODE_LABELS` is out of
sync with the manually-corrected `engine_benchmark.tex` (Index Summary 17 vs the
script's 83) and a full run would reopen that completed fix. Prototype bar now
reads 631.4s; other bars unchanged.

## Files created / edited (cumulative across both passes)

**New:** `tests/synthetic_data.py`, `tests/conftest.py`, `tests/test_paper_examples.py`
(2nd pass), `STAGE2_FIXTURE_SUMMARY.md`.

**Product code:** `tse_tick/__init__.py` (version 0.2.2→0.2.3), `tse_tick/query.py`
(fixes 1–3), `tse_tick/io/parquet.py` (fix 4 + indices fix 6), `tse_tick/features.py` (fix 5).

**Tests:** `tests/test_query.py` (Stage-2 bodies + indices test enabled, 2nd pass),
`tests/test_features.py`, `tests/test_event_window.py`, `tests/test_parquet_io.py`
(skip-stubs replaced with real bodies); `tests/test_ingest.py` (3 skip reasons made precise).

**Docs / assets:** `CHANGELOG.md` (`[0.2.3]`), `README.md` (Testing section),
`technical_paper/main.tex` (Table 10 + §7 + §9), `technical_paper/CLAUDE.md`
(status/decisions); 2nd pass: `benchmarks/paper_assets/benchmark_figure.pdf`
(regenerated) and `benchmarks/paper_assets/performance_section.tex` (field-count
convention).

## Surface agreement (all four reconciled to the passing suite)

- **Testing status:** package suite (ground truth) = 110 pass / 55 skip; README
  Testing section and paper §7/Table 10/§9 reflect the synthetic-fixture status.
- **Field counts:** all on the output-count convention — TICST120 = 95,
  TICSS110 = 82 (83 raw), TICIT110 = 10 (23 raw, 15 in 2016), TICIS110 = 17
  (now also applied in `performance_section.tex`).
- **Version:** `0.2.3` in `__init__.py` and CHANGELOG.
- **Partition layout:** `individual_stock/date=YYYYMMDD/ticker=NNNN.parquet` and,
  after the 2nd-pass fix, `indices/date=YYYYMMDD/ticker=<rawcode>.parquet`
  (event-window `year=/month=/DATE.parquet`) — consistent across code, README, paper.
- **Query-validated data types:** `individual_stock` (ticker + time-range + event
  windows) and `indices` (ticker, via the synthetic indices fixture).
  `stock_summary` / `indices_summary` querying is not yet covered by a fixture.
