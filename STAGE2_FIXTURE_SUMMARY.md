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
| Before | 160 | 42 | 118 |
| After  | 160 | **104** | **56** |

Stage-2 modules (before → after passing):

| Module | Before | After |
|---|---:|---:|
| `test_query.py` | 0 | 14 (1 skip) |
| `test_features.py` | 0 | 20 |
| `test_parquet_io.py` | 0 | 14 |
| `test_event_window.py` | 8 | 22 (14 `extract_*` enabled) |

## Remaining skips (56) and why

- **`test_real_data.py` (47)** — gated on real NEEDS files at hardcoded
  `G:\flash_crash_pilot\…` / `G:\HTIC*…` paths. Legitimately real-data only.
- **`test_ingest.py` (8)** — 5 need the real `2022/202202/HTICST120.20220201.1.zip`;
  3 are `stock_summary` / `indices` / `indices_summary` auto-detect stubs, out of
  the individual_stock fixture's scope (skip reasons updated to say so).
- **`test_query.py` (1)** — `test_query_ticks_indices_data_type`. The indices
  ingest decodes `Index Code` (`"101" → "Nikkei 225"`) *before* partitioning, so
  the indices store writes a garbled `ticker=Nike.parquet` filename; index ticker
  queries need a separate ingest-side fix. Documented as a follow-up, not a
  fixture gap.

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

## Files created / edited this pass

**New:** `tests/synthetic_data.py`, `tests/conftest.py`, `STAGE2_FIXTURE_SUMMARY.md`.

**Product code:** `tse_tick/__init__.py` (version 0.2.2→0.2.3), `tse_tick/query.py`
(fixes 1–3), `tse_tick/io/parquet.py` (fix 4), `tse_tick/features.py` (fix 5).

**Tests:** `tests/test_query.py`, `tests/test_features.py`,
`tests/test_event_window.py`, `tests/test_parquet_io.py` (skip-stubs replaced
with real bodies); `tests/test_ingest.py` (3 skip reasons made precise).

**Docs:** `CHANGELOG.md` (`[0.2.3]` Added + Fixed), `README.md` (new Testing
section), `technical_paper/main.tex` (Table 10 + §7 prose + §9 limitation).

## Surface agreement (all four reconciled to the passing suite)

- **Testing status:** package suite (ground truth) = 104 pass / 56 skip; README
  Testing section and paper §7/Table 10/§9 state the same.
- **Field counts:** unchanged, all on the output-count convention — TICST120 = 95,
  TICSS110 = 82 (83 raw), TICIT110 = 10 (23 raw, 15 in 2016), TICIS110 = 17.
- **Version:** `0.2.3` in `__init__.py` and CHANGELOG.
- **Partition layout:** `individual_stock/date=YYYYMMDD/ticker=NNNN.parquet`
  (and event-window `year=/month=/DATE.parquet`) consistent across code, README,
  and paper.
