# Round 20 — Morsel-bounded parse & ingest (memory independent of day size)

**Status:** proposed · **Target release:** 0.14.6 · **Author:** Claude (with Kevin)
**Origin:** a real-data `ingest_period("2023-2025", ticker_filter={"7203","9984"})` died with
`BrokenProcessPool`, twice, on a 34 GB box.

---

## 1. Problem

A ticker-filtered `individual_stock` ingest allocates memory proportional to **one trading
day's rows**. Rows/day are growing (raw TICST120: 2017 = 101.9 GB → 2025 = 351.9 GB, +30%
then +16% year over year; largest months 202504 = 42.0 GB, 202510 = 36.5 GB), so the peak
grows with the data and the ingest OOMs on the biggest days.

### Measured (real data, `G:\NEEDS`)

| Unit | Rows (7203+9984) | Peak RSS |
|------|------------------|----------|
| `_ingest_date_group` `20230104` (normal day) | 640,424 | ~2 GB |
| `_ingest_date_group` `20250409` (April 2025) | 4,673,760 | **24.52 GB** |
| `_ingest_date_group` `20250407` | 4,436,030 | **24.64 GB** |

A **single date** needs ~24.5 GB — the whole box. The date is already the unit of work
(`_ingest_grouped` submits one future per date), so no worker-count setting fixes it.

### Root cause: a 4.6x parse transient, paid per part, then concatenated per day

One pruned part of `20250409`, filtered to 7203:

| | |
|---|---|
| Final cleaned frame | **1.95 GB** (2,563,684 x 95 = 760 bytes/row) |
| Peak RSS during `create_df` | **9.03 GB** |
| Transient blowup | **4.6x the final frame** |

`create_df` hands the *whole* filtered part to one `pl.read_csv`, materialising an
all-String frame (~243M string cells) that stays alive while `clean_data` casts it. The
finished frame is only 1.95 GB; 14 of 95 columns stay String. `_ingest_date_group` then
pays that spike **per part** and `pl.concat`s the day (another ~2x).

### Why the RAM guard cannot be fixed

`_estimate_worker_gb` sizes workers from file bytes. Bytes do not predict filtered rows:
the ticker filter keeps ~15% of a part on a normal day but ~100% on an extreme day (where
the two names dominate their parts). `20250409` opens **fewer** pruned bytes than
`20240403` (0.09 vs 0.13 GB) yet yields **4.7x more rows** (4.67M vs 0.99M). No statistic
available up front distinguishes them. This is a known dead end, not a tuning miss — see
§6.

---

## 2. Goal

Make peak memory of a filtered ingest **independent of the day's size**: bounded by a
constant morsel, not by rows/day. Then per-worker memory is a known constant, the RAM
heuristic stops mattering, and 2026+ data works by construction.

**Non-goals:** the unfiltered (full-frame) ingest path; the summary/index types; the
one-shot `read_ticks` return contract; the store layout.

---

## 3. Design

### 3.1 Morsel the parse (`enhanced.py`)

The filtered fast path already produces **only matching lines** as `raw_bytes`
(`_read_individual_stock_matches`). Instead of one `read_csv` over all of it:

```
_MORSEL_BYTES = 64 * 1024 * 1024      # ~64 MB of CSV ~= 180k rows @ ~350 B/row

_iter_raw_morsels(raw_bytes) -> yields newline-aligned byte slices of <= _MORSEL_BYTES
```

Per morsel: `pl.read_csv(BytesIO(morsel))` -> `_finalize_raw(...)` -> a cleaned frame.

`_finalize_raw(df_raw, data_type, language)` is the existing shared seam ("Name, clean, and
project a raw NEEDS frame"), already used by `create_df` and `_empty_typed_frame`. It is
reused **unchanged**, per morsel.

- `create_df` keeps its eager contract: `pl.concat([cleaned morsels])`. Peak drops from
  ~4.6x the final frame to ~(final + one morsel transient).
- A new generator `iter_clean_morsels(...)` yields the cleaned morsels for the ingest path
  (below) so a date is never materialised.

**Newline splitting is safe:** `_read_individual_stock_matches` already filters the part
line-by-line on raw bytes, so "records do not contain embedded newlines" is an assumption
the codebase already makes and validates. Morsels reuse it.

### 3.2 Stream the date's write (`ingest.py`)

`_ingest_date_group`, **when a `ticker_filter` is present** (bounded number of output
files), replaces read-all-parts -> concat -> write with:

```
for part in pruned_parts:
    for morsel in iter_clean_morsels(part, ...):
        for code, slice in morsel.partition_by("Stock Code"):
            writer[code].write_table(slice.to_arrow())   # append a row group
close all writers; os.replace(tmp -> final) per ticker file; write coverage marker
```

Peak = one morsel (~0.3-0.8 GB) + writer buffers, **regardless of the day's size**.

Both anchors already exist in-repo and are reused, not invented:
- `export_query` (`query.py`) already appends day-frames as row groups via
  `pq.ParquetWriter` / `write_table`, including a defensive `table.cast(writer.schema)`.
- `write_partitioned_parquet` (`io/parquet.py`) already writes each `ticker=` file via a
  pid-unique `.tmp` + `os.replace` (atomic on Windows and POSIX).

**Atomicity is preserved:** each ticker file is still published by a single `os.replace`
after its writer closes, and the coverage marker is still written only after all files
land. The date remains an atomic unit; only the *in-memory* concat disappears.

**Scope gate:** stream only when `ticker_filter` is present and small
(`len(ticker_filter) <= _MAX_STREAM_TICKERS`, 64). An unfiltered day would need ~4000
concurrent writers; it keeps the existing concat path and its full-frame estimate.

### 3.3 Retire the guessing (`ingest.py`)

With 3.2 the filtered worker's peak is a constant. `_filtered_worker_gb`'s per-code scaling
(`_TICKER_WORKER_GB`, added in 0.14.5 as a stopgap) becomes a constant morsel-sized
estimate, so `_cap_workers` stops clamping filtered ingests on a heuristic that cannot be
made correct.

---

## 4. Why this is output-identical

`clean_data` contains **no cross-row operations** — no `group_by`, `over()`, `sort`,
`join`, `shift`, `cum*`, `rolling`, or aggregation; every op is element-wise (casts,
`str.*`, `fill_null`, `replace`). Therefore:

```
concat_i( _finalize_raw(morsel_i) )  ==  _finalize_raw( concat_i(morsel_i) )
```

Morsels are newline-aligned and processed in file order, and parts are processed in part
order, so row order is unchanged. This is the property the whole design rests on; it is
asserted directly by tests (§5) rather than assumed.

---

## 5. Test plan

Synthetic-first, real-data-gated, per repo convention (`tests/test_round20_fixes.py`):

1. **Morsel parse identity** — a part read with `_MORSEL_BYTES` forced small vs the
   unbatched read: `df.equals(...)`, same row order.
2. **Morsel boundary** — a morsel size that lands exactly on and between record
   boundaries; no row split, dropped, or duplicated.
3. **Streaming ingest identity** — a multi-part synthetic day ingested via the streaming
   path vs the concat path: identical per-ticker Parquet (rows, order, schema, dtypes).
4. **Atomicity** — no `.tmp` left behind; a ticker file appears only complete; coverage
   marker written after files.
5. **Multi-ticker split** — a morsel containing rows for several codes routes each row to
   the right `ticker=` file.
6. **Scope gate** — an unfiltered / >64-ticker ingest still takes the concat path.
7. **Memory bound (real-data gated)** — `_ingest_date_group("20250409", {"7203","9984"})`
   peak stays under ~2 GB (was 24.52 GB), and the written store is **row-identical** to the
   0.14.5 output.
8. **Guard** — a filtered estimate is a morsel-sized constant, no longer scaling per code.

Definition of done: full suite green (with-data profile), `flake8`/`mypy` no new findings,
real-data row-identity proved, and the measured before/after peak recorded in the CHANGELOG.

---

## 6. References

Prior art consulted; these directly shaped the design.

1. **Leis, Boncz, Kemper, Neumann — "Morsel-Driven Parallelism: A NUMA-Aware Query
   Evaluation Framework for the Many-Core Age", SIGMOD 2014.**
   <https://db.in.tum.de/~leis/papers/morsels.pdf>
   *The decisive reference.* Abstract: *"dividing the work evenly is difficult **even with
   accurate data statistics** … the existing approaches for 'plan-driven' parallelism run
   into load balancing … bottlenecks, and therefore no longer scale."* This is exactly our
   `_estimate_worker_gb`: a plan-driven unit (one date) sized from statistics (file bytes),
   defeated by skew (April 2025). Their prescription — constant-size work units taken at
   run time — is §3, and they give the size: *"We experimentally determined that a **morsel
   size of about 100,000 tuples** yields good tradeoff…"*, and note it yields *"perfect load
   balancing, even in the face of **uncertain size distributions**"*. `_MORSEL_BYTES` = 64 MB
   ~= 180k rows sits in this band, chosen in bytes because bytes are what we bound.

2. **Boncz, Zukowski, Nes — "MonetDB/X100: Hyper-Pipelining Query Execution", CIDR 2005.**
   <https://www.cidrdb.org/cidr2005/papers/P19.pdf>
   Establishes operating on small cache-resident *vectors* (~1000 values) rather than
   materialising whole columns. Our 4.6x transient is precisely the full-materialisation
   cost this design avoids; §3.1 is the same idea at CSV-parse granularity.

3. **Mühlbauer, Rödiger, Seilbeck, Reiser, Kemper, Neumann — "Instant Loading for Main
   Memory Databases", PVLDB 6(14), 2013.**
   <https://www.vldb.org/pvldb/vol6/p1702-muehlbauer.pdf>
   Bulk CSV loading at wire speed is done **chunk-parallel**, not whole-file — confirming
   chunked parse is the standard shape for this exact job (CSV -> columnar), not a
   workaround.

4. **Palkar, Abuzaid, Bailis, Zaharia — "Filter Before You Parse: Faster Analytics on Raw
   Data with Sparser", PVLDB 11(11), 2018.**
   <https://www.vldb.org/pvldb/vol11/p1576-palkar.pdf>
   Filters the raw bytestream *before* parsing; notes parsing is 80-90% of runtime.
   **Validates an existing tse_tick choice** — the field-5 raw-byte filter
   (`_read_individual_stock_matches`) is the same technique. Conclusion: keep it untouched;
   morsels slice its output.

Also surfaced, not load-bearing: ParPaRaw — "Massively Parallel Parsing of
Delimiter-Separated Raw Data" (<https://arxiv.org/pdf/1905.13415>), and simdjson —
"Parsing Gigabytes of JSON per Second" (<https://arxiv.org/pdf/1902.08318>); both are
SIMD-parser work, relevant only if the parse itself ever becomes the bottleneck.

---

## 7. Risks

| Risk | Mitigation |
|------|-----------|
| Morsel splitting corrupts a record | Newline-aligned slices only; the existing line-based byte filter already relies on the same no-embedded-newline property. Boundary test (§5.2). |
| Streaming write changes row order / content | Provable identity (§4) + real-data row-identity test vs 0.14.5 output (§5.7). |
| Partial file on crash | Unchanged: `.tmp` + `os.replace` per ticker; marker last. |
| Row groups fragment the output | One row group per morsel (~180k rows) is a normal Parquet row-group size; `export_query` already writes a row group per day. |
| Touches the locked "read all parts -> concat -> write one partition" | Locked property is *atomicity per date*, which is preserved; only the in-memory concat is removed. Flagged and approved by Kevin before implementation. |
