# Plan — documentation accuracy audit at 0.15.0

**Status:** executed in `docs/accuracy-audit-0.15.0`.
**Scope:** correct what the docs get *wrong*. No version bump, no API change, no re-ingest.
**Trigger:** a read-only audit of the project-root docs against the 0.15.0 source found that the
0.15.0-specific drift is small (the CHANGELOG is precise), but that older structural drift is not:
the documented contributor setup cannot run the test suite, and three docs assert three different
test counts, none of them current.

## Ground truth measured for this plan

Measured against the 0.15.0 source and, where noted, a real NEEDS store. Row counts are aggregates;
no raw records are reproduced here or in the diff.

| fact | value | how |
|---|---|---|
| test suite, no data | 603 collected / 555 pass / 48 skip | `pytest --no-cov`, `TSE_TICK_DATA_ROOT` → nonexistent |
| test suite, with data | 603 collected / 603 pass / 0 skip | `pytest --no-cov` against a local NEEDS store |
| skip attribution | 40 `test_real_data.py` + 8 `test_ingest.py` | unchanged — still exactly right |
| `_STREAM_WORKER_GB` | `3.0` | `ingest.py:60` |
| `_MAX_STREAM_TICKERS` | `64` | `ingest.py:64` |
| `_MORSEL_BYTES` | 64 MB | `enhanced.py:651` |
| zstd vs raw CSV | 70.77 MB vs 2209.59 MB = **31.2x**, read 3.0x faster than Snappy | `benchmarks/results_format.csv` |
| `results_query.csv` scope | `total_rows=1500000` (not 4.78M) | `benchmarks/results_query.csv` |

## Fixes — grouped by why they matter

### 1. Breaks today: the documented contributor setup cannot run the tests

`CONTRIBUTING.md`, `README.md` "Development setup" both say `pip install -e ".[dev]"` then
`pytest tests/ -v`. `[dev]` carries no duckdb (`pyproject.toml`), `query.py` hard-imports it, and the
test files import `tse_tick.query` directly — 9 collection errors, exit 2. The package itself is
fine: `__init__.py` guards the import and substitutes a stub raising a helpful
"install tse-tick[query]". Only the dev path is broken, and it is the path a new contributor
follows first.

- Fix both to `.[query,dev]` (matches CLAUDE.md; CI uses `.[query,test]`).
- README's Contributing block duplicates the strictly-richer `CONTRIBUTING.md` without linking to
  it. Collapse the README block to a pointer — removes the duplicate *and* the second copy of the
  bug.

### 2. Wrong because of 0.15.0

`README.md` — "It is an index, not data: it is **excluded from every result**, so `individual_stock`
still returns its **95** columns."

False for `query_sql`, which returns **96**. `_select_clause` (the only place the `EXCLUDE` lives)
is called once, from `query_ticks`' builder; `query_sql` registers its view as a bare
`SELECT * FROM read_parquet(...)`. The CHANGELOG scoped this correctly — "excluded from every
*documented output*", then explicitly carves out the `query_sql` escape hatch and frames the
exposure as *useful* ("use it for fast hand-written time predicates"). The README dropped the
carve-out and universalised the claim.

Fix: narrow the sentence and surface the carve-out. Do **not** delete the passage — the key should
be documented; the store gaining a column is why 0.15.0 was a minor bump.

### 3. Wrong because of 0.14.6 — "each worker holds a whole trading day's frame"

Stated in `README.md` (x2), `ARCHITECTURE.md`, and `CLAUDE.md`. Since 0.14.6 a ticker-filtered
ingest of <= `_MAX_STREAM_TICKERS` (64) codes bounds each worker at `_STREAM_WORKER_GB` (3.0 GB)
**regardless of the day's size** — that is the release's headline fix (24.52 GB -> 2.40 GB). The
docs undersell the release and could push users to a needlessly low `--parallel`.

The docs are faithfully echoing a **stale in-code comment**, so a docs-only fix leaves the source
misleading. The same false rationale is in the warning users actually see. Fix both:

- `ingest.py` `_cap_workers` docstring + the `Limiting workers ...` warning text.
- `ingest.py` `_estimate_worker_gb` docstring, `_worker_died_error` message.
- The 70% RAM mechanism itself is real (`_RAM_SAFETY_FRACTION = 0.7`) and stays — only the
  "holds a whole trading day" *rationale* is wrong, and only for the streaming path. The
  full-frame path still does hold the day, so the message must stay correct for both.

No test asserts the message text (`test_round19_fixes.py` asserts `_filtered_worker_gb` behaviour),
so the string is safe to change. User-visible => `[Unreleased]` CHANGELOG entry, no bump.

### 4. Stale: one fact, three inconsistent renderings

Test counts: `CLAUDE.md` 414/366/48, `README.md` 430/382/48, `ARCHITECTURE.md` **both** (414 in the
repo tree, 430 in Test Status). Each was true when written. Fix as one unit to 603/555/48.
Preserve what is still right: the 48 skips and their 40+8 attribution. `CLAUDE.md`'s "~2 min" now
describes the *no-data* run; with-data is ~5 min.

### 5. Stale: version stamps

- `CITATION.cff` — `version: 0.11.4`, 17 releases behind; no `date-released`. Author list is
  correct and is **not** touched.
- `ARCHITECTURE.md` — 0.14.3, 0.14.0, and 0.13.0 in three places inside one file.

### 6. Stale: ARCHITECTURE's reference sections describe superseded designs as current

The structural cause is worth naming: section 4 (the CHANGELOG mirror) is ~41% of the file and is
the *only* current part. The maintenance pattern has been "append to section 4, never touch 5-12",
so the doc now documents its own fixes in 4 while 5-12 present the bugs as the design.

- **5.3 ingest** — documents only the pre-0.14.6 concat path; `_ingest_date_group` checks for
  streaming first. Same box: resume is described as "date dir with `ticker=*.parquet` -> skip", but
  `_coverage_satisfied` reads a coverage marker *precisely because* file-existence-as-coverage was
  audit bug H2 (a store built for ticker A resume-skipped a later request for ticker B).
- **6.1 query** — two generations stale: bare `"Execution Time" >= ...` (the pre-0.9.0 bug that
  dropped ~94% of a liquid day), `ORDER BY "Data Date", "Execution Time"` (the pre-0.7.0 crash),
  `LIMIT 10M` (actually `limit + 1`), `.pl()` (actually `_execute_to_polars`). No footer probe, no
  `union_by_name` fallback, no `EXCLUDE`. `export_query` absent from section 6 entirely.
- **7 Parquet I/O** — key-functions table omits `PartitionedParquetAppender` (the whole 0.14.6
  streaming writer) and `_add_effective_time`.
- **5.1 parse** — "price/quote columns mostly kept as String" is contradicted by the doc's own
  section 4; `core.py` casts 27 float columns. Signatures miss `on_morsel` / `max_oneshot_bytes`.
  The linear pipeline describes only the unbounded path.

### 7. Stale: the summary-store layout error, duplicated into both docs

`ARCHITECTURE.md` section 7 and `README.md`'s layout block are near-verbatim duplicates and carry
the same error: they present the per-ticker path as universal. The two daily-aggregate summary
types partition **by date only**, code kept as a column — the 0.10.0 decision that stopped a 15 MB
month becoming a 2.4 GB store of ~87k files. The README states this correctly ~160 lines away, so
it contradicts itself.

### 8. Perf numbers that violate the repo's own hard rule

- **`README.md` "~25M rows"** for 7203+9984 in one January: exists nowhere else — no
  `results_*.csv`, no CHANGELOG. Violates "never state a performance number without a backing
  results file". Rewrite around the *sourced* fact (CHANGELOG: 9984 alone is >10M rows/month),
  which makes the same pedagogical point and drops both the invented 25M and the derived
  "about 10 of 19 days".
- **`README.md` Performance preamble** over-scopes: the 694.1x query row was measured on 1.5M rows
  (`results_query.csv`), and "one day" is really one ZIP *part* (`...20170104.1.zip`) — the README
  itself says days have 1-27 parts. The speedups all check out; only the attribution is wrong.
- **`README.md` storage headline** advertises Snappy 22x when the default since 0.14 is zstd, which
  the *same* results file shows at 31.2x with a 3.0x faster read. Understates what ships and
  contradicts the README's own `--compression` row.

### 9. Stale: smaller verified factual drift

`create_df` signature missing `on_morsel`; `agg_groups` (does not exist anywhere); `from_arrow`
attributed to `query.py`; `CODEMAP` vs `_CODE_TYPE_MAP`; `extract_event_window` missing
`columns`/`data_type`; CLI tree missing the `export` verb; `02_evaluation.ipynb` unlisted;
`constants.py` understated (it is the SSOT); `plans/` unlisted though CHANGELOG cites it;
`test_core.py` called a 1-line stub (it has 3 tests — `test_schemas.py` *is* still a stub).

## Verified NOT stale — explicitly do not "fix"

- **`CLAUDE.md`'s `gc.collect()` decision.** Still exactly right. 0.14.6 bounded the *streaming*
  path; the *full-frame* path this decision covers is untouched. The calls are live and the claim
  is restated verbatim in-code. Challenged directly during the audit; it held.
- **`rclone_guide.md`** — not an orphan (referenced from README and `01_basic_usage.ipynb`); no
  version-specific claims.
- **CHANGELOG's empty `[Unreleased]`** — the established convention, present at every prior release
  commit. 0.15.0's structure, ordering, and links all pass.
- **Author list**, all guard values, column counts 95/82/10/17, the 2016 fixed-width claims, CI
  matrix, family-code semantics, both writers materialising the key.
- **`README.md`'s "~100 GB" whole-frame figure** — era-stale (it is a 2017-2019 number) but
  CHANGELOG-cited, so it satisfies the hard rule. Re-basing it on a 2023-2025 measurement is a
  judgement call for Kevin, not a defect fix. See follow-ups.

## Out of scope — proposed follow-ups, each its own PR

1. **Collapse ARCHITECTURE section 4** (~41% of the file, mirrors the CHANGELOG that the doc's own
   header names as the doc of record). It is also *incomplete* (0.13.1-0.13.3 missing) and its
   ordering is scrambled. This is a structural decision about what ARCHITECTURE is for — Kevin's
   call, not a silent edit.
2. **The README redundancy pass.** `export_query`'s memory story appears 3x, the `max_workers` rule
   4x, part-pruning 5x, `date=` forms 4x; the 10M-cap blockquote is ~43 lines re-deriving what two
   earlier sections already say. All *correct* — just repeated. Judgement-heavy; separate PR.
3. **README reference gaps** (new content, not a fix): CLI Reference omits the entire `export` verb
   despite `export` having its own Quick Start; the Python API Reference documents 4 callables and
   none of `query_ticks` / `export_query` / `query_sql`.
4. **Re-base the era-stale 7203 figures** on a 2023-2025 measurement, with a `results_*.csv` to
   back them.

## Verification

- `pytest --no-cov` — 603 collected / 555 pass / 48 skip (no-data); **603 pass / 0 skip**
  measured with a local NEEDS store. Note `addopts` already carries `-q`; a second `-q` gives
  `-qq` and suppresses the summary line entirely.
- `black --check`, `flake8`, `mypy` on any touched source. Note local tooling drifts from the repo
  (a known baseline of pre-existing findings on `main`) — verify **no new** issues, do not
  repo-reformat. Compare like-for-like: a copy taken outside the repo misses `pyproject.toml`,
  so black silently falls back to line-length 88 and invents ~120 phantom findings.
- Re-grep each corrected claim against source rather than trusting the edit.
- `CLAUDE.md` is **git-ignored**: its fixes are real but will not appear in the PR diff.

### What the adversarial pass caught — keep this loop

An independent verifier re-checked the diff against the source and found **six** defects in
the first round of fixes. Recording them, because the pattern generalises: every one was a
claim that *looked* verified.

1. **`query_sql` returns 97, not 96.** The fix counted the *file* schema (95 + key) and forgot
   that `hive_partitioning=true` adds `date`. 96 is what `query_ticks` returns — the
   non-privileged path's count got attributed to the escape hatch. A column count is only
   knowable by measuring it.
2. **`on_morsel`'s contract was stated backwards.** `bounded = resolved_filter is not None` —
   the selector is `ticker_filter`, not `on_morsel`. Without a filter the callback is
   *silently discarded* and the whole frame returned: the exact blowup the parameter exists
   to prevent.
3. **The repo-map tree still held six stale per-file counts**, contradicting §10 — inside a
   block the fix had *edited*. Fixing a doc's header while leaving its body stale reproduces
   the disease being cured. Every count is now machine-checked against `pytest --collect-only`.
4. **and 5. Two CHANGELOG entries over-claimed.** "~25M was replaced" and "the RAM messages no
   longer claim…" were both written after fixing only *some* sites. The number still shipped in
   `read_ticks`'s docstring; the whole-day claim survived in five more places including the
   `--parallel` CLI help — which then **contradicted the README the same diff had rewritten**.
   A partial fix plus a total claim is worse than no fix: it closes the issue.
6. **A `%d` added to a log message without its argument.** Would have raised the moment the
   warning fired. Caught only by *executing* the message, not reading it.

For whoever picks up the follow-ups: **grep the claim across the whole repo — source, CLI
help, docstrings — before writing "fixed" in a changelog.**
