# tse_tick — Project Overview

*Status snapshot: 2026-06-13. Internal coordination document — review before the
repository is made public (see Flagged Items).*

`tse_tick` is a Python library that parses, cleans, and queries Nikkei NEEDS
tick-level market data for the Tokyo Stock Exchange. This document summarizes
the state of the **repository**, the **papers**, and the **package**, and
collects every currently flagged item in one place.

---

## 1. Repository

**Remote:** `https://github.com/jevwithwind/tse_tick` (currently **private**),
branch `main`, local and remote in sync.

**History was rewritten on 2026-06-12** (user-approved purge): the original
example-notebook blob, whose saved outputs contained non-redistributable NEEDS
records, was stripped from all history with `git filter-repo` and verified
gone from the object database. The rewrite was force-pushed; **any older clone
must be re-cloned**, not pulled. Because the repository has been private
throughout, the data was never world-visible.

### Tracked contents

| Area | Contents |
|------|----------|
| `tse_tick/` | 11 modules + `py.typed`: `enhanced.py` (entry points), `core.py` (cleaning/decoding, 2016 fixed-width parser), `schemas.py`, `ingest.py`, `io/parquet.py`, `query.py`, `event_window.py`, `features.py`, `cli.py` |
| `tests/` | 14 files, **181 tests** (see Package section for pass/skip profile) |
| `benchmarks/` | 9 scripts, 6 aggregate results CSVs (timings only — re-included past the global `*.csv` ignore via negation), `ENVIRONMENT.md`, `PARQUET_RATIONALE.md` |
| `examples/` | Tutorial notebook (outputs stripped, paths genericized) + script |
| `paper/` | JOSS draft: `paper.md`, `paper.bib`, this overview |
| `.github/workflows/tests.yml` | CI: pytest on ubuntu, Python 3.9 / 3.11 / 3.13 |
| Root docs | `README.md`, `LICENSE` (MIT), `CHANGELOG.md` (with `[Unreleased]` section), `CITATION.cff` (v0.2.3 + affiliations), `CONTRIBUTING.md`, `ARCHITECTURE.md`, `pyproject.toml` |

### Deliberately local-only (gitignored)

- `technical_paper/` — the full LaTeX manuscript (see Papers). **No version
  control anywhere; keep backups.**
- `benchmarks/paper_assets/` — hand-edited LaTeX tables/figure for the
  manuscript (same caveat).
- `descriptions/` — proprietary Nikkei manuals, exploration notebooks run on
  real data, real-data schema samples. Never tracked; must never be.
- Benchmark run scratch, internal working notes, caches, `dist/`.

Real NEEDS data lives outside the repo at `G:\flash_crash` (read-only for this
project); tests locate it via the `TSE_TICK_DATA_ROOT` environment variable.

---

## 2. The Papers

Two coordinated artifacts, kept mutually consistent (same numbers, claims,
authors, and affiliations):

### Technical manuscript — `technical_paper/main.tex` (local-only)

- Full-length software paper (SoftwareX-style structure), **23 pages**,
  compiles clean with `xelatex → bibtex → xelatex ×2` (Japanese fonts require
  XeLaTeX): 0 errors, 0 undefined references, 0 overfull hboxes.
- Authors: Peter Romero (Psychometrics Centre, University of Cambridge),
  Kazumi Li (Graduate School of Economics, Keio University), Masataka Hayashi
  (Faculty of Economics, Keio University).
- Integrity-audited 2026-06-12: every performance number traces to
  `benchmarks/results_*.csv`; claims verified against the code (notably:
  price columns are documented as strings in 0.2.3, the security table
  reflects the actual blocklist validation, event-window anchors are
  caller-supplied, the 2016 layout is fixed-width *text*, and the raw
  TICIS110 layout from 2017 onward is the 83-column summary layout).
- The JPX global/Asia market-cap ranking claim was removed (2026-06-12).

### JOSS paper — `paper/paper.md` + `paper/paper.bib` (tracked)

- ~700-word JOSS-format draft: Summary, Statement of need, Performance,
  Acknowledgements; 8 references (DuckDB entry carries its DOI).
- Reviewed line-by-line against the manuscript for consistency: speedups
  (85×/28×/7×, 506×, 22.3×), field counts (95; 82 of 83 raw; 10 of 23 raw,
  15 in 2016; 17 of 83 raw), 187-entry bilingual mapping, test counts
  (181 collected / 133 without proprietary data), future-tense PyPI claim.
- Not yet submission-ready: see Flagged Items (ORCIDs, public repo,
  affiliation confirmations).

---

## 3. The Package

**`tse_tick` v0.2.3** — Python ≥ 3.9; depends on Polars and PyArrow, with
DuckDB as the `[query]` extra (import degrades gracefully without it).
MIT-licensed; CLI entry point `tse-tick`.

### Functionality

- **Stage 1 (ingest):** `create_df()` auto-detects data type and format era
  from the filename, parses all four NEEDS record types (TICST120 individual
  stock ticks, TICSS110 stock summaries, TICIT110 index ticks, TICIS110 index
  summaries — including the 2016 fixed-width era), assigns English or
  Japanese column names, casts volume/flag/date fields, and decodes
  categorical codes. `tse-tick ingest` batch-converts date ranges into
  Hive-partitioned Parquet (per-date directories, per-ticker files), with
  parallel workers (cap 8), resume, ticker pre-filtering, and event-window
  mode.
- **Stage 2 (query):** DuckDB SQL over the Parquet store (`query_ticks`,
  `query_sql`) with identifier/date/time validation and a 10-million-row
  default limit; order-book features (spread, depth, order-flow imbalance,
  realized and Garman–Klass volatility); event-window extraction around
  per-event reaction anchors.
- **Security guards:** decompression-bomb limits (≤5 entries, ≤5 GB, ≤100:1
  ratio), in-memory-only archive handling, SQL identifier blocklist.

### Measured performance (January 2017 files; see `benchmarks/`)

| Comparison | Result |
|------------|--------|
| Polars (16 threads) vs. original pandas prototype | 85× faster |
| Polars (16 threads) vs. fair pandas C-engine baseline | 28× faster |
| Polars (1 thread) vs. same baseline (library-vs-library) | 7× faster |
| DuckDB + Hive Parquet vs. pandas CSV scan (1-ticker hour slice) | 506× faster |
| Parquet (Snappy) vs. raw CSV storage | 22.3× smaller |

### Quality and release readiness

- **Tests:** 181 collected; **133 pass / 48 skip with no proprietary data**
  (CI profile); **155 pass / 26 skip** with `TSE_TICK_DATA_ROOT` pointing at a
  local NEEDS store; 0 failures. Coverage 76% overall, `cli.py` 82%. A
  session-scoped fixture builds a synthetic Parquet store through the real
  ingest pipeline, so CI needs no data.
- **Build:** `python -m build` produces `tse_tick-0.2.3` sdist + wheel;
  **`twine check` passes both**. Wheel contents verified (all modules,
  `py.typed`, LICENSE, no data files). PyPI name `tse-tick` was unclaimed as
  of 2026-06-12.
- **Recent fixes (in `[Unreleased]`):** restored the `Index Code` column in
  `indices_summary` output (was silently dropped for the 83-column raw
  layout — found by a new real-data smoke test), version metadata aligned,
  notebook sanitized, test paths parameterized, CLI test coverage added.

### Known, documented limitation

Price and quote-price columns remain **strings** in the output for
`individual_stock` (only the best-bid price is `Float64`) and for
`stock_summary`. Documented in the paper (§5.3, §9) and README. The code fix
is one line, but it changes benchmark timings, so it is deferred to a
**0.2.4 release with a benchmark re-run** and corresponding paper updates.

---

## 4. Flagged Items (current)

**Authorship / paper — need author confirmation:**

1. **Romero affiliation** set to "Psychometrics Centre, University of
   Cambridge" per Kevin's instruction — confirm with Peter Romero.
2. **Hayashi affiliation** set to "Faculty of Economics, Keio University" by
   convention (where the work was done; he has graduated and will join
   Stevens Institute of Technology as a masters student) — confirm, and
   consider a "Present address: Stevens Institute of Technology" footnote at
   submission time.
3. **ORCIDs are missing** from `paper.md`; JOSS requires one for the
   submitting author at minimum.
4. **Corresponding author** set to Kazumi Li (package maintainer) — confirm.
5. **Acknowledgements** name the Nakatsuma Seminar, Keio University (taken
   from the package docstring) — confirm wording.

**Repository / release:**

6. The repo is **private**; JOSS requires a **public** repository at
   submission. Safe to flip after the history purge — but first review this
   document and the CHANGELOG wording about the purge for public consumption.
7. **CI is unverified**: the workflow is pushed but the first Actions run
   could not be observed from this environment (private repo, no `gh` CLI).
   Check the Actions tab; expect 133 passed / 48 skipped per job.
8. Collaborators with pre-2026-06-12 clones must **re-clone** (history
   rewrite).
9. `black` was not run on the recent edits (not installed in the working
   environment; flake8 is clean) — run `black .` from a `[dev]` environment
   before release.

**Package / data:**

10. **Float-cast fix deferred to 0.2.4** (see limitation above): requires a
    benchmark re-run and paper number updates — do not fix casually.
11. The 2016 and 2023 TICSS/TICIT/TICIS test files referenced by the suite
    are **not present** under `G:\flash_crash` (only the 201701 trio exists
    in `raw_other/`); 26 tests skip locally until those files are restored.
12. The TICIS110 **83-column raw layout is verified for 201701** and implied
    for 2023 by the code path; the exact era boundary could be confirmed
    against the Nikkei manuals in `descriptions/manuals/`.
13. Benchmark scripts retain the historical data paths they actually ran with
    (`run_all.py`/`run_format.py` reference the old pilot location); they are
    kept as the methodology record for the published numbers.
14. `scripts/ingest_event_windows.py` is deprecated in favor of the CLI and
    retained intentionally; remove in a future release if desired.

---

## Quick reference

```bash
# Tests (no data needed)            # Tests against a local NEEDS store
pytest tests/                       TSE_TICK_DATA_ROOT=/path/to/store pytest tests/

# Build + metadata check
python -m build && python -m twine check dist/*

# Manuscript (from technical_paper/, requires XeLaTeX + Japanese fonts)
xelatex main.tex && bibtex main && xelatex main.tex && xelatex main.tex
```
