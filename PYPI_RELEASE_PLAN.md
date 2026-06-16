# tse_tick — PyPI Publishing & API Polish Plan

**Status:** Proposal for team review — not yet implemented.

## Context

The goal is for users to `pip install tse-tick` and `import tse_tick` to process raw Nikkei
NEEDS tick data (choosing ticker / time period / data type freely). An audit found the repo is
**already ~95% a package**: a complete PEP 621 `pyproject.toml` (static version `0.2.3`,
dependencies, extras, the `tse-tick` console script, `py.typed`), `LICENSE`, `CITATION.cff`, a
30-function public API exported via `__all__` (`create_df`, `query_ticks`, `compute_*`, `ingest_*`,
…), 181 tests (133 pass on synthetic fixtures with no proprietary data; the rest are real-data tests
that skip without it), and CI across Python 3.9 / 3.11 / 3.13.

So this is **not** "make it a package" — it is two focused efforts:

1. **Publish to PyPI** — the only real blocker to `pip install tse-tick` (there is no
   build/publish automation, no release tag, and no PyPI project yet).
2. **Polish the import API & docs** — **keep the package's established names** and add an optional
   **translation layer** mapping yfinance / Polygon / ccxt names → ours (see Phase A0), plus a
   data-type enum, docstrings, and clearer validation errors.

**Recommended release version:** `0.3.0` (minor bump — the translation layer, enums, and one-shot
reader are new features), tagged `v0.3.0`. (`v0.2.3` was written into the changelog but never tagged
or published.)

**Decided (team): two access paths, both locked in.**
- **Two-stage (scale path):** `ingest_*` raw ZIPs → Hive-partitioned Parquet store, then
  `query_ticks`. Crucial for wide, repeated, or large work.
- **One-shot (exploration path):** a single call straight from raw ZIPs → ticker/time-filtered
  DataFrame, no store (specced in Phase A6). Easiest for a few tickers over a bounded window.

These are complementary, not competing — ship both.

---

## Phase A0 — Keep the package's names; add a translation layer

**Decided (team, 2026): do NOT adopt yfinance / Polygon / ccxt naming. Keep the package's established
public names** — `create_df`, `query_ticks`, `query_sql`, `get_available_dates` /
`get_available_tickers`, `discover_zips`, the `ingest_*` family, `read_parquet_partition` /
`write_partitioned_parquet` / `read_partitioned_parquet` / `write_event_window_parquet`, `compute_*`,
`export_to_csv`, `parse_period`. **Instead, ship an optional translation layer** mapping the external
conventions (yfinance / Polygon / ccxt) → our names, so users from those libraries can find our API
**without coupling us to their (changing) APIs**.

This **reverses** the earlier "rename everywhere" decision. Consequences: **nothing in the package is
renamed**, so there are **no deprecation aliases**, **no churn**, and **no breakage** — existing user
code, the README, and `tests/test_paper_examples.py` keep working unchanged.

> **Paper reverted:** the technical paper had been edited to the proposed new names; it has been
> **reverted to the package's real names** (`create_df`, `query_ticks`, …) as part of this change.

**Why keep our names + translate (rather than rename):**
- **No coupling.** A static mapping is a reference we maintain at our own pace. Renaming to mirror
  yfinance / Polygon / ccxt would chase libraries whose signatures change and whose semantics differ
  (yfinance hits a web API; we read local ZIPs).
- **No churn / no breakage.** No deprecation cycle, no alias maintenance; the paper's listings, the
  README, and the tests stay valid.
- **Discoverability without lock-in.** Newcomers from those ecosystems get a lookup ("what is the
  `tse_tick` equivalent of `get_aggs`?") without us adopting a foreign vocabulary.

### A0.1 — Translation module `tse_tick/translate.py` (new)

A small, **static, dependency-free** reference — we do **not** import yfinance / Polygon / ccxt:

- **Mapping tables** — per source, a dict from the external name → the `tse_tick` name (functions and
  arguments). The correspondence below is the content of these tables.
- **Lookup helper** — `translate(source, name) -> str | None`, e.g.
  `translate("polygon", "get_aggs") == "query_ticks"`,
  `translate("yfinance", "tickers") == "ticker_filter"`.
- **Readable dump** — `mapping(source=None)` returns the table(s) for docs / `help()`.
- Export from `tse_tick/__init__.py`, add to `__all__`. Purely additive.

**Function correspondence (external convention → our name):**

| Concept | yfinance | Polygon | ccxt | → `tse_tick` |
|---|---|---|---|---|
| Load a file → DataFrame | `download` | — | — | `create_df` |
| Query stored ticks | `Ticker.history` | `get_aggs` | `fetch_trades` / `fetch_ohlcv` | `query_ticks` |
| Raw-SQL escape hatch | — | — | — | `query_sql` |
| List available dates | — | `list_*` | — | `get_available_dates` |
| List available tickers | `tickers` | `list_*` | `symbols` | `get_available_tickers` |
| Find input files | — | — | — | `discover_zips` |
| Build the store (batch) | `download` (bulk) | — | — | `ingest_period` (+ `ingest_directory` / `ingest_year_from_root` / `ingest_single_zip`) |
| One-shot raw → DataFrame | `download` | — | `fetch_trades` | `read_ticks` (Phase A6) |

**Argument correspondence (external → our name):**

| Concept | yfinance | Polygon | ccxt | → `tse_tick` |
|---|---|---|---|---|
| Instrument | `tickers` | `ticker` | `symbol` | `ticker` (query) / `ticker_filter` (read & ingest) |
| Store / data location | — | — | — | `data_dir` (read & query) / `output_dir` (ingest) |
| Day | — | — | — | `date` (`"YYYYMMDD"`) |
| Intraday window | `start` / `end` | `from_` / `to` | `since` | `start_time` / `end_time` (`"HH:MM:SS"`) |
| Row cap | — | `limit` | `limit` | `limit` (`query_ticks`) / `rows` (`create_df`) |

Read these "external → ours." They are the **content of the translation module**, not a rename plan.

### Two store readers (both kept, original names)

The package exposes two functions that read the main Parquet store and return a DataFrame; keep both
and **document the difference in their docstrings** — a docs fix, since we are *not* renaming them, so
the near-identical `read_parquet_partition` vs `read_partitioned_parquet` pair stays (just better
explained):

- `query_ticks` — the **primary query**: DuckDB-backed; time filtering (`date` / `start_time` /
  `end_time`), column projection, ordering, `limit`. Needs the `[query]` extra (DuckDB; imported
  behind a `try/except ImportError`).
- `read_parquet_partition` — a **dependency-light direct read**: PyArrow only (no DuckDB); filters by
  `date` / `ticker`, projects `columns`; no time filter or ordering. (`read_partitioned_parquet` is
  the analogous reader for the separate event-window store.)

### Conventions referenced (source for the translation tables)
- yfinance — `download` / `Ticker.history` (`tickers`, `start`, `end`, `period`, `interval`):
  <https://ranaroussi.github.io/yfinance/>
- Polygon — `get_aggs` / `list_aggs` / `list_trades` (`ticker`, `timespan`, `from_`, `to`, `limit`):
  <https://polygon-api-client.readthedocs.io/>
- ccxt — `fetch_ohlcv` / `fetch_trades` (`symbol`, `timeframe`, `since`, `limit`):
  <https://docs.ccxt.com/>

---

## Phase A — Import API & docs polish

> Names below are the package's **current** names (unchanged — no rename).

### A1. Data-type and language enums (discoverability + IDE autocomplete)
- New module `tse_tick/constants.py` defining `class DataType(str, Enum)` with members
  `INDIVIDUAL_STOCK = "individual_stock"`, `STOCK_SUMMARY = "stock_summary"`,
  `INDICES = "indices"`, `INDICES_SUMMARY = "indices_summary"`, and `class Language(str, Enum)`
  (`EN = "en"`, `JP = "jp"`).
- **Must subclass `str`** so `data_type=DataType.INDIVIDUAL_STOCK` works everywhere a magic
  string is accepted today (string comparisons, filename building) — zero breakage.
- Export both from `tse_tick/__init__.py` and append to `__all__` (lines 78–111).
- Refactor `get_supported_data_types()` (`__init__.py:118`) to derive from `DataType` so the list
  can never drift from the enum.

### A2. Docstrings on public functions (PEP 257)
Pattern: add a concise docstring (summary, Args with exact formats, Returns, and a short example)
to every exported function currently missing one. Representative targets (current names):
- `tse_tick/query.py` — `query_ticks` (state it **requires a pre-built Parquet store** and the
  `[query]` / DuckDB extra), `get_available_dates`, `get_available_tickers`.
- `tse_tick/enhanced.py` — `create_df` (note its `ticker_filter` is a `set` of **string** stock
  codes, applied for `individual_stock` only) and `export_to_csv` (signature
  `folder_path, output_path, language, rows` — it wraps `create_df` for the whole file and has
  **no** `ticker_filter` parameter).
- `tse_tick/features.py` — `compute_spread`, `compute_depth`, `compute_flow_imbalance`,
  `compute_volatility`, `compute_all_features` (note they expect **English** column names).
- `tse_tick/ingest.py` — `ingest_single_zip`, `ingest_period` (describe the returned dict keys).
- `tse_tick/io/parquet.py` — `read_parquet_partition` vs `read_partitioned_parquet`: state which
  store each reads (main vs event-window) so the near-identical names are not confused.
- `parse_period` (`enhanced.py:65`) already has a good docstring — use it as the style template.

### A3. Clearer validation errors
- **Already done — do not redo.** The date/time `ValueError`s in `tse_tick/query.py`
  (`_validate_date` / `_validate_time`, lines 33–40) already name both the expected format and the
  offending value: `Invalid date format (expected YYYYMMDD): '2024-01-01'`. Leave them as-is.
- **Open work:** `query_ticks` ticker is typed `Optional[int]` (`query.py:46`); accept `str` or
  `int` and normalize (or raise a message naming the expected type). Keep behavior
  backward-compatible. (A `str` already works in practice via the `ticker={ticker}.parquet` glob at
  `query.py:77`, but the type hint and the lack of validation are misleading.)

### A4. Refresh `get_info()` quick start
- Update `__init__.py:127` `get_info()` to show the two-stage flow with the enums, e.g.
  `query_ticks(store, data_type=DataType.INDIVIDUAL_STOCK, ticker=7203, date="20240201", start_time="09:00:00", end_time="11:30:00")`.

### A5. Keep the public-namespace test green
- `tests/test_paper_examples.py` checks `PAPER_TOPLEVEL_NAMES` (8 names) via a **presence test**
  (`hasattr(tse_tick, n)` for each — `test_paper_toplevel_namespace`, line 48), **not** exact-equality
  against `__all__`. With **no rename**, every existing name still resolves and the paper's listings
  are unchanged, so this test stays green with no edits.
- Add a small test asserting `DataType` / `Language` are importable and `str`-valued, and (optionally)
  that `translate(...)` returns the expected names.

---

## Phase A6 — One-shot path: `read_ticks()` (raw ZIPs → filtered DataFrame, no store)

**Decided (team): ship this as the second access path** (the one-shot), alongside the two-stage
`ingest_*` + `query_ticks`. It answers the most common exploratory question — *"ticker 7203 on
2024-02-01, 09:00–11:30"* — in one call with no Parquet store to build first.

**Headline** (working name `read_ticks`; the final name is the team's call and must fit the package's
own conventions — candidates: `read_ticks`, `query_ticks_from_zips`, or an extended `create_df`):

```python
read_ticks(source, *, data_type="individual_stock", ticker_filter=None,
           date=None, start_time=None, end_time=None, columns=None,
           rows=10_000_000, language="en") -> pl.DataFrame
# read_ticks("G:/NEEDS_root", ticker_filter={"7203"},
#            date="20240201", start_time="09:00:00", end_time="11:30:00")
```

Argument names mirror the **existing package vocabulary** (`ticker_filter`, `date`, `start_time`,
`end_time`, `rows`), not the yfinance set — consistent with the Phase A0 decision.

**Semantics**
- `source` accepts a single `.zip`, a flat folder of ZIPs, or a structured NEEDS root
  (`{year}/{yearmonth}/…`) — the same inputs `create_df` and the `ingest_*` functions already take.
- `date` (or a date range) selects which ZIPs to open; `start_time` / `end_time` filter rows within
  each day.
- Returns the same cleaned Polars DataFrame as `create_df` / `query_ticks` (same columns, same
  language).

**Optimal implementation — compose what already exists; add no new parsing or cleaning:**
1. **Resolve input → ZIP list.** Single zip / flat dir: as `create_df` already does. Structured root:
   `discover_zips(input_root, data_type, years, months, dates)` driven by the date(s) (reuse
   `parse_period` + the discovery the `ingest_*` functions already use).
2. **Per-ZIP ticker-pruned read.** For each ZIP, call
   `create_df(zip, auto_detect=False, data_type=…, year=…, ticker_filter=…)` — pass
   `auto_detect=False` with an explicit `data_type` / `year`, because the default `auto_detect=True`
   re-detects and **overwrites** `data_type` (`enhanced.py:444`); this is exactly how
   `ingest_single_zip` (`ingest.py:36`) calls it. For `individual_stock` this reaches the existing
   **raw-byte fast-path** — the `ticker_filter and kind == "individual_stock"` branch inside
   `get_1y_dataframe` (the function begins at `enhanced.py:198`; the fast-path branch is at
   `enhanced.py:278`) — that parses only matching lines, the key to bounded memory on multi-GB files.
   Iterate one ZIP at a time, calling `gc.collect()` between, as `ingest_event_windows_period` already
   does (`ingest.py:421`, `:431`) for memory control.
3. **Time-window filter.** Build a tick timestamp from `Data Date` (Datetime) + `Execution Time`
   (`"HHMMSS"` string) and keep rows in `[start_time, end_time]` — exactly what
   `event_window._filter_ticks_for_events` (`event_window.py:42`) already does around event anchors.
   **Refactor that timestamp construction into one shared helper** (e.g. `_tick_datetime(df)`) reused
   by `read_ticks`, `_filter_ticks_for_events`, and (in spirit) `query_ticks`, consolidating the
   `HHMMSS`/colon handling currently repeated across `query.py`, `features._exec_time_index`, and both
   `event_window.py` paths (`_filter_ticks_for_events` and `extract_event_window`) — a correctness +
   readability win in its own right.
4. **Concat + `rows` cap.** Vertically concat the (small) filtered parts; short-circuit once the `rows`
   cap is reached, as `get_1y_dataframe` already does with its row limit.

**Scope & honest caveats (put these in the docstring):**
- **Time filtering is for tick types.** `individual_stock` / `indices` have `Execution Time`; the two
  `*_summary` types are daily aggregates, so `start_time` / `end_time` do not apply (filter on `date`
  only, or raise a clear error).
- **Fast-path is `individual_stock`-only.** Other types parse in full then filter (fine — those files
  are far smaller). For `indices` the "ticker" is the index code (101 = Nikkei 225); filter on
  `Index Code` after parsing, not via the byte fast-path.
- **Not a store replacement at scale.** With no `ticker_filter` over a wide span this re-scans raw
  ZIPs on every call; for repeated or large analyses, `ingest_*` once + `query_ticks` is far faster
  (that gap is the ~694× query speedup in the benchmarks — DuckDB+Parquet vs a raw scan). The one-shot
  is tuned for exploration: a few tickers, a bounded window, no store. A single liquid ticker over many
  years is a huge result — it will hit the `rows` cap (which silently truncates), the signal to use
  the store instead.

**New work:** a `read_ticks` function in `enhanced.py` (or a small `tse_tick/oneshot.py`), an `__all__`
export, the shared `_tick_datetime` refactor, and tests (reuse `tests/synthetic_data.py`, which
already builds real ZIPs). Purely additive and back-compatible — fits the 0.3.0 minor bump.

**Optional follow-on:** a `tse-tick read` (or `get`) CLI subcommand wrapping `read_ticks` (today the
CLI only exposes `ingest`).

---

## Phase B — Packaging finalization

### B1. Align license declaration with the build backend (`pyproject.toml`)
- `license = "MIT"` (line 11) is the PEP 639 SPDX string, but `build-system.requires` (line 2)
  pins only `setuptools>=61.0` (SPDX expression support landed in setuptools 77). Bump to
  `setuptools>=77.0.0` and add `license-files = ["LICENSE"]` under `[project]`. This guarantees a
  clean `python -m build`.

### B2. Version bump to 0.3.0 (3 files, keep in sync)
- `pyproject.toml:7`, `tse_tick/__init__.py:15` (`__version__`), and the `CITATION.cff` version field
  (`CITATION.cff:18`). (Verified: all three currently read `0.2.3`.)
- `CHANGELOG.md`: the `## [Unreleased]` section **already** holds the post-0.2.3 work (the
  `indices_summary` Index Code fix, the `test`-extra CI fix, real-data tests, the benchmark re-run,
  and `rclone_guide.md` — so do **not** re-add those). Rename it `## [0.3.0] - <release date>` to fold
  that in, then **add** the new 0.3.0 API work — the `tse_tick/translate.py` mapping layer (A0), the
  `DataType` / `Language` enums (A1), docstrings (A2), the `query_ticks` ticker `str`/`int` normalization (A3), and the
  `read_ticks` one-shot reader (A6) — and open a fresh empty `## [Unreleased]`.

### B3. README PyPI-safety (`README.md`)
- Convert **both** relative markdown links to absolute `…/blob/main/` URLs so they resolve on the
  PyPI project page (relative links 404 there):
  - `[the rclone download guide](rclone_guide.md)` (line 9) →
    `https://github.com/tse-tick/tse_tick/blob/main/rclone_guide.md`
  - `[MIT](LICENSE)` (line 382) → `https://github.com/tse-tick/tse_tick/blob/main/LICENSE`
  (In-page anchors — `(#performance)`, `(#two-access-patterns)` — and backtick'd paths are fine.)
- Update the Installation note (line 28: "Not yet on PyPI — install from source for now.") so
  `pip install tse-tick` becomes the primary instruction once the release is live.

### B4. Optional metadata polish (recommended, not required)
- `name = "tse_tick"` → `name = "tse-tick"` for display consistency with the README install
  command and the console script (PyPI normalizes both to `tse-tick`, so functionally identical).
- Bump `Development Status :: 3 - Alpha` → `4 - Beta` for a first public release.

---

## Phase C — Release automation

### C1. New workflow `.github/workflows/publish.yml` (model on `tests.yml`)
- Trigger: `on: release: types: [published]`.
- Steps: `actions/checkout@v4` → `actions/setup-python@v5` → `python -m pip install build` →
  `python -m build` (sdist + wheel) → `pipx run twine check dist/*` →
  `pypa/gh-action-pypi-publish@release/v1`.
- **Trusted publishing (OIDC)** — no stored token: set `permissions: id-token: write` and
  `environment: pypi` on the publish job.
- Optional: a `workflow_dispatch`-triggered job (or a TestPyPI `repository-url`) for dry runs.

### C2. One-time external setup (manual; performed in the PyPI / GitHub UIs)
- On PyPI: add a **pending trusted publisher** for project `tse-tick`, owner `tse-tick`, repo
  `tse_tick`, workflow `publish.yml`, environment `pypi` (enables the first publish with no API
  token). Fallback if OIDC is undesired: create a PyPI API token → GitHub secret `PYPI_API_TOKEN`
  and use it in the workflow.
- On GitHub: create the `pypi` Environment.

---

## Phase D — Verification (evidence before release)

1. **Local build & metadata:** `python -m build` then `twine check dist/*` (must PASS — catches
   the license/metadata issue from B1).
2. **Distribution contents (no proprietary data or bloat leaks in):** inspect the built artifacts —
   `tar tzf dist/tse_tick-0.3.0.tar.gz` and `unzip -l dist/tse_tick-0.3.0-*.whl` — and confirm they
   contain **only** the `tse_tick/` package plus `LICENSE` / `README.md` / `py.typed` / metadata. The
   repo has **no `MANIFEST.in`**, and the working tree holds material that must **never** ship:
   `descriptions/` (the copyrighted Nikkei NEEDS PDF manuals), and the large `technical_paper/` and
   `benchmarks/` assets. setuptools' defaults exclude non-package directories, but verify it — if
   anything leaks, add a `MANIFEST.in` (`prune descriptions`, `prune technical_paper`,
   `prune benchmarks`) or a `[tool.setuptools]` exclude rule.
3. **Clean-venv install smoke test:** in a fresh venv, `pip install dist/tse_tick-0.3.0-*.whl`,
   then `python -c "import tse_tick as t; print(t.__version__); from tse_tick import DataType; t.get_info()"`
   and `tse-tick --help`.
4. **Tests:** `pytest tests/` green (synthetic fixtures, no proprietary data) locally and in CI.
5. **TestPyPI dry run:** publish to TestPyPI, then in a clean venv
   `pip install -i https://test.pypi.org/simple/ tse-tick` and re-run the import + CLI smoke test.
6. **Real release:** tag `v0.3.0`, create the GitHub Release → `publish.yml` uploads to PyPI →
   confirm `pip install tse-tick` from PyPI works end to end in a clean venv.

---

## Critical files
- `pyproject.toml` — build-system requires, license-files, version, (optional) name / dev-status.
- `tse_tick/__init__.py` — export the enums and `translate` helper, `__all__`,
  `get_supported_data_types()`, `get_info()`.
- `tse_tick/translate.py` — **new**: static yfinance / Polygon / ccxt → `tse_tick` name mapping +
  `translate()` lookup (A0).
- `tse_tick/constants.py` — **new**: `DataType`, `Language` enums (A1).
- `tse_tick/query.py`, `tse_tick/enhanced.py`, `tse_tick/ingest.py`, `tse_tick/io/parquet.py`,
  `tse_tick/features.py` — docstrings, (query.py) `query_ticks` ticker `str`/`int` normalization
  (its date/time messages are already clear — A3), and
  (io/parquet.py) docstrings disambiguating the two store readers. **No renames.**
- `tse_tick/enhanced.py` (or new `tse_tick/oneshot.py`) + `tse_tick/event_window.py` — **Phase A6**:
  the `read_ticks` one-shot path and the shared `_tick_datetime` helper it factors out.
- `CHANGELOG.md`, `CITATION.cff`, `README.md` — version + PyPI-safe link.
- `.github/workflows/publish.yml` — **new**: build + trusted-publish.
- `tests/test_paper_examples.py` — stays green unchanged (no rename); add a small enum / `translate` test.

## Reuse (don't reinvent)
- `query.py`'s date/time validation messages already name the format **and** the offending value
  (A3) — leave them as-is; only the `query_ticks` ticker `str`/`int` normalization is open.
- `get_supported_data_types()` exists — point it at the new enum rather than a second list.
- The yfinance / Polygon / ccxt correspondence already worked out in Phase A0 **is** the content of
  `tse_tick/translate.py` — read it "external → ours"; no new analysis needed.
- `tests/conftest.py` + `tests/synthetic_data.py` build a real Parquet store with no proprietary
  data — Phase D library smoke tests can use the same fixtures/patterns.
- `tests.yml` is the template for `publish.yml` (same checkout / setup-python style).
- **Phase A6 reuses everything it needs:** `create_df`'s `individual_stock` raw-byte ticker fast-path
  (`enhanced.py:278`), `event_window._filter_ticks_for_events` (the tick-time-window filter), and
  `discover_zips` / `parse_period` (input discovery) — `read_ticks` orchestrates them; it does not
  reimplement parsing, ticker pruning, or time filtering.

## Open decisions / out of scope
- **Two access paths — DECIDED (team): ship both.** Two-stage (`ingest_*` → `query_ticks`) for scale;
  one-shot (`read_ticks`, Phase A6) for exploration. Locked. (Remaining detail: the one-shot's final
  function name, to fit the package's own conventions.)
- **Public naming — RESOLVED (team): keep the package's names; do NOT adopt yfinance / Polygon / ccxt.**
  Add a translation layer instead (Phase A0). This **reverses** the earlier "rename everywhere"
  decision; the technical paper has been reverted to the real names, and no deprecation aliases are
  needed.
- **Version choice** — `0.3.0` is recommended (new features: translation layer, enums, one-shot
  reader); the team may prefer to publish the existing `0.2.3` as-is if Phase A is split into a later
  release.
- **Trusted publishing vs API token** — trusted publishing (OIDC) is recommended; an API-token
  fallback is noted in C2.
