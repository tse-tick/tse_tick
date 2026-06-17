# Design: file-driven translation tables

**Date:** 2026-06-17
**Status:** Approved (brainstorm) — pending spec review
**Scope:** `tse_tick.translate` only. Additive + backward-compatible. Targets a future `0.3.1`/`0.4.0`.

## Context

The translation layer (`tse_tick/translate.py`, shipped in 0.3.0) maps yfinance / Polygon / ccxt
function and argument names to their `tse_tick` equivalents via two hand-maintained Python dicts
(`_FUNCTION_MAP`, `_ARGUMENT_MAP`). Amending the tables today means editing Python source. We want
the data to live in an external file so:

- **Contributors** can add/adjust mappings by editing a clear data file (small, reviewable PR diffs;
  no Python knowledge required).
- **Power users** can override the tables at runtime without touching `site-packages`.

The **average user is unaffected**: `pip install tse-tick` ships a complete built-in mapping file
that loads automatically; no configuration, no extra files, identical `translate()` behaviour.

## Goals

1. Move the mapping data out of `translate.py` into a shipped JSON data file.
2. Let power users override/extend the tables via an **optional** environment variable.
3. Keep the public API (`translate`, `mapping`, `SUPPORTED_SOURCES`) and its behaviour **identical**.
4. Never let translation loading break `import tse_tick`.

## Non-goals (out of scope / YAGNI)

- **TOML** — rejected: `tomllib` is 3.11+, so it would add a `tomli` backport dependency on 3.9/3.10;
  the package is intentionally dependency-free here. JSON is stdlib on all supported versions.
- **Public mutation API** (`add_translation()`, public `reload_translations()`) — not now; the env-var
  override + editable file cover the stated needs.
- **Changing the mapping content** — the JSON reproduces the current tables verbatim.

## Decisions (locked during brainstorm)

| Decision | Choice |
|----------|--------|
| Format | **JSON** (stdlib, no new dependency, Python 3.9+) |
| Override | **`TSE_TICK_TRANSLATIONS`** env var → path to a JSON file, **merged over** the built-in (optional) |
| New public functions | **None** |

## Design

### 1. Data file — `tse_tick/data/translations.json`

Shipped inside the package/wheel. Structure mirrors today's tables exactly:

```json
{
  "_meta": {
    "description": "external library name -> tse_tick name. Edit to amend mappings.",
    "resolution": "translate() checks 'arguments' before 'functions'; a list value returns its first item."
  },
  "yfinance": {
    "functions": {
      "download": ["create_df", "read_ticks", "ingest_period"],
      "Ticker.history": "query_ticks",
      "history": "query_ticks",
      "tickers": "get_available_tickers",
      "Tickers": "get_available_tickers"
    },
    "arguments": { "tickers": "ticker_filter", "start": "start_time", "end": "end_time" }
  },
  "polygon": {
    "functions": { "get_aggs": "query_ticks", "list_aggs": "query_ticks", "list_trades": "query_ticks", "list_tickers": "get_available_tickers" },
    "arguments": { "ticker": "ticker", "from_": "start_time", "to": "end_time", "limit": "limit" }
  },
  "ccxt": {
    "functions": { "fetch_ohlcv": "query_ticks", "fetch_trades": ["query_ticks", "read_ticks"], "symbols": "get_available_tickers", "load_markets": "get_available_tickers" },
    "arguments": { "symbol": "ticker", "since": "start_time", "limit": "limit" }
  }
}
```

Schema rules:
- Top-level keys are **source names**, except keys beginning with `_` (e.g. `_meta`), which are
  ignored by the loader.
- Each source is `{"functions": {name: target}, "arguments": {name: target}}`; both subtables
  optional (default empty).
- `target` is a `string` or an `array of strings` (a list means several `tse_tick` names map to one
  external name; `translate()` returns the first — the closest equivalent).

### 2. Loading (`translate.py` becomes a thin data-driven loader)

- **Built-in:** at import, read via
  `importlib.resources.files("tse_tick").joinpath("data/translations.json")` (stdlib, 3.9+).
- **Override:** if `os.environ["TSE_TICK_TRANSLATIONS"]` is set and the file is readable, **deep-merge**
  it over the built-in (see §3).
- The result builds module globals `_FUNCTION_MAP`, `_ARGUMENT_MAP`, and `SUPPORTED_SOURCES`
  (derived from the merged source keys, so a JSON-only new source needs no code change).
- Loaded once at import (the file is tiny). *Alternative considered — lazy load on first call —
  rejected as needless complexity.*

Internal shape:
- `_load_data(override_path: Optional[str]) -> Dict[str, Dict[str, Dict[str, _NameTarget]]]`
  — pure function: reads built-in, optionally merges override, returns `{source: {"functions": {...},
  "arguments": {...}}}` with metadata stripped. **Unit-testable in isolation.**
- `_reload(override_path: Optional[str] = None) -> None` — private; rebuilds the module globals
  (defaults to reading the env var). Called once at import; also the test hook for the override path
  (no subprocess needed). Not documented as public API.

### 3. Override merge semantics

Deep-merge, user wins:
- For each source in the override: if absent in built-in, add it wholesale; if present, merge its
  `functions` and `arguments` dicts at the **name** level — an override value replaces the built-in
  value, and new names are added.
- Metadata (`_*`) keys ignored in both files.

### 4. Error handling — `import tse_tick` must never break

- **Built-in** file unreadable / invalid JSON → `logging.getLogger(__name__).error(...)`, fall back to
  empty tables (`translate()` returns `None`, `mapping()` returns `{}`). A test guarantees the shipped
  file is valid, so this path never triggers in practice.
- **Override** file missing / invalid JSON / not a JSON object → `logging.warning(...)`, ignore the
  override and use the built-in only.
- A malformed individual source/subtable in either file → warn and skip that piece; keep the rest.

### 5. Public API — unchanged

`translate(source, name)`, `mapping(source=None)`, `SUPPORTED_SOURCES` keep identical signatures,
return shapes, and semantics (argument-table-before-function-table, exact-before-casefold,
list→first, `ValueError` for an unknown source). `mapping()` reflects the **effective** (merged)
tables and never exposes `_meta`.

## Packaging

- Add the data file to `[tool.setuptools.package-data]`: `tse_tick = ["py.typed", "data/*.json"]`.
- No `tse_tick/data/__init__.py` needed (accessed via `files("tse_tick").joinpath("data/...")`).
- Verify `python -m build` includes `tse_tick/data/translations.json` in **both** sdist and wheel.

## Testing

Keep every existing `test_api_additions.py` translate test (they assert behaviour, now data-driven).
Add `tests/test_translate_data.py`:

1. **Shipped file valid & accessible:** `importlib.resources.files("tse_tick").joinpath("data/translations.json").is_file()`; it parses; baseline entries match (`translate("polygon","get_aggs") == "query_ticks"`, `set(mapping()) == {"yfinance","polygon","ccxt"}`).
2. **Override merges (`_load_data`):** a temp override JSON that (a) overrides an existing name and
   (b) adds a new source+name → merged result reflects both; built-in untouched entries remain.
3. **End-to-end via env var:** monkeypatch `TSE_TICK_TRANSLATIONS` to a temp file, call `_reload()`,
   assert `translate()` / `mapping()` / `SUPPORTED_SOURCES` reflect the override, then `_reload()` in
   teardown to restore built-in state.
4. **Malformed override ignored:** bad JSON / non-object override → no exception, built-in tables
   intact (assert a baseline entry still resolves).
5. **Missing override path ignored:** non-existent path → built-in used, no error.

## Documentation updates

- `GETTING_STARTED.md` §6 — note the env override and that contributors edit `data/translations.json`.
- `README.md`, `ARCHITECTURE.md` — mention the file-driven tables + the new data file in the module map.
- `CONTRIBUTING.md` — "To add/adjust a name mapping, edit `tse_tick/data/translations.json` (no code)."
- `CHANGELOG.md` — `[Unreleased]` entry (Changed: translation tables externalized to JSON + optional
  `TSE_TICK_TRANSLATIONS` override; public API unchanged).

## Backward compatibility

Fully backward-compatible. The shipped JSON reproduces the current tables verbatim, so all public
behaviour and existing tests are unchanged. The only user-visible *addition* is the optional env var.

## Acceptance criteria

- `pip install tse-tick` → `translate()`/`mapping()` work with **no configuration** (built-in file).
- Setting `TSE_TICK_TRANSLATIONS` merges a user file over the built-in; unset = built-in only.
- A malformed/missing override never raises; `import tse_tick` always succeeds.
- Full test suite green; the built wheel contains `tse_tick/data/translations.json`.
