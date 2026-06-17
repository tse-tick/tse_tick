# File-Driven Translation Tables Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move `tse_tick.translate`'s mapping data out of Python into a shipped `tse_tick/data/translations.json`, loaded at import, with an optional `TSE_TICK_TRANSLATIONS` env-var override — public API and behaviour unchanged.

**Architecture:** `translate.py` becomes a thin loader: read the packaged JSON via `importlib.resources`, optionally deep-merge a user override file over it, and build the module's `_FUNCTION_MAP` / `_ARGUMENT_MAP` / `SUPPORTED_SOURCES` (now derived from the data). `translate()` / `mapping()` keep identical logic. Loading is defensive so `import tse_tick` can never break.

**Tech Stack:** Python 3.9+, stdlib only (`json`, `os`, `logging`, `importlib.resources`), pytest, setuptools `package-data`.

**Spec:** `docs/superpowers/specs/2026-06-17-translation-mapping-file-design.md`

## Global Constraints

- **Python 3.9+** — no 3.11-only stdlib (`tomllib` forbidden). `importlib.resources.files()` is OK (3.9+).
- **No new runtime dependencies** — stdlib only.
- **Public API unchanged** — `translate(source, name)`, `mapping(source=None)`, `SUPPORTED_SOURCES` keep identical signatures, return shapes, and behaviour (argument-table-before-function-table; exact-before-casefold; list→first; `ValueError` on unknown source).
- **`import tse_tick` must never raise** due to translation loading.
- **Mapping content reproduced verbatim** — no semantic changes to any entry.
- **All existing tests stay green** (especially `tests/test_api_additions.py`).

---

### Task 1: Ship the JSON data file + package it

**Files:**
- Create: `tse_tick/data/translations.json`
- Modify: `pyproject.toml:96-97` (`[tool.setuptools.package-data]`)
- Test: `tests/test_translate_data.py`

**Interfaces:**
- Produces: the packaged resource `tse_tick/data/translations.json`, accessible via `importlib.resources.files("tse_tick").joinpath("data/translations.json")`. Schema: `{source: {"functions": {name: str|list[str]}, "arguments": {name: str|list[str]}}}` plus optional `_`-prefixed metadata keys.

- [ ] **Step 1: Write the failing test**

Create `tests/test_translate_data.py`:

```python
# tests/test_translate_data.py
"""Tests for the file-driven translation tables (data file + env override)."""
import json
from importlib.resources import files

import pytest


def test_builtin_data_file_present_and_valid():
    resource = files("tse_tick").joinpath("data/translations.json")
    assert resource.is_file()
    data = json.loads(resource.read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    # the three baseline sources exist with both subtables
    for src in ("yfinance", "polygon", "ccxt"):
        assert set(data[src]) >= {"functions", "arguments"}
    # a couple of baseline entries are intact
    assert data["polygon"]["functions"]["get_aggs"] == "query_ticks"
    assert data["yfinance"]["arguments"]["tickers"] == "ticker_filter"
    assert data["ccxt"]["functions"]["fetch_trades"] == ["query_ticks", "read_ticks"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_translate_data.py -q -o addopts=""`
Expected: FAIL — `translations.json` does not exist yet (`is_file()` is False / `FileNotFoundError`).

- [ ] **Step 3: Create the data file (verbatim port of the current dicts)**

Create `tse_tick/data/translations.json`:

```json
{
  "_meta": {
    "description": "external library name -> tse_tick name. Edit this file to amend the translation tables; no code change needed.",
    "resolution": "translate() checks 'arguments' before 'functions'; a list value returns its first item (closest equivalent).",
    "override": "Set TSE_TICK_TRANSLATIONS to a JSON file of the same shape to merge your own entries over these."
  },
  "yfinance": {
    "functions": {
      "download": ["create_df", "read_ticks", "ingest_period"],
      "Ticker.history": "query_ticks",
      "history": "query_ticks",
      "tickers": "get_available_tickers",
      "Tickers": "get_available_tickers"
    },
    "arguments": {
      "tickers": "ticker_filter",
      "start": "start_time",
      "end": "end_time"
    }
  },
  "polygon": {
    "functions": {
      "get_aggs": "query_ticks",
      "list_aggs": "query_ticks",
      "list_trades": "query_ticks",
      "list_tickers": "get_available_tickers"
    },
    "arguments": {
      "ticker": "ticker",
      "from_": "start_time",
      "to": "end_time",
      "limit": "limit"
    }
  },
  "ccxt": {
    "functions": {
      "fetch_ohlcv": "query_ticks",
      "fetch_trades": ["query_ticks", "read_ticks"],
      "symbols": "get_available_tickers",
      "load_markets": "get_available_tickers"
    },
    "arguments": {
      "symbol": "ticker",
      "since": "start_time",
      "limit": "limit"
    }
  }
}
```

- [ ] **Step 4: Add the data file to packaging**

In `pyproject.toml`, change:

```toml
[tool.setuptools.package-data]
tse_tick = ["py.typed"]
```
to:
```toml
[tool.setuptools.package-data]
tse_tick = ["py.typed", "data/*.json"]
```

- [ ] **Step 5: Run test to verify it passes**

Run: `python -m pytest tests/test_translate_data.py -q -o addopts=""`
Expected: PASS (1 passed).

- [ ] **Step 6: Commit**

```bash
git add tse_tick/data/translations.json pyproject.toml tests/test_translate_data.py
git commit -m "feat(translate): ship mapping tables as packaged JSON data file"
```

---

### Task 2: Make `translate.py` load from the data file (built-in only)

**Files:**
- Modify: `tse_tick/translate.py` (replace the inline `_FUNCTION_MAP` / `_ARGUMENT_MAP` dicts and `SUPPORTED_SOURCES` constant with a loader; keep `translate` / `mapping` / `_normalize_source` / `_first`)
- Test: `tests/test_translate_data.py` (extend), and `tests/test_api_additions.py` (must stay green, unchanged)

**Interfaces:**
- Consumes: `tse_tick/data/translations.json` (Task 1).
- Produces (module-level, in `tse_tick.translate`): `translate(source, name)`, `mapping(source=None)`, `SUPPORTED_SOURCES: tuple`, and the private helpers `_load_data(override_path)`, `_reload(override_path=None)`, `_FUNCTION_MAP`, `_ARGUMENT_MAP`.

- [ ] **Step 1: Write the failing test (data is file-driven, sources derived)**

Append to `tests/test_translate_data.py`:

```python
def test_supported_sources_derived_from_file():
    import tse_tick.translate as t
    assert set(t.SUPPORTED_SOURCES) == {"yfinance", "polygon", "ccxt"}


def test_load_data_returns_normalized_structure():
    import tse_tick.translate as t
    data = t._load_data(None)              # built-in only
    assert set(data) == {"yfinance", "polygon", "ccxt"}
    for src in data.values():
        assert set(src) == {"functions", "arguments"}
    assert data["polygon"]["functions"]["get_aggs"] == "query_ticks"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_translate_data.py -q -o addopts=""`
Expected: FAIL — `tse_tick.translate` has no `_load_data` attribute (AttributeError).

- [ ] **Step 3: Replace `translate.py` with the loader (built-in path)**

Replace the entire contents of `tse_tick/translate.py` with:

```python
# tse_tick/translate.py
"""Static translation layer: external-library names -> tse_tick names.

Users coming from yfinance, the Polygon client, or ccxt can look up the
``tse_tick`` equivalent of a function or argument they already know, *without*
this package importing or depending on any of those libraries.

The mapping tables live in a shipped data file, ``tse_tick/data/translations.json``,
loaded at import. To amend them, edit that file (no code change). Power users can
point ``TSE_TICK_TRANSLATIONS`` at a JSON file of the same shape to merge their own
entries over the built-in ones (see ``_reload``); the average user needs no config.

    >>> from tse_tick import translate
    >>> translate("polygon", "get_aggs")
    'query_ticks'
    >>> translate("yfinance", "tickers")
    'ticker_filter'
    >>> translate("ccxt", "no_such_call") is None
    True
"""

import json
import logging
import os
from importlib.resources import files as _resource_files
from typing import Dict, List, Optional, Union

__all__ = ["translate", "mapping", "SUPPORTED_SOURCES"]

logger = logging.getLogger(__name__)

_OVERRIDE_ENV = "TSE_TICK_TRANSLATIONS"
_DATA_PACKAGE = "tse_tick"
_DATA_RESOURCE = "data/translations.json"

_NameTarget = Union[str, List[str]]
_SourceTables = Dict[str, Dict[str, _NameTarget]]  # {"functions": {...}, "arguments": {...}}


def _parse_json_object(text: str) -> Optional[dict]:
    """Parse JSON text; return the dict, or None if it is not a JSON object."""
    try:
        data = json.loads(text)
    except (ValueError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def _read_builtin() -> dict:
    """Read the packaged translations.json; ``{}`` on any failure (logged)."""
    try:
        text = _resource_files(_DATA_PACKAGE).joinpath(_DATA_RESOURCE).read_text(encoding="utf-8")
    except Exception as exc:  # pragma: no cover - shipped file is always present
        logger.error("tse_tick: could not read built-in translations %r: %s", _DATA_RESOURCE, exc)
        return {}
    data = _parse_json_object(text)
    if data is None:  # pragma: no cover - shipped file is valid (tested)
        logger.error("tse_tick: built-in translations %r is not a JSON object", _DATA_RESOURCE)
        return {}
    return data


def _read_override(override_path: Optional[str]) -> dict:
    """Read the user override file if set+valid; ``{}`` otherwise (warned)."""
    if not override_path:
        return {}
    if not os.path.isfile(override_path):
        logger.warning("tse_tick: %s=%r not found; ignoring override", _OVERRIDE_ENV, override_path)
        return {}
    try:
        with open(override_path, "r", encoding="utf-8") as fh:
            text = fh.read()
    except OSError as exc:
        logger.warning("tse_tick: could not read %s=%r: %s; ignoring", _OVERRIDE_ENV, override_path, exc)
        return {}
    data = _parse_json_object(text)
    if data is None:
        logger.warning("tse_tick: %s=%r is not a JSON object; ignoring", _OVERRIDE_ENV, override_path)
        return {}
    return data


def _coerce_source(raw_source: object) -> Optional[_SourceTables]:
    """Coerce one source's raw value into ``{"functions": {...}, "arguments": {...}}``.

    Returns None if the value is not an object. Keeps only ``str``-keyed entries
    whose value is a ``str`` or a ``list`` of ``str``; both subtables default to
    empty so downstream access is always safe.
    """
    if not isinstance(raw_source, dict):
        return None
    tables: _SourceTables = {}
    for kind in ("functions", "arguments"):
        sub = raw_source.get(kind, {})
        if isinstance(sub, dict):
            tables[kind] = {
                str(k): v
                for k, v in sub.items()
                if isinstance(v, str) or (isinstance(v, list) and all(isinstance(x, str) for x in v))
            }
        else:
            tables[kind] = {}
    return tables


def _normalize_tables(raw: dict) -> Dict[str, _SourceTables]:
    """Drop ``_``-prefixed metadata keys and coerce each remaining source."""
    out: Dict[str, _SourceTables] = {}
    for key, value in raw.items():
        if key.startswith("_"):
            continue
        coerced = _coerce_source(value)
        if coerced is None:
            logger.warning("tse_tick: translations source %r is not an object; skipping", key)
            continue
        out[key] = coerced
    return out


def _merge(base: Dict[str, _SourceTables], override: Dict[str, _SourceTables]) -> Dict[str, _SourceTables]:
    """Deep-merge ``override`` over ``base`` at the name level; user entries win."""
    merged: Dict[str, _SourceTables] = {
        src: {"functions": dict(t["functions"]), "arguments": dict(t["arguments"])}
        for src, t in base.items()
    }
    for src, tables in override.items():
        slot = merged.setdefault(src, {"functions": {}, "arguments": {}})
        for kind in ("functions", "arguments"):
            slot[kind].update(tables.get(kind, {}))
    return merged


def _load_data(override_path: Optional[str]) -> Dict[str, _SourceTables]:
    """Built-in tables plus an optional override file, merged. Never raises."""
    base = _normalize_tables(_read_builtin())
    override = _normalize_tables(_read_override(override_path))
    return _merge(base, override)


# --- module state, (re)built by _reload() ---------------------------------- #
_DATA: Dict[str, _SourceTables] = {}
SUPPORTED_SOURCES: tuple = ()
_FUNCTION_MAP: Dict[str, Dict[str, _NameTarget]] = {}
_ARGUMENT_MAP: Dict[str, Dict[str, _NameTarget]] = {}


def _reload(override_path: Optional[str] = None) -> None:
    """Rebuild the module tables from the built-in file + override.

    Private maintenance/test hook (not public API). ``override_path=None`` reads
    the ``TSE_TICK_TRANSLATIONS`` environment variable.
    """
    global _DATA, SUPPORTED_SOURCES, _FUNCTION_MAP, _ARGUMENT_MAP
    if override_path is None:
        override_path = os.environ.get(_OVERRIDE_ENV)
    data = _load_data(override_path)
    _DATA = data
    SUPPORTED_SOURCES = tuple(data.keys())
    _FUNCTION_MAP = {src: tables["functions"] for src, tables in data.items()}
    _ARGUMENT_MAP = {src: tables["arguments"] for src, tables in data.items()}


def _normalize_source(source: str) -> str:
    src = str(source).strip().lower()
    if src not in SUPPORTED_SOURCES:
        raise ValueError(
            f"Unknown source {source!r}. Supported sources: {', '.join(SUPPORTED_SOURCES)}"
        )
    return src


def _first(value: _NameTarget) -> str:
    return value[0] if isinstance(value, list) else value


def translate(source: str, name: str) -> Optional[str]:
    """Return the ``tse_tick`` name for an external function/argument, or ``None``.

    Args:
        source: One of the supported libraries (case-insensitive); see ``SUPPORTED_SOURCES``.
        name: A function or argument name from that library.

    Returns:
        The matching ``tse_tick`` name, or ``None`` if there is no entry. When one
        external call maps to several ``tse_tick`` functions, the first (closest) is returned.

    Resolution order: argument tables before function tables — so a token used as
    both (e.g. yfinance ``tickers``) resolves to the argument sense — and
    exact-case before case-insensitive.

    Raises:
        ValueError: If ``source`` is not a supported library.

    Example:
        >>> translate("polygon", "get_aggs")
        'query_ticks'
        >>> translate("yfinance", "tickers")
        'ticker_filter'
    """
    src = _normalize_source(source)
    args = _ARGUMENT_MAP.get(src, {})
    funcs = _FUNCTION_MAP.get(src, {})

    if name in args:
        return _first(args[name])
    if name in funcs:
        return _first(funcs[name])

    lowered = name.lower()
    for table in (args, funcs):
        for key, value in table.items():
            if key.lower() == lowered:
                return _first(value)
    return None


def mapping(source: Optional[str] = None) -> Dict:
    """Return the effective translation tables, for documentation or ``help()``.

    Args:
        source: A supported source name, or ``None`` for every source.

    Returns:
        With ``source=None``: ``{source: {"functions": {...}, "arguments": {...}}}``
        for all sources. With a source name: that source's tables. The returned
        dicts are shallow copies; callers may display or mutate them freely.

    Raises:
        ValueError: If ``source`` is given but not a supported library.
    """
    def _tables(src: str) -> Dict[str, Dict[str, _NameTarget]]:
        return {
            "functions": dict(_FUNCTION_MAP.get(src, {})),
            "arguments": dict(_ARGUMENT_MAP.get(src, {})),
        }

    if source is None:
        return {src: _tables(src) for src in SUPPORTED_SOURCES}

    src = _normalize_source(source)
    return _tables(src)


_reload()  # build the tables at import time
```

- [ ] **Step 4: Run the new + existing translate tests**

Run: `python -m pytest tests/test_translate_data.py tests/test_api_additions.py -q -o addopts=""`
Expected: PASS (all green — existing translate/mapping tests still pass against the file-driven tables; new derived-sources tests pass).

- [ ] **Step 5: Commit**

```bash
git add tse_tick/translate.py tests/test_translate_data.py
git commit -m "refactor(translate): load mapping tables from the data file at import"
```

---

### Task 3: Test the optional `TSE_TICK_TRANSLATIONS` override

**Files:**
- Test: `tests/test_translate_data.py` (extend) — exercises override code already present in `translate.py` from Task 2.

**Interfaces:**
- Consumes: `tse_tick.translate._reload(override_path)`, `_load_data(override_path)`, `translate`, `mapping`, `SUPPORTED_SOURCES` (Task 2).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_translate_data.py`:

```python
import tse_tick.translate as _t


@pytest.fixture
def restore_translations():
    """Ensure module tables are rebuilt from the built-in file after a test."""
    yield
    _t._reload(None)  # env is restored by monkeypatch before this runs


def test_override_merges_and_adds_source(tmp_path, monkeypatch, restore_translations):
    override = tmp_path / "ov.json"
    override.write_text(json.dumps({
        "polygon": {"functions": {"get_aggs": "OVERRIDDEN"}},   # override existing
        "alpaca": {"functions": {"get_bars": "query_ticks"}},   # brand-new source
    }), encoding="utf-8")

    monkeypatch.setenv("TSE_TICK_TRANSLATIONS", str(override))
    _t._reload()

    assert _t.translate("polygon", "get_aggs") == "OVERRIDDEN"      # user wins
    assert _t.translate("alpaca", "get_bars") == "query_ticks"      # new source works
    assert "alpaca" in _t.SUPPORTED_SOURCES
    assert "alpaca" in _t.mapping()
    assert _t.translate("ccxt", "fetch_ohlcv") == "query_ticks"     # untouched entry intact


def test_malformed_override_is_ignored(tmp_path, monkeypatch, restore_translations):
    bad = tmp_path / "bad.json"
    bad.write_text("{ not valid json", encoding="utf-8")
    monkeypatch.setenv("TSE_TICK_TRANSLATIONS", str(bad))
    _t._reload()  # must not raise
    assert _t.translate("polygon", "get_aggs") == "query_ticks"     # built-in intact
    assert set(_t.SUPPORTED_SOURCES) == {"yfinance", "polygon", "ccxt"}


def test_missing_override_path_is_ignored(tmp_path, monkeypatch, restore_translations):
    monkeypatch.setenv("TSE_TICK_TRANSLATIONS", str(tmp_path / "does_not_exist.json"))
    _t._reload()  # must not raise
    assert _t.translate("yfinance", "tickers") == "ticker_filter"


def test_unset_env_uses_builtin_only(monkeypatch, restore_translations):
    monkeypatch.delenv("TSE_TICK_TRANSLATIONS", raising=False)
    _t._reload()
    assert set(_t.SUPPORTED_SOURCES) == {"yfinance", "polygon", "ccxt"}
```

- [ ] **Step 2: Run the tests**

Run: `python -m pytest tests/test_translate_data.py -q -o addopts=""`
Expected: PASS — the override is merged, malformed/missing files are ignored without raising, and the `restore_translations` fixture leaves the built-in tables in place for other tests.

- [ ] **Step 3: Run the full fast suite to confirm no global-state leakage**

Run: `TSE_TICK_DATA_ROOT=/nonexistent_skip_realdata python -m pytest tests/ -q -o addopts="" -p no:cacheprovider`
Expected: PASS — `160 passed`-or-more, `48 skipped` (the new tests add to the pass count; nothing breaks).

- [ ] **Step 4: Commit**

```bash
git add tests/test_translate_data.py
git commit -m "test(translate): cover the TSE_TICK_TRANSLATIONS override path"
```

---

### Task 4: Update documentation

**Files:**
- Modify: `GETTING_STARTED.md` (§6 translation section)
- Modify: `ARCHITECTURE.md` (module map + translate description)
- Modify: `README.md` (Features / What's New mention of translate)
- Modify: `CONTRIBUTING.md` (how to add a mapping)
- Modify: `CHANGELOG.md` (`[Unreleased]`)

- [ ] **Step 1: GETTING_STARTED.md §6 — add the override note**

Replace the closing line of the "Coming from yfinance / Polygon / ccxt?" section:
```markdown
mapping()                            # the full table, for reference
```
```
with:
```
```markdown
mapping()                            # the full table, for reference
```

The tables live in `tse_tick/data/translations.json`. To amend them, edit that file
(contributors) or point `TSE_TICK_TRANSLATIONS` at your own JSON file of the same shape
to merge your entries over the built-in ones (power users):

```bash
export TSE_TICK_TRANSLATIONS=~/my_translations.json   # optional; unset = built-in default
```
```

- [ ] **Step 2: ARCHITECTURE.md — list the data file and note file-driven tables**

In the directory-structure block, add under `translate.py`:
```
│   ├── translate.py                 # Static yfinance/Polygon/ccxt → tse_tick name map; translate()/mapping() (0.3.0)
```
add the line:
```
│   ├── data/
│   │   └── translations.json        # Mapping tables loaded at import (override: TSE_TICK_TRANSLATIONS)
```
And update the `translate.py` comment tail to: `... loads tables from data/translations.json`.

- [ ] **Step 3: README.md — note the file-driven tables**

In the **Name translation** feature bullet, change:
```markdown
- **Name translation** (`translate`) — look up the `tse_tick` equivalent of a yfinance / Polygon / ccxt call
```
to:
```markdown
- **Name translation** (`translate`) — look up the `tse_tick` equivalent of a yfinance / Polygon / ccxt call (tables in `tse_tick/data/translations.json`; override with `TSE_TICK_TRANSLATIONS`)
```

- [ ] **Step 4: CONTRIBUTING.md — how to add a mapping**

Append a short section:
```markdown
## Adding a name-translation mapping

The yfinance / Polygon / ccxt → `tse_tick` tables live in
`tse_tick/data/translations.json` (no Python). To add or change a mapping, edit that
file: under the source, add the external name → our name to `functions` or `arguments`
(a list value means several of our names map to one external call; `translate()` returns
the first). Run `pytest tests/test_translate_data.py` and open a PR.
```

- [ ] **Step 5: CHANGELOG.md — `[Unreleased]` entry**

Under `## [Unreleased]`, add:
```markdown
### Changed
- **Translation tables externalized to data** (`tse_tick/data/translations.json`): the yfinance /
  Polygon / ccxt → `tse_tick` name maps now load from a shipped JSON file at import instead of inline
  Python dicts, so contributors can amend them with no code change. Power users can merge their own
  entries by pointing the optional `TSE_TICK_TRANSLATIONS` env var at a JSON file of the same shape.
  The public API (`translate` / `mapping` / `SUPPORTED_SOURCES`) and default behaviour are unchanged.
```

- [ ] **Step 6: Commit**

```bash
git add GETTING_STARTED.md ARCHITECTURE.md README.md CONTRIBUTING.md CHANGELOG.md
git commit -m "docs: document file-driven translations + TSE_TICK_TRANSLATIONS override"
```

> Note: if `GETTING_STARTED.md` is not yet tracked in git, `git add` will start tracking it; that is fine.

---

### Task 5: Integrity audit (packaging + full suite)

**Files:** none (verification only).

- [ ] **Step 1: Build and verify the data file ships in both artifacts**

Run:
```bash
rm -f dist/* && python -m build 2>&1 | tail -3
tar tzf dist/tse_tick-*.tar.gz | grep translations.json
python -c "import zipfile,glob; w=glob.glob('dist/tse_tick-*-py3-none-any.whl')[0]; print([n for n in zipfile.ZipFile(w).namelist() if 'translations.json' in n])"
```
Expected: build succeeds; both commands print `tse_tick/data/translations.json` (sdist path is prefixed with `tse_tick-<ver>/`).

- [ ] **Step 2: Confirm `twine check` still passes**

Run: `python -m twine check dist/*`
Expected: both artifacts PASSED.

- [ ] **Step 3: Full real-data test suite**

Run: `python -m pytest tests/ -q -o addopts="" -p no:cacheprovider`
Expected: all pass (the prior 208 plus the new `test_translate_data.py` tests), 0 failed.

- [ ] **Step 4: Clean-import smoke (default = no env var)**

Run:
```bash
python -c "import tse_tick as t; print(t.translate('polygon','get_aggs')); print(t.translate('yfinance','tickers')); print(sorted(t.mapping()))"
```
Expected: `query_ticks`, `ticker_filter`, `['ccxt', 'polygon', 'yfinance']` — works with zero configuration.

- [ ] **Step 5: No commit (verification only).** If any step fails, fix in the relevant task and re-run.

---

## Self-Review

**Spec coverage:** data file (T1) ✓; loader via importlib.resources (T2) ✓; env override + deep-merge (T2 code, T3 tests) ✓; SUPPORTED_SOURCES derived (T2) ✓; error handling / import never breaks (T2 `_read_*`, T3 malformed/missing tests) ✓; public API unchanged (T2 keeps signatures; existing tests green) ✓; packaging (T1 + T5 verify) ✓; tests (T1/T2/T3) ✓; docs (T4) ✓; backward-compat verbatim port (T1 JSON) ✓; out-of-scope TOML / public mutation API — not added ✓.

**Placeholder scan:** none — every step has the actual file content, code, or command.

**Type consistency:** `_load_data(override_path)` / `_reload(override_path=None)` / `_FUNCTION_MAP` / `_ARGUMENT_MAP` / `SUPPORTED_SOURCES` are defined in Task 2 and used consistently in Task 3 tests. `_SourceTables = {"functions": {...}, "arguments": {...}}` is consistent across `_coerce_source`, `_merge`, `_reload`.
