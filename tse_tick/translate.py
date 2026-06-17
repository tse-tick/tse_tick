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
