# tse_tick/translate.py
"""Static translation layer: external-library names -> tse_tick names.

Users coming from yfinance, the Polygon client, or ccxt can look up the
``tse_tick`` equivalent of a function or argument they already know, *without*
this package importing or depending on any of those libraries. The tables below
are a small, hand-maintained, dependency-free reference.

Why translate rather than rename: a static lookup is a reference we maintain at
our own pace. Renaming our API to mirror yfinance / Polygon / ccxt would chase
libraries whose signatures change and whose semantics differ (yfinance hits a
web API; we read local NEEDS ZIPs). See ``PYPI_RELEASE_PLAN.md`` for the full
rationale.

Read every entry "external name -> our name":

    >>> from tse_tick import translate
    >>> translate("polygon", "get_aggs")
    'query_ticks'
    >>> translate("yfinance", "tickers")
    'ticker_filter'
    >>> translate("ccxt", "no_such_call") is None
    True
"""

from typing import Dict, List, Optional, Union

__all__ = ["translate", "mapping", "SUPPORTED_SOURCES"]

SUPPORTED_SOURCES = ("yfinance", "polygon", "ccxt")

_NameTarget = Union[str, List[str]]

# External *function* name -> tse_tick function(s) covering the same concept.
# A list means one external call corresponds to several tse_tick functions;
# ``translate`` returns the first, i.e. the closest equivalent.
_FUNCTION_MAP: Dict[str, Dict[str, _NameTarget]] = {
    "yfinance": {
        # download a file -> DataFrame (one-shot), or build the store in bulk
        "download": ["create_df", "read_ticks", "ingest_period"],
        "Ticker.history": "query_ticks",
        "history": "query_ticks",
        # listing available instruments (yf.Tickers container / .tickers)
        "tickers": "get_available_tickers",
        "Tickers": "get_available_tickers",
    },
    "polygon": {
        "get_aggs": "query_ticks",
        "list_aggs": "query_ticks",
        "list_trades": "query_ticks",
        "list_tickers": "get_available_tickers",
    },
    "ccxt": {
        "fetch_ohlcv": "query_ticks",
        "fetch_trades": ["query_ticks", "read_ticks"],
        "symbols": "get_available_tickers",
        "load_markets": "get_available_tickers",
    },
}

# External *argument* name -> tse_tick argument name. When a token is used as
# both a function and an argument (e.g. yfinance ``tickers``), the argument sense
# wins in ``translate`` (see the resolution order in its docstring).
_ARGUMENT_MAP: Dict[str, Dict[str, _NameTarget]] = {
    "yfinance": {
        "tickers": "ticker_filter",   # read & ingest take a set of codes
        "start": "start_time",
        "end": "end_time",
    },
    "polygon": {
        "ticker": "ticker",           # query_ticks takes a single ticker
        "from_": "start_time",
        "to": "end_time",
        "limit": "limit",
    },
    "ccxt": {
        "symbol": "ticker",
        "since": "start_time",
        "limit": "limit",
    },
}


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
        source: One of ``"yfinance"``, ``"polygon"``, ``"ccxt"`` (case-insensitive).
        name: A function or argument name from that library.

    Returns:
        The matching ``tse_tick`` name, or ``None`` if there is no entry.
        When one external call maps to several ``tse_tick`` functions, the
        closest equivalent (the first listed) is returned.

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
    """Return the raw translation tables, for documentation or ``help()``.

    Args:
        source: A supported source name, or ``None`` for every source.

    Returns:
        With ``source=None``: ``{source: {"functions": {...}, "arguments": {...}}}``
        for all sources. With a source name: that source's
        ``{"functions": {...}, "arguments": {...}}``. The returned dicts are
        shallow copies, so callers can display or mutate them freely.

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
