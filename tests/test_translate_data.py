# tests/test_translate_data.py
"""Tests for the file-driven translation tables (data file + env override)."""
import importlib
import json
from importlib.resources import files

import pytest

# tse_tick/__init__ rebinds the attribute ``tse_tick.translate`` to the *function*,
# so ``import tse_tick.translate as x`` would yield the function, not the module.
# Use import_module to reliably reach the module internals (_load_data, _reload,
# SUPPORTED_SOURCES) that these tests need.
_translate = importlib.import_module("tse_tick.translate")


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


def test_supported_sources_derived_from_file():
    assert set(_translate.SUPPORTED_SOURCES) == {"yfinance", "polygon", "ccxt"}


def test_load_data_returns_normalized_structure():
    data = _translate._load_data(None)     # built-in only
    assert set(data) == {"yfinance", "polygon", "ccxt"}
    for src in data.values():
        assert set(src) == {"functions", "arguments"}
    assert data["polygon"]["functions"]["get_aggs"] == "query_ticks"
