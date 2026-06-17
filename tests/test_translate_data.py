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
