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


@pytest.fixture
def restore_translations():
    """Restore the built-in tables after a test (independent of env teardown order)."""
    yield
    _translate._reload("")  # empty path -> no override -> built-in only


def test_override_merges_and_adds_source(tmp_path, monkeypatch, restore_translations):
    override = tmp_path / "ov.json"
    override.write_text(json.dumps({
        "polygon": {"functions": {"get_aggs": "OVERRIDDEN"}},   # override existing
        "alpaca": {"functions": {"get_bars": "query_ticks"}},   # brand-new source
    }), encoding="utf-8")

    monkeypatch.setenv("TSE_TICK_TRANSLATIONS", str(override))
    _translate._reload()

    assert _translate.translate("polygon", "get_aggs") == "OVERRIDDEN"      # user wins
    assert _translate.translate("alpaca", "get_bars") == "query_ticks"      # new source works
    assert "alpaca" in _translate.SUPPORTED_SOURCES
    assert "alpaca" in _translate.mapping()
    assert _translate.translate("ccxt", "fetch_ohlcv") == "query_ticks"     # untouched entry intact


def test_malformed_override_is_ignored(tmp_path, monkeypatch, restore_translations):
    bad = tmp_path / "bad.json"
    bad.write_text("{ not valid json", encoding="utf-8")
    monkeypatch.setenv("TSE_TICK_TRANSLATIONS", str(bad))
    _translate._reload()  # must not raise
    assert _translate.translate("polygon", "get_aggs") == "query_ticks"     # built-in intact
    assert set(_translate.SUPPORTED_SOURCES) == {"yfinance", "polygon", "ccxt"}


def test_missing_override_path_is_ignored(tmp_path, monkeypatch, restore_translations):
    monkeypatch.setenv("TSE_TICK_TRANSLATIONS", str(tmp_path / "does_not_exist.json"))
    _translate._reload()  # must not raise
    assert _translate.translate("yfinance", "tickers") == "ticker_filter"


def test_unset_env_uses_builtin_only(monkeypatch, restore_translations):
    monkeypatch.delenv("TSE_TICK_TRANSLATIONS", raising=False)
    _translate._reload()
    assert set(_translate.SUPPORTED_SOURCES) == {"yfinance", "polygon", "ccxt"}
