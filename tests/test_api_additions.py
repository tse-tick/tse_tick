# tests/test_api_additions.py
"""Tests for the 0.3.0 additive API: the DataType/Language enums (A1), the
query_ticks str/int ticker normalization (A3), and the translate()/mapping()
name-translation layer (A0). No proprietary NEEDS data is used."""

import pytest

import tse_tick
from tse_tick import DataType, Language, translate, mapping


# --------------------------------------------------------------------------- #
# A1 — DataType / Language enums
# --------------------------------------------------------------------------- #

def test_enums_importable_and_str_valued():
    assert issubclass(DataType, str)
    assert issubclass(Language, str)
    # Members compare equal to their magic-string values.
    assert DataType.INDIVIDUAL_STOCK == "individual_stock"
    assert DataType.STOCK_SUMMARY == "stock_summary"
    assert DataType.INDICES == "indices"
    assert DataType.INDICES_SUMMARY == "indices_summary"
    assert Language.EN == "en"
    assert Language.JP == "jp"
    # str() / f-strings yield the bare value, not "DataType.INDIVIDUAL_STOCK".
    assert f"{DataType.INDICES}" == "indices"
    assert str(Language.JP) == "jp"


def test_get_supported_data_types_derives_from_enum():
    assert tse_tick.get_supported_data_types() == DataType.values()
    assert DataType.values() == [
        "individual_stock", "stock_summary", "indices", "indices_summary",
    ]


def test_enum_accepted_where_string_is(stock_store):
    """A DataType member must work anywhere the magic string works today."""
    df_enum = tse_tick.query_ticks(
        stock_store, data_type=DataType.INDIVIDUAL_STOCK, ticker=7203, date="20230704"
    )
    df_str = tse_tick.query_ticks(
        stock_store, data_type="individual_stock", ticker=7203, date="20230704"
    )
    assert df_enum.height == df_str.height > 0


# --------------------------------------------------------------------------- #
# A3 — query_ticks accepts str or int ticker
# --------------------------------------------------------------------------- #

def test_query_ticks_accepts_str_or_int_ticker(stock_store):
    df_int = tse_tick.query_ticks(stock_store, ticker=7203, date="20230704")
    df_str = tse_tick.query_ticks(stock_store, ticker="7203", date="20230704")
    assert df_int.height == df_str.height > 0


@pytest.mark.parametrize("bad", [True, "72*3", "../escape", 7.0, "code with space"])
def test_query_ticks_rejects_bad_ticker(stock_store, bad):
    with pytest.raises(ValueError):
        tse_tick.query_ticks(stock_store, ticker=bad, date="20230704")


# --------------------------------------------------------------------------- #
# A0 — translate() / mapping()
# --------------------------------------------------------------------------- #

def test_translate_function_names():
    assert translate("polygon", "get_aggs") == "query_ticks"
    assert translate("ccxt", "fetch_ohlcv") == "query_ticks"
    assert translate("yfinance", "download") == "create_df"


def test_translate_argument_names():
    # yfinance ``tickers`` is used as both a function and an argument; the
    # argument sense (ticker_filter) wins, per the documented resolution order.
    assert translate("yfinance", "tickers") == "ticker_filter"
    assert translate("polygon", "from_") == "start_time"
    assert translate("ccxt", "since") == "start_time"


def test_translate_unknown_and_bad_source():
    assert translate("polygon", "no_such_call") is None
    # Source matching is case-insensitive.
    assert translate("Polygon", "get_aggs") == "query_ticks"
    with pytest.raises(ValueError):
        translate("not_a_library", "download")


def test_mapping_structure_and_isolation():
    full = mapping()
    assert set(full) == {"yfinance", "polygon", "ccxt"}
    for tables in full.values():
        assert set(tables) == {"functions", "arguments"}

    one = mapping("ccxt")
    assert "fetch_ohlcv" in one["functions"]
    # Returned dicts are copies — mutating them must not change the package tables.
    one["functions"]["fetch_ohlcv"] = "MUTATED"
    assert mapping("ccxt")["functions"]["fetch_ohlcv"] == "query_ticks"
