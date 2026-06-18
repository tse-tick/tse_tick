# tests/test_empty_schema.py
"""A no-match read must return an empty-but-typed frame (full columns), not (0,0),
so chained code like df["Exchange Code"] doesn't raise ColumnNotFoundError."""
import tse_tick
from tests.synthetic_data import write_zip, individual_stock_csv


def _stock_zip(tmp_path):
    zp = tmp_path / "HTICST120.20240104.1.zip"
    write_zip(
        zp,
        "HTICST120.20240104.1.csv",
        individual_stock_csv("20240104", ["7203"], rows_per_ticker=6, base_prices={"7203": 2100}),
    )
    return zp


def test_read_ticks_no_match_keeps_schema(tmp_path):
    df = tse_tick.read_ticks(str(_stock_zip(tmp_path)), ticker_filter={"9999"}, date="20240104")
    assert df.height == 0
    assert df.width == 95
    assert "Exchange Code" in df.columns
    assert df.select("Exchange Code").height == 0   # chained selection must not raise


def test_create_df_no_match_keeps_schema(tmp_path):
    df = tse_tick.create_df(
        str(_stock_zip(tmp_path)),
        auto_detect=False,
        data_type="individual_stock",
        year=2024,
        ticker_filter={"9999"},
    )
    assert df.height == 0
    assert "Exchange Code" in df.columns


def test_query_ticks_unknown_ticker_keeps_schema(stock_store):
    df = tse_tick.query_ticks(stock_store, ticker=1, date="20230704")   # no such ticker
    assert df.height == 0
    assert "Exchange Code" in df.columns
