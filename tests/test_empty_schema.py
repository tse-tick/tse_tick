# tests/test_empty_schema.py
"""A no-match read must return an empty-but-typed frame (full columns), not (0,0),
so chained code like df["Exchange Code"] doesn't raise ColumnNotFoundError."""
import logging

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


# --- no-ZIPs path (e.g. a market holiday): must warn AND keep schema (BUG-1/BUG-3) ---

def _structured_root(tmp_path):
    """A nested NEEDS tree holding data only on 2023-05-02 (a real trading day)."""
    d = tmp_path / "個別株式2023" / "TICST120" / "202305"
    d.mkdir(parents=True)
    write_zip(
        d / "HTICST120.20230502.1.zip",
        "HTICST120.20230502.1.csv",
        individual_stock_csv("20230502", ["7203"], rows_per_ticker=6, base_prices={"7203": 2100}),
    )
    return tmp_path


def test_read_ticks_no_zips_returns_typed_empty(tmp_path):
    # 2023-05-04 is a Golden Week holiday: no ZIPs exist for it.
    df = tse_tick.read_ticks(str(_structured_root(tmp_path)), ticker_filter={"7203"}, date="20230504")
    assert df.height == 0
    assert df.width == 95                              # typed empty, not (0, 0)
    assert "Exchange Code" in df.columns
    assert df.select("Exchange Code").height == 0      # chained selection must not raise


def test_read_ticks_no_zips_warns(tmp_path, caplog):
    with caplog.at_level(logging.WARNING, logger="tse_tick.enhanced"):
        tse_tick.read_ticks(str(_structured_root(tmp_path)), ticker_filter={"7203"}, date="20230504")
    assert any(
        "no ZIP" in r.getMessage() or "trading day" in r.getMessage().lower()
        for r in caplog.records
    )


def test_read_ticks_empty_schema_is_consistent(tmp_path):
    """The no-ZIPs empty frame must have the SAME schema as the no-match empty frame."""
    root = str(_structured_root(tmp_path))
    no_zips = tse_tick.read_ticks(root, ticker_filter={"7203"}, date="20230504")   # holiday
    no_match = tse_tick.read_ticks(root, ticker_filter={"9999"}, date="20230502")  # day exists
    assert no_zips.schema == no_match.schema
