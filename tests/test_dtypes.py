# tests/test_dtypes.py
"""Price/quote columns must come back as Float64 consistently (not a str/f64 mix)."""
import polars as pl

import tse_tick
from tests.synthetic_data import individual_stock_csv, write_zip


def _zip(tmp_path):
    zp = tmp_path / "HTICST120.20240104.1.zip"
    write_zip(
        zp,
        "HTICST120.20240104.1.csv",
        individual_stock_csv("20240104", ["7203"], rows_per_ticker=10, base_prices={"7203": 2100}),
    )
    return zp


def test_price_quote_columns_are_float64(tmp_path):
    df = tse_tick.create_df(
        str(_zip(tmp_path)), auto_detect=False, data_type="individual_stock", year=2024
    )
    for col in ("Execution Price", "Sell Quote 1 Best", "Buy Quote 1 Best"):
        assert df[col].dtype == pl.Float64, f"{col} is {df[col].dtype}, expected Float64"
