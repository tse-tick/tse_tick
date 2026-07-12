# Tests for tse_tick.core module
"""Targeted clean_data cases; broad cleaning coverage lives in the
synthetic-fixture suites and (data-gated) tests/test_real_data.py."""
import polars as pl

import tse_tick
from tests.synthetic_data import individual_stock_csv, write_zip


def _zip_with_field(tmp_path, field_idx: int, value: str):
    """One-row TICST120 ZIP whose field ``field_idx`` is replaced by ``value``."""
    raw = individual_stock_csv("20240104", ["7203"], rows_per_ticker=2).decode("ascii")
    lines = raw.strip().split("\n")
    cells = lines[0].split('","')
    cells[field_idx] = value if field_idx else f'"{value}'
    lines[0] = '","'.join(cells)
    payload = ("\n".join(lines) + "\n").encode("ascii")
    zp = tmp_path / "HTICST120.20240104.1.zip"
    return write_zip(zp, "HTICST120.20240104.1.csv", payload)


def test_malformed_buy_quote_best_becomes_zero_not_error(tmp_path):
    # A garbage "Buy Quote 1 Best" (field 20) used to hit a strict Float64 cast
    # and abort the whole date group; it now follows the same non-strict path as
    # every sibling price column (null -> 0.0).
    zp = _zip_with_field(tmp_path, 20, "GARBAGE")
    df = tse_tick.create_df(
        str(zp), auto_detect=False, data_type="individual_stock", year=2024
    )
    assert df.height == 2  # rows_per_ticker=2: one AM + one PM row
    col = df["Buy Quote 1 Best"]
    assert col.dtype == pl.Float64
    assert col[0] == 0.0          # the malformed cell
    assert (col[1:] > 0).all()    # the intact cells


def test_buy_quote_vol_dtype_unchanged(tmp_path):
    zp = tmp_path / "HTICST120.20240104.1.zip"
    write_zip(zp, "HTICST120.20240104.1.csv",
              individual_stock_csv("20240104", ["7203"], rows_per_ticker=4))
    df = tse_tick.create_df(
        str(zp), auto_detect=False, data_type="individual_stock", year=2024
    )
    assert df["Buy Quote Vol 1"].dtype == pl.Int64
    assert (df["Buy Quote Vol 1"] > 0).all()
