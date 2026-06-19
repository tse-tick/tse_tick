# tests/test_run10_fixes.py
"""Regression tests for the run9 bug report, F1 (the run10 fix).

A bare ``ticker_filter`` code is a common novice mistake: a ``str`` like ``"101"``
was iterated into characters (``{'1','0','1'}`` → matched nothing → silent empty),
and an ``int`` like ``101`` raised a raw ``TypeError`` when iterated. A bare
``str``/``int`` is now treated as a single-element filter, on both read entry
points (``read_ticks`` and ``create_df``). A proper set/iterable is unchanged.
"""
import polars as pl

import tse_tick
from tse_tick import create_df, read_ticks
from tests.synthetic_data import individual_stock_csv, indices_csv, write_zip


def _indices_zip(tmp_path):
    zp = tmp_path / "HTICIT110.20230508.1.zip"
    write_zip(zp, "HTICIT110.20230508.1.csv",
              indices_csv("20230508", ["101", "113"], rows_per_code=4))
    return zp


def _stock_zip(tmp_path):
    zp = tmp_path / "HTICST120.20240104.1.zip"
    write_zip(zp, "HTICST120.20240104.1.csv",
              individual_stock_csv("20240104", ["7203", "6758"], rows_per_ticker=6,
                                   base_prices={"7203": 2100, "6758": 13000}))
    return zp


# --------------------------------------------------------------------------- #
# F1 — a bare string code is treated as a single code, not split into chars
# --------------------------------------------------------------------------- #
def test_f1_bare_string_ticker_filter_indices(tmp_path):
    df = read_ticks(str(_indices_zip(tmp_path)), data_type="indices",
                    ticker_filter="101", date="20230508")
    assert df.height == 4                                   # was 0 (split into {'1','0'})
    assert set(df["Index Code"].cast(pl.String).to_list()) == {"101"}


def test_f1_bare_string_ticker_filter_individual_stock_fast_path(tmp_path):
    # The raw-byte fast path must also see "7203" as one code, not {'7','2','0','3'}.
    df = read_ticks(str(_stock_zip(tmp_path)), ticker_filter="7203", date="20240104")
    assert df.height == 6
    assert set(df["Stock Code"].cast(pl.String).to_list()) == {"7203"}


# --------------------------------------------------------------------------- #
# F2 — a bare int code is treated as a single code, not a TypeError
# --------------------------------------------------------------------------- #
def test_f2_bare_int_ticker_filter_indices(tmp_path):
    df = read_ticks(str(_indices_zip(tmp_path)), data_type="indices",
                    ticker_filter=101, date="20230508")          # was: TypeError
    assert df.height == 4
    assert set(df["Index Code"].cast(pl.String).to_list()) == {"101"}


# --------------------------------------------------------------------------- #
# regression — a proper set / iterable still works unchanged
# --------------------------------------------------------------------------- #
def test_set_ticker_filter_unchanged(tmp_path):
    df = read_ticks(str(_indices_zip(tmp_path)), data_type="indices",
                    ticker_filter={"101"}, date="20230508")
    assert set(df["Index Code"].cast(pl.String).to_list()) == {"101"}


def test_multi_code_set_unchanged(tmp_path):
    df = read_ticks(str(_indices_zip(tmp_path)), data_type="indices",
                    ticker_filter={"101", "113"}, date="20230508")
    assert set(df["Index Code"].cast(pl.String).to_list()) == {"101", "113"}


# --------------------------------------------------------------------------- #
# the sibling public read entry (create_df) gets the same guard
# --------------------------------------------------------------------------- #
def test_create_df_bare_string_ticker_filter(tmp_path):
    df = create_df(str(_stock_zip(tmp_path)), auto_detect=False,
                   data_type="individual_stock", year=2024, ticker_filter="7203")
    assert df.height == 6
    assert set(df["Stock Code"].cast(pl.String).to_list()) == {"7203"}


def test_normalize_helper_handles_bare_and_iterable():
    from tse_tick.enhanced import _normalize_ticker_filter
    assert _normalize_ticker_filter(None) is None
    assert _normalize_ticker_filter("101") == {"101"}
    assert _normalize_ticker_filter(101) == {"101"}
    assert _normalize_ticker_filter({"101", "113"}) == {"101", "113"}
    assert _normalize_ticker_filter([101, "113"]) == {"101", "113"}
