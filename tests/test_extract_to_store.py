"""Tests for extract_to_store (two-stage in one call) — requires the [query] extra."""
import polars as pl
import pytest

import tse_tick
from tests.synthetic_data import individual_stock_csv, write_zip

pytest.importorskip("duckdb")  # extract_to_store queries the store via DuckDB

_CODE = pl.col("Stock Code").cast(pl.String).str.strip_chars().str.slice(0, 4)


def _codes(df):
    return set(df.select(_CODE.unique()).to_series().to_list())


def _seed(root, day, mapping):
    leaf = root / f"個別株式{day[:4]}" / "TICST120" / day[:6]
    leaf.mkdir(parents=True, exist_ok=True)
    for n, codes in mapping.items():
        write_zip(leaf / f"HTICST120.{day}.{n}.zip", f"HTICST120.{day}.{n}.csv",
                  individual_stock_csv(day, codes, rows_per_ticker=6))


def test_extract_to_store_matches_read_ticks(tmp_path):
    _seed(tmp_path / "src", "20240104", {1: ["1301"], 2: ["7203"], 3: ["9999"]})
    src = str(tmp_path / "src")
    store = str(tmp_path / "store")
    df = tse_tick.extract_to_store(src, store, "20240104", "7203")
    ref = tse_tick.read_ticks(src, ticker_filter={"7203"}, date="20240104")
    assert df.height == ref.height > 0            # query adds a `date` column (width+1)
    assert df.width == ref.width + 1


def test_extract_to_store_reuses_and_resumes(tmp_path):
    _seed(tmp_path / "src", "20240104", {1: ["1301"], 2: ["7203"]})
    src = str(tmp_path / "src")
    store = str(tmp_path / "store")
    first = tse_tick.extract_to_store(src, store, "20240104", "7203")
    # second call resumes off the existing store and returns the same rows
    second = tse_tick.extract_to_store(src, store, "20240104", "7203")
    assert second.height == first.height > 0


def test_extract_to_store_multiple_tickers(tmp_path):
    # 7203 and 9984 in different parts; one call ingests both and returns them combined.
    _seed(tmp_path / "src", "20240104",
          {1: ["1301"], 2: ["7203"], 3: ["9984"], 4: ["9999"]})
    src = str(tmp_path / "src")
    store = str(tmp_path / "store")
    df = tse_tick.extract_to_store(src, store, "20240104", ["7203", "9984"])
    assert _codes(df) == {"7203", "9984"}
    ref7203 = tse_tick.read_ticks(src, ticker_filter={"7203"}, date="20240104").height
    ref9984 = tse_tick.read_ticks(src, ticker_filter={"9984"}, date="20240104").height
    assert df.height == ref7203 + ref9984 > 0        # combined == sum of the two


def test_extract_to_store_one_absent_ticker(tmp_path):
    # 6758 falls in the code range but isn't in the data -> contributes 0 rows, no crash.
    _seed(tmp_path / "src", "20240104", {1: ["1301"], 2: ["7203"]})
    src = str(tmp_path / "src")
    store = str(tmp_path / "store")
    df = tse_tick.extract_to_store(src, store, "20240104", ["7203", "6758"])
    assert _codes(df) == {"7203"}
    ref = tse_tick.read_ticks(src, ticker_filter={"7203"}, date="20240104").height
    assert df.height == ref > 0


def test_extract_to_store_empty_tickers_raises(tmp_path):
    _seed(tmp_path / "src", "20240104", {1: ["7203"]})
    src = str(tmp_path / "src")
    store = str(tmp_path / "store")
    with pytest.raises(ValueError):
        tse_tick.extract_to_store(src, store, "20240104", [])
