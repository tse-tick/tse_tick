"""Tests for extract_to_store (two-stage in one call) — requires the [query] extra."""
import pytest

import tse_tick
from tests.synthetic_data import individual_stock_csv, write_zip

pytest.importorskip("duckdb")  # extract_to_store queries the store via DuckDB


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
