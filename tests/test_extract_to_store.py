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


def test_extract_to_store_warns_on_lost_parts(tmp_path):
    # A corrupt part is recorded (not fatal) by Stage 1 — but this call returns
    # the queried frame as if complete, so it must surface the loss loudly.
    _seed(tmp_path / "src", "20240104", {1: ["1301"], 2: ["7203"]})
    leaf = tmp_path / "src" / "個別株式2024" / "TICST120" / "202401"
    (leaf / "HTICST120.20240104.1.zip").write_bytes(b"not a zip")
    with pytest.warns(tse_tick.PartialIngestWarning, match="20240104"):
        df = tse_tick.extract_to_store(
            str(tmp_path / "src"), str(tmp_path / "store"), "20240104", "7203"
        )
    assert df.height > 0  # the surviving part's rows still come back


def test_extract_to_store_without_duckdb_fails_fast_and_guided(tmp_path, monkeypatch):
    import sys

    _seed(tmp_path / "src", "20240104", {1: ["7203"]})
    monkeypatch.setitem(sys.modules, "duckdb", None)          # import duckdb -> ImportError
    monkeypatch.delitem(sys.modules, "tse_tick.query", raising=False)  # drop the cached module
    store = tmp_path / "store"
    with pytest.raises(ImportError, match=r"tse-tick\[query\]"):
        tse_tick.extract_to_store(str(tmp_path / "src"), str(store), "20240104", "7203")
    assert not store.exists()  # failed BEFORE Stage 1 built anything


def test_extract_to_store_uses_single_uncapped_scan(tmp_path, monkeypatch):
    """extract_to_store must query via the single-scan batch (issue #44): ONE call for
    all tickers (no per-ticker N+1) and no row cap — a very active ticker over a whole
    month would otherwise be truncated at query_ticks's default 10M."""
    _seed(tmp_path / "src", "20240104", {1: ["1301"], 2: ["7203"], 3: ["9984"]})
    import tse_tick.query as q
    real = q._query_extract_batch
    calls = []

    def spy(data_dir, data_type, tickers, **kw):
        calls.append((sorted(tickers), kw))
        return real(data_dir, data_type, tickers, **kw)

    monkeypatch.setattr(q, "_query_extract_batch", spy)  # extract_to_store imports it lazily
    df = tse_tick.extract_to_store(
        str(tmp_path / "src"), str(tmp_path / "store"), "20240104", ["7203", "9984"]
    )
    assert len(calls) == 1, f"expected ONE scan for all tickers, got {len(calls)}"
    # the batch takes no `limit` (uncapped by construction) and covers every ticker
    assert "limit" not in calls[0][1]
    assert calls[0][0] == ["7203", "9984"]
    assert df.height > 0
