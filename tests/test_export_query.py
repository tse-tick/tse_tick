# tests/test_export_query.py
"""Output-identity + memory-safety gate for `export_query` (issue #59).

`export_query(store, out, …)` streams a store slice to a single Parquet file
**without materializing the whole result** — the memory-safe path for a multi-year
active ticker whose assembled frame overflows RAM (`query_ticks(..., limit=None)`
raises `QueryMemoryError` there, PR #58). It must be **row-identical** to
`query_ticks(..., limit=None)` over the same slice (modulo the already
non-deterministic same-timestamp tick tie order, PR #45), for all four data types,
while writing incrementally (one row group per stored day) so peak RAM stays ~one
trading day regardless of period length.

Synthetic NEEDS-format data only — no proprietary NEEDS files.
"""
import warnings

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import tse_tick

duckdb = pytest.importorskip("duckdb")

import pyarrow.parquet as pq  # noqa: E402

from tse_tick.query import query_ticks  # noqa: E402
from tse_tick.ingest import ingest_single_zip  # noqa: E402
from tests.synthetic_data import (  # noqa: E402
    indices_csv,
    individual_stock_csv,
    individual_stock_with_quote_rows_csv,
    stock_summary_csv,
    write_zip,
)

_DAYS = ("20240104", "20240105", "20240108")


def _canon(df: pl.DataFrame) -> pl.DataFrame:
    """Total-order a frame by all columns so a same-timestamp tie order (which both
    paths leave non-deterministic, PR #45) can't make an identity check flaky."""
    return df.sort(by=df.columns)


def _assert_identical(exported: pl.DataFrame, oracle: pl.DataFrame) -> None:
    # Strict: same columns, same dtypes (the Parquet round-trip preserves them), same
    # rows. Sorted first so a same-timestamp tie order can't make it flaky.
    assert exported.columns == oracle.columns
    assert exported.schema == oracle.schema
    assert_frame_equal(_canon(exported), _canon(oracle))


# --------------------------------------------------------------------------- #
# Fixtures — small real (synthetic) stores built through the real ingest path
# --------------------------------------------------------------------------- #
@pytest.fixture
def stock_store(tmp_path):
    store = tmp_path / "store"
    for date in _DAYS:
        zp = tmp_path / f"HTICST120.{date}.1.zip"
        write_zip(zp, f"HTICST120.{date}.1.csv",
                  individual_stock_csv(date, ["7203", "6758"], rows_per_ticker=12,
                                       base_prices={"7203": 2100, "6758": 13000}))
        ingest_single_zip(str(zp), str(store), data_type="individual_stock", year=2024)
    return str(store)


@pytest.fixture
def family_store(tmp_path):
    store = tmp_path / "store"
    for date in _DAYS:
        zp = tmp_path / f"HTICST120.{date}.1.zip"
        write_zip(zp, f"HTICST120.{date}.1.csv",
                  individual_stock_csv(date, ["7203", "72031"], rows_per_ticker=8,
                                       base_prices={"7203": 2100, "72031": 2100}))
        ingest_single_zip(str(zp), str(store), data_type="individual_stock", year=2024)
    return str(store)


@pytest.fixture
def indices_store(tmp_path):
    store = tmp_path / "store"
    for date in _DAYS:
        zp = tmp_path / f"HTICIT110.{date}.1.zip"
        write_zip(zp, f"HTICIT110.{date}.1.csv", indices_csv(date, ["101", "113"], rows_per_code=12))
        ingest_single_zip(str(zp), str(store), data_type="indices", year=2024)
    return str(store)


@pytest.fixture
def summary_store(tmp_path):
    store = tmp_path / "store"
    for date in _DAYS:
        zp = tmp_path / f"HTICSS110.{date}.1.zip"
        write_zip(zp, f"HTICSS110.{date}.1.csv",
                  stock_summary_csv(date, ["7203", "6758", "9984"], time_value="090005000000"))
        ingest_single_zip(str(zp), str(store), data_type="stock_summary", year=2024)
    return str(store)


# --------------------------------------------------------------------------- #
# Output identity vs query_ticks(limit=None), across data types
# --------------------------------------------------------------------------- #
def test_export_matches_query_ticks_single_ticker(stock_store, tmp_path):
    out = tmp_path / "toyota.parquet"
    manifest = tse_tick.export_query(stock_store, str(out),
                                     data_type="individual_stock", ticker="7203")
    oracle = query_ticks(stock_store, data_type="individual_stock", ticker="7203", limit=None)
    _assert_identical(pl.read_parquet(out), oracle)
    assert manifest["rows"] == oracle.height


def test_export_matches_query_ticks_all_tickers(stock_store, tmp_path):
    out = tmp_path / "all.parquet"
    tse_tick.export_query(stock_store, str(out), data_type="individual_stock", ticker=None)
    oracle = query_ticks(stock_store, data_type="individual_stock", ticker=None, limit=None)
    _assert_identical(pl.read_parquet(out), oracle)


def test_export_matches_query_ticks_indices(indices_store, tmp_path):
    out = tmp_path / "idx.parquet"
    tse_tick.export_query(indices_store, str(out), data_type="indices", ticker="101")
    oracle = query_ticks(indices_store, data_type="indices", ticker="101", limit=None)
    _assert_identical(pl.read_parquet(out), oracle)


def test_export_matches_query_ticks_summary(summary_store, tmp_path):
    out = tmp_path / "sum.parquet"
    tse_tick.export_query(summary_store, str(out), data_type="stock_summary", ticker="7203")
    oracle = query_ticks(summary_store, data_type="stock_summary", ticker="7203", limit=None)
    _assert_identical(pl.read_parquet(out), oracle)


def test_export_ticker_family(family_store, tmp_path):
    """A 4-char code exports its whole share-class family (7203 + 72031), the same
    rows query_ticks('7203') returns."""
    out = tmp_path / "fam.parquet"
    tse_tick.export_query(family_store, str(out), data_type="individual_stock", ticker="7203")
    oracle = query_ticks(family_store, data_type="individual_stock", ticker="7203", limit=None)
    exported = pl.read_parquet(out)
    _assert_identical(exported, oracle)
    assert exported.height > 0


def test_export_date_range_and_time_filter(tmp_path):
    """A date sub-range + a time window match query_ticks with the same filters,
    including the individual_stock quote-only Update Time fallback."""
    store = tmp_path / "store"
    for date in _DAYS:
        zp = tmp_path / f"HTICST120.{date}.1.zip"
        write_zip(zp, f"HTICST120.{date}.1.csv",
                  individual_stock_with_quote_rows_csv(
                      date, "7203",
                      trade_times=["090001", "090500", "100000"],
                      quote_times=["090000", "090200", "093000", "110000"]))
        ingest_single_zip(str(zp), str(store), data_type="individual_stock", year=2024)
    out = tmp_path / "win.parquet"
    kw = dict(data_type="individual_stock", ticker="7203",
              date="20240104-20240105", start_time="09:00:00", end_time="10:00:00")
    tse_tick.export_query(str(store), str(out), **kw)
    oracle = query_ticks(str(store), limit=None, **kw)
    _assert_identical(pl.read_parquet(out), oracle)


def test_export_columns_projection(stock_store, tmp_path):
    out = tmp_path / "cols.parquet"
    cols = ["Data Date", "Stock Code", "Execution Time"]
    tse_tick.export_query(stock_store, str(out), data_type="individual_stock",
                          ticker="7203", columns=cols)
    exported = pl.read_parquet(out)
    oracle = query_ticks(stock_store, data_type="individual_stock", ticker="7203",
                         columns=cols, limit=None)
    _assert_identical(exported, oracle)


# --------------------------------------------------------------------------- #
# Memory-safety: streamed incrementally (one row group per stored day)
# --------------------------------------------------------------------------- #
def test_export_streams_one_row_group_per_day(stock_store, tmp_path):
    """The file is written day-by-day (a row group per stored day), so the live
    working set is one trading day and peak memory stays bounded regardless of period
    length (measured: a 3-month 7203 export plateaued ~3.6 GB vs the ~100 GB a
    whole-frame query_ticks(limit=None) needs) — the property that makes the
    multi-year export possible where query_ticks OOMs."""
    out = tmp_path / "toyota.parquet"
    tse_tick.export_query(stock_store, str(out), data_type="individual_stock", ticker="7203")
    assert pq.ParquetFile(str(out)).num_row_groups == len(_DAYS)


# --------------------------------------------------------------------------- #
# Overwrite guard + no-data semantics
# --------------------------------------------------------------------------- #
def test_export_refuses_to_overwrite_without_flag(stock_store, tmp_path):
    out = tmp_path / "toyota.parquet"
    out.write_text("existing")
    with pytest.raises(FileExistsError):
        tse_tick.export_query(stock_store, str(out), data_type="individual_stock", ticker="7203")
    # overwrite=True proceeds and replaces it with a real Parquet file.
    tse_tick.export_query(stock_store, str(out), data_type="individual_stock",
                          ticker="7203", overwrite=True)
    assert pl.read_parquet(out).height > 0


def test_export_absent_ticker_writes_typed_empty_and_warns(stock_store, tmp_path):
    out = tmp_path / "absent.parquet"
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        manifest = tse_tick.export_query(stock_store, str(out),
                                         data_type="individual_stock", ticker="9999")
    exported = pl.read_parquet(out)
    assert exported.height == 0
    assert exported.width > 0  # typed-empty: full schema, no rows
    assert manifest["rows"] == 0
    assert any(issubclass(w.category, tse_tick.NoDataWarning) for w in caught)


def test_export_manifest_fields(stock_store, tmp_path):
    out = tmp_path / "toyota.parquet"
    manifest = tse_tick.export_query(stock_store, str(out),
                                     data_type="individual_stock", ticker="7203")
    assert manifest["path"] == str(out)
    assert manifest["rows"] > 0
    assert manifest["dates"] == len(_DAYS)
    assert manifest["data_type"] == "individual_stock"
