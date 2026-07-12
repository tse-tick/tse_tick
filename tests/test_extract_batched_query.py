"""Output-identity gate for the batched extract query (issue #44).

`_query_extract_batch` replaces extract_to_store's per-ticker `query_ticks(limit=None)`
loop with one connection / one scan / one conversion. It must be byte-identical to that
loop. `query_ticks` is unchanged by #44, so the old loop is reconstructed here verbatim
as the oracle and compared across single / multi / absent tickers, tick / summary types,
and date / time filters.
"""
import polars as pl
import pytest

from tse_tick.ingest import ingest_single_zip
from tse_tick.query import query_ticks, _query_extract_batch
from tests.synthetic_data import (
    indices_csv,
    individual_stock_csv,
    individual_stock_with_quote_rows_csv,
    stock_summary_csv,
    write_zip,
)

pytest.importorskip("duckdb")


def _oracle(store, data_type, tickers, **kw):
    """The exact pre-#44 Stage-2 loop: per-ticker query_ticks(limit=None) + concat."""
    frames = []
    for code in sorted(tickers):
        q = dict(data_type=data_type, ticker=code, limit=None)
        q.update(kw)
        frames.append(query_ticks(store, **q))
    non_empty = [f for f in frames if f.height > 0]
    if not non_empty:
        return frames[0]
    return non_empty[0] if len(non_empty) == 1 else pl.concat(non_empty, how="vertical")


def _identical(store, data_type, tickers, day=None, **kw):
    """`day` maps to the oracle's query_ticks(date=day) and the batch's inclusive
    (date_from, date_to) bounds — the same single-day filter in both dialects."""
    oracle = _oracle(store, data_type, tickers, **({"date": day} if day else {}), **kw)
    bounds = {"date_from": day, "date_to": day} if day else {}
    batch = _query_extract_batch(store, data_type, set(tickers), **bounds, **kw)
    assert batch.equals(oracle), (
        f"{data_type} tickers={tickers} day={day} kw={kw}: "
        f"batch {batch.shape} != oracle {oracle.shape}"
    )
    return batch


@pytest.fixture
def stock_store(tmp_path):
    store = tmp_path / "store"
    for date in ("20240104", "20240105"):
        zp = tmp_path / f"HTICST120.{date}.1.zip"
        write_zip(zp, f"HTICST120.{date}.1.csv",
                  individual_stock_csv(date, ["7203", "6758", "9984"], rows_per_ticker=10,
                                       base_prices={"7203": 2100, "6758": 13000, "9984": 6500}))
        ingest_single_zip(str(zp), str(store), data_type="individual_stock", year=2024)
    return str(store)


@pytest.fixture
def indices_store(tmp_path):
    store = tmp_path / "store"
    for date in ("20240104", "20240105"):
        zp = tmp_path / f"HTICIT110.{date}.1.zip"
        write_zip(zp, f"HTICIT110.{date}.1.csv", indices_csv(date, ["101", "113"], rows_per_code=12))
        ingest_single_zip(str(zp), str(store), data_type="indices", year=2024)
    return str(store)


@pytest.fixture
def summary_store(tmp_path):
    store = tmp_path / "store"
    for date in ("20240104", "20240105"):
        zp = tmp_path / f"HTICSS110.{date}.1.zip"
        write_zip(zp, f"HTICSS110.{date}.1.csv",
                  stock_summary_csv(date, ["7203", "6758", "9984"], time_value="090005000000"))
        ingest_single_zip(str(zp), str(store), data_type="stock_summary", year=2024)
    return str(store)


# --- individual_stock (tick) -------------------------------------------------
def test_stock_single_ticker(stock_store):
    df = _identical(stock_store, "individual_stock", ["7203"])
    assert df.height > 0 and "date" in df.columns


def test_stock_multi_ticker_block_order(stock_store):
    df = _identical(stock_store, "individual_stock", ["9984", "7203"])  # unsorted input
    # sorted-code block order: all 6758? no -> 7203 rows precede 9984 rows.
    codes = df.select(pl.col("Stock Code").cast(pl.String).str.slice(0, 4)).to_series().to_list()
    assert codes == sorted(codes, key=lambda c: c)  # non-decreasing by code block


def test_stock_one_absent(stock_store):
    _identical(stock_store, "individual_stock", ["7203", "0000"])


def test_stock_all_absent_shape(stock_store):
    df = _identical(stock_store, "individual_stock", ["0000", "1111"])
    assert df.height == 0
    assert "date" not in df.columns  # tick all-absent: 0-row WITHOUT date


def test_stock_single_day(stock_store):
    _identical(stock_store, "individual_stock", ["7203", "6758"], day="20240105")


def test_stock_time_window(stock_store):
    _identical(stock_store, "individual_stock", ["7203", "9984"],
               start_time="09:30:00", end_time="13:30:00")


def test_stock_quote_only_update_time_fallback(tmp_path):
    # Quote-only rows (blank Execution Time) must be matched on Update Time by BOTH the
    # oracle and the batch (shared time_expr) — the trickiest ordering case.
    store = tmp_path / "store"
    zp = tmp_path / "HTICST120.20240104.1.zip"
    write_zip(zp, "HTICST120.20240104.1.csv", individual_stock_with_quote_rows_csv(
        "20240104", "7203", trade_times=["090001", "100002"],
        quote_times=["093000", "110000", "143000"]))
    ingest_single_zip(str(zp), str(store), data_type="individual_stock", year=2024)
    _identical(str(store), "individual_stock", ["7203"], start_time="09:15:00", end_time="12:00:00")


# --- suffixed share-class families -------------------------------------------
@pytest.fixture
def suffixed_store(tmp_path):
    """A store holding a parent + suffixed share class, timestamps de-tied so the
    oracle comparison is deterministic (DuckDB tie order is arbitrary)."""
    store = tmp_path / "store"
    for date in ("20240104", "20240105"):
        zp = tmp_path / f"HTICST120.{date}.1.zip"
        write_zip(zp, f"HTICST120.{date}.1.csv",
                  individual_stock_csv(date, ["7203", "72031", "9984"], rows_per_ticker=8,
                                       minute_offsets={"72031": 1, "9984": 2}))
        ingest_single_zip(str(zp), str(store), data_type="individual_stock", year=2024)
    return str(store)


def test_suffixed_family_single(suffixed_store):
    # query_ticks("7203") and the batch both select the whole family; the family
    # is ONE (date, time)-ordered block in both.
    df = _identical(suffixed_store, "individual_stock", ["7203"])
    codes = set(df.select(pl.col("Stock Code").str.strip_chars().unique()).to_series().to_list())
    assert codes == {"7203", "72031"}


def test_suffixed_family_multi_and_windows(suffixed_store):
    _identical(suffixed_store, "individual_stock", ["9984", "7203"])
    _identical(suffixed_store, "individual_stock", ["7203"], day="20240105")
    _identical(suffixed_store, "individual_stock", ["7203", "9984"],
               start_time="09:30:00", end_time="13:30:00")


# --- indices (tick) ----------------------------------------------------------
def test_indices_multi_and_absent(indices_store):
    _identical(indices_store, "indices", ["113", "101"])
    _identical(indices_store, "indices", ["101", "999"])
    df = _identical(indices_store, "indices", ["999"])
    assert df.height == 0 and "date" not in df.columns


# --- stock_summary (daily aggregate) -----------------------------------------
def test_summary_multi_and_absent(summary_store):
    _identical(summary_store, "stock_summary", ["9984", "7203"])
    _identical(summary_store, "stock_summary", ["7203", "0000"])
    df = _identical(summary_store, "stock_summary", ["0000"])
    assert df.height == 0
    assert "date" in df.columns  # summary all-absent: 0-row WITH date


def test_summary_single_day(summary_store):
    _identical(summary_store, "stock_summary", ["7203", "6758"], day="20240104")
