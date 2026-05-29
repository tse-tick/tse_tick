# tests/conftest.py
"""Shared Stage-2 fixtures.

The ``stock_store`` fixture builds a tiny Hive-partitioned Parquet store at test
time by running synthetic NEEDS-format ZIPs through the *real* ingest pipeline
(``ingest_single_zip``). Nothing large is committed and the store is always
pipeline-fresh, so Stage-2 (query / features / event-window-from-Parquet) tests
run in CI without any proprietary NEEDS data.
"""
import pytest
import polars as pl

from tse_tick.ingest import ingest_single_zip
from tests.synthetic_data import individual_stock_csv, indices_csv, write_zip

# Tickers and dates the synthetic store is populated with. Tests rely on these.
STOCK_TICKERS = [7203, 6758, 9984]
STOCK_DATES = ["20230703", "20230704"]
BASE_PRICES = {"7203": 2100, "6758": 13000, "9984": 6500}
ROWS_PER_TICKER = 40

# Raw index codes for the indices fixture (decoded to display names on ingest).
INDEX_CODES = [101, 113]  # Nikkei 225, TOPIX
INDEX_DATE = "20230704"


@pytest.fixture(scope="session")
def stock_store(tmp_path_factory):
    """Build a synthetic ``individual_stock`` Parquet store via real ingest.

    Returns the store root (the directory that contains ``individual_stock/``),
    laid out as ``individual_stock/date=YYYYMMDD/ticker=NNNN.parquet``.
    """
    raw_dir = tmp_path_factory.mktemp("raw")
    store_dir = tmp_path_factory.mktemp("store")

    tickers = [str(t) for t in STOCK_TICKERS]
    for date in STOCK_DATES:
        payload = individual_stock_csv(
            date, tickers, rows_per_ticker=ROWS_PER_TICKER, base_prices=BASE_PRICES
        )
        zip_path = raw_dir / f"HTICST120.{date}.1.zip"
        write_zip(zip_path, f"HTICST120.{date}.1.csv", payload)
        ingest_single_zip(
            str(zip_path), str(store_dir), data_type="individual_stock", year=2023
        )

    return str(store_dir)


@pytest.fixture(scope="session")
def indices_store(tmp_path_factory):
    """Build a synthetic ``indices`` (TICIT110) Parquet store via real ingest.

    Index codes are decoded to display names during ingest, so this store also
    exercises the raw-code partitioning fix (filenames stay ``ticker=101`` etc.).
    """
    raw_dir = tmp_path_factory.mktemp("iraw")
    store_dir = tmp_path_factory.mktemp("istore")
    payload = indices_csv(INDEX_DATE, [str(c) for c in INDEX_CODES], rows_per_code=16)
    zip_path = raw_dir / f"HTICIT110.{INDEX_DATE}.1.zip"
    write_zip(zip_path, f"HTICIT110.{INDEX_DATE}.1.csv", payload)
    ingest_single_zip(str(zip_path), str(store_dir), data_type="indices", year=2023)
    return str(store_dir)


@pytest.fixture(scope="session")
def feature_ticks(stock_store):
    """A single ticker's ticks loaded from the store, for feature tests.

    The Hive ``date`` column is dropped so the frame is a clean 95-column
    TICST120 DataFrame (matching ``get_schema_individual_stock_95``).
    """
    from tse_tick.query import query_ticks
    from tse_tick.schemas import get_schema_individual_stock_95

    df = query_ticks(stock_store, ticker=7203, date="20230704")
    return df.select(get_schema_individual_stock_95())


@pytest.fixture(scope="session")
def events_csv(tmp_path_factory):
    """A tiny synthetic event filter CSV covering the event-window path.

    Columns match the event-window pipeline's expectations; an empty
    ``event_time`` denotes a full-day window.
    """
    path = tmp_path_factory.mktemp("events") / "events.csv"
    path.write_text(
        "ticker,event_date,event_time,event_type,session_type\n"
        "7203,20230704,13:00:00,earnings,intraday\n"
        "6758,20230704,,guidance,fullday\n"
        "9984,20230703,10:30:00,disclosure,intraday\n",
        encoding="utf-8",
    )
    return str(path)


@pytest.fixture(scope="session")
def events_df(events_csv):
    """The synthetic event CSV loaded as a Polars DataFrame (all-string)."""
    return pl.read_csv(events_csv, schema_overrides={"event_date": pl.String})
