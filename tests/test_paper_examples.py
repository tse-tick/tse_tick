# tests/test_paper_examples.py
"""Executable checks for the paper's Section 6 listings (Listings 1-4).

These are the most visible code in the manuscript. The tests run each listing
with the EXACT top-level namespace and call signatures it prints, so the paper
cannot silently drift from the package. Query/feature listings use the synthetic
``stock_store`` fixture (conftest.py); ``create_df`` / ``export_to_csv`` use a
synthetic NEEDS-style ZIP. No proprietary NEEDS data is used.
"""

from pathlib import Path

import polars as pl
import pytest

import tse_tick
from tests.synthetic_data import individual_stock_csv, write_zip


# Names the listings call as TOP-LEVEL attributes (tse_tick.<name>).
PAPER_TOPLEVEL_NAMES = [
    "create_df",
    "export_to_csv",
    "query_ticks",
    "compute_spread",
    "compute_depth",
    "compute_flow_imbalance",
    "compute_volatility",
    "compute_all_features",
]


@pytest.fixture(scope="module")
def single_zip(tmp_path_factory):
    """A synthetic NEEDS-style TICST120 ZIP for the create_df / export listings."""
    d = tmp_path_factory.mktemp("paper_zip")
    zip_path = d / "HTICST120.20230704.1.zip"
    write_zip(
        zip_path,
        "HTICST120.20230704.1.csv",
        individual_stock_csv("20230704", ["7203"], rows_per_ticker=40, base_prices={"7203": 2100}),
    )
    return str(zip_path)


def test_paper_toplevel_namespace():
    """Every name the Section 6 listings call must be exposed at the top level."""
    missing = [n for n in PAPER_TOPLEVEL_NAMES if not hasattr(tse_tick, n)]
    assert missing == [], f"Listings reference missing top-level names: {missing}"
    # query_ticks must be the real implementation, not the DuckDB-unavailable stub.
    assert tse_tick.query_ticks.__module__ == "tse_tick.query"


def test_listing1_create_df(single_zip):
    """Listing 1: create_df(path, language=...) and the rows= sample."""
    df = tse_tick.create_df(single_zip, language="en")
    assert df.shape[1] == 95
    df_jp = tse_tick.create_df(single_zip, language="jp")
    assert df_jp.shape[1] == 95
    df_sample = tse_tick.create_df(single_zip, rows=10)
    assert df_sample.height == 10


def test_listing2_export_to_csv(single_zip, tmp_path):
    """Listing 2: export_to_csv(path, output_path=..., language=..., rows=...)."""
    out = tse_tick.export_to_csv(
        single_zip,
        output_path=str(tmp_path / "cleaned_output.csv"),
        language="en",
        rows=5000,
    )
    assert Path(out).exists()
    assert Path(out).stat().st_size > 0


def test_listing3_query_ticks(stock_store):
    """Listing 3: query_ticks with data_type / ticker / date / time-range filters."""
    df = tse_tick.query_ticks(
        stock_store,
        data_type="individual_stock",
        ticker=7203,
        date="20230704",
        start_time="09:00:00",
        end_time="11:30:00",
    )
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    minutes = [int(t) for t in df["Execution Time"].to_list()]
    assert all(90000 <= m <= 113000 for m in minutes)


def test_listing4_feature_chain_on_raw_query_output(stock_store):
    """Listing 4: feed RAW query_ticks output straight into the compute_* funcs.

    The paper does NOT pre-clean or drop the Hive ``date`` column, so this runs
    exactly what a reader would get from query_ticks.
    """
    df = tse_tick.query_ticks(stock_store, ticker=7203, date="20230704")
    assert "date" in df.columns  # raw query output carries the Hive partition col

    spread = tse_tick.compute_spread(df)
    depth = tse_tick.compute_depth(df, levels=5, side="both")
    ofi = tse_tick.compute_flow_imbalance(df, window="5min")
    vol = tse_tick.compute_volatility(df, window="5min")
    features = tse_tick.compute_all_features(df)

    assert len(spread) == df.height
    assert depth.width == 10
    assert len(ofi) == df.height
    assert len(vol) == df.height
    # 96 raw columns + spread + 20 depth + flow_imbalance + volatility
    assert features.width == df.width + 23
