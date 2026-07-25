# tests/test_paper_examples.py
"""Executable checks for the paper's Section 6 listings (Listings 1-6).

These are the most visible code in the manuscript. The tests run each listing
with the EXACT top-level namespace and call signatures it prints, so the paper
cannot silently drift from the package. Query/feature listings use the synthetic
``stock_store`` fixture (conftest.py); the raw-ZIP listings use synthetic
NEEDS-format archives. No proprietary NEEDS data is used.

Section 8 of the manuscript claims its listings are locked by this file, so a
listing added to or changed in the paper belongs here too.
"""

from pathlib import Path

import polars as pl
import pytest

import tse_tick
from tse_tick.cli import _build_parser
from tests.synthetic_data import individual_stock_csv, write_zip


# Names the listings call as TOP-LEVEL attributes (tse_tick.<name>).
PAPER_TOPLEVEL_NAMES = [
    "create_df",  # Listing 1
    "read_ticks",  # Listing 2
    "extract_to_store",  # Listing 3
    "query_ticks",  # Listings 3, 4
    "compute_spread",  # Listing 4
    "compute_depth",
    "compute_flow_imbalance",
    "compute_volatility",
    "compute_all_features",
    "DataType",  # Listing 5
    "Language",
    "translate",
    "mapping",
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


@pytest.fixture(scope="module")
def raw_folder(tmp_path_factory):
    """A folder of raw ZIPs, as Listing 2 passes to ``read_ticks``.

    Two parts of one trading day with 7203 in the second, so the read also
    exercises the multi-part path the listing implies.
    """
    d = tmp_path_factory.mktemp("paper_raw")
    for part, codes in ((1, ["1301"]), (2, ["7203"])):
        write_zip(
            d / f"HTICST120.20230704.{part}.zip",
            f"HTICST120.20230704.{part}.csv",
            individual_stock_csv("20230704", codes, rows_per_ticker=40),
        )
    return str(d)


@pytest.fixture(scope="module")
def structured_root(tmp_path_factory):
    """A structured NEEDS root, as Listing 3 passes to ``extract_to_store``."""
    root = tmp_path_factory.mktemp("paper_root")
    leaf = root / "個別株式2023" / "TICST120" / "202307"
    leaf.mkdir(parents=True)
    for part, codes in ((1, ["1301"]), (2, ["7203"])):
        write_zip(
            leaf / f"HTICST120.20230704.{part}.zip",
            f"HTICST120.20230704.{part}.csv",
            individual_stock_csv("20230704", codes, rows_per_ticker=40),
        )
    return str(root)


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


def test_listing2_read_ticks(raw_folder):
    """Listing 2: read_ticks over a FOLDER, with a bare-string ticker_filter.

    The listing passes ``ticker_filter="7203"`` (a string, not a set) alongside a
    date and an intraday range — the exact shape a reader will copy.
    """
    df = tse_tick.read_ticks(
        raw_folder,
        ticker_filter="7203",
        date="20230704",
        start_time="09:00:00",
        end_time="11:30:00",
    )
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert df.width == 95
    codes = set(df["Stock Code"].cast(pl.String).str.strip_chars().str.slice(0, 4).to_list())
    assert codes == {"7203"}
    times = [int(t) for t in df["Execution Time"].to_list()]
    assert all(90000 <= t <= 113000 for t in times)


def test_listing3_two_stage(structured_root, tmp_path):
    """Listing 3: extract_to_store(..., max_workers="auto"), then query_ticks.

    ``max_workers="auto"`` is the drift-prone part — the listing advertises the
    string form of the worker selector.
    """
    pytest.importorskip("duckdb")
    store = str(tmp_path / "store")

    built = tse_tick.extract_to_store(
        structured_root, store, period="202307", ticker="7203", max_workers="auto"
    )
    assert built.height > 0

    df = tse_tick.query_ticks(
        store,
        data_type="individual_stock",
        ticker=7203,
        date="20230704",
        start_time="09:00:00",
        end_time="11:30:00",
    )
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    times = [int(t) for t in df["Execution Time"].to_list()]
    assert all(90000 <= t <= 113000 for t in times)


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


def test_listing5_enums_and_translation():
    """Listing 5: the string enums and the translation helpers, values included."""
    assert tse_tick.DataType.INDIVIDUAL_STOCK == "individual_stock"
    assert tse_tick.Language.JP == "jp"
    assert tse_tick.translate("yfinance", "download") == "create_df"
    assert tse_tick.translate("polygon", "from_") == "start_time"
    assert tse_tick.mapping("ccxt")["functions"]["fetch_trades"] == ["query_ticks", "read_ticks"]


def test_listing6_cli_flags_parse():
    """Listing 6: every flag the CLI listing prints must exist and bind."""
    parser = _build_parser()

    ingest = parser.parse_args(
        [
            "ingest",
            "--data-type",
            "individual_stock",
            "--period",
            "202301-202312",
            "--input-root",
            r"G:\data\needs",
            "--output-root",
            "store",
            "--tickers",
            "7203",
            "--parallel",
            "auto",
        ]
    )
    assert ingest.data_type == "individual_stock"
    assert ingest.period == "202301-202312"
    assert ingest.tickers == "7203"
    assert ingest.parallel == "auto"

    export = parser.parse_args(
        [
            "export",
            "--data-type",
            "individual_stock",
            "--input-root",
            r"G:\data\needs",
            "--period",
            "20230704",
            "--tickers",
            "7203",
            "--output",
            "toyota_20230704.csv",
        ]
    )
    assert export.data_type == "individual_stock"
    assert export.period == "20230704"
    assert export.output.endswith(".csv")
    # --store switches export to the two-stage path (named in the surrounding text).
    assert "store" in vars(export)


def test_export_to_csv_public_api(single_zip, tmp_path):
    """Not a listing since the 0.15.1 rebuild, but still a documented entry point."""
    out = tse_tick.export_to_csv(
        single_zip,
        output_path=str(tmp_path / "cleaned_output.csv"),
        language="en",
        rows=5000,
    )
    assert Path(out).exists()
    assert Path(out).stat().st_size > 0
