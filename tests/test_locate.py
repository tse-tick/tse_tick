# tests/test_locate.py
"""Auto-location: read_ticks/discovery must find the data no matter which level
of the real NEEDS tree the user points at (by file type + date, not folder names)."""
import pytest

import tse_tick
from tests.synthetic_data import individual_stock_csv, write_zip


def _seed(leaf):
    leaf.mkdir(parents=True, exist_ok=True)
    write_zip(
        leaf / "HTICST120.20240104.1.zip", "HTICST120.20240104.1.csv",
        individual_stock_csv("20240104", ["7203"], rows_per_ticker=8, base_prices={"7203": 2100}),
    )


@pytest.fixture
def needs_tree(tmp_path):
    """tmp/NEEDS/個別株式2024/TICST120/202401/HTICST120.20240104.1.zip"""
    _seed(tmp_path / "NEEDS" / "個別株式2024" / "TICST120" / "202401")
    return tmp_path


@pytest.mark.parametrize("level", [
    ".",                                    # the parent of NEEDS
    "NEEDS",                                # the NEEDS root
    "NEEDS/個別株式2024",                    # the year folder
    "NEEDS/個別株式2024/TICST120",           # the data-type folder
    "NEEDS/個別株式2024/TICST120/202401",    # the leaf {yyyymm} folder (flat dir of parts)
])
def test_read_ticks_locates_from_any_level(needs_tree, level):
    root = needs_tree if level == "." else needs_tree / level
    df = tse_tick.read_ticks(str(root), ticker_filter={"7203"}, date="20240104")
    assert df.height == 8


def test_read_ticks_locates_in_renamed_year_folder(tmp_path):
    # year folder renamed (no 個別株式 prefix) — still found, because matching is by filename
    _seed(tmp_path / "NEEDS" / "2024" / "TICST120" / "202401")
    df = tse_tick.read_ticks(str(tmp_path / "NEEDS"), ticker_filter={"7203"}, date="20240104")
    assert df.height == 8


def test_export_cli_locates_from_needs_root(tmp_path, monkeypatch, capsys):
    import polars as pl
    _seed(tmp_path / "NEEDS" / "個別株式2024" / "TICST120" / "202401")
    out = tmp_path / "t.csv"
    monkeypatch.setattr("sys.argv", [
        "tse-tick", "export", "--data-type", "individual_stock",
        "--input-root", str(tmp_path / "NEEDS"), "--tickers", "7203",
        "--period", "20240104", "--output", str(out),
    ])
    from tse_tick.cli import main
    main()
    assert pl.read_csv(out).height == 8
