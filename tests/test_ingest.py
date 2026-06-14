# tests/test_ingest.py
"""Tests for tse_tick.ingest — batch ZIP-to-Parquet ingestion pipeline."""

import os
import shutil
from pathlib import Path

import pytest

from tse_tick.ingest import ingest_single_zip, ingest_directory, ingest_year, ingest_event_windows_period

_DATA_ROOT = os.environ.get("TSE_TICK_DATA_ROOT", r"G:\flash_crash")
REAL_TICST120_ZIP = os.path.join(
    _DATA_ROOT, "raw_2022", "202202", "HTICST120.20220201.1.zip",
)
# Real summary/index files staged flat under raw_other/ (see test_real_data.py).
REAL_TICSS110_ZIP = os.path.join(_DATA_ROOT, "raw_other", "HTICSS110.201701.zip")
REAL_TICIT110_ZIP = os.path.join(_DATA_ROOT, "raw_other", "HTICIT110.201701.zip")
REAL_TICIS110_ZIP = os.path.join(_DATA_ROOT, "raw_other", "HTICIS110.201701.zip")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_corrupt_zip(path: Path) -> None:
    """Write a file that has a .zip extension but is not a valid ZIP."""
    path.write_bytes(b"this is not a zip file -- corrupt")


def _make_filter_csv(tmp_path: Path, date_str: str = "20170315") -> Path:
    """Write a minimal event filter CSV with one after-hours event."""
    # Use ISO date format so pd.to_datetime parses correctly
    iso_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    anchor = f"{iso_date} 16:00:00"
    csv_path = tmp_path / "event_filter_list.csv"
    csv_path.write_text(
        "ticker,event_date,event_time,event_type,headline,session_type,"
        "reaction_anchor_dt,zip_date\n"
        f"7203,{iso_date},16:00:00,earnings,Test disclosure,after_hours,"
        f"{anchor},{date_str}\n",
        encoding="utf-8",
    )
    return csv_path


# ---------------------------------------------------------------------------
# Working tests for ingest_event_windows_period
# ---------------------------------------------------------------------------

def test_ingest_event_windows_no_events_for_year(tmp_path):
    """When no events match the year, the function returns without error."""
    csv_path = _make_filter_csv(tmp_path, date_str="20170315")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    in_dir = tmp_path / "in"
    in_dir.mkdir()

    # Request year 2020 but events are in 2017 → should exit cleanly
    ingest_event_windows_period(
        input_root=str(in_dir),
        output_dir=str(out_dir),
        period="2020",
        filter_csv=str(csv_path),
    )

    # No Parquet files should have been written
    parquet_files = list(out_dir.rglob("*.parquet"))
    assert parquet_files == [], "No parquets expected when no events match the year"


def test_ingest_event_windows_corrupt_zip_logged(tmp_path):
    """A corrupt ZIP is logged to corrupt_zips.txt and the loop continues."""
    csv_path = _make_filter_csv(tmp_path, date_str="20170315")
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    # Create the expected directory structure for a corrupt zip
    zip_dir = tmp_path / "in" / "2017" / "201703"
    zip_dir.mkdir(parents=True)
    corrupt_zip = zip_dir / "HTICST120.20170315.1.zip"
    _make_corrupt_zip(corrupt_zip)

    ingest_event_windows_period(
        input_root=str(tmp_path / "in"),
        output_dir=str(out_dir),
        period="2017",
        filter_csv=str(csv_path),
    )

    log_path = out_dir / "_ingest_logs" / "corrupt_zips.txt"
    assert log_path.exists(), "corrupt_zips.txt should be created in _ingest_logs/"
    logged = log_path.read_text(encoding="utf-8")
    assert "HTICST120.20170315.1.zip" in logged


def test_ingest_event_windows_missing_zip_skipped(tmp_path):
    """When no ZIPs exist for a zip_date, the date is skipped gracefully."""
    csv_path = _make_filter_csv(tmp_path, date_str="20170315")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    in_dir = tmp_path / "in"
    in_dir.mkdir()

    # No actual ZIP files in input_dir
    ingest_event_windows_period(
        input_root=str(in_dir),
        output_dir=str(out_dir),
        period="2017",
        filter_csv=str(csv_path),
    )

    # Should complete without crashing; no output written
    assert not list(out_dir.rglob("*.parquet"))


def test_ingest_year_invalid_year_raises(tmp_path):
    """ingest_year no longer validates year range — any year is accepted."""
    in_dir = tmp_path / "in"
    in_dir.mkdir()
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    result = ingest_year(str(in_dir), str(out_dir), year=2000, data_type="individual_stock")
    assert len(result) == 0


def test_ingest_year_unknown_data_type_raises(tmp_path):
    """ingest_year raises ValueError for an unrecognised data_type."""
    tmp_path.mkdir(exist_ok=True)
    with pytest.raises(ValueError, match="Unknown data_type"):
        ingest_year(str(tmp_path), str(tmp_path), year=2023, data_type="bad_type")


def test_ingest_directory_missing_dir_raises(tmp_path):
    """ingest_directory raises FileNotFoundError when input_dir does not exist."""
    nonexistent = tmp_path / "does_not_exist"
    with pytest.raises(FileNotFoundError):
        ingest_directory(str(nonexistent), str(tmp_path))


def test_ingest_single_zip_returns_metadata(tmp_path):
    if not os.path.exists(REAL_TICST120_ZIP):
        pytest.skip(f"Real ZIP not found: {REAL_TICST120_ZIP}")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    meta = ingest_single_zip(REAL_TICST120_ZIP, str(out_dir))
    assert "zip_path" in meta
    assert "data_type" in meta
    assert "year" in meta
    assert "rows" in meta
    assert "output_path" in meta
    assert meta["data_type"] == "individual_stock"
    assert meta["year"] == 2022
    assert meta["rows"] > 0


def test_ingest_single_zip_detects_individual_stock(tmp_path):
    if not os.path.exists(REAL_TICST120_ZIP):
        pytest.skip(f"Real ZIP not found: {REAL_TICST120_ZIP}")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    meta = ingest_single_zip(REAL_TICST120_ZIP, str(out_dir))
    assert meta["data_type"] == "individual_stock"
    assert meta["year"] == 2022


def test_ingest_single_zip_detects_stock_summary(tmp_path):
    """HTICSS110.*.zip auto-detects as stock_summary and ingests to Parquet."""
    if not os.path.exists(REAL_TICSS110_ZIP):
        pytest.skip(f"Real ZIP not found: {REAL_TICSS110_ZIP}")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    meta = ingest_single_zip(REAL_TICSS110_ZIP, str(out_dir))
    assert meta["data_type"] == "stock_summary"
    assert meta["rows"] > 0
    assert list(out_dir.rglob("*.parquet"))


def test_ingest_single_zip_detects_indices(tmp_path):
    """HTICIT110.*.zip auto-detects as indices and ingests to Parquet."""
    if not os.path.exists(REAL_TICIT110_ZIP):
        pytest.skip(f"Real ZIP not found: {REAL_TICIT110_ZIP}")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    meta = ingest_single_zip(REAL_TICIT110_ZIP, str(out_dir))
    assert meta["data_type"] == "indices"
    assert meta["rows"] > 0
    assert list(out_dir.rglob("*.parquet"))


def test_ingest_single_zip_detects_indices_summary(tmp_path):
    """HTICIS110.*.zip auto-detects as indices_summary and ingests to Parquet."""
    if not os.path.exists(REAL_TICIS110_ZIP):
        pytest.skip(f"Real ZIP not found: {REAL_TICIS110_ZIP}")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    meta = ingest_single_zip(REAL_TICIS110_ZIP, str(out_dir))
    assert meta["data_type"] == "indices_summary"
    assert meta["rows"] > 0
    assert list(out_dir.rglob("*.parquet"))


def test_ingest_single_zip_creates_parquet_file(tmp_path):
    if not os.path.exists(REAL_TICST120_ZIP):
        pytest.skip(f"Real ZIP not found: {REAL_TICST120_ZIP}")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    ingest_single_zip(REAL_TICST120_ZIP, str(out_dir))
    expected = out_dir / "individual_stock" / "date=20220201"
    parquet_files = list(expected.rglob("*.parquet"))
    assert len(parquet_files) > 0


def test_ingest_single_zip_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        ingest_single_zip("/nonexistent/path.zip", "/tmp")


def test_ingest_directory_processes_all_zips(tmp_path):
    if not os.path.exists(REAL_TICST120_ZIP):
        pytest.skip(f"Real ZIP not found: {REAL_TICST120_ZIP}")
    # Copy the real zip into a temp directory
    in_dir = tmp_path / "in"
    in_dir.mkdir()
    import shutil
    shutil.copy(REAL_TICST120_ZIP, in_dir / "HTICST120.20220201.1.zip")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    results = ingest_directory(str(in_dir), str(out_dir), language="en")
    assert len(results) == 1
    assert results[0]["rows"] > 0


def test_ingest_directory_returns_error_key_on_failure(tmp_path):
    in_dir = tmp_path / "in"
    in_dir.mkdir()
    # Create a corrupt zip
    corrupt = in_dir / "bad.zip"
    corrupt.write_bytes(b"not a zip file")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    results = ingest_directory(str(in_dir), str(out_dir))
    assert any("error" in r for r in results)


def test_ingest_directory_with_explicit_data_type(tmp_path):
    if not os.path.exists(REAL_TICST120_ZIP):
        pytest.skip(f"Real ZIP not found: {REAL_TICST120_ZIP}")
    in_dir = tmp_path / "in"
    in_dir.mkdir()
    shutil.copy(REAL_TICST120_ZIP, in_dir / "HTICST120.20220201.1.zip")
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    results = ingest_directory(str(in_dir), str(out_dir), data_type="individual_stock")
    assert results[0]["data_type"] == "individual_stock"
