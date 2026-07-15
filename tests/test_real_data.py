# tests/test_real_data.py
"""Tests against real NEEDS data files.

These tests verify create_df(), export_to_csv(), detect_data_type_and_year(),
and schema correctness for all 4 data types across 2016 and 2021/2023 files.

The data root defaults to ``G:\\flash_crash`` and is overridden with the
``TSE_TICK_DATA_ROOT`` environment variable (required off Windows). Every test
here skips when its files are absent, so the suite is green without the data.
"""
import os
import sys
import tempfile

import polars as pl
import pytest

from tse_tick.enhanced import create_df, export_to_csv, detect_data_type_and_year
from tse_tick.schemas import (
    get_schema_individual_stock_95,
    get_schema_summary_83,
    get_schema_indices_23,
    get_schema_indices_15,
    get_schema_indices_summary,
    get_japanese_column_mapping,
)

# ---------------------------------------------------------------------------
# Test data paths (read-only). Override the root with TSE_TICK_DATA_ROOT.
# Expected layout: TICST120 parts under raw_{year}/{yearmonth}/; stock-summary
# and index files under raw_other/. Each test class skips on its own file set,
# so whatever data is present locally still gets tested.
#
# The default is Kevin's Windows root; off Windows it never exists, so set
# TSE_TICK_DATA_ROOT (see the "Testing" section of the README).
# ---------------------------------------------------------------------------

DATA_ROOT = os.environ.get("TSE_TICK_DATA_ROOT", r"G:\flash_crash")

DATA_ROOT_HINT = (
    f"Real NEEDS data not found under {DATA_ROOT!r}; set TSE_TICK_DATA_ROOT to "
    f"your NEEDS data root to run the data-gated tests"
)

TICST120_2016 = os.path.join(DATA_ROOT, "raw_2016", "201601", "HTICST120.20160104.1.zip")
TICST120_2021 = os.path.join(DATA_ROOT, "raw_2021", "202104", "HTICST120.20210401.1.zip")
TICST120_DIR_2016 = os.path.join(DATA_ROOT, "raw_2016", "201601")

TICSS110_2016 = os.path.join(DATA_ROOT, "raw_other", "HTICSS110.201601.zip")
TICSS110_2017 = os.path.join(DATA_ROOT, "raw_other", "HTICSS110.201701.zip")
TICSS110_2023 = os.path.join(DATA_ROOT, "raw_other", "HTICSS110.202302.zip")

TICIT_2016 = os.path.join(DATA_ROOT, "raw_other", "HTICIT010.201601.zip")
TICIT_2017 = os.path.join(DATA_ROOT, "raw_other", "HTICIT110.201701.zip")
TICIT_2023 = os.path.join(DATA_ROOT, "raw_other", "HTICIT110.202301.zip")

TICIS_2016 = os.path.join(DATA_ROOT, "raw_other", "HTICIS010.201601.zip")
TICIS_2017 = os.path.join(DATA_ROOT, "raw_other", "HTICIS110.201701.zip")
TICIS_2023 = os.path.join(DATA_ROOT, "raw_other", "HTICIS110.202301.zip")


def requires_files(*paths):
    """Skip a test class unless every listed real-data file exists.

    The reason names ``TSE_TICK_DATA_ROOT`` because the default root is a Windows
    path: on any other OS all of these skip and the suite looks green while the
    real-data half never ran, and nothing on screen said which knob turns it on.
    """
    missing = [p for p in paths if not os.path.exists(p)]
    return pytest.mark.skipif(
        bool(missing),
        reason=f"{DATA_ROOT_HINT}. Missing: {missing}",
    )


# ===================================================================
# TICST120 — Individual Stock Ticks (95 columns)
# ===================================================================

@requires_files(TICST120_2016, TICST120_2021, TICST120_DIR_2016)
class TestTICST120:
    def test_2016_column_count(self):
        df = create_df(TICST120_2016, language="en", rows=10)
        assert len(df.columns) == 95

    def test_2021_column_count(self):
        df = create_df(TICST120_2021, language="en", rows=10)
        assert len(df.columns) == 95

    def test_2016_schema_names_match(self):
        df = create_df(TICST120_2016, language="en", rows=5)
        expected = get_schema_individual_stock_95()
        assert df.columns == expected

    def test_2021_schema_names_match(self):
        df = create_df(TICST120_2021, language="en", rows=5)
        expected = get_schema_individual_stock_95()
        assert df.columns == expected

    def test_2016_japanese_columns(self):
        df = create_df(TICST120_2016, language="jp", rows=5)
        assert len(df.columns) == 95
        jp_mapping = get_japanese_column_mapping()
        assert df.columns[0] == jp_mapping["Record Type"]

    def test_2021_japanese_columns(self):
        df = create_df(TICST120_2021, language="jp", rows=5)
        assert len(df.columns) == 95

    def test_rows_parameter(self):
        df = create_df(TICST120_2021, language="en", rows=50)
        assert len(df) == 50

    def test_directory_input(self):
        df = create_df(TICST120_DIR_2016, language="en", rows=20)
        assert len(df.columns) == 95
        assert len(df) == 20

    def test_data_date_is_datetime(self):
        df = create_df(TICST120_2021, language="en", rows=5)
        assert df["Data Date"].dtype.is_temporal()

    def test_no_internal_columns_leak(self):
        df = create_df(TICST120_2021, language="en", rows=10)
        assert "_tick_dt" not in df.columns
        assert "_stock_4" not in df.columns
        assert "_date_str" not in df.columns


# ===================================================================
# TICSS110 — Daily Stock Summary (82 output columns, 83 raw)
# ===================================================================

@requires_files(TICSS110_2016, TICSS110_2023)
class TestTICSS110:
    def test_2016_column_count(self):
        df = create_df(TICSS110_2016, language="en", rows=10)
        assert len(df.columns) == 82

    def test_2023_column_count(self):
        df = create_df(TICSS110_2023, language="en", rows=10)
        assert len(df.columns) == 82

    def test_2016_japanese_columns(self):
        df = create_df(TICSS110_2016, language="jp", rows=5)
        assert len(df.columns) == 82

    def test_2023_japanese_columns(self):
        df = create_df(TICSS110_2023, language="jp", rows=5)
        assert len(df.columns) == 82

    def test_rows_parameter(self):
        df = create_df(TICSS110_2023, language="en", rows=25)
        assert len(df) == 25

    def test_has_expected_columns(self):
        df = create_df(TICSS110_2023, language="en", rows=5)
        for col in ["Record Type", "Data Date", "Stock Code", "AM Opening Price", "PM Close Price"]:
            assert col in df.columns

    def test_raw_schema_is_83(self):
        schema = get_schema_summary_83()
        assert len(schema) == 83


# ===================================================================
# TICIT110 — Index Ticks (10 output columns)
# ===================================================================

@requires_files(TICIT_2016, TICIT_2023)
class TestTICIT110:
    def test_2016_column_count(self):
        df = create_df(TICIT_2016, language="en", rows=10)
        assert len(df.columns) == 10

    def test_2023_column_count(self):
        df = create_df(TICIT_2023, language="en", rows=10)
        assert len(df.columns) == 10

    def test_2016_2023_same_output_columns(self):
        df16 = create_df(TICIT_2016, language="en", rows=5)
        df23 = create_df(TICIT_2023, language="en", rows=5)
        assert df16.columns == df23.columns

    def test_2016_japanese_columns(self):
        df = create_df(TICIT_2016, language="jp", rows=5)
        assert len(df.columns) == 10

    def test_2023_japanese_columns(self):
        df = create_df(TICIT_2023, language="jp", rows=5)
        assert len(df.columns) == 10

    def test_rows_parameter(self):
        df = create_df(TICIT_2023, language="en", rows=15)
        assert len(df) == 15

    def test_has_expected_columns(self):
        df = create_df(TICIT_2023, language="en", rows=5)
        for col in ["Record Type", "Data Date", "Index Code", "Index Value", "Execution Time"]:
            assert col in df.columns

    def test_raw_schemas_have_correct_lengths(self):
        assert len(get_schema_indices_15()) == 15
        assert len(get_schema_indices_23()) == 23


# ===================================================================
# TICIS110 — Daily Index Summary (17 columns)
# ===================================================================

@requires_files(TICIS_2016, TICIS_2023)
class TestTICIS110:
    def test_2016_column_count(self):
        df = create_df(TICIS_2016, language="en", rows=10)
        assert len(df.columns) == 17

    def test_2023_column_count(self):
        df = create_df(TICIS_2023, language="en", rows=10)
        assert len(df.columns) == 17

    def test_2016_2023_same_columns(self):
        df16 = create_df(TICIS_2016, language="en", rows=5)
        df23 = create_df(TICIS_2023, language="en", rows=5)
        assert df16.columns == df23.columns

    def test_2016_japanese_columns(self):
        df = create_df(TICIS_2016, language="jp", rows=5)
        assert len(df.columns) == 17

    def test_2023_japanese_columns(self):
        df = create_df(TICIS_2023, language="jp", rows=5)
        assert len(df.columns) == 17

    def test_rows_parameter(self):
        df = create_df(TICIS_2023, language="en", rows=8)
        assert len(df) == 8

    def test_has_expected_columns(self):
        df = create_df(TICIS_2023, language="en", rows=5)
        for col in ["Record Type", "Data Date", "Index Code", "AM Opening Price", "PM Close Price"]:
            assert col in df.columns

    def test_schema_length(self):
        assert len(get_schema_indices_summary()) == 17


# ===================================================================
# 2017 summary / index files (modern-era smoke tests)
# ===================================================================

@requires_files(TICSS110_2017, TICIT_2017, TICIS_2017)
class TestModernEra2017Files:
    def test_stock_summary_2017_column_count(self):
        df = create_df(TICSS110_2017, language="en", rows=10)
        assert len(df.columns) == 82

    def test_index_ticks_2017_column_count(self):
        df = create_df(TICIT_2017, language="en", rows=10)
        assert len(df.columns) == 10

    def test_index_summary_2017_column_count(self):
        df = create_df(TICIS_2017, language="en", rows=10)
        assert len(df.columns) == 17
        assert "Index Code" in df.columns


# ===================================================================
# detect_data_type_and_year (pure filename parsing — no data needed)
# ===================================================================

class TestDetectDataTypeAndYear:
    def test_ticst120_2016(self):
        dt, yr = detect_data_type_and_year(TICST120_2016)
        assert dt == "individual_stock"
        assert yr == 2016

    def test_ticst120_2021(self):
        dt, yr = detect_data_type_and_year(TICST120_2021)
        assert dt == "individual_stock"
        assert yr == 2021

    def test_ticss110_2016(self):
        dt, yr = detect_data_type_and_year(TICSS110_2016)
        assert dt == "stock_summary"
        assert yr == 2016

    def test_ticss110_2023(self):
        dt, yr = detect_data_type_and_year(TICSS110_2023)
        assert dt == "stock_summary"
        assert yr == 2023

    def test_ticit_2016(self):
        dt, yr = detect_data_type_and_year(TICIT_2016)
        assert dt == "indices"
        assert yr == 2016

    def test_ticit_2023(self):
        dt, yr = detect_data_type_and_year(TICIT_2023)
        assert dt == "indices"
        assert yr == 2023

    def test_ticis_2016(self):
        dt, yr = detect_data_type_and_year(TICIS_2016)
        assert dt == "indices_summary"
        assert yr == 2016

    def test_ticis_2023(self):
        dt, yr = detect_data_type_and_year(TICIS_2023)
        assert dt == "indices_summary"
        assert yr == 2023

    def test_no_year_in_path_raises(self):
        with pytest.raises(ValueError, match="Could not detect year"):
            detect_data_type_and_year(r"C:\data\somefile.zip")

    def test_no_type_in_path_raises(self):
        with pytest.raises(ValueError, match="Could not detect data type"):
            detect_data_type_and_year(r"C:\data\2023\unknown.zip")


# ===================================================================
# export_to_csv
# ===================================================================

@requires_files(TICIS_2017)
class TestExportToCsv:
    def test_export_with_explicit_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out = export_to_csv(
                TICIS_2017,
                output_path=os.path.join(tmpdir, "test.csv"),
                rows=5,
            )
            assert os.path.exists(out)
            assert os.path.getsize(out) > 0

    def test_export_auto_filename(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            orig_dir = os.getcwd()
            try:
                os.chdir(tmpdir)
                out = export_to_csv(TICIS_2017, rows=5)
                assert os.path.exists(out)
                assert "indices_summary" in out
            finally:
                os.chdir(orig_dir)


# ===================================================================
# Error paths
# ===================================================================

class TestErrorPaths:
    def test_nonexistent_file(self):
        with pytest.raises((FileNotFoundError, ValueError)):
            create_df(r"G:\nonexistent_file_12345.zip")

    def test_invalid_zip(self, tmp_path):
        bad_zip = tmp_path / "HTICST120.20230101.1.zip"
        bad_zip.write_bytes(b"not a zip file at all")
        with pytest.raises(Exception):
            create_df(str(bad_zip), auto_detect=False, data_type="individual_stock", year=2023)

    def test_empty_file(self, tmp_path):
        empty_zip = tmp_path / "HTICST120.20230101.1.zip"
        empty_zip.write_bytes(b"")
        with pytest.raises(Exception):
            create_df(str(empty_zip), auto_detect=False, data_type="individual_stock", year=2023)

    def test_auto_detect_false_requires_params(self):
        with pytest.raises(ValueError, match="auto_detect=False"):
            create_df("some_path.zip", auto_detect=False)


# ===================================================================
# _tick_dt / _stock_4 column leak bug
# ===================================================================

@requires_files(TICST120_2021)
class TestInternalColumnLeak:
    def test_filter_ticks_drops_internal_columns_with_matches(self):
        """When events match, _tick_dt and _stock_4 should be dropped."""
        from tse_tick.event_window import _filter_ticks_for_events
        import datetime
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from backports.zoneinfo import ZoneInfo

        df = create_df(TICST120_2021, language="en", rows=500)
        stock_code = df["Stock Code"][0]
        ticker_4 = str(stock_code).strip()[:4]
        data_date = df["Data Date"][0]
        year = data_date.year
        month = data_date.month
        day = data_date.day
        anchor = datetime.datetime(year, month, day, 10, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))

        events = pl.DataFrame({
            "ticker": [ticker_4],
            "event_type": ["test"],
            "session_type": ["intraday"],
            "reaction_anchor_dt": [anchor],
        })

        result = _filter_ticks_for_events(df, events, window_minutes=600)
        assert "_tick_dt" not in result.columns
        assert "_stock_4" not in result.columns
        if not result.is_empty():
            assert "event_ticker" in result.columns

    def test_filter_ticks_drops_internal_columns_empty_result(self):
        """When no events match, _tick_dt and _stock_4 should still be absent."""
        from tse_tick.event_window import _filter_ticks_for_events
        import datetime
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from backports.zoneinfo import ZoneInfo

        df = create_df(TICST120_2021, language="en", rows=10)
        anchor = datetime.datetime(2099, 1, 1, 10, 0, 0, tzinfo=ZoneInfo("Asia/Tokyo"))
        events = pl.DataFrame({
            "ticker": ["9999"],
            "event_type": ["test"],
            "session_type": ["intraday"],
            "reaction_anchor_dt": [anchor],
        })

        result = _filter_ticks_for_events(df, events, window_minutes=1)
        assert result.is_empty()
        assert "_tick_dt" not in result.columns
        assert "_stock_4" not in result.columns


# ===================================================================
# CLI help
# ===================================================================

class TestCLI:
    # sys.executable, not "python": stock Debian/Ubuntu ships only `python3`, so a
    # bare "python" is a FileNotFoundError there unless a venv happens to be
    # activated. It also pins the subprocess to the interpreter running pytest.
    def test_cli_help(self):
        import subprocess
        result = subprocess.run(
            [sys.executable, "-m", "tse_tick.cli", "ingest", "--help"],
            capture_output=True, text=True, timeout=15,
        )
        assert result.returncode == 0
        assert "--data-type" in result.stdout

    def test_cli_no_command(self):
        import subprocess
        result = subprocess.run(
            [sys.executable, "-m", "tse_tick.cli"],
            capture_output=True, text=True, timeout=15,
        )
        assert result.returncode != 0


# ===================================================================
# Schema counts
# ===================================================================

class TestSchemas:
    def test_individual_stock_95(self):
        assert len(get_schema_individual_stock_95()) == 95

    def test_summary_83(self):
        assert len(get_schema_summary_83()) == 83

    def test_indices_15(self):
        assert len(get_schema_indices_15()) == 15

    def test_indices_23(self):
        assert len(get_schema_indices_23()) == 23

    def test_indices_summary_17(self):
        assert len(get_schema_indices_summary()) == 17

    def test_japanese_mapping_covers_individual_stock(self):
        jp = get_japanese_column_mapping()
        for col in get_schema_individual_stock_95():
            assert col in jp, f"Missing Japanese mapping for: {col}"

    def test_japanese_mapping_covers_indices_summary(self):
        jp = get_japanese_column_mapping()
        for col in get_schema_indices_summary():
            assert col in jp, f"Missing Japanese mapping for: {col}"

    def test_no_duplicate_schema_names(self):
        for schema_fn in [
            get_schema_individual_stock_95,
            get_schema_summary_83,
            get_schema_indices_15,
            get_schema_indices_23,
            get_schema_indices_summary,
        ]:
            names = schema_fn()
            assert len(names) == len(set(names)), f"Duplicate names in {schema_fn.__name__}"
