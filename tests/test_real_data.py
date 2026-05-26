# tests/test_real_data.py
"""Tests against real NEEDS data files on G:\\ drive.

These tests verify create_df(), export_to_csv(), detect_data_type_and_year(),
and schema correctness for all 4 data types across 2016 and 2021/2023 files.
"""
import os
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
# Test data paths (read-only)
# ---------------------------------------------------------------------------

TICST120_2016 = r"G:\flash_crash_pilot\raw_2016\201601\HTICST120.20160104.1.zip"
TICST120_2021 = r"G:\flash_crash_pilot\raw_2021\202104\HTICST120.20210401.1.zip"
TICST120_DIR_2016 = r"G:\flash_crash_pilot\raw_2016\201601"

TICSS110_2016 = r"G:\HTICSS110.201601.zip"
TICSS110_2023 = r"G:\HTICSS110.202302.zip"

TICIT_2016 = r"G:\HTICIT010.201601.zip"
TICIT_2023 = r"G:\HTICIT110.202301.zip"

TICIS_2016 = r"G:\HTICIS010.201601.zip"
TICIS_2023 = r"G:\HTICIS110.202301.zip"

ALL_FILES = [
    TICST120_2016, TICST120_2021,
    TICSS110_2016, TICSS110_2023,
    TICIT_2016, TICIT_2023,
    TICIS_2016, TICIS_2023,
]

skip_if_no_data = pytest.mark.skipif(
    not all(os.path.exists(f) for f in ALL_FILES),
    reason="Real NEEDS data files not available",
)


# ===================================================================
# TICST120 — Individual Stock Ticks (95 columns)
# ===================================================================

@skip_if_no_data
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

@skip_if_no_data
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

@skip_if_no_data
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

@skip_if_no_data
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
        for col in ["Record Type", "Data Date", "Stock Code", "AM Opening Price", "PM Close Price"]:
            assert col in df.columns

    def test_schema_length(self):
        assert len(get_schema_indices_summary()) == 17


# ===================================================================
# detect_data_type_and_year
# ===================================================================

@skip_if_no_data
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

@skip_if_no_data
class TestExportToCsv:
    def test_export_with_explicit_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out = export_to_csv(
                TICIS_2023,
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
                out = export_to_csv(TICIS_2023, rows=5)
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

@skip_if_no_data
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
    def test_cli_help(self):
        import subprocess
        result = subprocess.run(
            ["python", "-m", "tse_tick.cli", "ingest", "--help"],
            capture_output=True, text=True, timeout=15,
        )
        assert result.returncode == 0
        assert "--data-type" in result.stdout

    def test_cli_no_command(self):
        import subprocess
        result = subprocess.run(
            ["python", "-m", "tse_tick.cli"],
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
