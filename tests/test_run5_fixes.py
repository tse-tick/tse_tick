# tests/test_run5_fixes.py
"""Regression tests for the run5 real-data QA report (all consistency/polish).

Covers F1 (capturable, consistent no-data warning across all four types),
F2 (era-independent fixed-width Execution Time), F3/F4 (docstring accuracy +
int ticker tolerance), F5 (get_available_tickers -> round-trippable string codes
that no longer drop alphanumeric codes), and F7 (UTF-8 stdout on Windows so a
naive print(df) does not crash).
"""
import sys
import warnings
from pathlib import Path

import polars as pl
import pytest

import tse_tick
from tse_tick import create_df, ingest_single_zip, read_ticks
from tse_tick.enhanced import NoDataWarning, parse_period
from tse_tick.query import get_available_tickers
from tests.synthetic_data import indices_csv, indices_2016_csv, write_zip


def _zip(path: Path, member: str, payload: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    return write_zip(path, member, payload)


# --------------------------------------------------------------------------- #
# F1 — no-data signaling must be consistent across types AND capturable
# --------------------------------------------------------------------------- #
def test_f1_empty_after_prune_warns(tmp_path):
    """A monthly ZIP holding only 05-08; requesting 05-09 prunes to empty and
    must warn (was silent for the non-individual_stock types)."""
    zp = _zip(tmp_path / "個別株式2023" / "TICIT110" / "202305" / "HTICIT110.202305.zip",
              "HTICIT110.202305.csv", indices_csv("20230508", ["101"], rows_per_code=8))
    with pytest.warns(NoDataWarning):
        df = read_ticks(str(tmp_path), data_type="indices", ticker_filter={"101"}, date="20230509")
    assert df.height == 0
    assert df.width > 0  # still a typed-empty frame with full columns


def test_f1_unknown_code_warns(tmp_path):
    zp = _zip(tmp_path / "HTICIT110.20230508.1.zip", "HTICIT110.20230508.1.csv",
              indices_csv("20230508", ["101"], rows_per_code=8))
    with pytest.warns(NoDataWarning):
        df = read_ticks(str(zp), data_type="indices", ticker_filter={"999"}, date="20230508")
    assert df.height == 0


def test_f1_no_zip_files_warns(tmp_path):
    (tmp_path / "個別株式2023" / "TICST120" / "202305").mkdir(parents=True)
    with pytest.warns(NoDataWarning):
        df = read_ticks(str(tmp_path), data_type="individual_stock", date="20230504")
    assert df.height == 0


def test_f1_warning_is_capturable_userwarning(tmp_path):
    """The novice's `warnings.catch_warnings(record=True)` must capture it (the
    old raw-stderr / logging message could not be trapped this way)."""
    zp = _zip(tmp_path / "HTICIT110.20230508.1.zip", "HTICIT110.20230508.1.csv",
              indices_csv("20230508", ["101"], rows_per_code=4))
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        read_ticks(str(zp), data_type="indices", ticker_filter={"999"}, date="20230508")
    assert any(isinstance(w.message, NoDataWarning) for w in rec)
    assert issubclass(NoDataWarning, UserWarning)


def test_f1_populated_read_does_not_warn(tmp_path):
    """A normal, non-empty read must stay quiet."""
    zp = _zip(tmp_path / "HTICIT110.20230508.1.zip", "HTICIT110.20230508.1.csv",
              indices_csv("20230508", ["101"], rows_per_code=4))
    with warnings.catch_warnings():
        warnings.simplefilter("error", NoDataWarning)  # any NoDataWarning -> failure
        df = read_ticks(str(zp), data_type="indices", ticker_filter={"101"}, date="20230508")
    assert df.height == 4


# --------------------------------------------------------------------------- #
# F2 — Execution Time must be a fixed-width 6-char HHMMSS string across eras
# --------------------------------------------------------------------------- #
def test_f2_2016_indices_execution_time_padded_to_6char(tmp_path):
    """2016 index ticks store HHMM ("0900"); they must normalize to HHMMSS
    ("090000") so the raw column compares/parses like the 2017+ data."""
    zp = _zip(tmp_path / "HTICIT010.201609.zip", "HTICIT010.201609.csv",
              indices_2016_csv("20160901", ["101"], times=["0900", "1030", "1515"]))
    df = create_df(str(zp), auto_detect=False, data_type="indices", year=2016)
    values = df["Execution Time"].to_list()
    assert all(len(v) == 6 for v in values)
    assert set(values) == {"090000", "103000", "151500"}


def test_f2_2023_indices_execution_time_is_6char(tmp_path):
    zp = _zip(tmp_path / "HTICIT110.20230508.1.zip", "HTICIT110.20230508.1.csv",
              indices_csv("20230508", ["101"], rows_per_code=4))
    df = create_df(str(zp), auto_detect=False, data_type="indices", year=2023)
    assert all(len(v) == 6 for v in df["Execution Time"].to_list())


# --------------------------------------------------------------------------- #
# F3 — docstrings must match what the code accepts; ints are tolerated
# --------------------------------------------------------------------------- #
def test_f3_parse_period_docstring_documents_single_forms():
    doc = parse_period.__doc__
    assert "single" in doc.lower()
    assert "YYYYMM" in doc and "YYYYMMDD" in doc


def test_f3_ingest_directory_has_docstring():
    assert (tse_tick.ingest_directory.__doc__ or "").strip()


def test_f3_ingest_period_docstring_documents_single_forms():
    assert "single" in (tse_tick.ingest_period.__doc__ or "").lower()


def test_f3_ticker_filter_accepts_ints(tmp_path):
    zp = _zip(tmp_path / "HTICIT110.20230508.1.zip", "HTICIT110.20230508.1.csv",
              indices_csv("20230508", ["101", "113"], rows_per_code=4))
    by_int = read_ticks(str(zp), data_type="indices", ticker_filter={101}, date="20230508")
    by_str = read_ticks(str(zp), data_type="indices", ticker_filter={"101"}, date="20230508")
    assert by_int.height == by_str.height == 4


# --------------------------------------------------------------------------- #
# F4 — the read_ticks/query_ticks schema difference must be documented honestly
# --------------------------------------------------------------------------- #
def test_f4_read_ticks_docstring_notes_query_date_column():
    doc = tse_tick.read_ticks.__doc__.lower()
    assert "date" in doc and "partition" in doc


def test_f4_query_ticks_docstring_notes_date_partition_column():
    doc = tse_tick.query_ticks.__doc__.lower()
    assert "date" in doc and "partition" in doc


# --------------------------------------------------------------------------- #
# F5 — get_available_tickers returns round-trippable string codes
# --------------------------------------------------------------------------- #
def test_f5_returns_sorted_string_codes(tmp_path):
    d = tmp_path / "indices" / "date=20230508"
    d.mkdir(parents=True)
    for code in ["113", "101"]:
        pl.DataFrame({"Index Code": [code]}).write_parquet(d / f"ticker={code}.parquet")
    out = get_available_tickers(str(tmp_path), "indices")
    assert out == ["101", "113"]
    assert all(isinstance(t, str) for t in out)


def test_f5_includes_alphanumeric_codes(tmp_path):
    """Modern TSE codes can be alphanumeric (e.g. 130A); the old int() parse
    silently dropped them."""
    d = tmp_path / "individual_stock" / "date=20240104"
    d.mkdir(parents=True)
    for code in ["7203", "130A"]:
        pl.DataFrame({"Stock Code": [code]}).write_parquet(d / f"ticker={code}.parquet")
    out = get_available_tickers(str(tmp_path), "individual_stock")
    assert "130A" in out
    assert out == ["7203", "130A"]  # pure-digit codes sort numerically, before alphanumerics


def test_f5_roundtrips_into_read_ticks(tmp_path):
    zp = _zip(tmp_path / "raw" / "HTICIT110.20230508.1.zip", "HTICIT110.20230508.1.csv",
              indices_csv("20230508", ["101", "113"], rows_per_code=4))
    store = tmp_path / "store"
    ingest_single_zip(str(zp), str(store), data_type="indices", year=2023)
    tickers = get_available_tickers(str(store), "indices")
    assert tickers == ["101", "113"]
    # Feeds straight back in with no {str(t) for t in ...} dance.
    df = read_ticks(str(zp), data_type="indices", ticker_filter=set(tickers), date="20230508")
    assert df.height == 8


# --------------------------------------------------------------------------- #
# F7 — importing tse_tick must make a naive print(df) safe on a Windows console
# --------------------------------------------------------------------------- #
class _RecordingStream:
    def __init__(self, encoding="cp1252"):
        self.encoding = encoding
        self.calls = []

    def reconfigure(self, **kwargs):
        self.calls.append(kwargs)
        if "encoding" in kwargs:
            self.encoding = kwargs["encoding"]


def test_f7_reconfigures_stdio_to_utf8_on_windows(monkeypatch):
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.delenv("TSE_TICK_ASCII_TABLES", raising=False)
    out, err = _RecordingStream(), _RecordingStream()
    monkeypatch.setattr(sys, "stdout", out)
    monkeypatch.setattr(sys, "stderr", err)
    tse_tick._configure_windows_console()
    assert out.calls and out.calls[0].get("encoding") == "utf-8"
    assert err.calls and err.calls[0].get("encoding") == "utf-8"


def test_f7_optout_env_skips_reconfigure(monkeypatch):
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setenv("TSE_TICK_ASCII_TABLES", "0")
    out = _RecordingStream()
    monkeypatch.setattr(sys, "stdout", out)
    tse_tick._configure_windows_console()
    assert out.calls == []


def test_f7_non_windows_is_noop(monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    out = _RecordingStream()
    monkeypatch.setattr(sys, "stdout", out)
    tse_tick._configure_windows_console()
    assert out.calls == []


def test_f7_skips_when_stream_already_utf8(monkeypatch):
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.delenv("TSE_TICK_ASCII_TABLES", raising=False)
    out = _RecordingStream(encoding="utf-8")
    monkeypatch.setattr(sys, "stdout", out)
    monkeypatch.setattr(sys, "stderr", _RecordingStream(encoding="utf-8"))
    tse_tick._configure_windows_console()
    assert out.calls == []
