# tests/test_quiet_and_unicode.py
"""Library functions must not crash on non-ASCII paths under a legacy-codepage
stdout, and must be quiet by default (diagnostics go to logging, not print).

Reproduces the cp1252 UnicodeEncodeError seen on Windows with 個別株式 paths.
"""
import io
import logging
import sys

import polars as pl
import pytest

import tse_tick
from tests.synthetic_data import individual_stock_csv, write_zip

_CJK = "個別株式"


@pytest.fixture
def cjk_zip(tmp_path):
    d = tmp_path / _CJK
    d.mkdir()
    zp = d / "HTICST120.20240104.1.zip"
    write_zip(
        zp,
        "HTICST120.20240104.1.csv",
        individual_stock_csv("20240104", ["7203"], rows_per_ticker=10, base_prices={"7203": 2100}),
    )
    return zp


def _cp1252_stdout(monkeypatch):
    """Replace sys.stdout with a strict cp1252 stream (a legacy Windows console)."""
    buf = io.TextIOWrapper(io.BytesIO(), encoding="cp1252", errors="strict", newline="")
    monkeypatch.setattr(sys, "stdout", buf)
    return buf


def test_create_df_no_crash_and_quiet_on_cjk_path(cjk_zip, monkeypatch):
    out = _cp1252_stdout(monkeypatch)
    df = tse_tick.create_df(str(cjk_zip), auto_detect=False, data_type="individual_stock", year=2024)
    out.flush()
    assert df.height == 10
    assert out.buffer.getvalue() == b""   # nothing printed; diagnostics go to logging


def test_read_ticks_no_crash_and_quiet_on_cjk_path(cjk_zip, monkeypatch):
    out = _cp1252_stdout(monkeypatch)
    df = tse_tick.read_ticks(str(cjk_zip), ticker_filter={"7203"}, date="20240104")
    out.flush()
    assert df.height == 10
    assert out.buffer.getvalue() == b""


def test_diagnostics_available_via_logging(cjk_zip, caplog):
    with caplog.at_level(logging.DEBUG, logger="tse_tick.enhanced"):
        tse_tick.create_df(str(cjk_zip), auto_detect=False, data_type="individual_stock", year=2024)
    assert any("ZIP file" in r.getMessage() for r in caplog.records)


# --- print(df) on Windows: cp1252 vs Polars box-drawing chars (BUG-2) ---

def test_display_writes_utf8_under_cp1252_stdout(monkeypatch):
    """tse_tick.display(df) renders to UTF-8 regardless of console encoding."""
    out = _cp1252_stdout(monkeypatch)
    df = pl.DataFrame({"Exchange Code": ["11", "11"], "Execution Price": [2100.0, 2101.0]})
    tse_tick.display(df)                       # must not raise under a strict cp1252 stream
    out.flush()
    written = out.buffer.getvalue()
    assert written                             # something was emitted
    assert "Exchange Code" in written.decode("utf-8")   # and it is valid UTF-8


@pytest.mark.skipif(sys.platform != "win32", reason="cp1252 console auto-fix is Windows-only")
def test_print_df_safe_on_windows_cp1252(monkeypatch):
    """On Windows, importing tse_tick switches Polars to ASCII tables so a bare
    print(df) to a legacy cp1252 console no longer raises UnicodeEncodeError."""
    out = _cp1252_stdout(monkeypatch)
    df = pl.DataFrame({"Exchange Code": ["11"], "Execution Price": [2100.0]})
    print(df)                                  # the naive inspection that used to crash
    out.flush()
    assert out.buffer.getvalue()               # rendered without raising
