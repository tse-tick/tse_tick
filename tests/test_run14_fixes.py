# tests/test_run14_fixes.py
"""Regression tests for the run14 real-data acceptance-test bug report.

Covers the four findings:

- Finding 1 (Major)  — ``compute_volatility`` returned NaN/inf/corrupted values on
  a standard ``individual_stock`` frame because quote-only book rows carry
  ``Execution Price = 0``; it must now exclude non-trade rows before log-returns
  and match a trades-only reference.
- Finding 4 (UX)     — ``compute_volatility`` must emit ``null`` (not ``NaN``) for
  undefined/warm-up positions, so ``drop_nulls`` removes them (as the sibling
  ``compute_spread`` / ``compute_flow_imbalance`` already do).
- Finding 2 (Minor)  — ``extract_event_window`` leaked an un-suppressable Polars
  ``String -> Date`` deprecation from worker threads to stderr.
- Finding 3 (UX)     — ``export_to_csv(language="jp")`` wrote UTF-8 without a BOM,
  causing mojibake in Excel on a Japanese Windows locale.
"""
import math
import subprocess
import sys
from pathlib import Path

import polars as pl

import tse_tick
from tse_tick.features import compute_volatility, compute_all_features
from tests.synthetic_data import individual_stock_csv, write_zip


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _quote_heavy_stock_frame() -> pl.DataFrame:
    """A minimal ``individual_stock``-shaped frame: 5 real trades (Execution
    Price > 0, a real ``HHMMSS`` time) interleaved with 15 quote-only book rows
    (Execution Price 0.0 and a blank Execution Time), all inside one 5-min window.

    This reproduces the real-data condition from Finding 1 — a liquid day is
    ~94% quote-only rows — without any proprietary NEEDS data.
    """
    exec_time, price = [], []
    t = 0
    for i in range(20):
        if i % 4 == 0:  # rows 0, 4, 8, 12, 16 are trades
            exec_time.append(f"0900{t:02d}")
            price.append(6260.0 + (i % 5))
            t += 3
        else:
            exec_time.append("")  # quote-only book row: blank Execution Time
            price.append(0.0)
    return pl.DataFrame({"Execution Time": exec_time, "Execution Price": price})


def _nan_count(values) -> int:
    return sum(1 for v in values if v is not None and math.isnan(v))


def _inf_count(values) -> int:
    return sum(1 for v in values if v is not None and math.isinf(v))


# --------------------------------------------------------------------------- #
# Finding 1 — compute_volatility must not produce NaN/inf on quote-heavy frames
# --------------------------------------------------------------------------- #
def test_f1_volatility_no_nan_or_inf_with_quote_rows():
    df = _quote_heavy_stock_frame()
    vol = compute_volatility(df, window="5min", method="realized")
    vals = vol.to_list()

    # aligned to df's rows (same convention as compute_spread)
    assert len(vol) == df.height
    # the whole point of the fix: log-returns never see a zero price
    assert _nan_count(vals) == 0
    assert _inf_count(vals) == 0
    # non-trade (quote-only) rows carry no volatility -> null
    for i in range(df.height):
        if i % 4 != 0:
            assert vals[i] is None


def test_f1_volatility_matches_trades_only_reference():
    """compute_volatility(full frame) at trade rows must equal
    compute_volatility(trades-only) — the exact cross-check from the report."""
    df = _quote_heavy_stock_frame()
    full = compute_volatility(df, window="5min", method="realized").to_list()
    ref = compute_volatility(
        df.filter(pl.col("Execution Price") > 0), window="5min", method="realized"
    ).to_list()

    trade_positions = [i for i in range(df.height) if df["Execution Price"][i] > 0]
    got = [full[i] for i in trade_positions]

    assert len(got) == len(ref)
    for a, b in zip(got, ref):
        assert (a is None and b is None) or (a is not None and b is not None and abs(a - b) < 1e-12)


def test_f1_garman_klass_no_nan_or_inf_with_quote_rows():
    df = _quote_heavy_stock_frame()
    vals = compute_volatility(df, window="5min", method="garman_klass").to_list()
    assert _nan_count(vals) == 0
    assert _inf_count(vals) == 0
    for i in range(df.height):
        if i % 4 != 0:
            assert vals[i] is None


def test_f1_compute_all_features_volatility_finite_with_quote_rows(feature_ticks):
    """End-to-end: compute_all_features on a 95-col frame with injected quote-only
    rows must yield a finite/null (never NaN/inf) volatility column."""
    n = feature_ticks.height
    idx = pl.int_range(0, pl.len())
    df = feature_ticks.with_columns(
        pl.when(idx % 2 == 0)
        .then(pl.lit(0.0))
        .otherwise(pl.col("Execution Price"))
        .alias("Execution Price"),
        pl.when(idx % 2 == 0)
        .then(pl.lit(""))
        .otherwise(pl.col("Execution Time"))
        .alias("Execution Time"),
    )
    out = compute_all_features(df, levels=5)
    vcol = out["volatility"].to_list()
    assert len(vcol) == n
    assert _nan_count(vcol) == 0
    assert _inf_count(vcol) == 0
    # the injected zero-price rows contribute no volatility
    assert out["volatility"].null_count() > 0


# --------------------------------------------------------------------------- #
# Finding 4 — undefined volatility must be null, not NaN (so drop_nulls works)
# --------------------------------------------------------------------------- #
def test_f4_volatility_undefined_is_null_not_nan():
    df = _quote_heavy_stock_frame()
    vol = compute_volatility(df, window="5min", method="realized")
    vals = vol.to_list()

    assert vol.null_count() > 0
    assert _nan_count(vals) == 0  # no NaN masquerading as an undefined value

    cleaned = vol.drop_nulls()
    # drop_nulls must actually shrink the series (NaN would have survived it)
    assert len(cleaned) < len(vol)
    assert _nan_count(cleaned.to_list()) == 0
    assert all(v >= 0.0 for v in cleaned.to_list())


# --------------------------------------------------------------------------- #
# Finding 2 — extract_event_window must not leak a String->Date deprecation
# --------------------------------------------------------------------------- #
def test_f2_event_window_no_string_to_date_deprecation(stock_store):
    """The deprecation is emitted from Polars worker threads to *stderr* (not via
    the Python ``warnings`` machinery), so it is checked by capturing a fresh
    subprocess's stderr. The subprocess also asserts the window is correct, so
    this stays a meaningful guard on Polars versions that do not emit the warning.
    """
    code = (
        "import tse_tick\n"
        f"df = tse_tick.extract_event_window({stock_store!r}, ticker=7203,\n"
        "    event_date='20230704', event_time='13:00:00', before='60min',\n"
        "    after='60min', data_type='individual_stock')\n"
        "assert 'seconds_from_event' in df.columns, 'missing seconds_from_event'\n"
        "assert df.height > 0, 'event window empty'\n"
        "print('OK', df.height)\n"
    )
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert proc.returncode == 0, f"subprocess failed:\n{proc.stderr}"
    assert proc.stdout.startswith("OK ")

    combined = (proc.stdout + proc.stderr).lower()
    assert "casting from string to date" not in combined
    assert "str.to_date" not in combined


# --------------------------------------------------------------------------- #
# Finding 3 — export_to_csv(language="jp") must write a UTF-8 BOM (utf-8-sig)
# --------------------------------------------------------------------------- #
def test_f3_export_jp_has_bom_en_does_not(tmp_path):
    payload = individual_stock_csv("20230704", ["7203"], rows_per_ticker=4)
    zp = tmp_path / "HTICST120.20230704.1.zip"
    write_zip(zp, "HTICST120.20230704.1.csv", payload)

    out_jp = tse_tick.export_to_csv(str(zp), output_path=str(tmp_path / "jp.csv"), language="jp")
    out_en = tse_tick.export_to_csv(str(zp), output_path=str(tmp_path / "en.csv"), language="en")
    jp_bytes = Path(out_jp).read_bytes()
    en_bytes = Path(out_en).read_bytes()

    assert jp_bytes[:3] == b"\xef\xbb\xbf"  # BOM present for jp
    assert en_bytes[:3] != b"\xef\xbb\xbf"  # en stays BOM-free (ASCII)
    # jp payload genuinely carries multibyte UTF-8 (the Japanese column names)
    assert any(b >= 0x80 for b in jp_bytes)
    assert all(b < 0x80 for b in en_bytes)

    # Content is still valid CSV — polars strips the BOM transparently.
    back = pl.read_csv(out_jp)
    assert back.height == 4
    assert not back.columns[0].startswith("﻿")  # BOM not glued to first header
