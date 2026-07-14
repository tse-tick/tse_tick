# tests/test_cli.py
"""Tests for tse_tick.cli — argument parsing, validation, and the ingest command."""

from pathlib import Path

import pytest

from tse_tick.cli import _build_parser, _parse_months, _parse_tickers, _parse_years, main
from tests.synthetic_data import individual_stock_csv, write_zip


# ---------------------------------------------------------------------------
# Helper parsing functions
# ---------------------------------------------------------------------------

def test_parse_years_range_and_list():
    assert _parse_years("2016-2018") == [2016, 2017, 2018]
    assert _parse_years("2019,2017,2017") == [2017, 2019]


def test_parse_years_invalid_raises():
    with pytest.raises(ValueError, match="Invalid year"):
        _parse_years("20xx")


def test_parse_months_range_and_list():
    assert _parse_months("1-3,7") == [1, 2, 3, 7]


def test_parse_months_invalid_raises():
    with pytest.raises(ValueError, match="Invalid month"):
        _parse_months("1-x")


def test_parse_tickers_inline():
    assert _parse_tickers("7203, 6758,,9984") == {"7203", "6758", "9984"}


def test_parse_tickers_from_file(tmp_path):
    f = tmp_path / "tickers.txt"
    f.write_text("7203\n6758\n\n", encoding="utf-8")
    assert _parse_tickers(f"@{f}") == {"7203", "6758"}


# ---------------------------------------------------------------------------
# Parser / main() validation
# ---------------------------------------------------------------------------

def test_main_no_command_prints_help_and_exits(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["tse-tick"])
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 1
    assert "usage" in capsys.readouterr().out.lower()


def test_ingest_requires_input_and_output_root():
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["ingest", "--data-type", "individual_stock"])


def test_ingest_rejects_unknown_data_type():
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            ["ingest", "--data-type", "bad_type", "--input-root", "x", "--output-root", "y"]
        )


def test_filter_csv_requires_individual_stock(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr(
        "sys.argv",
        [
            "tse-tick", "ingest",
            "--data-type", "indices",
            "--period", "2023",
            "--input-root", str(tmp_path),
            "--output-root", str(tmp_path / "out"),
            "--filter-csv", str(tmp_path / "events.csv"),
        ],
    )
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 1
    assert "individual_stock" in capsys.readouterr().err


def test_ingest_requires_some_year_selector(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr(
        "sys.argv",
        [
            "tse-tick", "ingest",
            "--data-type", "individual_stock",
            "--input-root", str(tmp_path),
            "--output-root", str(tmp_path / "out"),
        ],
    )
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 1
    assert "--years, --year, or --period" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# End-to-end ingest over synthetic data
# ---------------------------------------------------------------------------

def _make_synthetic_root(tmp_path: Path, date: str = "20240201") -> Path:
    """Build {root}/{year}/{yearmonth}/HTICST120.{date}.1.zip with synthetic rows."""
    root = tmp_path / "raw_root"
    month_dir = root / date[:4] / date[:6]
    month_dir.mkdir(parents=True)
    payload = individual_stock_csv(date, ["7203", "6758"], rows_per_ticker=8)
    write_zip(month_dir / f"HTICST120.{date}.1.zip", f"HTICST120.{date}.1.csv", payload)
    return root


def test_ingest_period_end_to_end(monkeypatch, capsys, tmp_path):
    root = _make_synthetic_root(tmp_path)
    out = tmp_path / "store"
    monkeypatch.setattr(
        "sys.argv",
        [
            "tse-tick", "ingest",
            "--data-type", "individual_stock",
            "--period", "20240201-20240201",
            "--input-root", str(root),
            "--output-root", str(out),
        ],
    )
    main()
    assert "Done: 1 succeeded, 0 failed" in capsys.readouterr().out
    assert (out / "individual_stock" / "date=20240201" / "ticker=7203.parquet").exists()
    assert (out / "individual_stock" / "date=20240201" / "ticker=6758.parquet").exists()


def test_ingest_flat_end_to_end(monkeypatch, capsys, tmp_path):
    flat = tmp_path / "flat"
    flat.mkdir()
    payload = individual_stock_csv("20240201", ["7203"], rows_per_ticker=8)
    write_zip(flat / "HTICST120.20240201.1.zip", "HTICST120.20240201.1.csv", payload)
    out = tmp_path / "store"
    monkeypatch.setattr(
        "sys.argv",
        [
            "tse-tick", "ingest",
            "--data-type", "individual_stock",
            "--year", "2024",
            "--flat",
            "--input-root", str(flat),
            "--output-root", str(out),
        ],
    )
    main()
    assert "Done: 1 succeeded, 0 failed" in capsys.readouterr().out
    assert (out / "individual_stock" / "date=20240201" / "ticker=7203.parquet").exists()


def test_cli_export_csv_from_nested_tree(tmp_path, monkeypatch, capsys):
    """`tse-tick export` reads raw ZIPs (any nesting) and writes a ticker slice to CSV."""
    import polars as pl

    month = tmp_path / "個別株式2024" / "TICST120" / "202401"
    month.mkdir(parents=True)
    write_zip(
        month / "HTICST120.20240104.1.zip", "HTICST120.20240104.1.csv",
        individual_stock_csv("20240104", ["7203", "6758"], rows_per_ticker=20,
                             base_prices={"7203": 2100, "6758": 13000}),
    )
    out = tmp_path / "toyota.csv"
    monkeypatch.setattr("sys.argv", [
        "tse-tick", "export", "--data-type", "individual_stock",
        "--input-root", str(tmp_path), "--tickers", "7203",
        "--period", "20240104", "--output", str(out),
    ])
    main()
    assert out.exists()
    df = pl.read_csv(out)
    assert set(df["Stock Code"].cast(str).str.slice(0, 4).to_list()) == {"7203"}
    assert df.height == 20
    assert "Wrote 20 rows" in capsys.readouterr().out


def test_cli_export_two_stage_store(tmp_path, monkeypatch, capsys):
    """`tse-tick export --store` builds a reusable store (two-stage) then writes CSV.

    7203 straddles both parts, exercising the run-scan + last-part pruning end to end.
    """
    import polars as pl
    pytest.importorskip("duckdb")

    month = tmp_path / "個別株式2024" / "TICST120" / "202401"
    month.mkdir(parents=True)
    write_zip(month / "HTICST120.20240104.1.zip", "HTICST120.20240104.1.csv",
              individual_stock_csv("20240104", ["1301", "7203"], rows_per_ticker=10))
    write_zip(month / "HTICST120.20240104.2.zip", "HTICST120.20240104.2.csv",
              individual_stock_csv("20240104", ["7203", "9999"], rows_per_ticker=10))
    out = tmp_path / "toyota.csv"
    store = tmp_path / "store"
    monkeypatch.setattr("sys.argv", [
        "tse-tick", "export", "--data-type", "individual_stock",
        "--input-root", str(tmp_path), "--tickers", "7203",
        "--period", "20240104", "--output", str(out), "--store", str(store),
    ])
    main()
    assert out.exists()
    assert store.exists()                       # reusable store was built and left on disk
    df = pl.read_csv(out)
    assert set(df["Stock Code"].cast(str).str.slice(0, 4).to_list()) == {"7203"}
    assert df.height == 20                       # 7203 rows from BOTH parts captured


# ---------------------------------------------------------------------------
# CLI presentation: friendly errors/notes instead of raw tracebacks / warning
# chrome for novice mistakes and expected no-data cases (QA papercuts #1, #2).
# ---------------------------------------------------------------------------

def test_export_time_filter_on_summary_prints_clean_error(tmp_path, monkeypatch, capsys):
    """A summary type rejects start/end-time with a friendly one-line CLI error,
    not a Python traceback exposing internal module paths (QA papercut #1)."""
    inroot = tmp_path / "NEEDS"
    inroot.mkdir()
    monkeypatch.setattr("sys.argv", [
        "tse-tick", "export", "--data-type", "indices_summary",
        "--tickers", "101", "--period", "202305",
        "--input-root", str(inroot), "--start-time", "09:00:00",
        "--output", str(tmp_path / "o.csv"),
    ])
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 1                       # failure still signalled
    err = capsys.readouterr().err
    assert "Error:" in err
    assert "start_time/end_time are not supported" in err   # the friendly message
    assert "Traceback" not in err                    # no internal traceback
    assert "constants.py" not in err                 # no internal file paths leak


def test_export_missing_input_root_prints_clean_error(tmp_path, monkeypatch, capsys):
    """A nonexistent --input-root surfaces as a one-line error, not a traceback
    (same FileNotFoundError family as papercut #1)."""
    monkeypatch.setattr("sys.argv", [
        "tse-tick", "export", "--data-type", "individual_stock",
        "--tickers", "7203", "--period", "20230504",
        "--input-root", str(tmp_path / "does_not_exist"),
        "--output", str(tmp_path / "o.csv"),
    ])
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "Error:" in err
    assert "does not exist" in err
    assert "Traceback" not in err


def test_ingest_bad_period_prints_clean_error(tmp_path, monkeypatch, capsys):
    """A malformed --period surfaces as a clean CLI error for `ingest` too — the
    ValueError from parse_period must not reach the user as a traceback."""
    monkeypatch.setattr("sys.argv", [
        "tse-tick", "ingest", "--data-type", "individual_stock",
        "--period", "notaperiod",
        "--input-root", str(tmp_path), "--output-root", str(tmp_path / "out"),
    ])
    with pytest.raises(SystemExit) as exc:
        main()
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "Error:" in err
    assert "Invalid period" in err
    assert "Traceback" not in err


def test_export_no_data_prints_clean_note(tmp_path, monkeypatch, capsys):
    """A no-data (holiday) read prints a clean note on stdout, not Python's raw
    NoDataWarning chrome with an internal file:line and source-line echo
    (QA papercut #2). The empty output file is still written (exit 0)."""
    month = tmp_path / "NEEDS" / "個別株式2023" / "TICST120" / "202305"
    month.mkdir(parents=True)                        # valid root, no ZIPs -> no data
    out = tmp_path / "goldenweek.csv"
    monkeypatch.setattr("sys.argv", [
        "tse-tick", "export", "--data-type", "individual_stock",
        "--tickers", "7203", "--period", "20230504-20230505",
        "--input-root", str(tmp_path / "NEEDS"), "--output", str(out),
    ])
    main()                                           # no crash, exit 0
    result = capsys.readouterr()
    assert "Warning:" in result.out                  # clean note, on stdout
    assert "no ZIP files found" in result.out        # the friendly message survives
    assert "Wrote 0 rows" in result.out              # empty file still written
    assert out.exists()
    # None of Python's default warning chrome leaks through:
    assert "NoDataWarning" not in result.out and "NoDataWarning" not in result.err
    assert "cli.py:" not in result.out and "cli.py:" not in result.err


def test_cli_export_two_stage_store_multi_ticker(tmp_path, monkeypatch):
    """`export --store` with several --tickers goes through extract_to_store
    (multi-ticker since 0.12.0) — it used to silently fall back to a one-shot
    read (capped at 10M rows) without building the store."""
    import polars as pl
    pytest.importorskip("duckdb")

    month = tmp_path / "個別株式2024" / "TICST120" / "202401"
    month.mkdir(parents=True)
    write_zip(month / "HTICST120.20240104.1.zip", "HTICST120.20240104.1.csv",
              individual_stock_csv("20240104", ["1301", "7203"], rows_per_ticker=10))
    write_zip(month / "HTICST120.20240104.2.zip", "HTICST120.20240104.2.csv",
              individual_stock_csv("20240104", ["9984", "9999"], rows_per_ticker=10))
    out = tmp_path / "pair.csv"
    store = tmp_path / "store"
    monkeypatch.setattr("sys.argv", [
        "tse-tick", "export", "--data-type", "individual_stock",
        "--input-root", str(tmp_path), "--tickers", "7203,9984",
        "--period", "20240104", "--output", str(out), "--store", str(store),
    ])
    main()
    date_dir = store / "individual_stock" / "date=20240104"
    assert (date_dir / "ticker=7203.parquet").exists()
    assert (date_dir / "ticker=9984.parquet").exists()
    df = pl.read_csv(out)
    assert set(df["Stock Code"].cast(str).str.slice(0, 4).to_list()) == {"7203", "9984"}
    assert df.height == 20  # 10 rows per ticker, one part each
