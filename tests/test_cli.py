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
