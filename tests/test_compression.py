"""Parquet codec plumbing: zstd default, snappy selectable, mixed stores read fine."""
import pyarrow.parquet as pq
import pytest

import tse_tick
from tests.synthetic_data import seed_structured_day

pytest.importorskip("duckdb")

_DAY1, _DAY2 = "20240104", "20240105"


def _codec_of(store_dir, day):
    f = next((store_dir / "individual_stock" / f"date={day}").glob("ticker=*.parquet"))
    return pq.ParquetFile(f).metadata.row_group(0).column(0).compression


def test_default_codec_is_zstd(tmp_path):
    seed_structured_day(tmp_path / "src", _DAY1, {1: ["7203"]})
    tse_tick.ingest_period(
        str(tmp_path / "src"), str(tmp_path / "store"), _DAY1, "individual_stock"
    )
    assert _codec_of(tmp_path / "store", _DAY1) == "ZSTD"


def test_snappy_still_selectable(tmp_path):
    seed_structured_day(tmp_path / "src", _DAY1, {1: ["7203"]})
    tse_tick.ingest_period(
        str(tmp_path / "src"), str(tmp_path / "store"), _DAY1, "individual_stock",
        compression="snappy",
    )
    assert _codec_of(tmp_path / "store", _DAY1) == "SNAPPY"


def test_mixed_codec_store_queries_fine(tmp_path):
    # A pre-0.14 (snappy) store extended with zstd days must read as one store:
    # the codec is per-file Parquet metadata, so resume/extend needs no re-ingest.
    seed_structured_day(tmp_path / "src", _DAY1, {1: ["7203"]})
    seed_structured_day(tmp_path / "src", _DAY2, {1: ["7203"]})
    src, store = str(tmp_path / "src"), str(tmp_path / "store")
    tse_tick.ingest_period(src, store, _DAY1, "individual_stock", compression="snappy")
    tse_tick.ingest_period(src, store, _DAY2, "individual_stock")  # default zstd
    assert _codec_of(tmp_path / "store", _DAY1) == "SNAPPY"
    assert _codec_of(tmp_path / "store", _DAY2) == "ZSTD"
    df = tse_tick.query_ticks(store, ticker="7203")
    assert set(df["date"].cast(str).unique().to_list()) == {_DAY1, _DAY2}


def test_extract_to_store_compression_passthrough(tmp_path):
    seed_structured_day(tmp_path / "src", _DAY1, {1: ["7203"]})
    df = tse_tick.extract_to_store(
        str(tmp_path / "src"), str(tmp_path / "store"), _DAY1, "7203",
        compression="snappy",
    )
    assert df.height > 0
    assert _codec_of(tmp_path / "store", _DAY1) == "SNAPPY"


def test_cli_compression_flag(tmp_path, monkeypatch, capsys):
    from tse_tick.cli import main

    seed_structured_day(tmp_path / "src", _DAY1, {1: ["7203"]})
    monkeypatch.setattr(
        "sys.argv",
        ["tse-tick", "ingest", "--data-type", "individual_stock",
         "--period", _DAY1, "--input-root", str(tmp_path / "src"),
         "--output-root", str(tmp_path / "store"), "--compression", "snappy"],
    )
    main()
    assert "1 succeeded" in capsys.readouterr().out
    assert _codec_of(tmp_path / "store", _DAY1) == "SNAPPY"
