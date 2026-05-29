# tse_tick/io/parquet.py
import datetime
from pathlib import Path
from typing import Optional

import polars as pl

import pyarrow.dataset as ds

_DEFAULT_PARTITION_COLS: dict[str, list[str]] = {
    "individual_stock": ["Data Date", "Stock Code"],
    "stock_summary": ["Data Date", "Stock Code"],
    "indices": ["Data Date", "Index Code"],
    "indices_summary": ["Data Date", "Index Code"],
}

_VALID_DATA_TYPES = set(_DEFAULT_PARTITION_COLS.keys())


def _coerce_time_cols(df: pl.DataFrame) -> pl.DataFrame:
    result = df.clone()
    for col, dtype in zip(result.columns, result.dtypes):
        if dtype == pl.String:
            sample_vals = result[col].drop_nulls()
            if len(sample_vals) > 0:
                sample = sample_vals[0]
                if isinstance(sample, datetime.time):
                    result = result.with_columns(
                        pl.col(col).map_elements(
                            lambda t: t.strftime("%H%M%S") if isinstance(t, datetime.time) else t,
                            return_dtype=pl.String,
                        )
                    )
    return result


def write_partitioned_parquet(
    df: pl.DataFrame,
    output_dir: str,
    data_type: str,
    partition_cols: Optional[list[str]] = None,
) -> str:
    if data_type not in _VALID_DATA_TYPES:
        raise ValueError(
            f"Unknown data_type {data_type!r}. Must be one of {sorted(_VALID_DATA_TYPES)}"
        )

    pcols = partition_cols if partition_cols is not None else _DEFAULT_PARTITION_COLS[data_type]
    for col in pcols:
        if col not in df.columns:
            raise ValueError(f"Partition column {col!r} not in DataFrame")

    type_dir = Path(output_dir) / data_type
    type_dir.mkdir(parents=True, exist_ok=True)

    df = _coerce_time_cols(df)

    date_col = pcols[0]
    ticker_col = pcols[1] if len(pcols) > 1 else None

    if df.schema[date_col].is_temporal():
        df = df.with_columns(
            pl.col(date_col).dt.strftime("%Y%m%d").alias("_date_str")
        )
    else:
        df = df.with_columns(
            pl.col(date_col).cast(pl.String).str.replace_all("-", "", literal=True).alias("_date_str")
        )

    grouped = df.group_by("_date_str", maintain_order=True)
    for (date_str,), date_group in grouped:
        date_str_val = str(date_str)
        date_dir = type_dir / f"date={date_str_val}"
        date_dir.mkdir(parents=True, exist_ok=True)

        if ticker_col is not None:
            ticker_groups = date_group.group_by(ticker_col, maintain_order=True)
            for (ticker_val,), ticker_group in ticker_groups:
                out_df = ticker_group.drop(["_date_str"])
                ticker_int = str(ticker_val).strip()[:4]
                try:
                    ticker_int = int(ticker_int)
                except ValueError:
                    pass
                fpath = date_dir / f"ticker={ticker_int}.parquet"
                out_df.write_parquet(fpath, compression="snappy")
        else:
            out_df = date_group.drop(["_date_str"])
            fpath = date_dir / f"{date_str_val}.parquet"
            out_df.write_parquet(fpath, compression="snappy")

    return str(type_dir.resolve())


def read_parquet_partition(
    data_dir: str,
    data_type: str,
    date: Optional[str] = None,
    ticker: Optional[int] = None,
    columns: Optional[list[str]] = None,
) -> pl.DataFrame:
    type_dir = Path(data_dir) / data_type
    if not type_dir.exists():
        raise FileNotFoundError(f"Parquet store not found: {type_dir}")

    dataset = ds.dataset(str(type_dir), format="parquet", partitioning="hive")

    # The Hive "date" partition is inferred as an integer; cast it to string so
    # the comparison against the "YYYYMMDD" argument has a matching kernel. The
    # ticker is encoded in the filename (ticker=NNNN.parquet), not a directory,
    # so it is not a partition column — filter the in-file code column instead.
    code_col = "Index Code" if data_type in ("indices", "indices_summary") else "Stock Code"

    expr = None
    if date is not None:
        expr = ds.field("date").cast("string") == date
    if ticker is not None:
        ticker_expr = ds.field(code_col).cast("string") == str(ticker)
        expr = ticker_expr if expr is None else (expr & ticker_expr)

    table = dataset.to_table(filter=expr, columns=columns)
    df = pl.from_arrow(table)

    return df


def write_event_window_parquet(df: pl.DataFrame, output_dir: str) -> None:
    out_root = Path(output_dir)
    df = _coerce_time_cols(df)

    if "Data Date" not in df.columns:
        raise ValueError("DataFrame must contain 'Data Date' column")

    if df.schema["Data Date"].is_temporal():
        date_strs = df["Data Date"].dt.strftime("%Y%m%d")
    else:
        date_strs = (
            df["Data Date"].cast(pl.String).str.replace_all("-", "", literal=True)
        )

    df = df.with_columns(date_strs.alias("_date_str"))

    grouped = df.group_by("_date_str", maintain_order=True)
    for (date_str,), group in grouped:
        date_str_val = str(date_str)
        year = date_str_val[:4]
        month = date_str_val[4:6]

        part_dir = out_root / f"year={year}" / f"month={month}"
        part_dir.mkdir(parents=True, exist_ok=True)
        fpath = part_dir / f"{date_str_val}.parquet"

        out_df = group.drop(["_date_str"])

        if fpath.exists():
            existing = pl.read_parquet(fpath)
            out_df = pl.concat([existing, out_df], how="vertical")

        out_df.write_parquet(fpath, compression="snappy")


def read_partitioned_parquet(
    data_dir: str,
    year: Optional[int] = None,
    month: Optional[int] = None,
) -> pl.DataFrame:
    root = Path(data_dir)
    if not root.exists():
        raise FileNotFoundError(f"Event window Parquet store not found: {root}")

    dataset = ds.dataset(str(root), format="parquet", partitioning="hive")

    expr = None
    if year is not None:
        year_expr = ds.field("year") == year
        expr = year_expr if expr is None else (expr & year_expr)
    if month is not None:
        month_expr = ds.field("month") == month
        expr = month_expr if expr is None else (expr & month_expr)

    table = dataset.to_table(filter=expr)
    return pl.from_arrow(table)
