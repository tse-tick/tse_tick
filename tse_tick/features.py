# tse_tick/features.py
import datetime

import polars as pl


def _exec_time_index(df: pl.DataFrame) -> pl.Series:
    et_col = df["Execution Time"]
    if len(et_col) == 0:
        return pl.Series([], dtype=pl.Datetime)

    sample = et_col.drop_nulls()
    if len(sample) == 0:
        return pl.Series([None] * len(df), dtype=pl.Datetime)

    first = sample[0]

    if isinstance(first, datetime.time):
        time_strs = et_col.cast(pl.String)
        return time_strs.str.to_datetime("%H:%M:%S", strict=False)
    elif isinstance(first, str) and len(first) <= 6:
        time_strs = et_col.cast(pl.String).str.zfill(6)
        return time_strs.str.to_datetime("%H%M%S", strict=False)
    else:
        return et_col.cast(pl.Datetime)


def compute_spread(df: pl.DataFrame) -> pl.Series:
    sell = df["Sell Quote 1 Best"].cast(pl.Float64)
    buy = df["Buy Quote 1 Best"].cast(pl.Float64)
    spread_series = sell - buy

    result = []
    for sell_val, buy_val, spread_val in zip(
        sell.to_list(), buy.to_list(), spread_series.to_list()
    ):
        if sell_val is not None and buy_val is not None and sell_val != 0 and buy_val != 0:
            result.append(spread_val)
        else:
            result.append(None)

    return pl.Series("spread", result, dtype=pl.Float64)


def compute_depth(
    df: pl.DataFrame,
    levels: int = 10,
    side: str = "both",
) -> pl.DataFrame:
    if not (1 <= levels <= 10):
        raise ValueError(f"levels must be 1-10, got {levels}")
    if side not in ("sell", "buy", "both"):
        raise ValueError(f"side must be 'sell', 'buy', or 'both', got {side!r}")

    cols = {}

    if side in ("sell", "both"):
        for i in range(1, levels + 1):
            col_name = f"Sell Quote Vol {i}"
            if col_name in df.columns:
                cols[f"sell_depth_{i}"] = df[col_name].cast(pl.Float64)

    if side in ("buy", "both"):
        for i in range(1, levels + 1):
            col_name = f"Buy Quote Vol {i}"
            if col_name in df.columns:
                cols[f"buy_depth_{i}"] = df[col_name].cast(pl.Float64)

    return pl.DataFrame(cols)


def compute_flow_imbalance(
    df: pl.DataFrame,
    window: str = "5min",
) -> pl.Series:
    sell_best = df["Sell Quote 1 Best"].cast(pl.Float64)
    buy_best = df["Buy Quote 1 Best"].cast(pl.Float64)
    mid = (sell_best + buy_best) * 0.5

    price = df["Execution Price"].cast(pl.Float64)
    volume = df["Execution Price"].cast(pl.Float64)

    time_idx = _exec_time_index(df)

    trade_df = pl.DataFrame({
        "time": time_idx,
        "price": price,
        "volume": volume,
        "mid": mid,
    })

    trade_df = trade_df.with_columns(
        pl.when(pl.col("price") >= pl.col("mid"))
        .then(pl.col("volume"))
        .otherwise(0.0)
        .alias("buy_vol"),
        pl.when(pl.col("price") < pl.col("mid"))
        .then(pl.col("volume"))
        .otherwise(0.0)
        .alias("sell_vol"),
    )

    trade_df = trade_df.sort("time")

    # Time-based rolling in polars
    rolling = trade_df.rolling(index_column="time", period=f"{window}")
    buy_rolling = rolling.agg(pl.col("buy_vol").sum().alias("buy_roll"))
    sell_rolling = rolling.agg(pl.col("sell_vol").sum().alias("sell_roll"))

    ofi_col = []
    buy_list = buy_rolling["buy_roll"].to_list()
    sell_list = sell_rolling["sell_roll"].to_list()

    for b, s in zip(buy_list, sell_list):
        denom = b + s
        if denom is not None and denom != 0:
            ofi_col.append((b - s) / denom)
        else:
            ofi_col.append(None)

    return pl.Series("flow_imbalance", ofi_col, dtype=pl.Float64)


def compute_volatility(
    df: pl.DataFrame,
    window: str = "5min",
    method: str = "realized",
) -> pl.Series:
    if method not in ("realized", "garman_klass"):
        raise ValueError(f"method must be 'realized' or 'garman_klass', got {method!r}")

    price = df["Execution Price"].cast(pl.Float64)
    time_idx = _exec_time_index(df)

    trade_df = pl.DataFrame({
        "time": time_idx,
        "price": price,
    }).sort("time")

    if method == "realized":
        trade_df = trade_df.with_columns(
            (pl.col("price") / pl.col("price").shift(1)).log().alias("log_ret")
        )
        rolling = trade_df.rolling(index_column="time", period=f"{window}")
        vol = rolling.agg(
            (pl.col("log_ret").pow(2).sum().sqrt()).alias("volatility")
        )
        return vol["volatility"]

    rolling = trade_df.rolling(index_column="time", period=f"{window}")
    hi = rolling.agg(pl.col("price").max().alias("high"))
    lo = rolling.agg(pl.col("price").min().alias("low"))
    op = rolling.agg(pl.col("price").first().alias("open"))
    cl = rolling.agg(pl.col("price").last().alias("close"))

    hi_vals = hi["high"].to_list()
    lo_vals = lo["low"].to_list()
    op_vals = op["open"].to_list()
    cl_vals = cl["close"].to_list()

    import math

    ln2 = math.log(2)
    gk_vals: list[float | None] = []
    for h, l, o, c in zip(hi_vals, lo_vals, op_vals, cl_vals):
        if None in (h, l, o, c) or l == 0 or o == 0:
            gk_vals.append(None)
        else:
            hl_term = math.log(h / l) ** 2
            oc_term = math.log(c / o) ** 2
            gk = math.sqrt(max(0.0, 0.5 * hl_term - (2 * ln2 - 1) * oc_term))
            gk_vals.append(gk)

    return pl.Series("volatility", gk_vals, dtype=pl.Float64)


def compute_all_features(
    df: pl.DataFrame,
    levels: int = 10,
    volatility_window: str = "5min",
    imbalance_window: str = "5min",
) -> pl.DataFrame:
    result = df.clone()
    result = result.with_columns(compute_spread(df).alias("spread"))

    depth = compute_depth(df, levels=levels, side="both")
    for col in depth.columns:
        result = result.with_columns(depth[col].alias(col))

    result = result.with_columns(
        compute_flow_imbalance(df, window=imbalance_window).alias("flow_imbalance")
    )
    result = result.with_columns(
        compute_volatility(df, window=volatility_window, method="realized").alias("volatility")
    )

    return result
