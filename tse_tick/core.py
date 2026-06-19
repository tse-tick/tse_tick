import polars as pl

from .constants import SUMMARY_TYPES, TICK_TYPES


def _tick_datetime_expr(
    date_col: str = "Data Date",
    time_col: str = "Execution Time",
) -> pl.Expr:
    """Polars expression for a tick's full timestamp (naive, no timezone).

    Combines ``date_col`` (a ``Data Date`` Datetime/Date) with ``time_col``
    (``Execution Time``, stored as ``"HHMMSS"`` or already ``"HH:MM:SS"``) into a
    ``%Y-%m-%d %H:%M:%S`` datetime. This centralises the ``HHMMSS``/colon handling
    that was duplicated across ``query.py``, ``features._exec_time_index`` and the
    two ``event_window`` paths. Parsing is non-strict, so malformed values become
    null rather than raising.
    """
    # Normalise to bare digits first ("HH:MM:SS" -> "HHMMSS"), then re-insert the
    # colons. 2016 index ticks store "HHMM" (no seconds), so default missing
    # seconds to "00" rather than producing an unparseable "HH:MM:" (which would
    # silently drop every 2016 row from a time-window filter).
    digits = pl.col(time_col).cast(pl.String).str.replace_all(":", "")
    seconds = digits.str.slice(4, 2)
    time_str = (
        digits.str.slice(0, 2) + ":"
        + digits.str.slice(2, 2) + ":"
        + pl.when(seconds.str.len_chars() == 2).then(seconds).otherwise(pl.lit("00"))
    )
    date_part = pl.col(date_col).cast(pl.Date).cast(pl.String)
    return (date_part + pl.lit(" ") + time_str).str.to_datetime(
        "%Y-%m-%d %H:%M:%S", strict=False
    )


def _tick_datetime(
    df: pl.DataFrame,
    date_col: str = "Data Date",
    time_col: str = "Execution Time",
) -> pl.Series:
    """Eager naive-``Datetime`` Series of tick timestamps for ``df``.

    Thin wrapper over :func:`_tick_datetime_expr` for call sites that already
    hold a DataFrame (e.g. event-window filtering).
    """
    return df.select(
        _tick_datetime_expr(date_col, time_col).alias("_tick_dt")
    ).to_series()


def parse_line(line: bytes, kind="indices_summary"):
    text = line.decode().rstrip("\r\n")

    record_type = text[0:4]
    data_date = text[4:12]
    id_flag = text[12:13]
    exchange_code = text[13:15]
    security_type = text[15:17]

    if kind == "indices_summary":
        stock_code = text[17:29]
        rec = {
            "0": record_type,
            "1": data_date,
            "2": id_flag,
            "3": exchange_code,
            "4": security_type,
            "5": stock_code,
        }

        rest = line[32:].decode()
        fields = rest.split("+")
        count = 5
        for i, v in enumerate(fields, start=1):
            if (i == 16 - 5) or (i == 20 - 5) or (i == 39 - 5) or (i == 43 - 5):
                rec[f"{i+count}"] = v[0:8]
                count += 1
                rec[f"{i+count}"] = v[8:len(v)]
            elif i == len(fields):
                rec[f"{i+count}"] = v[0:5]
            else:
                rec[f"{i+count}"] = v
    else:
        session = text[17:18]
        index_code = text[18:30]
        time = text[30:34]
        record_type2 = text[34:36]
        management_number = text[36:40]
        price = text[40:49]
        execution_type = text[49:52]
        ayumi_flag = text[52:55]
        volume = text[55:66]
        volume_flag = text[66:69]

        rec = {
            "0": record_type,
            "1": data_date,
            "2": id_flag,
            "3": exchange_code,
            "4": security_type,
            "5": session,
            "6": index_code,
            "7": time,
            "8": record_type2,
            "9": management_number,
            "10": price,
            "11": execution_type,
            "12": ayumi_flag,
            "13": volume,
            "14": volume_flag,
        }

    return rec


def _exec_time_6char(col: str = "Execution Time") -> pl.Expr:
    """Normalize an Execution Time string to a fixed-width 6-char ``HHMMSS``.

    2016 index ticks store ``HHMM`` (4 chars, no seconds) while 2017+ store
    ``HHMMSS`` (6); pad-then-slice yields a single era-independent width so the
    raw column parses/compares consistently across years (and between tick
    types). Defensive against an already colon-formatted value too.
    """
    return (
        pl.col(col).cast(pl.String).str.replace_all(":", "") + pl.lit("000000")
    ).str.slice(0, 6)


def clean_data(df, kind="individual_stock", language="en"):
    if language == "en":
        language = "name"

    df_cleaned = df.clone()

    if kind == "individual_stock":
        # 15 (Volume Flag) intentionally omitted: kept as String so the
        # categorical decode below maps "0"/"128" → "Final"/"Estimated".
        int_list = [14, 18, 19, 21, 22, 24, 25, 27, 28, 30, 31, 33, 34, 36, 37, 39, 40, 42, 43, 45, 46, 48, 49, 51, 52, 54, 55, 57, 58, 60, 61, 63, 64, 66, 67, 69, 70, 72, 73, 75, 76, 78, 79, 81, 82, 84, 85, 87, 88, 90, 91, 93, 94]
        float_list = [11, 17, 20, 23, 26, 29, 32, 35, 38, 41, 44, 47, 50, 53, 56, 59, 62, 65, 68, 71, 74, 77, 80, 83, 86, 89, 92]
        time_list = [6, 7, 8]
        df_cleaned = df_cleaned.with_columns(
            pl.col("Buy Quote 1 Best").cast(pl.Float64),
            pl.col("Buy Quote Vol 1").cast(pl.Float64),
        )
    elif kind in SUMMARY_TYPES:
        int_list = []
        float_list = []
        time_list = [17, 22, 42, 47]
        if kind == "indices_summary":
            price_columns = [col for col in df.columns if "Price" in col]
            price_exprs = [(pl.col(c).cast(pl.Float64) * 0.01).alias(c) for c in price_columns]
            if price_exprs:
                df_cleaned = df_cleaned.with_columns(price_exprs)
        else:  # stock_summary
            # Every stock_summary measure (OHLC, VWAP, volumes, amounts, counts)
            # is numeric; without this they came back as String and aggregations
            # (.mean(), arithmetic, …) silently returned null. Only the id/code
            # columns and the HHMMSS time columns stay String (times are sliced
            # below; codes/Record Type/Exchange/Security are handled separately).
            _summary_id_cols = {
                "Record Type", "Data Date", "Identification Flag",
                "Exchange Code", "Security Type", "Stock Code",
            }
            measure_cols = [
                c for c in df_cleaned.columns
                if c not in _summary_id_cols and "Time" not in c
            ]
            df_cleaned = df_cleaned.with_columns(
                [pl.col(c).str.strip_chars().cast(pl.Float64, strict=False) for c in measure_cols]
            )
    elif (kind == "indices") and df_cleaned.height and (df_cleaned["Data Date"][0][:4] == "2016"):
        time_list = []
        int_list = []
        float_list = []
        df_cleaned = df_cleaned.with_columns(
            pl.col("Volume").cast(pl.Int64),
            (pl.col("Index Value").cast(pl.Float64) * 0.01).alias("Index Value"),
        )
    elif kind == "indices":
        int_list = []
        float_list = []
        time_list = [6, 9]
        df_cleaned = df_cleaned.with_columns(
            (pl.col("Index Value").cast(pl.Float64) * 0.01).alias("Index Value"),
        )
    else:
        pass

    for col_idx in time_list:
        col = df_cleaned.columns[col_idx]
        df_cleaned = df_cleaned.with_columns(pl.col(col).fill_null(pl.lit(None)))

    for i in int_list:
        col = df_cleaned.columns[i]
        df_cleaned = df_cleaned.with_columns(
            pl.col(col).fill_null(0).cast(pl.Int64)
        )

    for i in float_list:
        col = df_cleaned.columns[i]
        df_cleaned = df_cleaned.with_columns(
            pl.col(col).cast(pl.Float64, strict=False).fill_null(0.0)
        )

    df_cleaned = df_cleaned.with_columns(
        pl.col("Data Date").str.to_datetime("%Y%m%d", strict=False)
    )

    if kind == "individual_stock":
        for time_col in ["Execution Time", "Sell Quote Time", "Buy Quote Time"]:
            df_cleaned = df_cleaned.with_columns(
                pl.col(time_col).str.slice(0, 6).alias(time_col)
            )
        df_cleaned = df_cleaned.with_columns(
            pl.col("Update Time").str.slice(0, 12).alias("Update Time")
        )

    elif (kind == "indices") and df_cleaned.height and (df_cleaned["Data Date"][0].year == 2016):
        df_cleaned = df_cleaned.with_columns(
            _exec_time_6char().alias("Execution Time")  # 2016 HHMM -> HHMMSS
        )

    elif kind in SUMMARY_TYPES:
        # Normalize every time-of-day column to a fixed-width 6-char HHMMSS across
        # eras: 2016 (…010) emits 4-char HHMM, 2017+ (…110) emits 12-char
        # HHMMSSffffff. (Was: slice to 12, leaving 2016 at 4 chars and 2017+ at 12
        # — inconsistent widths across eras for the same column.)
        time_cols = [c for c in df_cleaned.columns if "Time" in c]
        if time_cols:
            df_cleaned = df_cleaned.with_columns(
                [_exec_time_6char(c).alias(c) for c in time_cols]
            )

    elif kind == "indices":
        exprs = [_exec_time_6char().alias("Execution Time")]  # era-independent HHMMSS
        if "Update Time" in df_cleaned.columns:  # 2016 (15-field) indices have none
            exprs.append(pl.col("Update Time").str.slice(0, 12).alias("Update Time"))
        df_cleaned = df_cleaned.with_columns(exprs)

    string_cols = [c for c, d in zip(df_cleaned.columns, df_cleaned.dtypes) if d == pl.String]
    for col in string_cols:
        df_cleaned = df_cleaned.with_columns(pl.col(col).str.strip_chars())

    schemas_categorical = get_schemas_categorical()

    if kind in TICK_TYPES:
        col_names = df_cleaned.columns
    elif kind in SUMMARY_TYPES:
        col_names = df_cleaned.columns[:5]
    else:
        col_names = []

    skip_exact = {
        "Data Date", "Management Number", "Identification Flag", "Index Value",
        "Record Type (Executions/Quotes)",
        # Keep Index Code as the raw numeric code (e.g. "101") for both index
        # types: it then equals the ticker_filter input and the partition
        # filename (ticker=101), is language-independent, lets the two index
        # types be joined, and avoids an "Unknown (NNN)" string for codes not in
        # the name table (e.g. 108).
        "Index Code",
    }

    for col in col_names:
        if col in skip_exact:
            continue
        dtype = df_cleaned.schema[col]
        if dtype == pl.Float64:
            continue
        if "Time" in col:
            continue
        if "Vol" in col and col != "Volume Flag":
            continue
        if "Reserved" in col:
            continue
        if ("Buy" in col) or ("Sell" in col):
            continue

        unique_vars = df_cleaned[col].unique().to_list()

        if col == "Record Type" or col == "Exchange Code":
            mapping_dict = {}
            for var in unique_vars:
                if var is None:
                    continue
                mapping = schemas_categorical[col]["all"].get(var)
                if mapping is None:
                    df_cleaned = df_cleaned.with_columns(
                        pl.when(pl.col(col) == var)
                        .then(pl.lit(f"Unknown ({var})"))
                        .otherwise(pl.col(col))
                        .alias(col)
                    )
                else:
                    mapping_dict[var] = mapping[language]
            if mapping_dict:
                df_cleaned = df_cleaned.with_columns(
                    pl.col(col).replace(mapping_dict).alias(col)
                )
        elif col == "Stock Code":
            for var in unique_vars:
                if var is None or len(var) == 4:
                    continue
                suffix = var[-1]
                suffix_mapping = schemas_categorical["Stock Code Suffix"].get(suffix)
                if suffix_mapping is None:
                    continue
                var_schema = var + suffix_mapping[language]
                df_cleaned = df_cleaned.with_columns(
                    pl.when(pl.col(col) == var)
                    .then(pl.lit(var_schema))
                    .otherwise(pl.col(col))
                    .alias(col)
                )
        elif col == "Execution Type":
            if kind == "individual_stock":
                schema_key = "Execution Type Stocks"
            elif kind == "indices":
                schema_key = "Execution Type Indices"
            else:
                continue
            mapping_dict = {}
            for var in unique_vars:
                if var is None:
                    continue
                mapping = schemas_categorical[schema_key].get(str(var))
                if mapping is None:
                    df_cleaned = df_cleaned.with_columns(
                        pl.when(pl.col(col) == var)
                        .then(pl.lit(f"Unknown ({var})"))
                        .otherwise(pl.col(col))
                        .alias(col)
                    )
                else:
                    mapping_dict[var] = mapping[language]
            if mapping_dict:
                df_cleaned = df_cleaned.with_columns(
                    pl.col(col).replace(mapping_dict).alias(col)
                )
        elif col == "Ayumi Flag":
            if kind == "individual_stock":
                schema_key = "Ayumi Flag Stocks"
            elif kind == "indices":
                schema_key = "Ayumi Flag Indices"
            else:
                continue
            mapping_dict = {}
            for var in unique_vars:
                if var is None:
                    continue
                mapping = schemas_categorical[schema_key].get(str(var))
                if mapping is None:
                    df_cleaned = df_cleaned.with_columns(
                        pl.when(pl.col(col) == var)
                        .then(pl.lit(f"Unknown ({var})"))
                        .otherwise(pl.col(col))
                        .alias(col)
                    )
                else:
                    mapping_dict[var] = mapping[language]
            if mapping_dict:
                df_cleaned = df_cleaned.with_columns(
                    pl.col(col).replace(mapping_dict).alias(col)
                )
        elif col == "Volume Flag":
            mapping_dict = {}
            for var in unique_vars:
                if var is None:
                    continue
                mapping = schemas_categorical["Volume Flag"].get(str(var))
                if mapping is None:
                    df_cleaned = df_cleaned.with_columns(
                        pl.when(pl.col(col) == var)
                        .then(pl.lit(f"Unknown ({var})"))
                        .otherwise(pl.col(col))
                        .alias(col)
                    )
                else:
                    mapping_dict[var] = mapping[language]
            if mapping_dict:
                df_cleaned = df_cleaned.with_columns(
                    pl.col(col).replace(mapping_dict).alias(col)
                )
        elif "Flag" in col:
            mapping_dict = {}
            for var in unique_vars:
                if var is None:
                    continue
                mapping = schemas_categorical["Quote Flag"].get(str(var))
                if mapping is None:
                    df_cleaned = df_cleaned.with_columns(
                        pl.when(pl.col(col) == var)
                        .then(pl.lit(f"Unknown ({var})"))
                        .otherwise(pl.col(col))
                        .alias(col)
                    )
                else:
                    mapping_dict[var] = mapping[language]
            if mapping_dict:
                df_cleaned = df_cleaned.with_columns(
                    pl.col(col).replace(mapping_dict).alias(col)
                )
        else:
            if col not in schemas_categorical:
                continue
            mapping_dict = {}
            for var in unique_vars:
                if var is None:
                    continue
                mapping = schemas_categorical[col].get(str(var))
                if mapping is None:
                    df_cleaned = df_cleaned.with_columns(
                        pl.when(pl.col(col) == var)
                        .then(pl.lit(f"Unknown ({var})"))
                        .otherwise(pl.col(col))
                        .alias(col)
                    )
                else:
                    mapping_dict[var] = mapping[language]
            if mapping_dict:
                df_cleaned = df_cleaned.with_columns(
                    pl.col(col).replace(mapping_dict).alias(col)
                )

    return df_cleaned


def get_schemas_categorical():
    schemas_categorical = {
        "metadata": {
            "title": "Nikkei Tick Data Categorical Value Schemas",
            "version": "2017-2020",
            "created": "2025-11",
            "description": "Comprehensive categorical variable definitions",
        },
        "Record Type": {
            "field_name": "Record Type",
            "field_name_jp": "レコード種別",
            "data_type": "C4",
            "format": "xynn",
            "all": {
                "DB13": {"name": "Stocks", "jp": "株式"},
                "DB23": {"name": "Indices", "jp": "指数"},
                "DB33": {"name": "Futures", "jp": "先物"},
                "DB43": {"name": "Options", "jp": "オプション"},
                "DB53": {"name": "Convertible Bonds", "jp": "CB"},
                "1100": {"name": "Stocks - Best Quote", "jp": "株式（約定・最良気配）"},
                "1200": {"name": "Stocks - Multiple Quote", "jp": "株式（約定・最良気配・複数気配）"},
                "2100": {"name": "Indices - Execution", "jp": "株価指数（約定・最良気配）"},
            },
        },
        "Exchange Code": {
            "field_name": "Exchange Code",
            "field_name_jp": "取引所コード",
            "data_type": "C2",
            "all": {
                "11": {"name": "Tokyo Stock Exchange (TSE)", "jp": "東証"},
                "31": {"name": "Nagoya Stock Exchange (NSE)", "jp": "名証"},
                "61": {"name": "Fukuoka Stock Exchange (FSE)", "jp": "福証"},
                "81": {"name": "Sapporo Securities Exchange (SSE)", "jp": "札証"},
                "21": {"name": "Osaka Securities Exchange (OSE)", "jp": "大証", "until": "2013-07-12"},
                "91": {"name": "JASDAQ", "jp": "JASDAQ", "until": "2013-07-12"},
                "A1": {"name": "Hercules", "jp": "ヘラクレス", "until": "2010-10-12"},
            },
        },
        "Security Type": {
            "1": {"name": "First Section", "jp": "一部株式", "liquidity": "High"},
            "2": {"name": "Second Section", "jp": "二部株式", "liquidity": "Medium"},
            "3": {"name": "Foreign Stocks", "jp": "外国株式", "liquidity": "Varies"},
            "4": {"name": "TSE Mothers", "jp": "東証マザーズ", "liquidity": "Medium-Low"},
            "5": {"name": "TOKYO PRO Market (Domestic)", "jp": "TOKYO PRO Market内国株式", "from": "2012-07-02"},
            "6": {"name": "TOKYO PRO Market (Foreign)", "jp": "TOKYO PRO Market外国株式", "from": "2012-07-02"},
            "7": {"name": "TSE JASDAQ (Domestic)", "jp": "東証JASDAQ内国株式", "from": "2013-07-16"},
            "11": {"name": "TSE JASDAQ (Foreign)", "jp": "東証JASDAQ外国株式", "from": "2013-07-16"},
            "8": {"name": "Under Supervision", "jp": "監理", "trading": "Restricted"},
            "9": {"name": "Delisting", "jp": "整理", "trading": "Delisting"},
            "10": {"name": "Cash Index", "jp": "現物指数"},
            "20": {"name": "Index Futures", "jp": "指数先物"},
            "30": {"name": "Index Options", "jp": "指数オプション"},
            "40": {"name": "Convertible Bonds", "jp": "CB"},
        },
        "Session": {
            "1": {"name": "Morning / Day", "jp": "前場 / 日中", "time": "09:00-11:30"},
            "2": {"name": "Afternoon", "jp": "後場", "time": "12:30-15:00"},
        },
        "Stock Code Suffix": {
            " ": {"name": "Parent Stock", "jp": "親株式"},
            "1": {"name": "New Shares", "jp": "新株式"},
            "2": {"name": "Second New Shares", "jp": "第二新株式"},
            "3": {"name": "Third New Shares", "jp": "第三新株式"},
            "5": {"name": "Preferred Stock", "jp": "優先株式"},
            "6": {"name": "Preferred New Shares", "jp": "優先新株式"},
            "7": {"name": "Deferred Stock", "jp": "後配株式"},
            "8": {"name": "Deferred New Shares", "jp": "後配新株式"},
            "9": {"name": "Stock Subscription Warrants", "jp": "新株引受権証書"},
        },
        "Index Code": {
            "101": {"name": "Nikkei 225", "jp": "日経平均株価", "calc": "5-sec from 2017-07-18"},
            "102": {"name": "Nikkei 300", "jp": "日経株価指数300", "calc": "5-sec from 2017-07-18"},
            "105": {"name": "JPX-Nikkei 400", "jp": "JPX日経インデックス400"},
            "113": {"name": "TOPIX", "jp": "東証株価指数"},
            "121": {"name": "TOPIX Electric Appliances", "jp": "東証電気機器株価指数"},
            "122": {"name": "TOPIX Transportation Equipment", "jp": "東証輸送用機器株価指数"},
            "123": {"name": "TOPIX Banks", "jp": "東証銀行業株価指数"},
            "145": {"name": "Nikkei VI", "jp": "日経平均ボラティリティー・インデックス"},
            "154": {"name": "TSE Mothers Index", "jp": "東証マザーズ指数"},
            "155": {"name": "TSE REIT Index", "jp": "東証REIT指数"},
            "171": {"name": "TOPIX Core30", "jp": "TOPIX Core30指数"},
            "181": {"name": "S&P/TOPIX 150", "jp": "S&P/TOPIX150"},
            "211": {"name": "Dow Jones Industrial Average", "jp": "ダウジョーンズ工業株価平均"},
            "214": {"name": "FTSE Japan Index", "jp": "FTSE日本指数"},
        },
        "Execution Type Stocks": {
            "1": {"name": "Opening", "jp": "寄付", "stop_high": "101", "stop_low": "201"},
            "16": {"name": "At Buy Quote", "jp": "買い気配で約定", "stop_high": "116", "stop_low": "216"},
            "32": {"name": "Between Quotes", "jp": "気配間で約定", "stop_high": "132", "stop_low": "232"},
            "48": {"name": "At Sell Quote", "jp": "売り気配で約定", "stop_high": "148", "stop_low": "248"},
            "64": {"name": "Outside Quotes", "jp": "気配外で約定", "stop_high": "164", "stop_low": "264"},
            "0": {"name": "Other", "jp": "その他", "stop_high": "100", "stop_low": "200"},
        },
        "Execution Type Indices": {
            "1": {"name": "Opening", "jp": "寄付レコード"},
            "2": {"name": "Post-Closing", "jp": "終了後約定レコード"},
            "0": {"name": "Other", "jp": "その他"},
        },
        "Ayumi Flag Stocks": {
            "0": {"name": "Regular", "jp": "通常約定", "status": "Normal"},
            "4": {"name": "System Halt", "jp": "システム停止", "status": "Halted"},
            "8": {"name": "Temporary Suspension", "jp": "一時停止", "status": "Suspended"},
            "12": {"name": "Interruption", "jp": "中断", "status": "Interrupted"},
            "16": {"name": "Call Auction", "jp": "板寄せ", "status": "Auction"},
            "17": {"name": "Auction Released", "jp": "板寄せ解除", "status": "Resumed"},
            "18": {"name": "Circuit Breaker", "jp": "サーキットブレーカ", "status": "CB", "exchange": "OSE"},
            "19": {"name": "CB Released", "jp": "CB解除", "status": "Resumed", "exchange": "OSE"},
            "22": {"name": "Reference", "jp": "参考値", "status": "Reference"},
            "33": {"name": "Discontinuous", "jp": "不連続歩み", "status": "Normal"},
            "64": {"name": "Suspension Released", "jp": "停止解除", "status": "Resumed"},
            "128": {"name": "Closing (volume>0)", "jp": "終了約定（売買高>0）", "status": "Close"},
            "160": {"name": "Closing (volume=0)", "jp": "終了約定（売買高=0）", "status": "Close"},
        },
        "Ayumi Flag Indices": {
            "0": {"name": "Regular", "jp": "通常"},
            "128": {"name": "Closing", "jp": "終了"},
        },
        "Volume Flag": {
            "0": {"name": "Final", "jp": "精算", "status": "Confirmed"},
            "128": {"name": "Estimated", "jp": "概算", "status": "Preliminary"},
        },
        "Quote Flag": {
            "0": {"name": "No Quote", "jp": "気配なし", "tradable": False},
            "1": {"name": "Special Quote Cancelled", "jp": "特別気配取消", "tradable": False},
            "8": {"name": "Pre-Suspension Special", "jp": "停止前特別気配", "tradable": False},
            "16": {"name": "Quote Omitted", "jp": "気配省略", "tradable": False},
            "32": {"name": "Special Quote", "jp": "特別気配", "tradable": "Limited"},
            "33": {"name": "Special Quote Opposite", "jp": "特別気配相方", "tradable": "Limited"},
            "64": {"name": "Market Order", "jp": "成行気配", "tradable": True},
            "66": {"name": "Continuous Execution", "jp": "連続約定気配", "tradable": "Limited"},
            "67": {"name": "Continuous Exec Opposite", "jp": "連続約定相方", "tradable": "Limited"},
            "68": {"name": "Pre-Suspension Continuous", "jp": "停止前連続約定", "tradable": False},
            "111": {"name": "Pre-Opening Expected", "jp": "寄前・予定値段", "tradable": False, "exchange": "OSE"},
            "112": {"name": "Pre-Opening", "jp": "寄り前気配", "tradable": False},
            "127": {"name": "Market at Same Price", "jp": "同値成行", "tradable": True},
            "128": {"name": "Regular Quote", "jp": "一般気配", "tradable": True},
            "129": {"name": "Attention Quote", "jp": "注意気配", "tradable": True, "exchange": "OSE"},
            "130": {"name": "Final Quote", "jp": "最終気配", "tradable": False},
            "131": {"name": "Regular (Improving)", "jp": "一般気配（値上中）", "tradable": True},
        },
    }
    return schemas_categorical
