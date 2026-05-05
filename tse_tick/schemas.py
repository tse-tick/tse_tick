# tse_tick/schemas.py

def get_schema_individual_stock_95() -> list:
    """Schema for TICST120 (95 fields) - extended quote version"""
    base = [
        "Record Type", "Data Date", "Exchange Code", "Security Type", "Session",
        "Stock Code", "Execution Time", "Sell Quote Time", "Buy Quote Time",
        "Update Time", "Management Number", "Execution Price", "Execution Type",
        "Ayumi Flag", "Volume", "Volume Flag", "Close Quote Flag",
        "Sell Quote 1 Best", "Sell Quote Vol 1", "Sell Quote Flag 1",
        "Buy Quote 1 Best", "Buy Quote Vol 1", "Buy Quote Flag 1"
    ]
    
    extended = [
        "Sell Limit Quote", "Sell Limit Vol", "Sell Limit Flag",
        "Sell Market Quote", "Sell Market Vol", "Sell Market Flag"
    ]
    
    # Sell quotes 2-10
    for i in range(2, 11):
        extended.extend([
            f"Sell Quote {i}",
            f"Sell Quote Vol {i}",
            f"Sell Quote Flag {i}"
        ])
    
    extended.extend([
        "Sell Quote OVER", "Sell Quote Vol OVER", "Sell Quote Flag OVER"
    ])
    
    # Buy side
    extended.extend([
        "Buy Limit Quote", "Buy Limit Vol", "Buy Limit Flag",
        "Buy Market Quote", "Buy Market Vol", "Buy Market Flag"
    ])
    
    for i in range(2, 11):
        extended.extend([
            f"Buy Quote {i}",
            f"Buy Quote Vol {i}",
            f"Buy Quote Flag {i}"
        ])
    
    extended.extend([
        "Buy Quote UNDER", "Buy Quote Vol UNDER", "Buy Quote Flag UNDER"
    ])
    
    return base + extended

def get_schema_summary_83() -> list:
    """Schema for TICSS110 (83 fields)"""
    return [
        "Record Type", "Data Date", "Identification Flag", "Exchange Code", "Security Type", 
        "Stock Code","Trading Unit", "Issued Shares",
        "Executions ≤3 units", "Executions 3<x≤6 units", "Executions 6<x≤9 units",
        "Executions 9<x≤29 units", "Executions 29<x≤49 units", "Executions 49<x≤99 units",
        "Executions 99<x≤199 units", "Executions 199<x≤299 units",
        "AM Opening Price", "AM Opening Time", "AM Opening Volume",
        "AM High Price", "AM Low Price", "AM Close Price", "AM Close Time", "AM Close Volume",
        "AM UpTick Volume", "AM UpTick Amount", "AM UpTick Count",
        "AM DownTick Volume", "AM DownTick Amount", "AM DownTick Count",
        "AM Total Volume", "AM Total Amount", "AM Execution Count",
        "AM VWAP", "AM Std Dev",
        "AM Sell Quote Time", "AM Buy Quote Time", "AM Spread Time",
        "AM Avg Sell Quote Vol", "AM Avg Buy Quote Vol", "AM Avg Spread",
        "PM Opening Price", "PM Opening Time", "PM Opening Volume",
        "PM High Price", "PM Low Price", "PM Close Price", "PM Close Time", "PM Close Volume",
        "PM UpTick Volume", "PM UpTick Amount", "PM UpTick Count",
        "PM DownTick Volume", "PM DownTick Amount", "PM DownTick Count",
        "PM Total Volume", "PM Total Amount", "PM Execution Count",
        "PM VWAP", "PM Std Dev",
        "PM Sell Quote Time", "PM Buy Quote Time", "PM Spread Time",
        "PM Avg Sell Quote Vol", "PM Avg Buy Quote Vol", "PM Avg Spread",
        "Daily VWAP", "Daily Std Dev",
        "Daily Weighted Avg Sell Quote", "Daily Weighted Avg Buy Quote", "Daily Avg Spread",
        "AM Sell Quote Execution Vol", "AM Sell Quote Execution Amt", "AM Sell Quote Execution Cnt",
        "AM Buy Quote Execution Vol", "AM Buy Quote Execution Amt", "AM Buy Quote Execution Cnt",
        "PM Sell Quote Execution Vol", "PM Sell Quote Execution Amt", "PM Sell Quote Execution Cnt",
        "PM Buy Quote Execution Vol", "PM Buy Quote Execution Amt", "PM Buy Quote Execution Cnt"
    ]

def get_schema_indices_15() -> list:
    """Schema for TICIT010 (15 fields)"""
    return [
    "Record Type", "Data Date", "Identification Flag", "Exchange Code",
    "Security Type", "Session", "Index Code", "Execution Time",
    "Record Type (Executions/Quotes)", "Management Number", "Index Value",
    "Execution Type", "Ayumi Flag", "Volume", "Volume Flag",
]

def get_schema_indices_23() -> list:
    """Schema for TICIT110 (23 fields)"""
    return [
    "Record Type", "Data Date", "Exchange Code", "Security Type", "Session",
    "Index Code", "Execution Time", "Reserved 1", "Reserved 2", "Update Time",
    "Management Number", "Index Value", "Execution Type", "Ayumi Flag",
    "Reserved 3", "Reserved 4", "Reserved 5", "Reserved 6", "Reserved 7",
    "Reserved 8", "Reserved 9", "Reserved 10", "Reserved 11",
]


def get_schema_indices_summary() -> list:
    """Schema for TICIS110 (17 fields)"""
    return [
        "Record Type", "Data Date", "Exchange Code", "Security Type", "Stock Code",
        "AM Opening Price", "AM Opening Time", "AM High Price", "AM Low Price",
        "AM Close Price", "AM Close Time",
        "PM Opening Price", "PM Opening Time", "PM High Price", "PM Low Price",
        "PM Close Price", "PM Close Time"
    ]

def get_japanese_column_mapping() -> dict:
    """
    Returns complete mapping of English column names to Japanese.
    Covers all schemas: individual stock (23/95 fields), stock summary (82 fields),
    indices (10 fields), and indices summary (17 fields).
    """
    return {
        # ========== Common fields ==========
        "Record Type": "レコード種別",
        "Data Date": "データ日付",
        "Exchange Code": "取引所コード",
        "Security Type": "証券種別",
        "Session": "場区分",
        "Stock Code": "銘柄コード",
        "Index Code": "指数コード",
        "Identification Flag": "識別フラグ",
        "Record Type 2": "レコード種別２",

        # ========== Time fields ==========
        "Execution Time": "約定時刻",
        "Sell Quote Time": "売り気配時刻",
        "Buy Quote Time": "買い気配時刻",
        "Update Time": "銘柄更新時刻",
        "AM Opening Time": "前場始値時刻",
        "AM Close Time": "前場終値時刻",
        "PM Opening Time": "後場始値時刻",
        "PM Close Time": "後場終値時刻",
        "AM Sell Quote Time": "前場売り気配時刻",
        "AM Buy Quote Time": "前場買い気配時刻",
        "AM Spread Time": "前場スプレッド時刻",
        "PM Sell Quote Time": "後場売り気配時刻",
        "PM Buy Quote Time": "後場買い気配時刻",
        "PM Spread Time": "後場スプレッド時刻",

        # ========== Price/Value fields ==========
        "Execution Price": "約定価格",
        "Index Value": "指数値",
        "AM Opening Price": "前場始値",
        "AM High Price": "前場高値",
        "AM Low Price": "前場安値",
        "AM Close Price": "前場終値",
        "PM Opening Price": "後場始値",
        "PM High Price": "後場高値",
        "PM Low Price": "後場安値",
        "PM Close Price": "後場終値",

        # ========== Volume fields ==========
        "Volume": "売買高",
        "Trading Unit": "単位株数",
        "Issued Shares": "発行済株式数",
        "AM Opening Volume": "前場始値約定株数",
        "AM Close Volume": "前場終値約定株数",
        "AM Total Volume": "前場約定株数",
        "PM Opening Volume": "後場始値約定株数",
        "PM Close Volume": "後場終値約定株数",
        "PM Total Volume": "後場約定株数",

        # ========== Flag fields ==========
        "Execution Type": "約定種別",
        "Ayumi Flag": "歩み値フラグ",
        "Volume Flag": "売買高フラグ",
        "Close Quote Flag": "終了時気配フラグ",

        # ========== Management ==========
        "Management Number": "管理番号",

        # ========== Basic Quotes (Best 1) ==========
        "Sell Quote 1 Best": "売り気配１",
        "Sell Quote Vol 1": "売り気配数量１",
        "Sell Quote Flag 1": "売り気配フラグ１",
        "Buy Quote 1 Best": "買い気配１",
        "Buy Quote Vol 1": "買い気配数量１",
        "Buy Quote Flag 1": "買い気配フラグ１",

        # ========== Extended Quotes (95-field schema) ==========
        # Sell Limit/Market
        "Sell Limit Quote": "売り成行気配",
        "Sell Limit Vol": "売り成行数量",
        "Sell Limit Flag": "売り成行フラグ",
        "Sell Market Quote": "売り特別気配",
        "Sell Market Vol": "売り特別数量",
        "Sell Market Flag": "売り特別フラグ",

        # Sell Quotes 2-10
        "Sell Quote 2": "売り気配２",
        "Sell Quote Vol 2": "売り気配数量２",
        "Sell Quote Flag 2": "売り気配フラグ２",
        "Sell Quote 3": "売り気配３",
        "Sell Quote Vol 3": "売り気配数量３",
        "Sell Quote Flag 3": "売り気配フラグ３",
        "Sell Quote 4": "売り気配４",
        "Sell Quote Vol 4": "売り気配数量４",
        "Sell Quote Flag 4": "売り気配フラグ４",
        "Sell Quote 5": "売り気配５",
        "Sell Quote Vol 5": "売り気配数量５",
        "Sell Quote Flag 5": "売り気配フラグ５",
        "Sell Quote 6": "売り気配６",
        "Sell Quote Vol 6": "売り気配数量６",
        "Sell Quote Flag 6": "売り気配フラグ６",
        "Sell Quote 7": "売り気配７",
        "Sell Quote Vol 7": "売り気配数量７",
        "Sell Quote Flag 7": "売り気配フラグ７",
        "Sell Quote 8": "売り気配８",
        "Sell Quote Vol 8": "売り気配数量８",
        "Sell Quote Flag 8": "売り気配フラグ８",
        "Sell Quote 9": "売り気配９",
        "Sell Quote Vol 9": "売り気配数量９",
        "Sell Quote Flag 9": "売り気配フラグ９",
        "Sell Quote 10": "売り気配１０",
        "Sell Quote Vol 10": "売り気配数量１０",
        "Sell Quote Flag 10": "売り気配フラグ１０",

        # Sell OVER
        "Sell Quote OVER": "売り気配OVER",
        "Sell Quote Vol OVER": "売り気配数量OVER",
        "Sell Quote Flag OVER": "売り気配フラグOVER",

        # Buy Limit/Market
        "Buy Limit Quote": "買い成行気配",
        "Buy Limit Vol": "買い成行数量",
        "Buy Limit Flag": "買い成行フラグ",
        "Buy Market Quote": "買い特別気配",
        "Buy Market Vol": "買い特別数量",
        "Buy Market Flag": "買い特別フラグ",

        # Buy Quotes 2-10
        "Buy Quote 2": "買い気配２",
        "Buy Quote Vol 2": "買い気配数量２",
        "Buy Quote Flag 2": "買い気配フラグ２",
        "Buy Quote 3": "買い気配３",
        "Buy Quote Vol 3": "買い気配数量３",
        "Buy Quote Flag 3": "買い気配フラグ３",
        "Buy Quote 4": "買い気配４",
        "Buy Quote Vol 4": "買い気配数量４",
        "Buy Quote Flag 4": "買い気配フラグ４",
        "Buy Quote 5": "買い気配５",
        "Buy Quote Vol 5": "買い気配数量５",
        "Buy Quote Flag 5": "買い気配フラグ５",
        "Buy Quote 6": "買い気配６",
        "Buy Quote Vol 6": "買い気配数量６",
        "Buy Quote Flag 6": "買い気配フラグ６",
        "Buy Quote 7": "買い気配７",
        "Buy Quote Vol 7": "買い気配数量７",
        "Buy Quote Flag 7": "買い気配フラグ７",
        "Buy Quote 8": "買い気配８",
        "Buy Quote Vol 8": "買い気配数量８",
        "Buy Quote Flag 8": "買い気配フラグ８",
        "Buy Quote 9": "買い気配９",
        "Buy Quote Vol 9": "買い気配数量９",
        "Buy Quote Flag 9": "買い気配フラグ９",
        "Buy Quote 10": "買い気配１０",
        "Buy Quote Vol 10": "買い気配数量１０",
        "Buy Quote Flag 10": "買い気配フラグ１０",

        # Buy UNDER
        "Buy Quote UNDER": "買い気配UNDER",
        "Buy Quote Vol UNDER": "買い気配数量UNDER",
        "Buy Quote Flag UNDER": "買い気配フラグUNDER",

        # ========== Stock Summary Statistics ==========
        # Execution size buckets
        "Executions ≤3 units": "約定≤3単位",
        "Executions 3<x≤6 units": "約定3<x≤6単位",
        "Executions 6<x≤9 units": "約定6<x≤9単位",
        "Executions 9<x≤29 units": "約定9<x≤29単位",
        "Executions 29<x≤49 units": "約定29<x≤49単位",
        "Executions 49<x≤99 units": "約定49<x≤99単位",
        "Executions 99<x≤199 units": "約定99<x≤199単位",
        "Executions 199<x≤299 units": "約定199<x≤299単位",

        # AM Session
        "AM UpTick Volume": "前場値上がり株数",
        "AM UpTick Amount": "前場値上がり金額",
        "AM UpTick Count": "前場値上がり回数",
        "AM DownTick Volume": "前場値下がり株数",
        "AM DownTick Amount": "前場値下がり金額",
        "AM DownTick Count": "前場値下がり回数",
        "AM Total Amount": "前場約定金額",
        "AM Execution Count": "前場約定回数",
        "AM VWAP": "前場VWAP",
        "AM Std Dev": "前場標準偏差",
        "AM Avg Sell Quote Vol": "前場平均売り気配数量",
        "AM Avg Buy Quote Vol": "前場平均買い気配数量",
        "AM Avg Spread": "前場平均スプレッド",
        "AM Sell Quote Execution Vol": "前場売り気配約定株数",
        "AM Sell Quote Execution Amt": "前場売り気配約定金額",
        "AM Sell Quote Execution Cnt": "前場売り気配約定回数",
        "AM Buy Quote Execution Vol": "前場買い気配約定株数",
        "AM Buy Quote Execution Amt": "前場買い気配約定金額",
        "AM Buy Quote Execution Cnt": "前場買い気配約定回数",

        # PM Session
        "PM UpTick Volume": "後場値上がり株数",
        "PM UpTick Amount": "後場値上がり金額",
        "PM UpTick Count": "後場値上がり回数",
        "PM DownTick Volume": "後場値下がり株数",
        "PM DownTick Amount": "後場値下がり金額",
        "PM DownTick Count": "後場値下がり回数",
        "PM Total Amount": "後場約定金額",
        "PM Execution Count": "後場約定回数",
        "PM VWAP": "後場VWAP",
        "PM Std Dev": "後場標準偏差",
        "PM Avg Sell Quote Vol": "後場平均売り気配数量",
        "PM Avg Buy Quote Vol": "後場平均買い気配数量",
        "PM Avg Spread": "後場平均スプレッド",
        "PM Sell Quote Execution Vol": "後場売り気配約定株数",
        "PM Sell Quote Execution Amt": "後場売り気配約定金額",
        "PM Sell Quote Execution Cnt": "後場売り気配約定回数",
        "PM Buy Quote Execution Vol": "後場買い気配約定株数",
        "PM Buy Quote Execution Amt": "後場買い気配約定金額",
        "PM Buy Quote Execution Cnt": "後場買い気配約定回数",

        # Daily Statistics
        "Daily VWAP": "全日VWAP",
        "Daily Std Dev": "全日標準偏差",
        "Daily Weighted Avg Sell Quote": "全日加重平均売り気配",
        "Daily Weighted Avg Buy Quote": "全日加重平均買い気配",
        "Daily Avg Spread": "全日平均スプレッド",

        # Others
        "Reserved 1": "予備 1",
        "Reserved 2": "予備 2",
        "Reserved 3": "予備 3",
        "Reserved 4": "予備 4",
        "Reserved 5": "予備 5",
        "Reserved 6": "予備 6",
        "Reserved 7": "予備 7",
        "Reserved 8": "予備 8",
        "Reserved 9": "予備 9",
        "Reserved 10": "予備 10",
        "Reserved 11": "予備 11"

    }
