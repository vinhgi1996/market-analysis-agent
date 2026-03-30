"""
constant/sql_queries.py

This module centralizes all general SQL queries used in the project.
- Each query is defined as an Enum member.
- The Enum approach provides:
    1. Strong names for queries to prevent typos
    2. IDE auto-completion
    3. Easy import and maintenance
- Queries can be retrieved using the `.value` property when executing via run_sql or other DB functions.
"""

from enum import Enum

class SQLQueries(str, Enum):
    """
    Enum for all general SQL queries in the project.

    Each member's value is the actual SQL string. Use `.value` when passing to
    database execution functions.
    """

    # -----------------------------
    # Query to get user access authority
    # -----------------------------
    GET_HOSE_ENERGY_COMPANY_SYMBOL = (
                            "select sbi.symbol as symbol from symbol_by_industry sbi "
                            "INNER JOIN symbol_by_exchange sbe "
                            "ON sbi.symbol = sbe.symbol "
                            "where sbi.icb_name4 in ('Thiết bị và Dịch vụ Dầu khí', 'Sản xuất & Phân phối Điện', 'Phân phối xăng dầu & khí đốt', 'Sản xuất và Khai thác dầu khí') "
                            "and sbe.exchange = 'HSX' and sbi.symbol != 'TTE'"
    )

    GET_TOP_OIL_SECTOR_COMPANY_SYMBOL = (
                                        "SELECT symbol " 
                                        "FROM symbol_by_industry "
                                        "WHERE symbol IN ('PVS','OIL','PLX','PVD','PVC','POW','BSR') "
    )

    GET_HOSE_TOP_COMPANY_LIQUIDITY_SYMBOL = (
        "SELECT DISTINCT(symbol) "
        "FROM ohlcv_prices "
        "WHERE symbol IN ('EVF', 'HPG', 'PVT', 'SSI', 'VIX', 'VCG', 'DXG', 'BSR', 'SHB', 'MSN', 'POW', 'KHG', 'CII', "
        "'DPM', 'TPB', 'DIG', 'MBB', 'TCB', 'PDR', 'PLX', 'GEL', 'MWG', 'NVL', 'FPT', 'VPB', 'TCH', "
        "'VND', 'EIB', 'PC1', 'HDB', 'VCI', 'GEX', 'DGC', 'ACB', 'VIB', 'GVR', 'HQC', 'CTG', 'DCM', "
        "'VNM', 'VRE', 'HCM', 'VPI', 'VSC', 'LCG', 'HHV', 'VCB', 'KDH', 'PVD', 'GAS', 'VOS', 'STB', "
        "'NLG', 'DGW', 'GMD', 'HAG', 'NKG', 'PVP', 'VHM', 'BID', 'ORS', 'HDC', 'HSL', 'DBC', 'VIC', "
        "'KBC', 'HDG', 'SHI', 'SCR', 'HSG', 'HAH', 'PET', 'CRC', 'MSB', 'ANV', 'VJC', 'LPB', 'LDG', "
        "'HVN', 'VCK', 'FCN', 'BAF', 'IJC','TCX', 'HHS', 'TCM', 'VPX', 'SSB', 'GEG', 'NAB', 'BVH',"
        "'PAC', 'KSB', 'OCB', 'DXS')"
    )





