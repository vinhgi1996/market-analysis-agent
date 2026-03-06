"""
constant/sql_queries.py

This module centralizes all Volatility pipeline SQL queries used in the project.
- Each query is defined as an Enum member.
- The Enum approach provides:
    1. Strong names for queries to prevent typos
    2. IDE auto-completion
    3. Easy import and maintenance
- Queries can be retrieved using the `.value` property when executing via run_sql or other DB functions.
"""

from enum import Enum

class VolatilitySQLQueries(str, Enum):
    """
    Enum for all Volatility pipeline SQL queries in the project.

    Each member's value is the actual SQL string. Use `.value` when passing to
    database execution functions.
    """

    # -----------------------------
    # Query to get close data of a specific symbol of time ( the unit is trading session )
    # -----------------------------
    GET_DATA_BY_SYMBOL_OHLVC = (
        "SELECT symbol, time, open, high, low, close FROM ohlcv_prices "
        "WHERE symbol = %s "
        "ORDER BY time ASC "
    )

    GET_DATA_BY_DATE_SYMBOL_OHLVC = (
        "SELECT * "
        "FROM ( "
            "SELECT symbol, time, high, low, close FROM ohlcv_prices "
            "WHERE symbol = %s "
            "AND time < (%s::date + INTERVAL '1 day') "
            "ORDER BY time DESC "
            "LIMIT 272 "
        ") sub "
        "ORDER BY time ASC "
    )

    GET_DATA_BY_DATE_SYMBOL_ATR_14_W = (
        "SELECT symbol, time, atr_14_w "
        "FROM volatility "
        "WHERE symbol = %s "
        "AND time < (%s::date + INTERVAL '1 day') "
        "ORDER BY time DESC "
        "LIMIT 1 "
    )

    DELETE_VOLATILITY_DATA = (
        "DELETE FROM stock_volatility "
        "WHERE symbol = %s "
        "AND time >= %s "
    )

    COPY_VOLATILITY_DATA = (
                                """
                                COPY stock_volatility
                                (symbol, time, vol_20d, vol_60d, vol_126d, vol_252d, atr_14_r, atr_14_w, parkinson_20d, vol_20d_pct_126, vol_20d_pct_252)
                                FROM STDIN WITH (FORMAT CSV)
                                """
    )






