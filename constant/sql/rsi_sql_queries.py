"""
constant/sql_queries.py

This module centralizes all Moving Average pipeline SQL queries used in the project.
- Each query is defined as an Enum member.
- The Enum approach provides:
    1. Strong names for queries to prevent typos
    2. IDE auto-completion
    3. Easy import and maintenance
- Queries can be retrieved using the `.value` property when executing via run_sql or other DB functions.
"""

from enum import Enum

class RsiSQLQueries(str, Enum):
    """
    Enum for all Moving average pipeline SQL queries in the project.

    Each member's value is the actual SQL string. Use `.value` when passing to
    database execution functions.
    """

    # -----------------------------
    # Query to get close data of a specific symbol of time ( the unit is trading session )
    # -----------------------------
    GET_DATA_BY_SYMBOL_OHLVC = (
        "SELECT symbol, time, close FROM ohlcv_prices "
        "WHERE symbol = %s "
        "ORDER BY time ASC "
    )

    GET_DATA_BY_DATE_SYMBOL_OHLVC = (
        "SELECT * "
        "FROM ( "
            "SELECT symbol, time, close FROM ohlcv_prices "
            "WHERE symbol = %s "
            "AND time < (%s::date + INTERVAL '1 day') "
            "ORDER BY time DESC "
            "LIMIT 2 "
        ") sub "
        "ORDER BY time ASC "
    )

    GET_DATA_BY_DATE_SYMBOL_RSI = (
        "SELECT symbol, time, average_gain_14, average_loss_14 "
        "FROM relative_strength_index "
        "WHERE symbol = %s "
        "AND time < (%s::date + INTERVAL '1 day') "
        "ORDER BY time DESC "
        "LIMIT 1 "
    )

    DELETE_RSI_DATA = (
        "DELETE FROM relative_strength_index "
        "WHERE symbol = %s "
        "AND time >= %s "
    )

    COPY_RSI_DATA = (
                                """
                                COPY relative_strength_index
                                (symbol, time, rsi_14, average_gain_14,average_loss_14)
                                FROM STDIN WITH (FORMAT CSV)
                                """
    )






