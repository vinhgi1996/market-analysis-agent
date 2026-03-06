"""
constant/sql_queries.py

This module centralizes all Momentum for stock pipeline SQL queries used in the project.
- Each query is defined as an Enum member.
- The Enum approach provides:
    1. Strong names for queries to prevent typos
    2. IDE auto-completion
    3. Easy import and maintenance
- Queries can be retrieved using the `.value` property when executing via run_sql or other DB functions.
"""

from enum import Enum

class MomentumSQLQueries(str, Enum):
    """
    Enum for all Momentum for stock pipeline SQL queries in the project.

    Each member's value is the actual SQL string. Use `.value` when passing to
    database execution functions.
    """

    # -----------------------------
    # Query to get close data of a specific symbol of time T , T-63 , T-126, T-252 ( the unit is trading session )
    # -----------------------------
    GET_DATA_BY_SYMBOL = (
        "SELECT "
        "symbol, "
        "time, "
        "close, "
        "LAG(close, 21)  OVER w AS close_21, "
        "LAG(close, 63)  OVER w AS close_63, "
        "LAG(close, 126) OVER w AS close_126, "
        "LAG(close, 252) OVER w AS close_252 "
        "FROM ohlcv_prices "
        "WHERE symbol = %s "
        "WINDOW w AS (PARTITION BY symbol ORDER BY time)"
    )
    GET_DATA_BY_DATE_SYMBOL = (
        "WITH base AS ( "
            "SELECT * "
            "FROM ohlcv_prices "
            "WHERE symbol = %s "
            "AND time < (%s::date + INTERVAL '1 day') "
            "ORDER BY time DESC "
            "LIMIT 253 "
        ") "
        "SELECT "
            "symbol, "
            "time, "
            "close, "
            "LAG(close, 21)  OVER w AS close_21, "
            "LAG(close, 63)  OVER w AS close_63, "
            "LAG(close, 126) OVER w AS close_126, "
            "LAG(close, 252) OVER w AS close_252 "
        "FROM base "
        "WINDOW w AS (PARTITION BY symbol ORDER BY time) "
        "ORDER BY time DESC "
        "LIMIT 1"
    )

    DELETE_MOMENTUM_DATA = (
        "DELETE FROM stock_momentum "
        "WHERE symbol = %s "
        "AND time >= %s "
    )

    COPY_MOMENTUM_DATA = (
                                """
                                COPY stock_momentum
                                (symbol, time, m_1 , m_3, m_6, m_12)
                                FROM STDIN WITH (FORMAT CSV)
                                """
    )





