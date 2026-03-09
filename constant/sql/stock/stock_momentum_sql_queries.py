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

    # Fetch data up to a target date and return only the latest row with all momentum lags.
    # Why LIMIT 253 in `base`:
    # - The furthest lag is 252 sessions (`close_252`), so one additional current row
    #   is required to compute all lag fields for the newest observation.
    # Date boundary logic:
    # - `time < (%s::date + INTERVAL '1 day')` includes all records on `%s::date`
    #   when `time` is a timestamp.
    # Query flow:
    # - CTE `base` captures recent history for one symbol.
    # - Window `w` computes 21/63/126/252-session lag closes.
    # - Final `ORDER BY time DESC LIMIT 1` returns a point-in-time snapshot.
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

    # Delete momentum rows for one symbol from the given start date/time onward.
    # Typical use case:
    # - Recompute recent momentum values after OHLCV corrections or formula updates.
    # Safety note:
    # - Combined symbol + time filter prevents cross-symbol data loss.
    DELETE_MOMENTUM_DATA = (
        "DELETE FROM stock_momentum "
        "WHERE symbol = %s "
        "AND time >= %s "
    )

    # Bulk-load calculated momentum metrics via PostgreSQL COPY.
    # Expected input:
    # - CSV stream with fields matching the listed order.
    # Column mapping:
    # - `m_1`, `m_3`, `m_6`, `m_12` represent 1/3/6/12-month style momentum horizons
    #   (implemented by the pipeline's trading-session lag conventions).
    # Performance:
    # - COPY is significantly faster than issuing many individual INSERT statements.
    COPY_MOMENTUM_DATA = (
                                """
                                COPY stock_momentum
                                (symbol, time, m_1 , m_3, m_6, m_12)
                                FROM STDIN WITH (FORMAT CSV)
                                """
    )





