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

    # Return the latest 2 close-price rows up to (and including) the target date.
    # Why 2 rows:
    # - RSI update logic often needs current close and previous close to compute
    #   one-step gain/loss before smoothing.
    # Date boundary logic:
    # - `time < (%s::date + INTERVAL '1 day')` includes all rows on `%s::date`
    #   when `time` is a timestamp.
    # Ordering strategy:
    # - Inner query gets newest records fast via DESC + LIMIT 2.
    # - Outer query reorders ASC so calculation can run in time order.
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

    # Fetch the latest stored RSI smoothing state for a symbol on/before the date.
    # Returned fields:
    # - `average_gain_14`, `average_loss_14` are the rolling components used by
    #   Wilder RSI smoothing to continue incremental computation.
    # Result behavior:
    # - DESC + LIMIT 1 returns the most recent prior state snapshot.
    GET_DATA_BY_DATE_SYMBOL_RSI = (
        "SELECT symbol, time, average_gain_14, average_loss_14 "
        "FROM relative_strength_index "
        "WHERE symbol = %s "
        "AND time < (%s::date + INTERVAL '1 day') "
        "ORDER BY time DESC "
        "LIMIT 1 "
    )

    # Delete RSI rows for one symbol from a specified start timestamp/date.
    # Typical use case:
    # - Recompute recent RSI after source price corrections or formula changes.
    # Safety note:
    # - Scoped by both `symbol` and `time` to avoid affecting other tickers.
    DELETE_RSI_DATA = (
        "DELETE FROM stock_relative_strength_index "
        "WHERE symbol = %s "
        "AND time >= %s "
    )

    # Bulk-load RSI output rows using PostgreSQL COPY FROM STDIN.
    # Expected input:
    # - CSV stream with exact column order shown below.
    # Column mapping:
    # - `rsi_14`         -> computed RSI value
    # - `average_gain_14`/`average_loss_14` -> persisted smoothing state
    # Performance:
    # - COPY is much faster than iterative INSERT for ETL batches.
    COPY_RSI_DATA = (
                                """
                                COPY stock_relative_strength_index
                                (symbol, time, rsi_14, average_gain_14,average_loss_14)
                                FROM STDIN WITH (FORMAT CSV)
                                """
    )






