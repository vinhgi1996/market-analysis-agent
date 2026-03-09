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

    # Return a bounded history window for one symbol up to a target date.
    # Why 272 rows:
    # - Volatility features include long windows up to 252 sessions.
    # - Extra buffer rows support warm-up/alignment and any intermediate transforms.
    # Date boundary logic:
    # - `time < (%s::date + INTERVAL '1 day')` includes all rows on `%s::date`
    #   when `time` is timestamp-based.
    # Ordering strategy:
    # - Inner query fetches newest rows efficiently with DESC + LIMIT.
    # - Outer query reorders ASC for chronological indicator computation.
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

    # Fetch the latest available ATR(14, weekly) value for one symbol
    # on or before the provided date.
    # Result behavior:
    # - Ordered DESC and limited to 1 row, so caller gets the most recent snapshot.
    # - Useful when joining precomputed ATR values into current volatility output.
    GET_DATA_BY_DATE_SYMBOL_ATR_14_W = (
        "SELECT symbol, time, atr_14_w "
        "FROM volatility "
        "WHERE symbol = %s "
        "AND time < (%s::date + INTERVAL '1 day') "
        "ORDER BY time DESC "
        "LIMIT 1 "
    )

    # Delete stock-volatility rows for a symbol from a start date/time onward.
    # Typical use case:
    # - Recompute recent ranges for a single ticker after price corrections.
    # Safety note:
    # - Filtered by both symbol and time to avoid cross-symbol data removal.
    DELETE_VOLATILITY_DATA = (
        "DELETE FROM stock_volatility "
        "WHERE symbol = %s "
        "AND time >= %s "
    )

    # Bulk-load computed stock volatility metrics via PostgreSQL COPY.
    # Expected input:
    # - CSV stream from ETL process, column order must match exactly.
    # Main fields:
    # - Core volatility windows: `vol_20d`, `vol_60d`, `vol_126d`, `vol_252d`
    # - Range-based metrics: `atr_14_r`, `atr_14_w`, `parkinson_20d`
    # - Percentile context: `vol_20d_pct_126`, `vol_20d_pct_252`
    # Performance:
    # - COPY is preferred for high-volume ingestion over iterative INSERTs.
    COPY_VOLATILITY_DATA = (
                                """
                                COPY stock_volatility
                                (symbol, time, vol_20d, vol_60d, vol_126d, vol_252d, atr_14_r, atr_14_w, parkinson_20d, vol_20d_pct_126, vol_20d_pct_252)
                                FROM STDIN WITH (FORMAT CSV)
                                """
    )






