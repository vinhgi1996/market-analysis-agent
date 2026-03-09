"""
constant/sql_queries.py

This module centralizes all Vnindex Volatility pipeline SQL queries used in the project.
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
    Enum for all Vnindex Volatility pipeline SQL queries in the project.

    Each member's value is the actual SQL string. Use `.value` when passing to
    database execution functions.
    """

    # -----------------------------
    # Query to get close data of a specific symbol of time ( the unit is trading session )
    # -----------------------------
    GET_DATA = (
        "SELECT time, open, high, low, close FROM vnindex_history "
        "ORDER BY time ASC "
    )

    # Return the latest 21 records up to (and including) the provided date.
    # Why 21:
    # - This pipeline computes 20-day volatility and typically needs a lookback window
    #   that includes prior sessions around the target date.
    # Date boundary logic:
    # - `time < (%s::date + INTERVAL '1 day')` makes the filter inclusive for `%s::date`
    #   when `time` is a timestamp (all rows before next day 00:00:00 are included).
    # Ordering strategy:
    # - Inner query sorts DESC + LIMIT 21 to efficiently pick most recent rows.
    # - Outer query re-sorts ASC so downstream calculations run in chronological order.
    GET_DATA_BY_DATE= (
        "SELECT * "
        "FROM ( "
            "SELECT time, high, low, close FROM vnindex_history "
            "WHERE time < (%s::date + INTERVAL '1 day') "
            "ORDER BY time DESC "
            "LIMIT 21"
        ") sub "
        "ORDER BY time ASC "
    )

    # Delete volatility rows from a starting timestamp/date onward.
    # Typical use case:
    # - Rebuild/recompute recent data after logic changes or backfilling corrections.
    # Safety note:
    # - Scope is intentionally bounded by `time >= %s` to avoid full-table deletion.
    DELETE_VOLATILITY_DATA = (
        "DELETE FROM vnindex_volatility "
        "WHERE time >= %s "
    )

    # Bulk-load computed volatility into the destination table using PostgreSQL COPY.
    # Expected input:
    # - CSV stream provided through STDIN by the caller.
    # Column mapping:
    # - `time`    -> trading session date/time key
    # - `vol_20d` -> calculated rolling 20-day volatility
    # Performance:
    # - COPY is significantly faster than row-by-row INSERT for large batches.
    COPY_VOLATILITY_DATA = (
                                """
                                COPY vnindex_volatility
                                (time, vol_20d)
                                FROM STDIN WITH (FORMAT CSV)
                                """
    )






