"""
constant/sql_queries.py

This module centralizes all Vnindex Drawdown pipeline SQL queries used in the project.
- Each query is defined as an Enum member.
- The Enum approach provides:
    1. Strong names for queries to prevent typos
    2. IDE auto-completion
    3. Easy import and maintenance
- Queries can be retrieved using the `.value` property when executing via run_sql or other DB functions.
"""

from enum import Enum

class DrawdownSQLQueries(str, Enum):
    """
    Enum for all Vnindex Drawdown pipeline SQL queries in the project.

    Each member's value is the actual SQL string. Use `.value` when passing to
    database execution functions.
    """

    # -----------------------------
    # Query to get close data of a specific symbol of time ( the unit is trading session )
    # -----------------------------
    GET_DATA = (
        "SELECT time, close FROM vnindex_history "
        "ORDER BY time ASC "
    )

    # Return the latest 40 rows up to (and including) the provided date.
    # Why 40:
    # - This drawdown pipeline computes a rolling 40-session drawdown metric.
    # - The calculation needs a bounded historical window ending at the target date.
    # Date boundary logic:
    # - `time < (%s::date + INTERVAL '1 day')` includes all rows on `%s::date`
    #   when `time` is stored as timestamp.
    # Ordering strategy:
    # - Inner query gets newest 40 rows efficiently (DESC + LIMIT 40).
    # - Outer query reorders ASC for chronological processing in Python/ETL steps.
    GET_DATA_BY_DATE= (
        "SELECT * "
        "FROM ( "
            "SELECT time, close FROM vnindex_history "
            "WHERE time < (%s::date + INTERVAL '1 day') "
            "ORDER BY time DESC "
            "LIMIT 40"
        ") sub "
        "ORDER BY time ASC "
    )

    # Delete drawdown records from a specified date/time forward.
    # Typical use case:
    # - Refresh recent drawdown values after source-data updates or logic changes.
    # Safety note:
    # - `WHERE time >= %s` limits deletion scope and protects older historical data.
    DELETE_DRAWDOWN_DATA = (
        "DELETE FROM vnindex_drawdown "
        "WHERE time >= %s "
    )

    # Bulk-insert computed drawdown values using PostgreSQL COPY FROM STDIN.
    # Expected input:
    # - CSV stream supplied by the calling ETL process.
    # Column mapping:
    # - `time`         -> trading session key
    # - `drawdown_40d` -> 40-session rolling drawdown metric
    # Performance:
    # - COPY is preferred for large batches versus many row-by-row INSERTs.
    COPY_DRAWDOWN_DATA = (
                                """
                                COPY vnindex_drawdown
                                (time, drawdown_40d)
                                FROM STDIN WITH (FORMAT CSV)
                                """
    )






