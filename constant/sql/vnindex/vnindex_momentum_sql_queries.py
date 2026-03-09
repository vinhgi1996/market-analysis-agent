"""
constant/sql_queries.py

This module centralizes all Momentum for vnindex pipeline SQL queries used in the project.
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
    Enum for all Momentum for vnindex pipeline SQL queries in the project.

    Each member's value is the actual SQL string. Use `.value` when passing to
    database execution functions.
    """

    # -----------------------------
    # Query to get close data of a specific symbol of time T , T-63 , T-126, T-252 ( the unit is trading session )
    # -----------------------------
    GET_DATA = (
        "SELECT "
        "time, "
        "close, "
        "LAG(close, 60) OVER (ORDER BY time) AS close_60 "
        "FROM vnindex_history "
        "ORDER BY time "
    )

    # Fetch data up to a target date and return only the latest computable point.
    # Query flow:
    # - `base` CTE pulls the most recent 61 rows up to (and including) `%s::date`.
    # - 61 rows are needed so `LAG(close, 60)` can be resolved for the newest row.
    # Date boundary logic:
    # - `time < (%s::date + INTERVAL '1 day')` includes all rows on `%s::date`
    #   when `time` is a timestamp.
    # Result shape:
    # - Final SELECT computes `close_60` over `base`, then returns only 1 row
    #   (`ORDER BY time DESC LIMIT 1`) for point-in-time momentum calculation.
    GET_DATA_BY_DATE = (
        "WITH base AS ( "
            "SELECT * "
            "FROM vnindex_history "
            "WHERE time < (%s::date + INTERVAL '1 day') "
            "ORDER BY time DESC "
            "LIMIT 61 "
        ") "
        "SELECT "
            "time, "
            "close, "
            "LAG(close, 60) OVER (ORDER BY time) AS close_60 "
        "FROM base "
        "ORDER BY time DESC "
        "LIMIT 1"
    )

    # Delete momentum rows from a chosen start timestamp/date onward.
    # Typical use case:
    # - Recalculate recent momentum after data corrections or logic updates.
    # Safety note:
    # - Restricting by `time >= %s` prevents accidental full-table deletion.
    DELETE_MOMENTUM_DATA = (
        "DELETE FROM vnindex_momentum "
        "WHERE time >= %s "
    )

    # Bulk-load momentum output into PostgreSQL via COPY FROM STDIN.
    # Expected input:
    # - CSV stream provided by the ETL caller.
    # Column mapping:
    # - `time` -> trading session key
    # - `m_60d` -> 60-session momentum metric
    # Performance:
    # - COPY is faster and more scalable than many single-row INSERT statements.
    COPY_MOMENTUM_DATA = (
                                """
                                COPY vnindex_momentum
                                (time, m_60d)
                                FROM STDIN WITH (FORMAT CSV)
                                """
    )





