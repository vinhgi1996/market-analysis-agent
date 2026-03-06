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

    DELETE_MOMENTUM_DATA = (
        "DELETE FROM vnindex_momentum "
        "WHERE time >= %s "
    )

    COPY_MOMENTUM_DATA = (
                                """
                                COPY vnindex_momentum
                                (time, m_60d)
                                FROM STDIN WITH (FORMAT CSV)
                                """
    )





