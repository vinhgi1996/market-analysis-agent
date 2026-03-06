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
    DELETE_DRAWDOWN_DATA = (
        "DELETE FROM vnindex_drawdown "
        "WHERE time >= %s "
    )

    COPY_DRAWDOWN_DATA = (
                                """
                                COPY vnindex_drawdown
                                (time, drawdown_40d)
                                FROM STDIN WITH (FORMAT CSV)
                                """
    )






