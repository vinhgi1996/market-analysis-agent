"""
constant/sql_queries.py

This module centralizes all SQL queries used in the project.
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
    Enum for all SQL queries in the project.

    Each member's value is the actual SQL string. Use `.value` when passing to
    database execution functions.
    """

    # -----------------------------
    # Query to get data of a specific symbol by a specific date range
    # -----------------------------
    GET_DATA_BY_DATE_AND_SYMBOL = (
        "SELECT * "
        "FROM ohlcv "
        "WHERE symbol = %s "
        "AND time <= %s "
        "ORDER BY time DESC"
        "LIMIT 252 "
    )

    # -----------------------------
    # Query to get data of all symbol by a specific date range
    # -----------------------------
    GET_DATA_BY_DATE = (
        "SELECT * "
        "FROM ohlcv_prices "
        "WHERE time <= %s "
        "ORDER BY time DESC "
        "LIMIT 252 "
    )




