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

class VolumeSQLQueries(str, Enum):
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
        "volume "
        "FROM ohlcv_prices "
        "WHERE symbol = %s "
        "ORDER BY time ASC "
    )


    GET_DATA_BY_DATE_SYMBOL = (
        "SELECT * FROM "
            "(SELECT "
                "symbol, "
                "time, "
                "volume "
            "FROM ohlcv_prices "
            "WHERE symbol = %s "
            "AND time < (%s::date + INTERVAL '1 day') "
            "ORDER BY time DESC "
            "LIMIT 60) sub "
        "ORDER BY time ASC "
    )

    DELETE_VOLUME_DATA = (
        "DELETE FROM stock_volume "
        "WHERE symbol = %s "
        "AND time >= %s "
    )


    COPY_VOLUME_DATA = (
                                """
                                COPY stock_volume
                                (symbol, time, vol_20d , vol_60d, volume_ratio, volume_spike)
                                FROM STDIN WITH (FORMAT CSV)
                                """
    )





