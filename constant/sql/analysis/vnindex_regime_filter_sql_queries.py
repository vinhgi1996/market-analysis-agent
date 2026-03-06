"""
constant/sql_queries.py

This module centralizes all Vnindex Regime Filter pipeline SQL queries used in the project.
- Each query is defined as an Enum member.
- The Enum approach provides:
    1. Strong names for queries to prevent typos
    2. IDE auto-completion
    3. Easy import and maintenance
- Queries can be retrieved using the `.value` property when executing via run_sql or other DB functions.
"""

from enum import Enum

class VnIndexRegimeFilterSQLQueries(str, Enum):
    """
    Enum for all Vnindex Regime Filter pipeline SQL queries in the project.

    Each member's value is the actual SQL string. Use `.value` when passing to
    database execution functions.
    """

    # -----------------------------
    # Query to get close data of a specific symbol of time ( the unit is trading session )
    # -----------------------------
    GET_DATA = (
        "SELECT vm.time AS time, "
            "vh.close AS close, "
            "vm.m_60d AS m_60d, "
            "vma.sma_150 AS sma_150d, "
            "vd.drawdown_40d AS drawdown_40d, "
            "vv.vol_20d AS vol_20d "
        "FROM vnindex_momentum vm "
        "JOIN vnindex_history vh USING(time) "
        "JOIN vnindex_moving_average vma USING(time) "
        "JOIN vnindex_drawdown vd USING(time) "
        "JOIN vnindex_volatility vv USING(time) "
        "ORDER BY time ASC "
    )

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
    DELETE_REGIME_DATA = (
        "DELETE FROM vnindex_regime_filter "
        "WHERE time >= %s "
    )

    COPY_REGIME_DATA = (
                                """
                                COPY vnindex_regime_filter
                                (time, m_60d_threshold, 
                                drawdown_40d_threshold, 
                                vol_20d_threshold, 
                                suggestion) 
                                FROM STDIN WITH (FORMAT CSV)
                                """
    )






