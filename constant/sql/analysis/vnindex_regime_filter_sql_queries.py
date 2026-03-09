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

    # Return the latest fully-joined feature row up to the provided date.
    # Join behavior:
    # - Uses INNER JOIN (`JOIN ... USING(time)`) across all feature tables.
    # - A row is returned only when the same `time` exists in every source table,
    #   ensuring feature completeness for regime decision logic.
    # Date boundary logic:
    # - `time < (%s::date + INTERVAL '1 day')` includes all rows on `%s::date`
    #   when `time` is stored as timestamp.
    # Result shape:
    # - `ORDER BY time DESC LIMIT 1` returns one point-in-time snapshot.
    GET_DATA_BY_DATE = (
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
        "WHERE time < (%s::date + INTERVAL '1 day') "
        "ORDER BY time DESC "
        "LIMIT 1"
    )

    # Fetch the most recent `positive_streak` value on or before a target date.
    # Purpose:
    # - Supports stateful regime computation where current streak depends on
    #   the previously stored streak from the latest prior session.
    # Result behavior:
    # - DESC + LIMIT 1 returns a single latest state row.
    GET_PREVIOUS_POSITIVE_STREAK = (
        "SELECT positive_streak "
        "FROM vnindex_regime_filter "
        "WHERE time < (%s::date + INTERVAL '1 day') "
        "ORDER BY time DESC "
        "LIMIT 1 "
    )

    # Delete regime-filter rows from a chosen start date/time onward.
    # Typical use case:
    # - Recompute recent signals after upstream indicator recalculation.
    # Safety note:
    # - Bounded by `time >= %s` to preserve older historical snapshots.
    DELETE_REGIME_DATA = (
        "DELETE FROM vnindex_regime_filter "
        "WHERE time >= %s "
    )

    # Bulk-load computed regime-filter output via PostgreSQL COPY FROM STDIN.
    # Expected input:
    # - CSV stream from ETL process with exact column order below.
    # Column mapping:
    # - Threshold fields: `m_60d_threshold`, `drawdown_40d_threshold`, `vol_20d_threshold`
    # - Decision/state fields: `suggestion`, `positive_streak`
    # Performance:
    # - COPY is preferred for batch ingestion over many individual INSERTs.
    COPY_REGIME_DATA = (
                                """
                                COPY vnindex_regime_filter
                                (time, m_60d_threshold, 
                                drawdown_40d_threshold, 
                                vol_20d_threshold, 
                                suggestion,positive_streak) 
                                FROM STDIN WITH (FORMAT CSV)
                                """
    )






