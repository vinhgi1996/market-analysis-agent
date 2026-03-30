

from enum import Enum

class StockStructuralStrengthFilterSQLQueries(str, Enum):


    # -----------------------------
    # Query to get close data of a specific symbol of time ( the unit is trading session )
    # -----------------------------
    GET_DATA_BY_SYMBOL = (
        "SELECT op.symbol AS symbol , "
            "op.time AS time, "
            "op.close AS close, "
            "op.volume AS volume, "
            "stma.sma_20 AS sma_20, "
            "stma.sma_50 AS sma_50, "
            "stm.m_1 AS m_1, "
            "stm.m_3 AS m_3, "
            "srsi.rsi_14 AS rsi_14, "
            "sv.vol_20d_pct_126 AS vol_20d_pct_126, "
            "sv.atr_14_w AS atr_14_w, "
            "svol.volume_ratio AS volume_ratio "
        "FROM ohlcv_prices op "
        "JOIN stock_moving_average stma USING(symbol,time) "
        "JOIN stock_momentum stm USING(symbol,time) "
        "JOIN stock_relative_strength_index srsi USING(symbol,time) "
        "JOIN stock_volatility sv USING(symbol, time) "
        "JOIN stock_volume svol USING(symbol, time) "
        "WHERE symbol = %s "
        "ORDER BY time ASC "
    )

    GET_DATA_BY_SYMBOL_DATE = (
        "SELECT op.symbol AS symbol , "
            "op.time AS time, "
            "op.close AS close, "
            "stma.sma_20 AS sma_20, "
            "stma.sma_50 AS sma_50, "
            "stm.m_1 AS m_1, "
            "stm.m_3 AS m_3, "
            "srsi.rsi_14 AS rsi_14, "
            "sv.vol_20d_pct_126 AS vol_20d_pct_126, "
            "sv.atr_14_w AS atr_14_w, "
            "svol.volume_ratio AS volume_ratio "
        "FROM ohlcv_prices op "
        "JOIN stock_moving_average stma USING(symbol,time) "
        "JOIN stock_momentum stm USING(symbol,time) "
        "JOIN stock_relative_strength_index srsi USING(symbol,time) "
        "JOIN stock_volatility sv USING(symbol, time) "
        "JOIN stock_volume svol USING(symbol, time) "
        "WHERE symbol = %s "
        "AND time < (%s::date + INTERVAL '1 day') "
        "ORDER BY time DESC "
        "LIMIT 1 "
    )

    GET_PREVIOUS_POSITIVE_STREAK = (
        "SELECT positive_streak "
        "FROM stock_structural_strength_filter "
        "WHERE symbol = %s "
        "AND time < (%s::date + INTERVAL '1 day') "
        "ORDER BY time DESC "
        "LIMIT 1 "
    )


    DELETE_STOCK_STRUCTURAL_STRENGTH_DATA = (
        "DELETE FROM stock_structural_strength_filter "
        "WHERE symbol = %s "
        "AND time >= %s "
    )

    COPY_STOCK_STRUCTURAL_STRENGTH_DATA = (
                                """
                                COPY stock_structural_strength_filter
                                (symbol, time, m_3_threshold, m_1_threshold,
                                rsi_14_threshold, vol_20d_pct_126_threshold,
                                rsi_14_close_threshold, volume_ratio_threshold, 
                                suggestion, positive_streak) 
                                FROM STDIN WITH (FORMAT CSV)
                                """
    )






