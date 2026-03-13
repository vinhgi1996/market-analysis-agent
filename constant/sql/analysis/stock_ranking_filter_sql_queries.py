

from enum import Enum

class StockRankingFilterSQLQueries(str, Enum):


    # -----------------------------
    # Query to get close data of a specific symbol of time ( the unit is trading session )
    # -----------------------------
    GET_DATA_BY_DATE = (
        "SELECT op.symbol AS symbol , "
            "op.time AS time, "
            "op.close AS close, "
            "stma.sma_20 AS sma_20, "
            "stma.sma_50 AS sma_50, "
            "stm.m_1 AS sm_1, "
            "stm.m_3 AS sm_3, "
            "stm.m_6 AS sm_6, "
            "vnim.m_21d AS vnim_1, "
            "vnim.m_60d AS vnim_3, "
            "vnim.m_126d AS vnim_6, "
            "srsi.rsi_14 AS rsi_14, "
            "sv.vol_20d_pct_126 AS vol_20d_pct_126, "
            "sv.atr_14_w AS atr_14_w "
        "FROM ohlcv_prices op "
        "JOIN stock_moving_average stma USING(symbol,time) "
        "JOIN stock_momentum stm USING(symbol,time) "
        "JOIN vnindex_momentum vnim USING(time) "
        "JOIN stock_relative_strength_index srsi USING(symbol,time) "
        "JOIN stock_volatility sv USING(symbol, time) "
        "JOIN stock_structural_strength_filter sss USING(symbol, time) "
        "WHERE time = %s "
        "AND sss.suggestion ='POSSIBLE' "
    )

    GET_DATA_BY_DATE_RANGE = (
        "SELECT op.symbol AS symbol , "
            "op.time AS time, "
            "op.close AS close, "
            "stma.sma_20 AS sma_20, "
            "stma.sma_50 AS sma_50, "
            "stm.m_1 AS sm_1, "
            "stm.m_3 AS sm_3, "
            "stm.m_6 AS sm_6, "
            "vnim.m_21d AS vnim_1, "
            "vnim.m_60d AS vnim_3, "
            "vnim.m_126d AS vnim_6, "
            "srsi.rsi_14 AS rsi_14, "
            "sv.vol_20d_pct_126 AS vol_20d_pct_126, "
            "sv.atr_14_w AS atr_14_w "
            "FROM ohlcv_prices op "
        "JOIN stock_moving_average stma USING(symbol,time) "
        "JOIN stock_momentum stm USING(symbol,time) "
        "JOIN vnindex_momentum vnim USING(time) "
        "JOIN stock_relative_strength_index srsi USING(symbol,time) "
        "JOIN stock_volatility sv USING(symbol, time) "
        "JOIN stock_structural_strength_filter sss USING(symbol, time) "
        "WHERE time BETWEEN %s AND %s "
        "AND sss.suggestion ='POSSIBLE' "
        "ORDER BY time ASC "
    )

    DELETE_STOCK_RANKING_DATA_BY_DATE = (
        "DELETE FROM stock_ranking_filter "
        "WHERE time = %s "
    )

    DELETE_STOCK_RANKING_DATA_BY_DATE_RANGE = (
        "DELETE FROM stock_ranking_filter "
        "WHERE time BETWEEN %s AND %s "
    )

    COPY_STOCK_RANKING_DATA = (
                                """ 
                                COPY stock_ranking_filter
                                (time, symbol, alpha_score) 
                                FROM STDIN WITH (FORMAT CSV)
                                """
    )






