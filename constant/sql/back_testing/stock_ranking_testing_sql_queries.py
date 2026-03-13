

from enum import Enum

class StockRankingTestingSQLQueries(str, Enum):


    # -----------------------------
    # Query to get close data of a specific symbol of time ( the unit is trading session )
    # -----------------------------
    GET_STOCK_OHLVC_DATA_BY_DATE_RANGE = (
        "WITH symbols AS ( "
            "SELECT DISTINCT symbol "
            "FROM stock_ranking_filter "
            "WHERE time BETWEEN %s AND %s "
        ") "
        "SELECT "
            "symbol, "
            "time, "
            "close, "
            "close_20 "
        "FROM ( "
            "SELECT "
                "op.symbol, "
                "op.time, "
                "op.close, "
                "LAG(op.close, 5) OVER ( "
                    "PARTITION BY op.symbol "
                    "ORDER BY op.time DESC "
                ") AS close_20 "
            "FROM ohlcv_prices op "
            "JOIN symbols s USING(symbol) "
        ") t "
        "WHERE time BETWEEN %s AND %s "
    )

    GET_STOCK_RANKING_DATA_BY_DATE_RANGE = (
        "SELECT * FROM stock_ranking_filter "
        "WHERE time BETWEEN %s AND %s "
        "ORDER BY  time DESC, alpha_score DESC "
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






