

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
                "LAG(op.close, 7) OVER ( "
                    "PARTITION BY op.symbol "
                    "ORDER BY op.time DESC "
                ") AS close_20 "
            "FROM ohlcv_prices op "
            "JOIN symbols s USING(symbol) "
        ") t "
        "WHERE time BETWEEN %s AND %s "
    )

    GET_STOCK_OHLVC_DATA_BY_DATE_RANGE_V2 = (
        "WITH symbols AS ( "
            "SELECT DISTINCT symbol "
            "FROM stock_ranking_filter "
            "WHERE time BETWEEN %s AND %s "
        ") "
        "SELECT "
            "symbol, "
            "time, "
            "close, "
            "close_5, "
            "close_10, "
            "close_15, "
            "close_20, "
            "close_25, "
            "close_30,"
            "close_35, "
            "close_40, "
            "close_45, "
            "close_50, "
            "close_55, "
            "close_60 "
        "FROM ( "
                "SELECT "
                    "op.symbol, "
                    "op.time, "
                    "op.close, "
                    "LAG(op.close, 5) OVER w  AS close_5, "
                    "LAG(op.close, 10) OVER w AS close_10, "
                    "LAG(op.close, 15) OVER w AS close_15, "
                    "LAG(op.close, 20) OVER w AS close_20, "
                    "LAG(op.close, 25) OVER w AS close_25, "
                    "LAG(op.close, 30) OVER w AS close_30, "
                     "LAG(op.close, 35) OVER w AS close_35, "
                     "LAG(op.close, 40) OVER w AS close_40, "
                     "LAG(op.close, 45) OVER w AS close_45,"
                     "LAG(op.close, 50) OVER w AS close_50, "
                     "LAG(op.close, 55) OVER w AS close_55, "
                     "LAG(op.close, 60) OVER w AS close_60 "
                "FROM ohlcv_prices op "
                "JOIN symbols s USING(symbol) "
                "WINDOW w AS ( "
                "PARTITION BY op.symbol "
                "ORDER BY op.time DESC "
            ") "
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






