import time

import numpy as np
from datetime import datetime, timedelta
from business_logic.analysis.stock_ranking_filter_v1 import StockRankingPipeline
from business_logic.analysis.stock_structural_strength_filter_v1 import StockStructuralStrengthPipeline
from business_logic.analysis.vnindex_regime_filter_v1 import VnIndexRegimeFilterPipeline
from business_logic.stock_ohlvc.stock_momentum_pipeline_v1 import StockMomentumPipeline
from business_logic.stock_ohlvc.stock_moving_average_pipeline_v1 import StockMovingAveragePipeline
from business_logic.stock_ohlvc.stock_rsi_pipeline_v1 import StockRsiPipeline
from business_logic.stock_ohlvc.stock_volatility_pipeline_v2 import  StockVolatilityPipelineV2
from business_logic.stock_ohlvc.stock_volume_pipeline_v1 import StockVolumePipeline
from business_logic.vnindex_ohlvc.vnindex_drawdown_pipeline_v1 import VnIndexDrawdownPipeline
from business_logic.vnindex_ohlvc.vnindex_momentum_pipeline_v1 import VnIndexMomentumPipeline
from business_logic.vnindex_ohlvc.vnindex_moving_average_pipeline_v1 import VnIndexMovingAveragePipeline
from business_logic.vnindex_ohlvc.vnindex_volatility_pipeline_v1 import VnIndexVolatilityPipeline
from config.postgre_manager import PostgresManager
from constant.constants.stock.stock_rsi_constant import RsiConstant
from constant.constants.stock.stock_volatility_constant import VolatilityConstant
from constant.sql.stock.stock_momentum_sql_queries import MomentumSQLQueries
from constant.sql.stock.stock_rsi_sql_queries import RsiSQLQueries
from constant.sql.sql_queries import SQLQueries
from constant.sql.stock.stock_volatility_sql_queries import VolatilitySQLQueries
from util.pandas_util import PandasUtil
from util.postgre_sql import PostgresSQLUtil
import pandas as pd
from io import StringIO
import logging

logger = logging.getLogger(__name__)

def momentum_ohlcv():
    logger.info("🔄 OHLCV momentum business logic start")

    symbols = PostgresSQLUtil.run_sql(SQLQueries.GET_HOSE_ENERGY_COMPANY_SYMBOL)

    for symbol in symbols:
        df = synthesize_momentum_metrics(symbol['symbol'])
        store_momentum_metrics(df)
    logger.info("🔄 OHLCV momentum business logic end")

def synthesize_momentum_metrics(symbol:str)->pd.DataFrame:
    logger.info("Momentum metrics synthesys started.")
    # --------------------------------------------------
    # Data retrieving
    # --------------------------------------------------
    data =  PostgresSQLUtil.run_sql(MomentumSQLQueries.GET_DATA_BY_DATE_AND_SYMBOL,(symbol,))
    # --------------------------------------------------
    # Ensure pandas DataFrame
    # --------------------------------------------------
    if not isinstance(data, pd.DataFrame):
        df = pd.DataFrame(data)
    else:
        df = data.copy()

    # this line of code handling the mismatch data type problem
    # between numeric data type in postgre and float type in Pandas
    df = df.astype({col: "float64" for col in df.columns if df[col].dtype == "object"})

    # --------------------------------------------------
    # Momentum calculating
    # --------------------------------------------------

    #standard momentum
    df["m_3"] = df["close"] / df["close_63"] - 1
    df["m_6"] = df["close"] / df["close_126"] - 1
    df["m_12"] = df["close"] / df["close_252"] - 1

    #composite momentum
    df["m_composite"] = (df["m_3"] + df["m_6"] + df["m_12"]) /3

    required_columns = [
        "symbol",
        "time",
        "m_3",
        "m_6",
        "m_12",
        "m_composite"
    ]

    df = df[required_columns]

    logger.info("Momentum metrics synthesys ended.")

    return df

def store_momentum_metrics(df:pd.DataFrame)->None:
    print("Momentum metrics store started.")
    try:
        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:

                # 🔥 Speed boost for bulk ingest
                # this line of code turn the disk flush in fsync process of Postgre into async
                # so the commit will return immediately and boost the insert performance
                # WARNING: do not use for the pipeline that the data is extremely important and
                # can't be recovered, only used to process data that can be recovered.
                cursor.execute("SET LOCAL synchronous_commit = OFF;")

                # Create temp staging table
                cursor.execute("""
                    CREATE TEMP TABLE tmp_momentum 
                    (LIKE momentum INCLUDING DEFAULTS)
                    ON COMMIT DROP;
                """)

                # Convert dataframe to CSV buffer
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # COPY to temp table
                cursor.copy_expert("""
                    COPY tmp_momentum
                    (symbol, time, m_3, m_6,m_12,m_composite)
                    FROM STDIN WITH (FORMAT CSV)
                """, buffer)

                # UPSERT into main table
                cursor.execute("""
                    INSERT INTO momentum 
                    (symbol, time, m_3, m_6,m_12,m_composite)
                    SELECT symbol, time, m_3, m_6,m_12,m_composite
                    FROM tmp_momentum
                    ON CONFLICT (symbol, time)
                    DO UPDATE SET
                        m_3   = EXCLUDED.m_3,
                        m_6   = EXCLUDED.m_6,
                        m_12    = EXCLUDED.m_12,
                        m_composite  = EXCLUDED.m_composite;
                """)

            conn.commit()

        logger.info(f"✅ Successfully inserted/updated {len(df)} records.")

    except Exception as e:
        logger.error(f"[store_momentum_metrics] Bulk upsert failed: {e}")
        raise

def synthesize_momentum_metrics(symbol:str)->pd.DataFrame:
    logger.info("Momentum metrics synthesys started.")
    # --------------------------------------------------
    # Data retrieving
    # --------------------------------------------------
    data =  PostgresSQLUtil.run_sql(MomentumSQLQueries.GET_DATA_BY_DATE_SYMBOL,(symbol,))
    # --------------------------------------------------
    # Ensure pandas DataFrame
    # --------------------------------------------------
    if not isinstance(data, pd.DataFrame):
        df = pd.DataFrame(data)
    else:
        df = data.copy()

    # this line of code handling the mismatch data type problem
    # between numeric data type in postgre and float type in Pandas
    df = df.astype({col: "float64" for col in df.columns if df[col].dtype == "object"})

    # --------------------------------------------------
    # Momentum calculating
    # --------------------------------------------------

    #standard momentum
    df["m_3"] = df["close"] / df["close_63"] - 1
    df["m_6"] = df["close"] / df["close_126"] - 1
    df["m_12"] = df["close"] / df["close_252"] - 1

    #composite momentum
    df["m_composite"] = (df["m_3"] + df["m_6"] + df["m_12"]) /3

    required_columns = [
        "symbol",
        "time",
        "m_3",
        "m_6",
        "m_12",
        "m_composite"
    ]

    df = df[required_columns]

    logger.info("Momentum metrics synthesys ended.")

    return df

def rsi_incremental_synthesize( symbol: str, date :str) -> pd.DataFrame:
    ohlvc_data = PostgresSQLUtil.run_sql(
        RsiSQLQueries.GET_DATA_BY_DATE_SYMBOL_OHLVC,
        (symbol, date)
    )
    rsi_data = PostgresSQLUtil.run_sql(
        RsiSQLQueries.GET_DATA_BY_DATE_SYMBOL_RSI,
        (symbol, date)
    )
    if len(ohlvc_data) < 2 or not rsi_data:
        raise ValueError("Insufficient data for incremental RSI computation")

        # Extract rows directly (no DataFrame needed)
    prev_close = float(ohlvc_data[-2]["close"])
    current_row = ohlvc_data[-1]
    current_close = float(current_row["close"])

    prev_avg_gain = float(rsi_data[-1]["average_gain_14"])
    prev_avg_loss = float(rsi_data[-1]["average_loss_14"])

    # Compute delta manually
    delta = current_close - prev_close
    gain = max(delta, 0)
    loss = max(-delta, 0)

    # Wilder smoothing formula
    avg_gain = (prev_avg_gain * 13 + gain) / 14
    avg_loss = (prev_avg_loss * 13 + loss) / 14

    # Prevent division by zero
    if avg_loss == 0:
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

    # Build result DataFrame properly
    result_df = pd.DataFrame([{
        RsiConstant.SYMBOL_KEY.value: symbol,
        RsiConstant.TIME_KEY.value: current_row[RsiConstant.TIME_KEY.value],
        RsiConstant.RSI_14.value: rsi,
        RsiConstant.AVG_GAIN_14.value: avg_gain,
        RsiConstant.AVG_LOSS_14.value: avg_loss
    }])
    return

def volatility_backfill_synthesize(symbol: str) -> pd.DataFrame:

    # Backfill: fetch broad historical set for symbol.
    data = PostgresSQLUtil.run_sql(
        VolatilitySQLQueries.GET_DATA_BY_SYMBOL_OHLVC,
        (symbol,)
    )

    # Convert row dict/list into DataFrame for vectorized computation.
    df = pd.DataFrame(data).copy()

    # Convert object columns to float64 (commonly NUMERIC from DB).
    # CAUTION: This attempts conversion for all object columns, which may fail
    # if non-numeric string columns exist in result set.
    df = PandasUtil.cast_object_columns_to_float64(df)

    # Log returns
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))

    # Rolling volatility
    for w in [20, 60, 252]:
        df[f"vol_{w}d"] = (
            df["log_return"]
            .rolling(w)
            .std() * np.sqrt(252)
        )

    # ATR
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs()
    ], axis=1).max(axis=1)

    df["atr_14_w"] = tr.ewm(alpha=1/14, adjust=False).mean()
    df["atr_14_r"] = tr.rolling(14).mean()

    # Parkinson 20d
    pk = np.log(df["high"] / df["low"])**2
    df["parkinson_20d"] = (
        (pk.rolling(20).sum()) /
        (4 * 20 * np.log(2))
    )**0.5 * np.sqrt(252)

    # Volatility percentile
    df["vol_20d_pct"] = (
        df["vol_20d"]
        .rolling(252)
        .apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1])
    )
    print(f"Volatility backfill synthesize: {df.tail(20)}")
    # Keep only persistence contract columns (order matters for COPY).
    return df[
        [
            VolatilityConstant.SYMBOL_KEY.value,
            VolatilityConstant.TIME_KEY.value,
            VolatilityConstant.VOL_20D.value,
            VolatilityConstant.VOL_60D.value,
            VolatilityConstant.VOL_252D.value,
            VolatilityConstant.ATR_14_R.value,
            VolatilityConstant.ATR_14_W.value,
            VolatilityConstant.PARKINSON_20D.value,
            VolatilityConstant.VOL_20D_PCT.value,
        ]
    ]

def volatility_incremental_synthesize(self, symbol: str, date: str) -> pd.DataFrame:
    ohlvc_data = PostgresSQLUtil.run_sql(
        VolatilitySQLQueries.GET_DATA_BY_DATE_SYMBOL_OHLVC,
        (symbol, date)
    )
    volatility_data = PostgresSQLUtil.run_sql(
        VolatilitySQLQueries.GET_DATA_BY_DATE_SYMBOL_ATR_14_W,
        (symbol, date)
    )
    if len(ohlvc_data) < 253 or not volatility_data:
        raise ValueError("Insufficient data for incremental computation")

    # Convert row dict/list into DataFrame for vectorized computation.
    ohlvc_df = pd.DataFrame(ohlvc_data).copy()
    volatility_df = pd.DataFrame(volatility_data).copy()

    # Convert object columns to float64 (commonly NUMERIC from DB).
    # CAUTION: This attempts conversion for all object columns, which may fail
    # if non-numeric string columns exist in result set.
    ohlvc_df = PandasUtil.cast_object_columns_to_float64(ohlvc_df)
    volatility_df = PandasUtil.cast_object_columns_to_float64(volatility_df)

    # Log returns
    ohlvc_df["log_return"] = self._calculate_log_return(ohlvc_df)

    # Rolling volatility
    vol_map = self._calculate_rolling_volatility(ohlvc_df["log_return"])
    for col_name, series in vol_map.items():
        ohlvc_df[col_name] = series

    # Volatility percentile 20d (annualized)
    ohlvc_df["vol_20d_pct"] = self._calculate_vol_20d_pct(ohlvc_df)

    parkinson_ohlvc_df = ohlvc_df.tail(20).copy()



    # Build result DataFrame properly
    result_df = pd.DataFrame([{
        RsiConstant.SYMBOL_KEY.value: symbol,

    }])
    return result_df

# def generate_dates(start_date_str):
#     start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
#     today = datetime.today().date()
#
#     dates = []
#     current = start_date
#
#     while current <= today:
#         dates.append(current.strftime("%Y-%m-%d"))
#         current += timedelta(days=1)
#
#     return dates

def generate_dates(start_date_str, end_date_str):
    start = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end  = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    delta = (end - start).days

    return [
        (start + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(delta + 1)
    ]


if __name__ == "__main__":
    #momentum_ohlcv()


    # pipeline = StockMomentumPipeline(max_workers=5,
    #                             symbol_queries=SQLQueries.GET_HOSE_TOP_83_COMPANY_LIQUIDITY_SYMBOL,
    #                             mode="backfill",
    #                             current_time='2023-01-03')
    # pipeline.run_all_parallel()

    # pipeline = StockMovingAveragePipeline(max_workers=5,
    #                             symbol_queries=SQLQueries.GET_HOSE_TOP_83_COMPANY_LIQUIDITY_SYMBOL,
    #                             mode="backfill",
    #                             current_time='2023-01-03')
    # pipeline.run_all_parallel()

    # start = time.perf_counter()
    # pipeline = StockRsiPipeline(max_workers=4,
    #                        symbol_queries=SQLQueries.GET_HOSE_TOP_83_COMPANY_LIQUIDITY_SYMBOL,
    #                        mode="backfill",
    #                        current_time='2023-01-03')
    # pipeline.run_all_parallel()
    # end = time.perf_counter()
    # print(f":Total execution time: {end - start:.6f}")


    # start = time.perf_counter()
    # pipeline = StockVolatilityPipelineV2(max_workers=4,
    #                               symbol_queries=SQLQueries.GET_HOSE_TOP_83_COMPANY_LIQUIDITY_SYMBOL,
    #                               mode="backfill",
    #                               current_time='2023-01-03')
    # pipeline.run_all_parallel()
    # end = time.perf_counter()
    # print(f"Total execution time: {end - start:.6f}")


    # start = time.perf_counter()
    # pipeline = VnIndexMomentumPipeline(max_workers=4,
    #                               mode="backfill",
    #                               current_time='2023-01-03')
    # pipeline.run()
    # end = time.perf_counter()
    # print(f"Total execution time: {end - start:.6f}")


    # start = time.perf_counter()
    # pipeline = VnIndexMovingAveragePipeline(max_workers=4,
    #                               mode="backfill",
    #                               current_time='2023-01-03')
    # pipeline.run()
    # end = time.perf_counter()
    # print(f"Total execution time: {end - start:.6f}")

    # start = time.perf_counter()
    # pipeline = VnIndexVolatilityPipeline(max_workers=4,
    #                                         mode="backfill",
    #                                         current_time='2023-01-03')
    # pipeline.run()
    # end = time.perf_counter()
    # print(f"Total execution time: {end - start:.6f}")

    # start = time.perf_counter()
    # pipeline = VnIndexDrawdownPipeline(max_workers=4,
    #                                         mode="backfill",
    #                                         current_time='2023-01-03')
    # pipeline.run()
    # end = time.perf_counter()
    # print(f"Total execution time: {end - start:.6f}")
    #
    # start = time.perf_counter()
    # pipeline = VnIndexRegimeFilterPipeline(max_workers=4,
    #                                    mode="backfill",
    #                                    current_time='2023-01-03')
    # pipeline.run()
    # end = time.perf_counter()
    # print(f"Total execution time: {end - start:.6f}")

    # start = time.perf_counter()
    # pipeline = StockVolumePipeline(max_workers=4,
    #                                        mode="backfill",
    #                                        symbol_queries=SQLQueries.GET_HOSE_TOP_83_COMPANY_LIQUIDITY_SYMBOL,
    #                                        current_time='2023-01-03')
    # pipeline.run_all_parallel()
    # end = time.perf_counter()
    # print(f"Total execution time: {end - start:.6f}")

    # start = time.perf_counter()
    # pipeline = StockStructuralStrengthPipeline(max_workers=4,
    #                                mode="backfill",
    #                                symbol_queries=SQLQueries.GET_HOSE_TOP_83_COMPANY_LIQUIDITY_SYMBOL,
    #                                current_time='2023-01-03')
    # pipeline.run_all_parallel()
    # end = time.perf_counter()
    # print(f"Total execution time: {end - start:.6f}")

    start = time.perf_counter()
    pipeline = StockRankingPipeline(max_workers=4,
                                               mode="incremental",
                                               current_time='2025-10-09')
    pipeline.run()
    end = time.perf_counter()
    print(f"Total execution time: {end - start:.6f}")




    




