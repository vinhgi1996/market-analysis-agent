from business_logic.momentum_pipeline_v1 import MomentumPipeline
from config.postgre_manager import PostgresManager
from constant.momentum_sql_queries import MomentumSQLQueries
from constant.sql_queries import SQLQueries
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

if __name__ == "__main__":
    #momentum_ohlcv()
    pipeline = MomentumPipeline(max_workers=5,
                                symbol_queries=SQLQueries.GET_HOSE_ENERGY_COMPANY_SYMBOL,
                                mode="incremental",
                                current_time='2026-02-26')
    pipeline.run_all_parallel()
