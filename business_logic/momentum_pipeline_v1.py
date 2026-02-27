from config.postgre_manager import PostgresManager
from constant.momentum_sql_queries import MomentumSQLQueries
from constant.sql_queries import SQLQueries
from util.postgre_sql import PostgresSQLUtil
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import pandas as pd
from io import StringIO

class MomentumPipeline:

    def __init__(self, max_workers: int = 5,symbol_queries: str = "", current_time:str="", mode:str="" ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.max_workers = max_workers
        self.symbol_queries = symbol_queries
        self.current_time = current_time
        self.mode = mode


    # =========================================================
    # Public API
    # =========================================================

    def run(self, symbol: str):
        """
        Process a single symbol:
        - Fetch data
        - Compute momentum
        - Store results
        """
        try:
            self.logger.info(f"Start processing {symbol}")
            df = self._synthesize(symbol,self.current_time,self.mode)
            self._store(df,self.current_time)
            self.logger.info(f"Finished processing {symbol}")
        except Exception as e:
            self.logger.error(f"Error processing {symbol}: {e}")

    def run_all_parallel(self):
        """
        Run momentum pipeline for all symbols in parallel
        using ThreadPoolExecutor.
        """

        self.logger.info("🚀 Parallel momentum pipeline started")

        symbols = PostgresSQLUtil.run_sql(self.symbol_queries)

        symbol_list = [s["symbol"] for s in symbols]

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:

            futures = [
                executor.submit(self.run, symbol) for symbol in symbol_list
            ]

            # Optional: Wait and raise unexpected errors
            for future in as_completed(futures):
                future.result()

        self.logger.info("✅ Parallel momentum pipeline completed")

    # =========================================================
    # Feature Engineering
    # =========================================================

    def _synthesize(self, symbol: str, date:str, mode:str) -> pd.DataFrame:

        # data = PostgresSQLUtil.run_sql(
        #     MomentumSQLQueries.GET_DATA_BY_DATE_AND_SYMBOL,
        #     (symbol,)
        # )
        data = None
        if mode == "backfill":
            data = PostgresSQLUtil.run_sql(MomentumSQLQueries.GET_DATA_BY_SYMBOL,(symbol,))
        elif mode == "incremental":
            data = PostgresSQLUtil.run_sql(MomentumSQLQueries.GET_DATA_BY_DATE_SYMBOL,(symbol,date))

        df = pd.DataFrame(data).copy()

        # Fix dtype mismatch (NUMERIC -> float)
        df = df.astype({
            col: "float64"
            for col in df.columns
            if df[col].dtype == "object"
        })

        # Momentum calculations
        df["m_3"] = df["close"] / df["close_63"] - 1
        df["m_6"] = df["close"] / df["close_126"] - 1
        df["m_12"] = df["close"] / df["close_252"] - 1

        df["m_composite"] = (
            df["m_3"] +
            df["m_6"] +
            df["m_12"]
        ) / 3
        return df[[
            "symbol",
            "time",
            "m_3",
            "m_6",
            "m_12",
            "m_composite"
        ]]

    # =========================================================
    # Persistence
    # =========================================================

    def _store(self, df: pd.DataFrame,date:str):

        if df.empty:
            return

        symbol = df["symbol"].iloc[0]
        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:

                cursor.execute("SET LOCAL synchronous_commit = OFF;")

                # Remove existing data for symbol
                # The query work for both mode incremental and backfill
                # since it will delete everything if in backfill mode, you input the oldest data time
                # and in incremental date, you input only the new time.
                cursor.execute(
                    MomentumSQLQueries.DELETE_MOMENTUM_DATA,
                    (symbol,date)
                )

                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                cursor.copy_expert( MomentumSQLQueries.COPY_MOMENTUM_DATA, buffer)

            conn.commit()