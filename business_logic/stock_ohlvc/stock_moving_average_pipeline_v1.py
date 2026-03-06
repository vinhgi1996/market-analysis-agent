import logging

import numpy as np

from config.postgre_manager import PostgresManager
from constant.constants.stock.stock_moving_average_constant import MaConstant
from constant.sql.stock.stock_moving_average_sql_queries import MovingAverageSQLQueries
from util.postgre_sql import PostgresSQLUtil
# Thread pool for running symbol jobs concurrently.
from concurrent.futures import ThreadPoolExecutor, as_completed


import pandas as pd
from io import StringIO

class MovingAveragePipeline:

    def __init__(
            self,
            max_workers: int = MaConstant.MAX_WORKERS.value,
            symbol_queries: str = "",
            current_time: str = "",
            mode: str = ""
    ):
        # Class-scoped logger name, e.g., "MovingAveragePipeline".
        self.logger = logging.getLogger(self.__class__.__name__)

        # Number of worker threads used in run_all_parallel.
        self.max_workers = max_workers

        # SQL text used to fetch symbol universe.
        self.symbol_queries = symbol_queries

        # Pipeline "effective time" used by incremental mode and delete range.
        self.current_time = current_time

        # Expected: "backfill" or "incremental".
        self.mode = mode

    # =========================================================
    # Public API
    # =========================================================

    def run(self, symbol: str):

        try:
            self.logger.info(
                MaConstant.LOG_START.value.format(symbol=symbol)
            )

            df = None

            if self.mode == MaConstant.MODE_BACKFILL.value:
                df = self._backfill_synthesize(symbol)
            elif self.mode == MaConstant.MODE_INCREMENTAL.value:
                df = self._incremental_synthesize(symbol,self.current_time)

            # Persist dataframe (delete existing range first, then bulk copy).
            self._store(df, self.current_time)

            self.logger.info(
                MaConstant.LOG_FINISH.value.format(symbol=symbol)
            )
        except Exception as e:
            # Error is logged, not re-raised -> job continues for other symbols.
            self.logger.error(
                MaConstant.LOG_ERROR.value.format(symbol=symbol, error=e)
            )

    def run_all_parallel(self):
        """
        Run pipeline for every symbol returned by `self.symbol_queries`
        using ThreadPoolExecutor.
        """
        # NOTE: This string appears to have encoding artifacts in current source.
        self.logger.info(MaConstant.LOG_PARALLEL_START.value)

        # Expect list[dict], each dict has at least key: "symbol".
        symbols = PostgresSQLUtil.run_sql(self.symbol_queries)
        symbol_list = [s[MaConstant.SYMBOL_KEY.value] for s in symbols]

        # Thread pool dispatch: each symbol runs independently.
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.run, symbol) for symbol in symbol_list]

            # Consume futures and re-surface unexpected thread exceptions.
            for future in as_completed(futures):
                future.result()

                # NOTE: This string appears to have encoding artifacts in current source.
        self.logger.info(MaConstant.LOG_PARALLEL_FINISH.value)

    # =========================================================
    # Feature Engineering
    # =========================================================

    def _backfill_synthesize(self, symbol: str) -> pd.DataFrame:

        # Backfill: fetch broad historical set for symbol.
        data = PostgresSQLUtil.run_sql(
            MovingAverageSQLQueries.GET_DATA_BY_SYMBOL,
            (symbol,)
        )

        # Convert row dict/list into DataFrame for vectorized computation.
        df = pd.DataFrame(data).copy()

        # Convert object columns to float64 (commonly NUMERIC from DB).
        # CAUTION: This attempts conversion for all object columns, which may fail
        # if non-numeric string columns exist in result set.
        df = df.astype({
            col: MaConstant.FLOAT64_DTYPE.value
            for col in df.columns
            if df[col].dtype == MaConstant.OBJECT_DTYPE.value
        })

        # Simple moving average features based on lagged closes (20/50/200/ trading days).
        df[MaConstant.SMA_20_KEY.value] = (
            df[MaConstant.CLOSE_KEY.value]
            .rolling(MaConstant.SMA_20_WINDOW.value)
            .mean()
            .shift(1)
        )
        df[MaConstant.SMA_50_KEY.value] = (
            df[MaConstant.CLOSE_KEY.value]
            .rolling(MaConstant.SMA_50_WINDOW.value)
            .mean()
            .shift(1)
        )
        df[MaConstant.SMA_200_KEY.value] = (
            df[MaConstant.CLOSE_KEY.value]
            .rolling(MaConstant.SMA_200_WINDOW.value)
            .mean()
            .shift(1)
        )

        # Keep only persistence contract columns (order matters for COPY).
        return df[
            [
                MaConstant.SYMBOL_KEY.value,
                MaConstant.TIME_KEY.value,
                MaConstant.SMA_20_KEY.value,
                MaConstant.SMA_50_KEY.value,
                MaConstant.SMA_200_KEY.value,
            ]
        ]

    def _incremental_synthesize(self, symbol: str, date: str) -> pd.DataFrame:

        data = PostgresSQLUtil.run_sql(
            MovingAverageSQLQueries.GET_DATA_BY_DATE_SYMBOL,
            (symbol,date)
        )

        # Convert row dict/list into DataFrame for vectorized computation.
        df = pd.DataFrame(data).copy()

        # Convert object columns to float64 (commonly NUMERIC from DB).
        # CAUTION: This attempts conversion for all object columns, which may fail
        # if non-numeric string columns exist in result set.
        df = df.astype({
            col: MaConstant.FLOAT64_DTYPE.value
            for col in df.columns
            if df[col].dtype == MaConstant.OBJECT_DTYPE.value
        })

        # Simple moving average features based on lagged closes which using data up to T-1 day (50/200/ trading days).
        last_row = df.iloc[-1]

        close_values = df[MaConstant.CLOSE_KEY.value].values
        n = len(close_values)

        sma_20 = close_values[-21:-1].mean() if n >= 21 else np.nan
        sma_50 = close_values[-51:-1].mean() if n >= 51 else np.nan
        sma_200 = close_values[-201:-1].mean() if n >= 201 else np.nan

        return pd.DataFrame([{
            MaConstant.SYMBOL_KEY.value: last_row[MaConstant.SYMBOL_KEY.value],
            MaConstant.TIME_KEY.value: last_row[MaConstant.TIME_KEY.value],
            MaConstant.SMA_20_KEY.value: sma_20,
            MaConstant.SMA_50_KEY.value: sma_50,
            MaConstant.SMA_200_KEY.value: sma_200
        }])


    # =========================================================
    # Persistence
    # =========================================================

    def _store(self, df: pd.DataFrame, date: str):
        """
        Persist one symbol dataframe:
        - Skip if empty
        - Delete existing target rows in scope
        - Bulk insert via COPY for speed
        """

        if df.empty:
            return

        # Assumes dataframe contains a single symbol only.
        symbol = df[MaConstant.SYMBOL_KEY.value].iloc[0]

        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:
                # Performance optimization:
                # allow faster commit behavior for this transaction scope.
                # Do not using this option for important data which can't be recovered
                cursor.execute(MaConstant.SET_SYNC_COMMIT_OFF.value)

                # Remove existing rows before reloading.
                # For backfill: date is oldest bound -> broad cleanup.
                # For incremental: date is new boundary -> narrow cleanup.
                cursor.execute(
                    MovingAverageSQLQueries.DELETE_MA_DATA,
                    (symbol, date)
                )

                # Prepare CSV in-memory buffer for COPY command.
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # High-throughput insert into momentum target table.
                cursor.copy_expert(
                    MovingAverageSQLQueries.COPY_MA_DATA,
                    buffer
                )

                # Commit after delete + copy.
            conn.commit()

