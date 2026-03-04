import logging

import numpy as np

from config.postgre_manager import PostgresManager
from constant.constants.rsi_constant import RsiConstant
from constant.sql.rsi_sql_queries import RsiSQLQueries
from util.pandas_util import PandasUtil
from util.postgre_sql import PostgresSQLUtil
# Thread pool for running symbol jobs concurrently.
from concurrent.futures import ThreadPoolExecutor, as_completed


import pandas as pd
from io import StringIO

class RsiPipeline:

    def __init__(
            self,
            max_workers: int = 5,
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
                RsiConstant.LOG_START.value.format(symbol=symbol)
            )

            df = None

            if self.mode == RsiConstant.MODE_BACKFILL.value:
                df = self._backfill_synthesize(symbol)
            elif self.mode == RsiConstant.MODE_INCREMENTAL.value:
                df = self._incremental_synthesize(symbol,self.current_time)
            # Persist dataframe (delete existing range first, then bulk copy).
            self._store(df, self.current_time)

            self.logger.info(
                RsiConstant.LOG_FINISH.value.format(symbol=symbol)
            )
        except Exception as e:
            # Error is logged, not re-raised -> job continues for other symbols.
            self.logger.error(
                RsiConstant.LOG_ERROR.value.format(symbol=symbol, error=e)
            )

    def run_all_parallel(self):
        """
        Run pipeline for every symbol returned by `self.symbol_queries`
        using ThreadPoolExecutor.
        """
        # NOTE: This string appears to have encoding artifacts in current source.
        self.logger.info(RsiConstant.LOG_PARALLEL_START.value)

        # Expect list[dict], each dict has at least key: "symbol".
        symbols = PostgresSQLUtil.run_sql(self.symbol_queries)
        symbol_list = [s[RsiConstant.SYMBOL_KEY.value] for s in symbols]

        # Thread pool dispatch: each symbol runs independently.
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.run, symbol) for symbol in symbol_list]

            # Consume futures and re-surface unexpected thread exceptions.
            for future in as_completed(futures):
                future.result()

                # NOTE: This string appears to have encoding artifacts in current source.
        self.logger.info(RsiConstant.LOG_PARALLEL_FINISH.value)

    # =========================================================
    # Feature Engineering
    # =========================================================

    def _backfill_synthesize(self, symbol: str) -> pd.DataFrame:

        # Backfill: fetch broad historical set for symbol.
        data = PostgresSQLUtil.run_sql(
            RsiSQLQueries.GET_DATA_BY_SYMBOL_OHLVC,
            (symbol,)
        )

        # Convert row dict/list into DataFrame for vectorized computation.
        df = pd.DataFrame(data).copy()

        # Convert object columns to float64 (commonly NUMERIC from DB).
        # CAUTION: This attempts conversion for all object columns, which may fail
        # if non-numeric string columns exist in result set.
        df = PandasUtil.cast_object_columns_to_float64(df)

        period = 14

        delta = df["close"].diff()

        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)

        avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        df[RsiConstant.RSI_14.value] = rsi
        df[RsiConstant.AVG_GAIN_14.value] = avg_gain
        df[RsiConstant.AVG_LOSS_14.value] = avg_loss
        # Keep only persistence contract columns (order matters for COPY).
        return df[
            [
                RsiConstant.SYMBOL_KEY.value,
                RsiConstant.TIME_KEY.value,
                RsiConstant.RSI_14.value,
                RsiConstant.AVG_GAIN_14.value,
                RsiConstant.AVG_LOSS_14.value,
            ]
        ]

    def _incremental_synthesize(self, symbol: str, date: str) -> pd.DataFrame:
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
        return result_df


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
        symbol = df[RsiConstant.SYMBOL_KEY.value].iloc[0]

        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:
                # Performance optimization:
                # allow faster commit behavior for this transaction scope.
                # Do not using this option for important data which can't be recovered
                cursor.execute(RsiConstant.SET_SYNC_COMMIT_OFF.value)

                # Remove existing rows before reloading.
                # For backfill: date is oldest bound -> broad cleanup.
                # For incremental: date is new boundary -> narrow cleanup.
                cursor.execute(
                    RsiSQLQueries.DELETE_RSI_DATA,
                    (symbol, date)
                )

                # Prepare CSV in-memory buffer for COPY command.
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # High-throughput insert into momentum target table.
                cursor.copy_expert(
                    RsiSQLQueries.COPY_RSI_DATA,
                    buffer
                )

                # Commit after delete + copy.
            conn.commit()

