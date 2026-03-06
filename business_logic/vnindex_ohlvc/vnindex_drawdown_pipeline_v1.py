import logging

import numpy as np

from config.postgre_manager import PostgresManager

from constant.constants.vnindex.vnindex_drawdown_constant import DrawdownConstant
from constant.sql.vnindex.vnindex_drawdown_sql_queries import DrawdownSQLQueries
from util.pandas_util import PandasUtil
from util.postgre_sql import PostgresSQLUtil



import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
from io import StringIO

class VnIndexDrawdownPipeline:

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

    def run(self):

        try:
            self.logger.info(
                DrawdownConstant.LOG_START.value
            )

            df = None

            if self.mode == DrawdownConstant.MODE_BACKFILL.value:
                df = self._backfill_synthesize()
            elif self.mode == DrawdownConstant.MODE_INCREMENTAL.value:
                df = self._incremental_synthesize(self.current_time)
            # Persist dataframe (delete existing range first, then bulk copy).
            self._store(df, self.current_time)

            self.logger.info(
                DrawdownConstant.LOG_FINISH.value
            )
        except Exception as e:
            # Error is logged, not re-raised -> job continues for other symbols.
            self.logger.error(
                DrawdownConstant.LOG_ERROR.value.format(symbol='VNINDEX', error=e)
            )

    # =========================================================
    # Feature Engineering
    # =========================================================

    def _backfill_synthesize(self) -> pd.DataFrame:

        # Backfill: fetch broad historical set for symbol.
        data = PostgresSQLUtil.run_sql(
            DrawdownSQLQueries.GET_DATA,
        )

        # Convert row dict/list into DataFrame for vectorized computation.
        df = pd.DataFrame(data).copy()

        # Convert object columns to float64 (commonly NUMERIC from DB).
        # CAUTION: This attempts conversion for all object columns, which may fail
        # if non-numeric string columns exist in result set.
        df = PandasUtil.cast_object_columns_to_float64(df)

        # calculate draw down 40 days
        rolling_peak = df[DrawdownConstant.CLOSE_KEY.value].rolling(40).max()
        df[DrawdownConstant.DRAWDOWN_40D.value] = (df[DrawdownConstant.CLOSE_KEY.value] - rolling_peak) / rolling_peak

        # Keep only persistence contract columns (order matters for COPY).
        return df[
            [
                DrawdownConstant.TIME_KEY.value,
                DrawdownConstant.DRAWDOWN_40D.value,
            ]
        ]

    def _incremental_synthesize(self, date: str) -> pd.DataFrame:
        data = PostgresSQLUtil.run_sql(
            DrawdownSQLQueries.GET_DATA_BY_DATE,
            (date,)
        )

        if len(data) < 20 :
            raise ValueError("Insufficient data for incremental computation")

        # Convert row dict/list into DataFrame for vectorized computation.
        data_df = pd.DataFrame(data).copy()


        # Convert object columns to float64 (commonly NUMERIC from DB).
        # CAUTION: This attempts conversion for all object columns, which may fail
        # if non-numeric string columns exist in result set.
        data_df = PandasUtil.cast_object_columns_to_float64(data_df)

        # calculate draw down 40 days
        rolling_peak = data_df[DrawdownConstant.CLOSE_KEY.value].rolling(40).max()
        data_df[DrawdownConstant.DRAWDOWN_40D.value] = (data_df[DrawdownConstant.CLOSE_KEY.value] - rolling_peak) / rolling_peak

        last_row = data_df.iloc[-1]

        # Keep only persistence contract columns (order matters for COPY).
        return pd.DataFrame([{
            DrawdownConstant.TIME_KEY.value: last_row[DrawdownConstant.TIME_KEY.value],
            DrawdownConstant.DRAWDOWN_40D.value: last_row[DrawdownConstant.DRAWDOWN_40D.value],
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

        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:
                # Performance optimization:
                # allow faster commit behavior for this transaction scope.
                # Do not using this option for important data which can't be recovered
                cursor.execute(DrawdownConstant.SET_SYNC_COMMIT_OFF.value)

                # Remove existing rows before reloading.
                # For backfill: date is oldest bound -> broad cleanup.
                # For incremental: date is new boundary -> narrow cleanup.
                cursor.execute(
                    DrawdownSQLQueries.DELETE_DRAWDOWN_DATA,
                    (date,)
                )

                # Prepare CSV in-memory buffer for COPY command.
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # High-throughput insert into momentum target table.
                cursor.copy_expert(
                    DrawdownSQLQueries.COPY_DRAWDOWN_DATA,
                    buffer
                )

                # Commit after delete + copy.
            conn.commit()

