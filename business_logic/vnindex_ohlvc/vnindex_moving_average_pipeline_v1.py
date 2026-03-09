import logging

import numpy as np

from config.postgre_manager import PostgresManager
from constant.constants.vnindex.vnindex_moving_average_constant import MaConstant
from constant.sql.vnindex.vnindex_moving_average_sql_queries import MovingAverageSQLQueries
from util.postgre_sql import PostgresSQLUtil
# Thread pool for running symbol jobs concurrently.
from concurrent.futures import ThreadPoolExecutor, as_completed


import pandas as pd
from io import StringIO

class VnIndexMovingAveragePipeline:
    """
    VNINDEX moving-average feature pipeline.

    Class-scale responsibilities:
    - Orchestrate moving-average calculation for VNINDEX data.
    - Support `backfill` (full-history rebuild) and `incremental` (latest snapshot) modes.
    - Persist derived features into `vnindex_moving_average` using delete-then-COPY.

    Current output contract:
    - `time`
    - `sma_150` (computed from prior 150 closes, i.e., shifted by one session in backfill path)
    """

    def __init__(
            self,
            max_workers: int = MaConstant.MAX_WORKERS.value,
            symbol_queries: str = "",
            current_time: str = "",
            mode: str = ""
    ):
        # Logger name is bound to the concrete class for easy pipeline-level tracing.
        self.logger = logging.getLogger(self.__class__.__name__)

        # Reserved concurrency configuration (not used directly in this v1 run path).
        self.max_workers = max_workers

        # Reserved query text for broader orchestration patterns.
        self.symbol_queries = symbol_queries

        # Effective processing time; used by incremental read and delete scope.
        self.current_time = current_time

        # Execution mode selector. Expected values: "backfill" or "incremental".
        self.mode = mode

    # =========================================================
    # Public API
    # =========================================================

    def run(self):
        """
        Main pipeline entry point.

        Function-scale flow:
        - Log start.
        - Choose synthesis strategy by mode.
        - Persist result set.
        - Log finish or log failure.
        """

        try:
            self.logger.info(
                MaConstant.LOG_START.value
            )

            df = None

            if self.mode == MaConstant.MODE_BACKFILL.value:
                df = self._backfill_synthesize()
            elif self.mode == MaConstant.MODE_INCREMENTAL.value:
                df = self._incremental_synthesize(self.current_time)

            # Persist output with idempotent pattern: delete scoped rows, then COPY insert.
            self._store(df, self.current_time)

            self.logger.info(
                MaConstant.LOG_FINISH.value
            )
        except Exception as e:
            # Error is logged and swallowed to keep batch orchestration resilient.
            self.logger.error(
                MaConstant.LOG_ERROR.value
            )

    # =========================================================
    # Feature Engineering
    # =========================================================

    def _backfill_synthesize(self) -> pd.DataFrame:
        """
        Build full-history SMA dataset for VNINDEX.

        Function-scale steps:
        - Read complete source history.
        - Normalize numeric dtypes for vectorized math.
        - Compute shifted SMA(150) so value at T uses data up to T-1.
        - Return persistence columns in COPY-compatible order.
        """

        # Backfill path reads complete VNINDEX history.
        data = PostgresSQLUtil.run_sql(
            MovingAverageSQLQueries.GET_DATA,
        )

        # Convert row dict/list into DataFrame for vectorized computation.
        df = pd.DataFrame(data).copy()

        # Convert DB object-like numeric columns into float64 for stable rolling math.
        # CAUTION: If a non-numeric object column exists, astype conversion will fail.
        df = df.astype({
            col: MaConstant.FLOAT64_DTYPE.value
            for col in df.columns
            if df[col].dtype == MaConstant.OBJECT_DTYPE.value
        })

        # SMA(150) computed from trailing close history and shifted 1 session:
        # value at time T uses closes through T-1 (no same-day look-ahead).
        df[MaConstant.SMA_150_KEY.value] = (
            df[MaConstant.CLOSE_KEY.value]
            .rolling(MaConstant.SMA_150_WINDOW.value)
            .mean()
            .shift(1)
        )

        # Keep only persistence contract columns (order matters for COPY).
        return df[
            [
                MaConstant.TIME_KEY.value,
                MaConstant.SMA_150_KEY.value,

            ]
        ]

    def _incremental_synthesize(self,date: str) -> pd.DataFrame:
        """
        Build one-row SMA snapshot for an incremental date.

        Function-scale steps:
        - Fetch bounded recent history ending at `date` (inclusive by SQL boundary).
        - Normalize dtypes for numeric operations.
        - Compute latest SMA(150) from prior 150 closes.
        - Return a single-row dataframe for upsert-style reload.
        """

        # Pull bounded history window required for latest SMA(150) computation.
        data = PostgresSQLUtil.run_sql(
            MovingAverageSQLQueries.GET_DATA_BY_DATE,
            (date,)
        )

        # Convert row dict/list into DataFrame for vectorized computation.
        df = pd.DataFrame(data).copy()

        # Convert DB object-like numeric columns into float64 for stable calculations.
        df = df.astype({
            col: MaConstant.FLOAT64_DTYPE.value
            for col in df.columns
            if df[col].dtype == MaConstant.OBJECT_DTYPE.value
        })

        # Select the latest row as target timestamp for incremental output.
        last_row = df.iloc[-1]

        # Use raw numpy array for efficient slicing and mean calculation.
        close_values = df[MaConstant.CLOSE_KEY.value].values
        n = len(close_values)

        # Compute SMA(150) from prior 150 closes, excluding current bar (`-1`).
        # If history is insufficient, emit NaN (caller/database policy handles it).
        sma_20 = close_values[-151:-1].mean() if n >= 151 else np.nan

        return pd.DataFrame([{
            MaConstant.SYMBOL_KEY.value: last_row[MaConstant.SYMBOL_KEY.value],
            MaConstant.TIME_KEY.value: last_row[MaConstant.TIME_KEY.value],
            MaConstant.SMA_150_KEY.value: sma_20,

        }])


    # =========================================================
    # Persistence
    # =========================================================

    def _store(self, df: pd.DataFrame, date: str):
        """
        Persist synthesized moving-average rows:
        - Skip if empty
        - Delete existing target rows in date scope
        - Bulk insert via COPY for throughput

        Operation-scale notes:
        - Delete + COPY run in one transaction.
        - `synchronous_commit = off` is used as ETL performance optimization.
        """

        # No data generated -> nothing to persist.
        if df.empty:
            return


        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:
                # Performance optimization:
                # allow faster commit behavior for this transaction scope.
                # Do not using this option for important data which can't be recovered
                cursor.execute(MaConstant.SET_SYNC_COMMIT_OFF.value)

                # Idempotent reload pattern: remove rows from date boundary onward,
                # then write fresh computed values for that same scope.
                cursor.execute(
                    MovingAverageSQLQueries.DELETE_MA_DATA,
                    (date,)
                )

                # Prepare in-memory CSV buffer to avoid row-by-row INSERT overhead.
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # High-throughput insert into moving-average target table.
                cursor.copy_expert(
                    MovingAverageSQLQueries.COPY_MA_DATA,
                    buffer
                )

                # Commit after delete + copy.
            conn.commit()

