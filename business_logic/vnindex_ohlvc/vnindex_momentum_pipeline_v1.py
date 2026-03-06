# Database connection/context manager for synchronous Postgres operations.
from config.postgre_manager import PostgresManager
from constant.constants.vnindex.vnindex_momentum_constant import MomentumConstant

# SQL templates specific to momentum pipeline operations.
from constant.sql.vnindex.vnindex_momentum_sql_queries import MomentumSQLQueries

# Utility wrapper to execute SQL and return Python-friendly results.
from util.postgre_sql import PostgresSQLUtil

# Thread pool for running symbol jobs concurrently.
from concurrent.futures import ThreadPoolExecutor, as_completed

import logging
import pandas as pd
from io import StringIO


class VnIndexMomentumPipeline:
    """
    Momentum pipeline:
    1) Pull price snapshots (depends on mode: backfill/incremental)
    2) Compute momentum features (60 days)
    3) Upsert behavior via delete-then-copy into target table
    """

    def __init__(
            self,
            max_workers: int = MomentumConstant.MAX_WORKERS.value,
            current_time: str = "",
            mode: str = ""
    ):
        # Class-scoped logger name, e.g., "MomentumPipeline".
        self.logger = logging.getLogger(self.__class__.__name__)

        # Number of worker threads used in run_all_parallel.
        self.max_workers = max_workers

        # Pipeline "effective time" used by incremental mode and delete range.
        self.current_time = current_time

        # Expected: "backfill" or "incremental".
        self.mode = mode

    # =========================================================
    # Public API
    # =========================================================

    def run(self):
        """
        Process one symbol end-to-end:
        - Pull source rows for that symbol
        - Compute momentum columns
        - Persist computed rows
        """
        try:
            self.logger.info(MomentumConstant.LOG_START.value)

            # Build momentum dataframe for this symbol.
            df = self._synthesize(self.current_time, self.mode)

            # Persist dataframe (delete existing range first, then bulk copy).
            self._store(df, self.current_time)

            self.logger.info(MomentumConstant.LOG_FINISH.value)
        except Exception as e:
            # Error is logged, not re-raised -> job continues for other symbols.
            self.logger.error(MomentumConstant.LOG_ERROR.value.format( symbol ='VNINDEX',error=e))


    # =========================================================
    # Feature Engineering
    # =========================================================

    def _synthesize(self, date: str, mode: str) -> pd.DataFrame:

        data = None

        # Backfill: fetch broad historical set for symbol.
        if mode == MomentumConstant.MODE_BACKFILL.value:
            data = PostgresSQLUtil.run_sql(
                MomentumSQLQueries.GET_DATA
            )

        # Incremental: fetch only rows relevant to current date boundary.
        elif mode == MomentumConstant.MODE_INCREMENTAL.value:
            data = PostgresSQLUtil.run_sql(
                MomentumSQLQueries.GET_DATA_BY_DATE,
            )

        # Convert row dict/list into DataFrame for vectorized computation.
        df = pd.DataFrame(data).copy()

        # Convert object columns to float64 (commonly NUMERIC from DB).
        # CAUTION: This attempts conversion for all object columns, which may fail
        # if non-numeric string columns exist in result set.
        df = df.astype({
            col: MomentumConstant.FLOAT64_DTYPE.value
            for col in df.columns
            if df[col].dtype == MomentumConstant.OBJECT_DTYPE.value
        })

        # Momentum features based on lagged closes (60 trading days).
        df[MomentumConstant.M_60D_KEY.value] = (
                df[MomentumConstant.CLOSE_KEY.value] / df[MomentumConstant.CLOSE_60_KEY.value] - MomentumConstant.ONE.value
        )

        # Keep only persistence contract columns (order matters for COPY).
        return df[
            [
                MomentumConstant.TIME_KEY.value,
                MomentumConstant.M_60D_KEY.value,
            ]
        ]

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



        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:
                # Performance optimization:
                # allow faster commit behavior for this transaction scope.
                # Do not using this option for important data which can't be recovered
                cursor.execute(MomentumConstant.SET_SYNC_COMMIT_OFF.value)

                # Remove existing rows before reloading.
                # For backfill: date is oldest bound -> broad cleanup.
                # For incremental: date is new boundary -> narrow cleanup.
                cursor.execute(
                    MomentumSQLQueries.DELETE_MOMENTUM_DATA,
                    (date,)
                )

                # Prepare CSV in-memory buffer for COPY command.
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # High-throughput insert into momentum target table.
                cursor.copy_expert(
                    MomentumSQLQueries.COPY_MOMENTUM_DATA,
                    buffer
                )

            # Commit after delete + copy.
            conn.commit()
