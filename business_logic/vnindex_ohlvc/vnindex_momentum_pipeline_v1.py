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
    VNINDEX momentum feature pipeline.

    Class-scale responsibilities:
    - Load VNINDEX price inputs for either full rebuild (`backfill`) or
      point-in-time recomputation (`incremental`).
    - Compute 60-session momentum (`m_60d`) from close and lagged close.
    - Persist output into `vnindex_momentum` using delete-then-COPY semantics.

    Output contract:
    - `time`
    - `m_60d` = close_t / close_{t-60} - 1
    """

    def __init__(
            self,
            max_workers: int = MomentumConstant.MAX_WORKERS.value,
            current_time: str = "",
            mode: str = ""
    ):
        # Logger is namespaced by class for pipeline-specific tracing.
        self.logger = logging.getLogger(self.__class__.__name__)

        # Reserved concurrency config for broader orchestrations.
        self.max_workers = max_workers

        # Effective processing timestamp/date boundary.
        self.current_time = current_time

        # Execution mode selector. Expected: "backfill" or "incremental".
        self.mode = mode

    # =========================================================
    # Public API
    # =========================================================

    def run(self):
        """
        Pipeline entry point.

        Function-scale flow:
        - Log start.
        - Synthesize momentum features for the configured mode/time.
        - Persist results with scoped delete + bulk copy.
        - Log finish or log error.
        """
        try:
            self.logger.info(MomentumConstant.LOG_START.value)

            # Build momentum dataframe for VNINDEX.
            df = self._synthesize(self.current_time, self.mode)

            # Persist using idempotent reload pattern.
            self._store(df, self.current_time)

            self.logger.info(MomentumConstant.LOG_FINISH.value)
        except Exception as e:
            # Error is logged and swallowed to keep orchestrator resilient.
            self.logger.error(MomentumConstant.LOG_ERROR.value.format( symbol ='VNINDEX',error=e))


    # =========================================================
    # Feature Engineering
    # =========================================================

    def _synthesize(self, date: str, mode: str) -> pd.DataFrame:
        """
        Create momentum dataframe from source history.

        Function-scale steps:
        - Choose SQL source by mode (backfill vs incremental).
        - Materialize DataFrame and normalize numeric dtypes.
        - Compute `m_60d` from close and 60-session lagged close.
        - Return only persistence columns in COPY-compatible order.
        """

        data = None

        # Backfill: fetch full VNINDEX history for complete recomputation.
        if mode == MomentumConstant.MODE_BACKFILL.value:
            data = PostgresSQLUtil.run_sql(
                MomentumSQLQueries.GET_DATA
            )

        # Incremental: fetch bounded history needed for latest-date computation.
        elif mode == MomentumConstant.MODE_INCREMENTAL.value:
            data = PostgresSQLUtil.run_sql(
                MomentumSQLQueries.GET_DATA_BY_DATE,
            )

        # Convert row dict/list into DataFrame for vectorized computation.
        df = pd.DataFrame(data).copy()

        # Convert numeric-like object columns (common from DB NUMERIC) to float64.
        # CAUTION: Non-numeric object columns will raise during astype conversion.
        df = df.astype({
            col: MomentumConstant.FLOAT64_DTYPE.value
            for col in df.columns
            if df[col].dtype == MomentumConstant.OBJECT_DTYPE.value
        })

        # 21-session momentum: relative change from close_{t-21} to close_t.
        df[MomentumConstant.M_21D_KEY.value] = (
                df[MomentumConstant.CLOSE_KEY.value] / df[MomentumConstant.CLOSE_21_KEY.value] - MomentumConstant.ONE.value
        )

        # 60-session momentum: relative change from close_{t-60} to close_t.
        df[MomentumConstant.M_60D_KEY.value] = (
                df[MomentumConstant.CLOSE_KEY.value] / df[MomentumConstant.CLOSE_60_KEY.value] - MomentumConstant.ONE.value
        )

        # 126-session momentum: relative change from close_{t-126} to close_t.
        df[MomentumConstant.M_126D_KEY.value] = (
                df[MomentumConstant.CLOSE_KEY.value] / df[MomentumConstant.CLOSE_126_KEY.value] - MomentumConstant.ONE.value
        )

        # Keep only persistence contract columns (order matters for COPY).
        return df[
            [
                MomentumConstant.TIME_KEY.value,
                MomentumConstant.M_21D_KEY.value,
                MomentumConstant.M_60D_KEY.value,
                MomentumConstant.M_126D_KEY.value,
            ]
        ]

    # =========================================================
    # Persistence
    # =========================================================

    def _store(self, df: pd.DataFrame, date: str):
        """
        Persist synthesized momentum rows:
        - Skip if empty
        - Delete existing target rows in scope
        - Bulk insert via COPY for throughput

        Operation-scale notes:
        - Delete and COPY execute in one transaction.
        - `synchronous_commit = off` is used to speed up recoverable ETL writes.
        """

        # No generated output means no DB operations are required.
        if df.empty:
            return



        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:
                # Performance optimization:
                # allow faster commit behavior for this transaction scope.
                # Do not using this option for important data which can't be recovered
                cursor.execute(MomentumConstant.SET_SYNC_COMMIT_OFF.value)

                # Idempotent refresh: clear affected date range before COPY insert.
                cursor.execute(
                    MomentumSQLQueries.DELETE_MOMENTUM_DATA,
                    (date,)
                )

                # Serialize dataframe to in-memory CSV for high-throughput COPY.
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
