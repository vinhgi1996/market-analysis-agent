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
    """
    Stock moving-average feature pipeline.

    Class-scale responsibilities:
    - Compute lagged simple moving averages per symbol:
      `sma_20`, `sma_50`, `sma_200`.
    - Support both execution modes:
      1) backfill: full-history recompute.
      2) incremental: latest-row recompute at/near `current_time`.
    - Persist output into `stock_moving_average` using a scoped
      delete-then-COPY strategy for idempotent reload behavior.
    - Provide parallel symbol execution via `run_all_parallel`.

    Output contract:
    - `symbol`, `time`, `sma_20`, `sma_50`, `sma_200`

    Important convention:
    - All SMAs are shifted by one session (use history up to T-1 for row at time T),
      which avoids look-ahead bias for downstream signal logic.
    """

    def __init__(
            self,
            max_workers: int = MaConstant.MAX_WORKERS.value,
            symbol_queries: str = "",
            current_time: str = "",
            mode: str = ""
    ):
        # Logger is namespaced by class for pipeline-scoped observability.
        self.logger = logging.getLogger(self.__class__.__name__)

        # Worker count used by ThreadPoolExecutor in `run_all_parallel`.
        self.max_workers = max_workers

        # SQL statement used to load symbol universe for batch processing.
        self.symbol_queries = symbol_queries

        # Effective processing boundary used in incremental reads and delete scope.
        self.current_time = current_time

        # Mode selector. Expected values: "backfill" or "incremental".
        self.mode = mode

    # =========================================================
    # Public API
    # =========================================================

    def run(self, symbol: str):
        """
        Execute moving-average computation for a single symbol.

        Function-scale flow:
        - Log start.
        - Build feature dataframe based on configured mode.
        - Persist using scoped delete + COPY.
        - Log finish.

        Error behavior:
        - Exceptions are logged and swallowed to keep multi-symbol runs resilient.
        """

        try:
            self.logger.info(
                MaConstant.LOG_START.value.format(symbol=symbol)
            )

            df = None

            if self.mode == MaConstant.MODE_BACKFILL.value:
                df = self._backfill_synthesize(symbol)
            elif self.mode == MaConstant.MODE_INCREMENTAL.value:
                df = self._incremental_synthesize(symbol,self.current_time)

            # Persist with idempotent refresh pattern (delete scoped rows, then COPY).
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
        Run pipeline for all symbols returned by `self.symbol_queries`.

        Function-scale flow:
        - Query symbol universe.
        - Dispatch one worker per symbol (up to `max_workers`).
        - Re-surface worker exceptions through `future.result()`.

        Operational notes:
        - Work units are symbol-isolated (read/compute/write per symbol).
        - Suitable for IO-heavy workloads dominated by DB calls.
        """
        # NOTE: This string appears to have encoding artifacts in current source.
        self.logger.info(MaConstant.LOG_PARALLEL_START.value)

        # Expected query shape: list[dict] with at least symbol field present.
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
        """
        Build full-history SMA output for a symbol.

        Function-scale steps:
        - Load complete symbol price history.
        - Normalize numeric-like dtypes for stable arithmetic.
        - Compute lagged SMA(20/50/200) across entire timeline.
        - Return persistence columns in exact COPY order.

        Input assumptions:
        - Source rows are time-ordered ascending from SQL.
        - Source includes at least `symbol`, `time`, `close`.
        """

        # Backfill reads broad/full available history for the symbol.
        data = PostgresSQLUtil.run_sql(
            MovingAverageSQLQueries.GET_DATA_BY_SYMBOL,
            (symbol,)
        )

        # Convert DB rows into DataFrame for vectorized rolling computations.
        df = pd.DataFrame(data).copy()

        # Normalize numeric-like object columns (e.g., DB NUMERIC) to float64.
        # CAUTION: Any non-numeric object column included here would raise on cast.
        df = df.astype({
            col: MaConstant.FLOAT64_DTYPE.value
            for col in df.columns
            if df[col].dtype == MaConstant.OBJECT_DTYPE.value
        })

        # Lagged SMA(20): average of closes over prior 20 sessions (exclude current).
        # .shift(1) ensures value at T only uses data through T-1.
        df[MaConstant.SMA_20_KEY.value] = (
            df[MaConstant.CLOSE_KEY.value]
            .rolling(MaConstant.SMA_20_WINDOW.value)
            .mean()
            .shift(1)
        )
        # Lagged SMA(50): same convention as SMA(20), larger smoothing window.
        df[MaConstant.SMA_50_KEY.value] = (
            df[MaConstant.CLOSE_KEY.value]
            .rolling(MaConstant.SMA_50_WINDOW.value)
            .mean()
            .shift(1)
        )
        # Lagged SMA(200): long-term trend baseline used in many regime filters.
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
        """
        Build one-row SMA snapshot for incremental processing.

        Function-scale steps:
        - Load bounded recent history ending at `date` (query handles boundary).
        - Normalize numeric-like columns.
        - Compute lagged SMA values only for the newest row.
        - Return single-row dataframe matching persistence schema.

        Why bounded history:
        - Only latest row is needed for incremental writes.
        - Maximum lookback is 200 sessions; query includes enough buffer.
        """

        # Query returns a recent historical slice for one symbol up to target date.
        data = PostgresSQLUtil.run_sql(
            MovingAverageSQLQueries.GET_DATA_BY_DATE_SYMBOL,
            (symbol,date)
        )

        # Materialize into DataFrame for consistent indexing/slicing.
        df = pd.DataFrame(data).copy()

        # Cast numeric-like object columns to float64 for deterministic math behavior.
        df = df.astype({
            col: MaConstant.FLOAT64_DTYPE.value
            for col in df.columns
            if df[col].dtype == MaConstant.OBJECT_DTYPE.value
        })

        # Latest row is the incremental target timestamp.
        last_row = df.iloc[-1]

        # Convert close series to numpy array for fast fixed-window slicing.
        close_values = df[MaConstant.CLOSE_KEY.value].values
        n = len(close_values)

        # Compute lagged SMAs by excluding current close (slice ends at -1).
        # If history is shorter than required window+1, return NaN for that metric.
        sma_20 = close_values[-21:-1].mean() if n >= 21 else np.nan
        sma_50 = close_values[-51:-1].mean() if n >= 51 else np.nan
        sma_200 = close_values[-201:-1].mean() if n >= 201 else np.nan

        # Return exactly one output row aligned to persistence column contract.
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
        Persist synthesized moving-average rows:
        - Skip if empty
        - Delete existing target rows in scope
        - Bulk insert via COPY for speed

        Transaction semantics:
        - DELETE and COPY run in one DB transaction.
        - Commit occurs only after both steps succeed.
        - Failure before commit rolls back both operations together.

        Idempotency:
        - Re-running same symbol/date scope leads to consistent end state because
          old scoped rows are removed before inserting fresh computation output.
        """

        # No output rows means no DB operation needed.
        if df.empty:
            return

        # Per-symbol run contract: dataframe should contain exactly one symbol.
        symbol = df[MaConstant.SYMBOL_KEY.value].iloc[0]

        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:
                # Performance optimization for recoverable ETL workloads:
                # relax commit durability guarantees for faster write throughput.
                cursor.execute(MaConstant.SET_SYNC_COMMIT_OFF.value)

                # Step 1: delete existing rows in symbol+date scope.
                cursor.execute(
                    MovingAverageSQLQueries.DELETE_MA_DATA,
                    (symbol, date)
                )

                # Step 2: serialize dataframe into in-memory CSV buffer.
                # header=False because COPY query defines the destination column order.
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # Step 3: bulk insert into moving-average target table.
                cursor.copy_expert(
                    MovingAverageSQLQueries.COPY_MA_DATA,
                    buffer
                )

                # Step 4: commit atomic refresh.
            conn.commit()
