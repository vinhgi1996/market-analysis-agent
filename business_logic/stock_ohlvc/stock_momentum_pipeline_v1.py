# Database connection/context manager for synchronous Postgres operations.                                                                                                        
from config.postgre_manager import PostgresManager
from constant.constants.stock.stock_momentum_constant import MomentumConstant

# SQL templates specific to momentum pipeline operations.
from constant.sql.stock.stock_momentum_sql_queries import MomentumSQLQueries

# Utility wrapper to execute SQL and return Python-friendly results.
from util.postgre_sql import PostgresSQLUtil

# Thread pool for running symbol jobs concurrently.
from concurrent.futures import ThreadPoolExecutor, as_completed

import logging
import pandas as pd
from io import StringIO


class StockMomentumPipeline:
    """
    Stock momentum feature pipeline.

    Class-scale responsibilities:
    - Build per-symbol momentum features from OHLCV close prices.
    - Support two execution modes:
      1) backfill: recompute momentum across full available history.
      2) incremental: recompute only the newest point-in-time row.
    - Persist results into `stock_momentum` with delete-then-COPY semantics.
    - Provide parallel symbol orchestration via `run_all_parallel`.

    Feature definitions (trading-session approximations):
    - `m_1`:  close_t / close_{t-21}  - 1
    - `m_3`:  close_t / close_{t-63}  - 1
    - `m_6`:  close_t / close_{t-126} - 1
    - `m_12`: close_t / close_{t-252} - 1

    Output contract:
    - `symbol`, `time`, `m_1`, `m_3`, `m_6`, `m_12`

    Operational notes:
    - Early rows without sufficient lag history will naturally produce NaN values.
    - Persistence path assumes per-symbol processing (single symbol per dataframe).
    """

    def __init__(
            self,
            max_workers: int = MomentumConstant.MAX_WORKERS.value,
            symbol_queries: str = "",
            current_time: str = "",
            mode: str = ""
    ):
        # Logger is namespaced by class for pipeline-specific traceability.
        self.logger = logging.getLogger(self.__class__.__name__)

        # Worker count for concurrent symbol execution in run_all_parallel().
        self.max_workers = max_workers

        # SQL that returns symbol universe (used only in parallel entrypoint).
        self.symbol_queries = symbol_queries

        # Effective processing boundary date/time for incremental reads and deletes.
        self.current_time = current_time

        # Execution mode selector. Expected: "backfill" or "incremental".
        self.mode = mode

    # =========================================================
    # Public API
    # =========================================================

    def run(self, symbol: str):
        """
        Execute one symbol end-to-end.

        Function-scale flow:
        - Log start event for observability.
        - Build momentum features from source data (`_synthesize`).
        - Persist with scoped delete + COPY (`_store`).
        - Log finish event.

        Args:
        - symbol: ticker identifier to process.

        Error behavior:
        - Any exception is logged and swallowed so batch execution can continue
          with remaining symbols.
        """
        try:
            self.logger.info(MomentumConstant.LOG_START.value.format(symbol=symbol))

            # Build computed feature dataframe for this symbol and current mode.
            df = self._synthesize(symbol, self.current_time, self.mode)

            # Persist using idempotent refresh pattern.
            self._store(df, self.current_time)

            self.logger.info(MomentumConstant.LOG_FINISH.value.format(symbol=symbol))
        except Exception as e:
            # Error is logged, not re-raised -> job continues for other symbols.
            self.logger.error(MomentumConstant.LOG_ERROR.value.format(symbol=symbol, error=e))

    def run_all_parallel(self):
        """
        Run pipeline for all symbols returned by `self.symbol_queries`.

        Function-scale flow:
        - Query symbol universe.
        - Submit `run(symbol)` tasks to a thread pool.
        - Consume each future to re-surface hidden worker exceptions.

        Concurrency characteristics:
        - Work units are symbol-isolated (read/compute/write per symbol).
        - Threading is generally effective for IO-bound DB workloads.
        """
        # NOTE: This string appears to have encoding artifacts in current source.
        self.logger.info(MomentumConstant.LOG_PARALLEL_START.value)

        # Expected result shape: list[dict], each row includes symbol key.
        symbols = PostgresSQLUtil.run_sql(self.symbol_queries)
        symbol_list = [s[MomentumConstant.SYMBOL_KEY.value] for s in symbols]

        # Thread pool dispatch: each symbol runs independently.
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.run, symbol) for symbol in symbol_list]

            # Consume futures and re-surface unexpected thread exceptions.
            for future in as_completed(futures):
                future.result()

        # NOTE: This string appears to have encoding artifacts in current source.
        self.logger.info(MomentumConstant.LOG_PARALLEL_FINISH.value)

    # =========================================================
    # Feature Engineering
    # =========================================================

    def _synthesize(self, symbol: str, date: str, mode: str) -> pd.DataFrame:
        """
        Build momentum features for one symbol.

        Function-scale steps:
        - Select SQL source based on runtime mode.
        - Convert results to DataFrame and normalize dtypes.
        - Compute momentum across four horizons (1/3/6/12 month proxies).
        - Return output in persistence column order.

        Args:
        - symbol: ticker identifier.
        - date: processing boundary (used in incremental SQL).
        - mode: backfill or incremental selector.

        Returns:
        - DataFrame containing persistence columns only.
        """

        data = None

        # Backfill mode: fetch broad/full historical dataset for this symbol.
        if mode == MomentumConstant.MODE_BACKFILL.value:
            data = PostgresSQLUtil.run_sql(
                MomentumSQLQueries.GET_DATA_BY_SYMBOL,
                (symbol,)
            )

        # Incremental mode: fetch bounded rows up to target date.
        elif mode == MomentumConstant.MODE_INCREMENTAL.value:
            data = PostgresSQLUtil.run_sql(
                MomentumSQLQueries.GET_DATA_BY_DATE_SYMBOL,
                (symbol, date)
            )

        # Materialize SQL result for vectorized arithmetic operations.
        df = pd.DataFrame(data).copy()

        # Normalize numeric-like object columns (commonly DB NUMERIC) to float64.
        # CAUTION: conversion will fail if any object column is non-numeric text.
        df = df.astype({
            col: MomentumConstant.FLOAT64_DTYPE.value
            for col in df.columns
            if df[col].dtype == MomentumConstant.OBJECT_DTYPE.value
        })

        # Momentum features from lagged close ratios:
        # Each metric is return over lag window: (close_t / close_{t-k}) - 1.
        # Lag windows approximate 1/3/6/12 months using trading sessions.
        df[MomentumConstant.M_1_KEY.value] = (
                df[MomentumConstant.CLOSE_KEY.value] / df[MomentumConstant.CLOSE_21_KEY.value] - MomentumConstant.ONE.value
        )
        df[MomentumConstant.M_3_KEY.value] = (
            df[MomentumConstant.CLOSE_KEY.value] / df[MomentumConstant.CLOSE_63_KEY.value] - MomentumConstant.ONE.value
        )
        df[MomentumConstant.M_6_KEY.value] = (
            df[MomentumConstant.CLOSE_KEY.value] / df[MomentumConstant.CLOSE_126_KEY.value] - MomentumConstant.ONE.value
        )
        df[MomentumConstant.M_12_KEY.value] = (
            df[MomentumConstant.CLOSE_KEY.value] / df[MomentumConstant.CLOSE_252_KEY.value] - MomentumConstant.ONE.value
        )

        # Optional equal-weight composite momentum score.
        # Kept commented intentionally to preserve current table contract.
        # df[MomentumConstant.M_COMPOSITE_KEY.value] = (
        #     df[MomentumConstant.M_3_KEY.value] + df[MomentumConstant.M_6_KEY.value] + df[MomentumConstant.M_12_KEY.value]
        # ) / MomentumConstant.COMPOSITE_DIVISOR.value

        # Keep only persistence contract columns (order matters for COPY).
        return df[
            [
                MomentumConstant.SYMBOL_KEY.value,
                MomentumConstant.TIME_KEY.value,
                MomentumConstant.M_1_KEY.value,
                MomentumConstant.M_3_KEY.value,
                MomentumConstant.M_6_KEY.value,
                MomentumConstant.M_12_KEY.value,
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

        Transaction behavior:
        - DELETE and COPY run in one transaction.
        - Commit happens after both operations complete.
        - If an exception occurs before commit, transaction is rolled back.

        Idempotency:
        - Re-running for same symbol/date scope yields consistent final state
          because scoped rows are deleted before fresh insert.
        """

        # No output rows means nothing to persist.
        if df.empty:
            return

        # Per-symbol processing contract: dataframe contains exactly one symbol.
        symbol = df[MomentumConstant.SYMBOL_KEY.value].iloc[0]

        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:
                # ETL performance optimization:
                # disable synchronous commit for faster write throughput.
                # Safe only when data can be recomputed in recovery scenarios.
                cursor.execute(MomentumConstant.SET_SYNC_COMMIT_OFF.value)

                # Step 1: clear symbol rows from boundary date onward.
                cursor.execute(
                    MomentumSQLQueries.DELETE_MOMENTUM_DATA,
                    (symbol, date)
                )

                # Step 2: serialize dataframe to in-memory CSV for COPY.
                # `header=False` because COPY statement already defines column order.
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # Step 3: bulk insert into target table using COPY for throughput.
                cursor.copy_expert(
                    MomentumSQLQueries.COPY_MOMENTUM_DATA,
                    buffer
                )

            # Step 4: finalize atomic refresh.
            conn.commit()
