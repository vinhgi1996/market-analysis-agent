import logging

import numpy as np

from config.postgre_manager import PostgresManager

from constant.constants.analysis.vnindex_regime_filter_constant import VnIndexRegimeFilterConstant
from constant.sql.analysis.vnindex_regime_filter_sql_queries import VnIndexRegimeFilterSQLQueries
from util.pandas_util import PandasUtil
from util.postgre_sql import PostgresSQLUtil

import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
from io import StringIO

class VnIndexRegimeFilterPipeline:
    """
    VNINDEX regime-filter pipeline.

    Class-scale responsibilities:
    - Combine upstream VNINDEX features (price, momentum, moving average, drawdown,
      volatility) into a binary risk regime suggestion.
    - Produce a persistent, daily state record that includes:
      - threshold values used for decisioning,
      - current `suggestion` label,
      - `positive_streak` state for consecutive SAFE periods.
    - Support two runtime modes:
      1) backfill: recompute full historical regime series.
      2) incremental: compute only the latest row at the processing boundary.
    - Persist output into `vnindex_regime_filter` using delete-then-COPY semantics.

    Regime logic (high level):
    - SAFE when all conditions are true:
      - close > sma_150d
      - m_60d > m_60d_threshold
      - drawdown_40d > drawdown_40d_threshold
      - vol_20d < vol_20d_threshold
    - Otherwise DEFENSIVE.

    Output contract:
    - `time`, `m_60d_threshold`, `drawdown_40d_threshold`,
      `vol_20d_threshold`, `suggestion`, `positive_streak`
    """

    def __init__(
            self,
            max_workers: int = 5,
            symbol_queries: str = "",
            current_time: str = "",
            mode: str = ""
    ):
        # Logger is class-scoped for pipeline-specific tracing.
        self.logger = logging.getLogger(self.__class__.__name__)

        # Reserved concurrency setting for broader orchestrations.
        self.max_workers = max_workers

        # Reserved query text for caller-managed universes (not used directly here).
        self.symbol_queries = symbol_queries

        # Effective processing date/time; used by incremental read + scoped delete.
        self.current_time = current_time

        # Mode selector. Expected values: "backfill" or "incremental".
        self.mode = mode

    # =========================================================
    # Public API
    # =========================================================

    def run(self):
        """
        Main pipeline entrypoint.

        Function-scale flow:
        - Log start.
        - Select synthesis strategy by mode.
        - Persist synthesized dataframe.
        - Log finish.

        Error handling:
        - Exceptions are logged and not re-raised, keeping batch orchestration resilient.
        """

        try:
            self.logger.info(
                VnIndexRegimeFilterConstant.LOG_START.value
            )

            df = None

            if self.mode == VnIndexRegimeFilterConstant.MODE_BACKFILL.value:
                df = self._backfill_synthesize()
            elif self.mode == VnIndexRegimeFilterConstant.MODE_INCREMENTAL.value:
                df = self._incremental_synthesize(self.current_time)
            # Persist with idempotent refresh pattern (delete scope then COPY).
            self._store(df, self.current_time)

            self.logger.info(
                VnIndexRegimeFilterConstant.LOG_FINISH.value
            )
        except Exception as e:
            # Error is logged, not re-raised -> job continues for other symbols.
            self.logger.error(
                VnIndexRegimeFilterConstant.LOG_ERROR.value.format(symbol='VNINDEX', error=e)
            )

    # =========================================================
    # Feature Engineering
    # =========================================================

    def _backfill_synthesize(self) -> pd.DataFrame:
        """
        Build full-history regime-filter output.

        Function-scale steps:
        - Read fully joined upstream feature dataset from SQL.
        - Normalize numeric-like columns for vectorized comparisons.
        - Evaluate SAFE/DEFENSIVE condition mask for each row.
        - Compute `positive_streak` across the full series.
        - Return persistence columns in COPY-compatible order.
        """

        # Backfill mode reads broad historical feature set for VNINDEX.
        data = PostgresSQLUtil.run_sql(
            VnIndexRegimeFilterSQLQueries.GET_DATA,
        )

        # Materialize SQL rows into DataFrame for vectorized logic.
        df = pd.DataFrame(data).copy()

        # Convert numeric-like object columns (common DB NUMERIC) to float64.
        # CAUTION: conversion fails if any object column contains non-numeric values.
        df = PandasUtil.cast_object_columns_to_float64(df)

        # Regime decision mask:
        # SAFE only if all four conditions hold simultaneously.
        # Numpy arrays are used for efficient vectorized boolean evaluation.
        cond = (
                (df[VnIndexRegimeFilterConstant.CLOSE_KEY.value].to_numpy() > df[VnIndexRegimeFilterConstant.SOURCE_SMA_150D.value].to_numpy()) &
                (df[VnIndexRegimeFilterConstant.SOURCE_M_60D.value].to_numpy() > VnIndexRegimeFilterConstant.M_60D_THRESHOLD_VALUE.value) &
                (df[VnIndexRegimeFilterConstant.SOURCE_DRAWDOWN_40D.value].to_numpy() > VnIndexRegimeFilterConstant.DRAWDOWN_40D_THRESHOLD_VALUE.value) &
                (df[VnIndexRegimeFilterConstant.SOURCE_VOL_20D.value].to_numpy() < VnIndexRegimeFilterConstant.VOL_20D_THRESHOLD_VALUE.value)
        )

        # Map condition mask to categorical regime label.
        df[VnIndexRegimeFilterConstant.SUGGESTION.value] = np.where(
            cond,
            VnIndexRegimeFilterConstant.SUGGESTION_SAFE.value,
            VnIndexRegimeFilterConstant.SUGGESTION_DEFENSIVE.value
        )
        # Persist threshold values alongside outputs for auditability/reproducibility.
        df[VnIndexRegimeFilterConstant.M_60D_THRESHOLD.value] = VnIndexRegimeFilterConstant.M_60D_THRESHOLD_VALUE.value
        df[VnIndexRegimeFilterConstant.DRAWDOWN_40D_THRESHOLD.value] = VnIndexRegimeFilterConstant.DRAWDOWN_40D_THRESHOLD_VALUE.value
        df[VnIndexRegimeFilterConstant.VOL_20D_THRESHOLD.value] = VnIndexRegimeFilterConstant.VOL_20D_THRESHOLD_VALUE.value

        # Positive streak logic over full history:
        # - Identify SAFE rows.
        # - Create groups split by non-SAFE rows via cumulative sum over (~safe).
        # - Cumulative sum within each group yields consecutive SAFE count.
        safe = df[VnIndexRegimeFilterConstant.SUGGESTION.value].eq(
            VnIndexRegimeFilterConstant.SUGGESTION_SAFE.value
        )

        df[VnIndexRegimeFilterConstant.POSITIVE_STREAK.value] = VnIndexRegimeFilterConstant.ZERO.value
        df.loc[safe, VnIndexRegimeFilterConstant.POSITIVE_STREAK.value] = safe.groupby((~safe).cumsum()).cumsum()

        # Keep only persistence contract columns (order matters for COPY).
        return df[
            [
                VnIndexRegimeFilterConstant.TIME_KEY.value,
                VnIndexRegimeFilterConstant.M_60D_THRESHOLD.value,
                VnIndexRegimeFilterConstant.DRAWDOWN_40D_THRESHOLD.value,
                VnIndexRegimeFilterConstant.VOL_20D_THRESHOLD.value,
                VnIndexRegimeFilterConstant.SUGGESTION.value,
                VnIndexRegimeFilterConstant.POSITIVE_STREAK.value,
            ]
        ]


    def _incremental_synthesize(self, date: str) -> pd.DataFrame:
        """
        Build one-row regime-filter snapshot for incremental mode.

        Function-scale steps:
        - Load latest joined feature row up to `date`.
        - Load previous stored `positive_streak`.
        - Evaluate SAFE/DEFENSIVE for current row.
        - Advance/reset streak state.
        - Return one-row dataframe with output schema.

        Failure condition:
        - Raises ValueError when required feature row or previous streak is missing.
        """
        # Latest feature snapshot up to boundary date (inclusive by SQL boundary logic).
        data = PostgresSQLUtil.run_sql(
            VnIndexRegimeFilterSQLQueries.GET_DATA_BY_DATE,
            (date,)
        )

        # Previous streak state needed to continue incremental streak progression.
        previous_streak = PostgresSQLUtil.run_sql(
            VnIndexRegimeFilterSQLQueries.GET_PREVIOUS_POSITIVE_STREAK,
            (date,)
        )

        # Incremental computation requires both current features and prior state.
        if not data or not previous_streak:
            raise ValueError(VnIndexRegimeFilterConstant.ERROR_INSUFFICIENT_INCREMENTAL_DATA.value)

        # SQL returns latest point; keep explicit index access for clarity.
        row = data[-1]
        prev_streak = previous_streak[-1][VnIndexRegimeFilterConstant.POSITIVE_STREAK.value]

        # Cast numerics defensively (DB adapters may return Decimal).
        close = float(row[VnIndexRegimeFilterConstant.CLOSE_KEY.value])
        sma_150d = float(row[VnIndexRegimeFilterConstant.SOURCE_SMA_150D.value])
        m_60d = float(row[VnIndexRegimeFilterConstant.SOURCE_M_60D.value])
        drawdown_40d = float(row[VnIndexRegimeFilterConstant.SOURCE_DRAWDOWN_40D.value])
        vol_20d = float(row[VnIndexRegimeFilterConstant.SOURCE_VOL_20D.value])

        # Regime decision logic for the current snapshot.
        is_safe = (
                close > sma_150d and
                m_60d > VnIndexRegimeFilterConstant.M_60D_THRESHOLD_VALUE.value and
                drawdown_40d > VnIndexRegimeFilterConstant.DRAWDOWN_40D_THRESHOLD_VALUE.value and
                vol_20d < VnIndexRegimeFilterConstant.VOL_20D_THRESHOLD_VALUE.value
        )

        # Map boolean decision into output label.
        suggestion = (
            VnIndexRegimeFilterConstant.SUGGESTION_SAFE.value
            if is_safe
            else VnIndexRegimeFilterConstant.SUGGESTION_DEFENSIVE.value
        )

        # Stateful streak update:
        # - SAFE => continue streak by +1
        # - DEFENSIVE => reset to 0
        positive_streak = (
            prev_streak + VnIndexRegimeFilterConstant.ONE.value
            if is_safe
            else VnIndexRegimeFilterConstant.ZERO.value
        )

        # Return single-row dataframe aligned with persistence contract.
        return pd.DataFrame([{
            VnIndexRegimeFilterConstant.TIME_KEY.value: row[VnIndexRegimeFilterConstant.TIME_KEY.value],
            VnIndexRegimeFilterConstant.M_60D_THRESHOLD.value: VnIndexRegimeFilterConstant.M_60D_THRESHOLD_VALUE.value,
            VnIndexRegimeFilterConstant.DRAWDOWN_40D_THRESHOLD.value: VnIndexRegimeFilterConstant.DRAWDOWN_40D_THRESHOLD_VALUE.value,
            VnIndexRegimeFilterConstant.VOL_20D_THRESHOLD.value: VnIndexRegimeFilterConstant.VOL_20D_THRESHOLD_VALUE.value,
            VnIndexRegimeFilterConstant.SUGGESTION.value: suggestion,
            VnIndexRegimeFilterConstant.POSITIVE_STREAK.value: positive_streak,
        }])

    # =========================================================
    # Persistence
    # =========================================================

    def _store(self, df: pd.DataFrame, date: str):
        """
        Persist regime-filter dataframe:
        - Skip if empty
        - Delete existing target rows in scope
        - Bulk insert via COPY for throughput

        Transaction semantics:
        - DELETE + COPY run in one transaction.
        - Commit occurs only after both operations succeed.
        - On failure, DB context manager rolls back uncommitted changes.

        Idempotency:
        - Re-running the same date range yields stable final state because
          previous scoped rows are removed before inserting recalculated output.
        """

        # No synthesized rows => no persistence work.
        if df.empty:
            return

        # VNINDEX pipeline is single-series; symbol dimension is implicit.

        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:
                # ETL performance optimization:
                # reduce commit latency for recoverable/recomputable workloads.
                cursor.execute(VnIndexRegimeFilterConstant.SET_SYNC_COMMIT_OFF.value)

                # Step 1: clear affected date scope before writing refreshed rows.
                cursor.execute(
                    VnIndexRegimeFilterSQLQueries.DELETE_REGIME_DATA,
                    (date,)
                )

                # Step 2: serialize dataframe into in-memory CSV for COPY.
                # header=False because COPY SQL explicitly declares destination columns.
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # Step 3: bulk insert into regime target table.
                cursor.copy_expert(
                    VnIndexRegimeFilterSQLQueries.COPY_REGIME_DATA,
                    buffer
                )

                # Step 4: finalize atomic refresh (delete + copy).
            conn.commit()

