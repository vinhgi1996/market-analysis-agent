import logging

import numpy as np

from config.postgre_manager import PostgresManager
from constant.constants.analysis.stock_structural_strength_filter_constant import StockStructuralStrengthFilterConstant
from constant.constants.stock.stock_moving_average_constant import MaConstant
from constant.sql.analysis.stock_structural_strength_filter_sql_queries import StockStructuralStrengthFilterSQLQueries
from util.pandas_util import PandasUtil
from util.postgre_sql import PostgresSQLUtil
# Thread pool for running symbol jobs concurrently.
from concurrent.futures import ThreadPoolExecutor, as_completed


import pandas as pd
from io import StringIO

class StockStructuralStrengthPipelineV2:

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

        try:
            self.logger.info(
                StockStructuralStrengthFilterConstant.LOG_START.value.format(symbol=symbol)
            )

            df = None

            if self.mode == StockStructuralStrengthFilterConstant.MODE_BACKFILL.value:
                df = self._backfill_synthesize(symbol)
            elif self.mode == StockStructuralStrengthFilterConstant.MODE_INCREMENTAL.value:
                df = self._incremental_synthesize(symbol,self.current_time)

            # Persist with idempotent refresh pattern (delete scoped rows, then COPY).
            self._store(df, self.current_time)

            self.logger.info(
                StockStructuralStrengthFilterConstant.LOG_FINISH.value.format(symbol=symbol)
            )
        except Exception as e:
            # Error is logged, not re-raised -> job continues for other symbols.
            self.logger.error(
                StockStructuralStrengthFilterConstant.LOG_ERROR.value.format(symbol=symbol, error=e)
            )

    def run_all_parallel(self):

        # NOTE: This string appears to have encoding artifacts in current source.
        self.logger.info(StockStructuralStrengthFilterConstant.LOG_PARALLEL_START.value)

        # Expected query shape: list[dict] with at least symbol field present.
        symbols = PostgresSQLUtil.run_sql(self.symbol_queries)
        symbol_list = [s[StockStructuralStrengthFilterConstant.SYMBOL_KEY.value] for s in symbols]

        # Thread pool dispatch: each symbol runs independently.
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.run, symbol) for symbol in symbol_list]

            # Consume futures and re-surface unexpected thread exceptions.
            for future in as_completed(futures):
                future.result()

                # NOTE: This string appears to have encoding artifacts in current source.
        self.logger.info(StockStructuralStrengthFilterConstant.LOG_PARALLEL_FINISH.value)

    # =========================================================
    # Feature Engineering
    # =========================================================

    def _backfill_synthesize(self, symbol:str) -> pd.DataFrame:
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
                StockStructuralStrengthFilterSQLQueries.GET_DATA_BY_SYMBOL,(symbol,)
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
                (df[StockStructuralStrengthFilterConstant.CLOSE_KEY.value].to_numpy() > df[StockStructuralStrengthFilterConstant.SOURCE_SMA_50.value].to_numpy()) &
                #(df[StockStructuralStrengthFilterConstant.SOURCE_SMA_20.value].to_numpy() > df[StockStructuralStrengthFilterConstant.SOURCE_SMA_50.value].to_numpy()) &
                (df[StockStructuralStrengthFilterConstant.SOURCE_M_3.value].to_numpy() > StockStructuralStrengthFilterConstant.M_3_THRESHOLD_VALUE.value) &
                #(df[StockStructuralStrengthFilterConstant.SOURCE_M_1.value].to_numpy() > StockStructuralStrengthFilterConstant.M_1_THRESHOLD_VALUE.value) &
                #(df[StockStructuralStrengthFilterConstant.SOURCE_RSI_14.value].to_numpy() < StockStructuralStrengthFilterConstant.RSI_14_THRESHOLD_VALUE.value) &
                (df[StockStructuralStrengthFilterConstant.SOURCE_VOL_20D_PCT_126.value].to_numpy() < StockStructuralStrengthFilterConstant.VOL_20D_PCT_126_THRESHOLD_VALUE.value) &
                (df[StockStructuralStrengthFilterConstant.SOURCE_ATR_14_W.value].to_numpy()/df[StockStructuralStrengthFilterConstant.CLOSE_KEY.value].to_numpy()
                                                                                          < StockStructuralStrengthFilterConstant.ATR_14_CLOSE_THRESHOLD_VALUE.value) &
                (df[StockStructuralStrengthFilterConstant.SOURCE_VOLUME_RATIO.value].to_numpy() < StockStructuralStrengthFilterConstant.VOLUME_RATIO_THRESHOLD_VALUE.value)
        )



        # Map condition mask to categorical regime label.
        df[StockStructuralStrengthFilterConstant.SUGGESTION.value] = np.where(
            cond,
            StockStructuralStrengthFilterConstant.SUGGESTION_POSSIBLE.value,
            StockStructuralStrengthFilterConstant.SUGGESTION_AVOID.value
        )


        # Persist threshold values alongside outputs for auditability/reproducibility.
        df[StockStructuralStrengthFilterConstant.M_3_THRESHOLD.value] = StockStructuralStrengthFilterConstant.M_3_THRESHOLD_VALUE.value
        df[StockStructuralStrengthFilterConstant.M_1_THRESHOLD.value] = StockStructuralStrengthFilterConstant.M_1_THRESHOLD_VALUE.value
        df[StockStructuralStrengthFilterConstant.RSI_14_THRESHOLD.value] = StockStructuralStrengthFilterConstant.RSI_14_THRESHOLD_VALUE.value
        df[StockStructuralStrengthFilterConstant.VOL_20D_PCT_126_THRESHOLD.value] = StockStructuralStrengthFilterConstant.VOL_20D_PCT_126_THRESHOLD_VALUE.value
        df[StockStructuralStrengthFilterConstant.ATR_14_CLOSE_THRESHOLD.value] = StockStructuralStrengthFilterConstant.ATR_14_CLOSE_THRESHOLD_VALUE.value
        df[StockStructuralStrengthFilterConstant.VOLUME_RATIO_THRESHOLD.value] = StockStructuralStrengthFilterConstant.VOLUME_RATIO_THRESHOLD_VALUE.value


        # Positive streak logic over full history:
        # - Identify POSSIBLE rows.
        # - Create groups split by non-POSSIBLE rows via cumulative sum over (~possible).
        # - Cumulative sum within each group yields consecutive POSSIBLE count.
        possible = df[StockStructuralStrengthFilterConstant.SUGGESTION.value].eq(
            StockStructuralStrengthFilterConstant.SUGGESTION_POSSIBLE.value
        )

        df[StockStructuralStrengthFilterConstant.POSITIVE_STREAK.value] = StockStructuralStrengthFilterConstant.ZERO.value
        df.loc[possible, StockStructuralStrengthFilterConstant.POSITIVE_STREAK.value] = possible.groupby((~possible).cumsum()).cumsum()

        # Keep only persistence contract columns (order matters for COPY).
        return df[
            [
                StockStructuralStrengthFilterConstant.SYMBOL_KEY.value,
                StockStructuralStrengthFilterConstant.TIME_KEY.value,
                StockStructuralStrengthFilterConstant.M_3_THRESHOLD.value,
                StockStructuralStrengthFilterConstant.M_1_THRESHOLD.value,
                StockStructuralStrengthFilterConstant.RSI_14_THRESHOLD.value,
                StockStructuralStrengthFilterConstant.VOL_20D_PCT_126_THRESHOLD.value,
                StockStructuralStrengthFilterConstant.ATR_14_CLOSE_THRESHOLD.value,
                StockStructuralStrengthFilterConstant.VOLUME_RATIO_THRESHOLD.value,
                StockStructuralStrengthFilterConstant.SUGGESTION.value,
                StockStructuralStrengthFilterConstant.POSITIVE_STREAK.value,
            ]
        ]

    def _incremental_synthesize(self, symbol: str, date: str) -> pd.DataFrame:

        # Query returns a recent historical slice for one symbol up to target date.
        data = PostgresSQLUtil.run_sql(
            StockStructuralStrengthFilterSQLQueries.GET_DATA_BY_SYMBOL_DATE,
            (symbol,date)
        )

        # Previous streak state needed to continue incremental streak progression.
        previous_streak = PostgresSQLUtil.run_sql(
            StockStructuralStrengthFilterSQLQueries.GET_PREVIOUS_POSITIVE_STREAK,
            (symbol,date)
        )

        # Incremental computation requires both current features and prior state.
        if not data or not previous_streak:
            raise ValueError(StockStructuralStrengthFilterConstant.ERROR_INSUFFICIENT_INCREMENTAL_DATA.value)


        # SQL returns latest point; keep explicit index access for clarity.
        row = data[-1]
        prev_streak = previous_streak[-1][StockStructuralStrengthFilterConstant.POSITIVE_STREAK.value]

        # Cast numerics defensively (DB adapters may return Decimal).
        close = float(row[StockStructuralStrengthFilterConstant.CLOSE_KEY.value])
        sma_50 = float(row[StockStructuralStrengthFilterConstant.SOURCE_SMA_50.value])
        sma_20 = float(row[StockStructuralStrengthFilterConstant.SOURCE_SMA_20.value])
        m_1 = float(row[StockStructuralStrengthFilterConstant.SOURCE_M_1.value])
        m_3 = float(row[StockStructuralStrengthFilterConstant.SOURCE_M_3.value])
        rsi_14 = float(row[StockStructuralStrengthFilterConstant.SOURCE_RSI_14.value])
        vol_20d_pct_126 = float(row[StockStructuralStrengthFilterConstant.SOURCE_VOL_20D_PCT_126.value])
        atr_14_w = float(row[StockStructuralStrengthFilterConstant.SOURCE_ATR_14_W.value])
        volume_ratio = float(row[StockStructuralStrengthFilterConstant.SOURCE_VOLUME_RATIO.value])

        is_positive = (
                close > sma_50 and
                sma_20 > sma_50 and
                m_3 > StockStructuralStrengthFilterConstant.M_3_THRESHOLD_VALUE.value and
                m_1 > StockStructuralStrengthFilterConstant.M_1_THRESHOLD_VALUE.value and
                rsi_14 < StockStructuralStrengthFilterConstant.RSI_14_THRESHOLD_VALUE.value and
                vol_20d_pct_126 < StockStructuralStrengthFilterConstant.VOL_20D_PCT_126_THRESHOLD_VALUE.value and
                atr_14_w/close < StockStructuralStrengthFilterConstant.ATR_14_CLOSE_THRESHOLD_VALUE.value and
                volume_ratio < StockStructuralStrengthFilterConstant.VOLUME_RATIO_THRESHOLD_VALUE.value
        )

        suggestion = (
            StockStructuralStrengthFilterConstant.SUGGESTION_POSSIBLE.value
            if is_positive
            else StockStructuralStrengthFilterConstant.SUGGESTION_AVOID.value
        )

        # Stateful streak update:
        # - SAFE => continue streak by +1
        # - DEFENSIVE => reset to 0
        positive_streak = (
            prev_streak + StockStructuralStrengthFilterConstant.ONE.value
            if is_positive
            else StockStructuralStrengthFilterConstant.ZERO.value
        )

        # Return single-row dataframe aligned with persistence contract.
        return pd.DataFrame([{
            StockStructuralStrengthFilterConstant.SYMBOL_KEY.value: row[StockStructuralStrengthFilterConstant.SYMBOL_KEY.value],
            StockStructuralStrengthFilterConstant.TIME_KEY.value: row[StockStructuralStrengthFilterConstant.TIME_KEY.value],
            StockStructuralStrengthFilterConstant.M_3_THRESHOLD.value: StockStructuralStrengthFilterConstant.M_3_THRESHOLD_VALUE.value,
            StockStructuralStrengthFilterConstant.M_1_THRESHOLD.value: StockStructuralStrengthFilterConstant.M_1_THRESHOLD_VALUE.value,
            StockStructuralStrengthFilterConstant.RSI_14_THRESHOLD.value: StockStructuralStrengthFilterConstant.RSI_14_THRESHOLD_VALUE.value,
            StockStructuralStrengthFilterConstant.VOL_20D_PCT_126_THRESHOLD.value: StockStructuralStrengthFilterConstant.VOL_20D_PCT_126_THRESHOLD_VALUE.value,
            StockStructuralStrengthFilterConstant.ATR_14_CLOSE_THRESHOLD.value: StockStructuralStrengthFilterConstant.ATR_14_CLOSE_THRESHOLD_VALUE.value,
            StockStructuralStrengthFilterConstant.VOLUME_RATIO_THRESHOLD.value: StockStructuralStrengthFilterConstant.VOLUME_RATIO_THRESHOLD_VALUE.value,
            StockStructuralStrengthFilterConstant.SUGGESTION.value: suggestion,
            StockStructuralStrengthFilterConstant.POSITIVE_STREAK.value: positive_streak,
        }])


    # =========================================================
    # Persistence
    # =========================================================

    def _store(self, df: pd.DataFrame, date: str):

        # No output rows means no DB operation needed.
        if df.empty:
            return

        # Per-symbol run contract: dataframe should contain exactly one symbol.
        symbol = df[StockStructuralStrengthFilterConstant.SYMBOL_KEY.value].iloc[0]

        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:
                # Performance optimization for recoverable ETL workloads:
                # relax commit durability guarantees for faster write throughput.
                cursor.execute(StockStructuralStrengthFilterConstant.SET_SYNC_COMMIT_OFF.value)

                # Step 1: delete existing rows in symbol+date scope.
                cursor.execute(
                    StockStructuralStrengthFilterSQLQueries.DELETE_STOCK_STRUCTURAL_STRENGTH_DATA,
                    (symbol, date)
                )

                # Step 2: serialize dataframe into in-memory CSV buffer.
                # header=False because COPY query defines the destination column order.
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # Step 3: bulk insert into moving-average target table.
                cursor.copy_expert(
                    StockStructuralStrengthFilterSQLQueries.COPY_STOCK_STRUCTURAL_STRENGTH_DATA,
                    buffer
                )

                # Step 4: commit atomic refresh.
            conn.commit()
