import logging

import numpy as np

from config.postgre_manager import PostgresManager
from constant.constants.stock.stock_rsi_constant import RsiConstant
from constant.constants.stock.stock_volatility_constant import VolatilityConstant
from constant.sql.stock.stock_volatility_sql_queries import VolatilitySQLQueries
from util.pandas_util import PandasUtil
from util.postgre_sql import PostgresSQLUtil
# Thread pool for running symbol jobs concurrently.
from concurrent.futures import ThreadPoolExecutor, as_completed


import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
from io import StringIO

class StockVolatilityPipelineV2:
    """
    Stock volatility feature pipeline (v2).

    Class-scale responsibilities:
    - Compute multi-horizon volatility and range-based risk metrics per symbol.
    - Support both full-history rebuild (`backfill`) and point-in-time update
      (`incremental`) execution modes.
    - Persist results into `stock_volatility` via scoped delete + COPY bulk load.

    Output features:
    - `vol_20d`, `vol_60d`, `vol_126d`, `vol_252d` (annualized log-return volatility)
    - `atr_14_r` (simple rolling ATR), `atr_14_w` (Wilder ATR)
    - `parkinson_20d` (range-based volatility estimator)
    - `vol_20d_pct_126`, `vol_20d_pct_252` (rolling percentile rank context)
    """

    def __init__(
            self,
            max_workers: int = 5,
            symbol_queries: str = "",
            current_time: str = "",
            mode: str = ""
    ):
        # Logger is bound to class name for pipeline-specific traceability.
        self.logger = logging.getLogger(self.__class__.__name__)

        # Worker count used by run_all_parallel thread pool.
        self.max_workers = max_workers

        # SQL that returns the symbol universe for parallel execution.
        self.symbol_queries = symbol_queries

        # Effective processing date/timestamp used by incremental mode and delete scope.
        self.current_time = current_time

        # Expected mode values: "backfill" or "incremental".
        self.mode = mode

    # =========================================================
    # Public API
    # =========================================================

    def run(self, symbol: str):
        """
        Execute pipeline for one symbol end-to-end.

        Function-scale flow:
        - Select synthesis path from configured mode.
        - Compute volatility features.
        - Persist with delete-then-COPY idempotent pattern.
        """

        try:
            self.logger.info(
                VolatilityConstant.LOG_START.value.format(symbol=symbol)
            )

            df = None

            if self.mode == VolatilityConstant.MODE_BACKFILL.value:
                df = self._backfill_synthesize(symbol)
            elif self.mode == VolatilityConstant.MODE_INCREMENTAL.value:
                df = self._incremental_synthesize(symbol,self.current_time)
            # Persist computed dataframe after scoped cleanup.
            self._store(df, self.current_time)

            self.logger.info(
                VolatilityConstant.LOG_FINISH.value.format(symbol=symbol)
            )
        except Exception as e:
            # Error is logged, not re-raised -> job continues for other symbols.
            self.logger.error(
                VolatilityConstant.LOG_ERROR.value.format(symbol=symbol, error=e)
            )

    def run_all_parallel(self):
        """
        Run this pipeline across all symbols using a thread pool.

        Function-scale flow:
        - Load symbols from `self.symbol_queries`.
        - Dispatch one `run(symbol)` task per symbol.
        - Re-raise worker exceptions via `future.result()` to avoid silent failures.
        """
        # NOTE: This string appears to have encoding artifacts in current source.
        self.logger.info(RsiConstant.LOG_PARALLEL_START.value)

        # Expected shape: list[dict], each dict contains at least the symbol key.
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

    def _calculate_log_return(self, df: pd.DataFrame) -> pd.Series:
        """Compute one-period log return: ln(close_t / close_{t-1})."""
        return np.log(df["close"] / df["close"].shift(1))

    def _calculate_rolling_volatility(self, log_return: pd.Series) -> dict[str, pd.Series]:
        """
        Compute annualized rolling volatility for configured horizons.

        Formula per window `w`:
        - vol_w = std(log_return over w sessions) * sqrt(252)
        """
        volatility_map = {}
        for w in [20, 60, 126, 252]:
            volatility_map[f"vol_{w}d"] = (
                    log_return
                    .rolling(w)
                    .std() * np.sqrt(252)
            )
        return volatility_map

    def _calculate_true_range(self, df: pd.DataFrame) -> pd.Series:
        """
        Compute True Range (TR) per row.

        TR_t = max(
          high_t - low_t,
          |high_t - close_{t-1}|,
          |low_t  - close_{t-1}|
        )
        """
        prev_close = df["close"].shift(1)
        return pd.concat([
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs()
        ], axis=1).max(axis=1)

    def _calculate_atr_14_r(self, tr: pd.Series) -> pd.Series:
        """Compute simple rolling ATR(14) as mean(TR) over 14 sessions."""
        return tr.rolling(14).mean()

    def _calculate_parkinson_20d(self, df: pd.DataFrame) -> pd.Series:
        """
        Compute Parkinson volatility estimator over 20 sessions, annualized.

        Uses high/low range information:
        sigma = sqrt( sum(log(H/L)^2) / (4 * n * ln(2)) ) * sqrt(252), n=20
        """
        pk = np.log(df["high"] / df["low"]) ** 2
        return ((pk.rolling(20).sum()) /(4 * 20 * np.log(2))) ** 0.5 * np.sqrt(252)

    def _calculate_vol_20d_pct_252(self, df: pd.DataFrame) -> pd.Series:
        """Percentile rank of latest `vol_20d` inside each 252-session rolling window."""
        return (
            df["vol_20d"]
            .rolling(252)
            .apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1])
        )

    def _calculate_vol_20d_pct_126(self, df: pd.DataFrame) -> pd.Series:
        """Percentile rank of latest `vol_20d` inside each 126-session rolling window."""
        return (
            df["vol_20d"]
            .rolling(126)
            .apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1])
        )

    def _calculate_wilder_atr_incremental(
            self,
            ohlvc_df: pd.DataFrame,
            volatility_df: pd.DataFrame,
            n: int = 14
    ) -> pd.DataFrame:
        """
        Incrementally update Wilder ATR(14) for the newest bar only.

        Function-scale requirements:
        - `ohlvc_df` must include at least previous + latest rows.
        - `volatility_df` must provide prior stored `atr_14_w`.
        - Returns one-row dataframe with latest feature snapshot.
        """
        # Need latest bar + previous close to compute TR for the latest bar.
        if len(ohlvc_df) < 2:
            raise ValueError("ohlvc_df must contain at least 2 rows for incremental ATR.")
        if volatility_df.empty:
            raise ValueError("volatility_df is empty; previous atr_14_w is required.")

        prev_row = ohlvc_df.iloc[-2]
        last_row = ohlvc_df.iloc[-1]

        high_last = float(last_row[VolatilityConstant.HIGH_KEY.value])
        low_last = float(last_row[VolatilityConstant.LOW_KEY.value])
        prev_close = float(prev_row[VolatilityConstant.CLOSE_KEY.value])
        prev_atr = float(volatility_df.iloc[0][VolatilityConstant.ATR_14_W.value])

        tr_last = max(
            high_last - low_last,
            abs(high_last - prev_close),
            abs(low_last - prev_close),
        )
        # Wilder recursive update:
        # ATR_t = (ATR_{t-1} * (n - 1) + TR_t) / n
        atr_14_w = (prev_atr * (n - 1) + tr_last) / n

        return pd.DataFrame([{
            VolatilityConstant.SYMBOL_KEY.value: last_row[VolatilityConstant.SYMBOL_KEY.value],
            VolatilityConstant.TIME_KEY.value: last_row[VolatilityConstant.TIME_KEY.value],
            VolatilityConstant.VOL_20D.value: last_row[VolatilityConstant.VOL_20D.value],
            VolatilityConstant.VOL_60D.value: last_row[VolatilityConstant.VOL_60D.value],
            VolatilityConstant.VOL_126D.value: last_row[VolatilityConstant.VOL_126D.value],
            VolatilityConstant.VOL_252D.value: last_row[VolatilityConstant.VOL_252D.value],
            VolatilityConstant.ATR_14_R.value: last_row[VolatilityConstant.ATR_14_R.value],
            VolatilityConstant.ATR_14_W.value: atr_14_w,
            VolatilityConstant.PARKINSON_20D.value: last_row[VolatilityConstant.PARKINSON_20D.value],
            VolatilityConstant.VOL_20D_PCT_126.value: last_row[VolatilityConstant.VOL_20D_PCT_126.value],
            VolatilityConstant.VOL_20D_PCT_252.value: last_row[VolatilityConstant.VOL_20D_PCT_252.value],
        }])

    def _calculate_wilder_atr_backfill(self,df, n=14):
        """
        Compute full-series Wilder ATR for backfill mode.

        Operation-scale flow:
        - Vectorize TR computation.
        - Seed ATR at index n-1 with mean(TR[:n]).
        - Apply Wilder recursion for the remaining rows.
        """
        high = df["high"].to_numpy()
        low = df["low"].to_numpy()
        close = df["close"].to_numpy()

        length = len(df)

        # --- True Range ---
        prev_close = np.roll(close, 1)
        prev_close[0] = close[0]

        tr = np.maximum.reduce([
            high - low,
            np.abs(high - prev_close),
            np.abs(low - prev_close)
        ])

        # --- ATR ---
        atr = np.full(length, np.nan)

        # Seed
        atr[n - 1] = tr[:n].mean()

        # Wilder recursion
        for i in range(n, length):
            atr[i] = (atr[i - 1] * (n - 1) + tr[i]) / n

        return atr

    def _backfill_synthesize(self, symbol: str) -> pd.DataFrame:
        """
        Build full-history volatility features for one symbol.

        Function-scale steps:
        - Load all OHLCV rows for symbol.
        - Normalize dtypes for numeric operations.
        - Compute return-based, ATR-based, Parkinson, and percentile features.
        - Return persistence columns in COPY order.
        """

        # Backfill path loads full symbol history.
        data = PostgresSQLUtil.run_sql(
            VolatilitySQLQueries.GET_DATA_BY_SYMBOL_OHLVC,
            (symbol,)
        )

        # Convert row dict/list into DataFrame for vectorized computation.
        df = pd.DataFrame(data).copy()

        # Convert numeric-like object columns (from DB NUMERIC) to float64.
        df = PandasUtil.cast_object_columns_to_float64(df)

        # Step 1: return series for volatility estimation.
        df["log_return"] = self._calculate_log_return(df)

        # Step 2: annualized rolling volatility across multiple windows.
        vol_map = self._calculate_rolling_volatility(df["log_return"])
        for col_name, series in vol_map.items():
            df[col_name] = series

        # Step 3: ATR variants from True Range.
        tr = self._calculate_true_range(df)

        df["atr_14_r"] = self._calculate_atr_14_r(tr)
        df["atr_14_w"] = self._calculate_wilder_atr_backfill(df, 14)

        # Step 4: Parkinson range-based volatility.
        df["parkinson_20d"] = self._calculate_parkinson_20d(df)

        # Step 5: context percentile of current 20d vol in 126-session history.
        df["vol_20d_pct_126"] = self._calculate_vol_20d_pct_126(df)

        # Step 6: context percentile of current 20d vol in 252-session history.
        df["vol_20d_pct_252"] = self._calculate_vol_20d_pct_252(df)

        # Keep only persistence contract columns (order matters for COPY).
        return df[
            [
                VolatilityConstant.SYMBOL_KEY.value,
                VolatilityConstant.TIME_KEY.value,
                VolatilityConstant.VOL_20D.value,
                VolatilityConstant.VOL_60D.value,
                VolatilityConstant.VOL_126D.value,
                VolatilityConstant.VOL_252D.value,
                VolatilityConstant.ATR_14_R.value,
                VolatilityConstant.ATR_14_W.value,
                VolatilityConstant.PARKINSON_20D.value,
                VolatilityConstant.VOL_20D_PCT_126.value,
                VolatilityConstant.VOL_20D_PCT_252.value,
            ]
        ]

    def _incremental_synthesize(self, symbol: str, date: str) -> pd.DataFrame:
        """
        Build one-row latest volatility snapshot for incremental mode.

        Function-scale steps:
        - Load bounded OHLCV window and prior ATR state.
        - Recompute latest volatility features on the bounded window.
        - Update Wilder ATR using recursion from previous stored ATR.
        - Return one-row dataframe for persistence.
        """
        # Bounded OHLCV history that supports max lookback windows.
        ohlvc_data = PostgresSQLUtil.run_sql(
            VolatilitySQLQueries.GET_DATA_BY_DATE_SYMBOL_OHLVC,
            (symbol, date)
        )
        # Previous Wilder ATR state used for recursive incremental update.
        volatility_data = PostgresSQLUtil.run_sql(
            VolatilitySQLQueries.GET_DATA_BY_DATE_SYMBOL_ATR_14_W,
            (symbol, date)
        )
        # 253 rows support up to 252-session lookback + latest row.
        if len(ohlvc_data) < 253 or not volatility_data:
            raise ValueError("Insufficient data for incremental computation")

        # Convert row dict/list into DataFrame for vectorized computation.
        ohlvc_df = pd.DataFrame(ohlvc_data).copy()
        volatility_df = pd.DataFrame(volatility_data).copy()

        # Normalize numeric-like object dtypes for stable math.
        ohlvc_df = PandasUtil.cast_object_columns_to_float64(ohlvc_df)
        volatility_df = PandasUtil.cast_object_columns_to_float64(volatility_df)

        # Step 1: return series for rolling volatility.
        ohlvc_df["log_return"] = self._calculate_log_return(ohlvc_df)

        # Step 2: recompute rolling vol features in bounded context.
        vol_map = self._calculate_rolling_volatility(ohlvc_df["log_return"])
        for col_name, series in vol_map.items():
            ohlvc_df[col_name] = series

        # Step 3: percentile context features for vol_20d.
        ohlvc_df["vol_20d_pct_252"] = self._calculate_vol_20d_pct_252(ohlvc_df)

        # 126-session percentile companion.
        ohlvc_df["vol_20d_pct_126"] = self._calculate_vol_20d_pct_126(ohlvc_df)

        # Parkinson and rolling ATR are computed on a 20-row tail context.
        parkinson_ohlvc_df = ohlvc_df.tail(20).copy().reset_index(drop=True)

        # Step 4: range-based Parkinson volatility.
        parkinson_ohlvc_df["parkinson_20d"] = self._calculate_parkinson_20d(parkinson_ohlvc_df)

        # Step 5: simple rolling ATR(14) from TR.
        tr = self._calculate_true_range(parkinson_ohlvc_df)
        parkinson_ohlvc_df["atr_14_r"] = self._calculate_atr_14_r(tr)
        # Step 6: Wilder ATR(14) recursive update from previous persisted ATR.
        atr_14_w = parkinson_ohlvc_df.tail(2).copy().reset_index(drop=True)
        final_df = self._calculate_wilder_atr_incremental(atr_14_w,volatility_df, 14)

        return final_df

    # =========================================================
    # Persistence
    # =========================================================

    def _store(self, df: pd.DataFrame, date: str):
        """
        Persist synthesized volatility rows:
        - Skip if empty
        - Delete existing target rows in scope
        - Bulk insert via COPY for speed

        Operation-scale notes:
        - Runs delete + copy in one transaction.
        - Uses `synchronous_commit = off` for ETL throughput on recoverable workloads.
        """

        # No generated rows => no persistence action required.
        if df.empty:
            return

        # Assumes dataframe contains a single symbol only.
        symbol = df[VolatilityConstant.SYMBOL_KEY.value].iloc[0]

        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:
                # Performance optimization:
                # allow faster commit behavior for this transaction scope.
                # Do not using this option for important data which can't be recovered
                cursor.execute(VolatilityConstant.SET_SYNC_COMMIT_OFF.value)

                # Idempotent reload: clear symbol rows from date boundary onward.
                cursor.execute(
                    VolatilitySQLQueries.DELETE_VOLATILITY_DATA,
                    (symbol, date)
                )

                # Stream dataframe as in-memory CSV for high-throughput COPY.
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # High-throughput insert into stock volatility target table.
                cursor.copy_expert(
                    VolatilitySQLQueries.COPY_VOLATILITY_DATA,
                    buffer
                )

                # Commit after delete + copy.
            conn.commit()

