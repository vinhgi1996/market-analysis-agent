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

class VolatilityPipelineV2:

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
                VolatilityConstant.LOG_START.value.format(symbol=symbol)
            )

            df = None

            if self.mode == VolatilityConstant.MODE_BACKFILL.value:
                df = self._backfill_synthesize(symbol)
            elif self.mode == VolatilityConstant.MODE_INCREMENTAL.value:
                df = self._incremental_synthesize(symbol,self.current_time)
            # Persist dataframe (delete existing range first, then bulk copy).
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

    def _calculate_log_return(self, df: pd.DataFrame) -> pd.Series:
        return np.log(df["close"] / df["close"].shift(1))

    def _calculate_rolling_volatility(self, log_return: pd.Series) -> dict[str, pd.Series]:
        volatility_map = {}
        for w in [20, 60, 126, 252]:
            volatility_map[f"vol_{w}d"] = (
                    log_return
                    .rolling(w)
                    .std() * np.sqrt(252)
            )
        return volatility_map

    def _calculate_true_range(self, df: pd.DataFrame) -> pd.Series:
        prev_close = df["close"].shift(1)
        return pd.concat([
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs()
        ], axis=1).max(axis=1)

    def _calculate_atr_14_r(self, tr: pd.Series) -> pd.Series:
        return tr.rolling(14).mean()

    def _calculate_parkinson_20d(self, df: pd.DataFrame) -> pd.Series:
        pk = np.log(df["high"] / df["low"]) ** 2
        return ((pk.rolling(20).sum()) /(4 * 20 * np.log(2))) ** 0.5 * np.sqrt(252)

    def _calculate_vol_20d_pct_252(self, df: pd.DataFrame) -> pd.Series:
        return (
            df["vol_20d"]
            .rolling(252)
            .apply(lambda x: pd.Series(x).rank(pct=True).iloc[-1])
        )

    def _calculate_vol_20d_pct_126(self, df: pd.DataFrame) -> pd.Series:
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

        # Backfill: fetch broad historical set for symbol.
        data = PostgresSQLUtil.run_sql(
            VolatilitySQLQueries.GET_DATA_BY_SYMBOL_OHLVC,
            (symbol,)
        )

        # Convert row dict/list into DataFrame for vectorized computation.
        df = pd.DataFrame(data).copy()

        # Convert object columns to float64 (commonly NUMERIC from DB).
        # CAUTION: This attempts conversion for all object columns, which may fail
        # if non-numeric string columns exist in result set.
        df = PandasUtil.cast_object_columns_to_float64(df)

        # Log returns
        df["log_return"] = self._calculate_log_return(df)

        # Rolling volatility
        vol_map = self._calculate_rolling_volatility(df["log_return"])
        for col_name, series in vol_map.items():
            df[col_name] = series

        # ATR
        tr = self._calculate_true_range(df)

        df["atr_14_r"] = self._calculate_atr_14_r(tr)
        df["atr_14_w"] = self._calculate_wilder_atr_backfill(df, 14)

        # Parkinson 20d
        df["parkinson_20d"] = self._calculate_parkinson_20d(df)

        # Volatility percentile 20d (annualized - 126 days)
        df["vol_20d_pct_126"] = self._calculate_vol_20d_pct_126(df)

        # Volatility percentile 20d (annualized - 252 days)
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
        ohlvc_data = PostgresSQLUtil.run_sql(
            VolatilitySQLQueries.GET_DATA_BY_DATE_SYMBOL_OHLVC,
            (symbol, date)
        )
        volatility_data = PostgresSQLUtil.run_sql(
            VolatilitySQLQueries.GET_DATA_BY_DATE_SYMBOL_ATR_14_W,
            (symbol, date)
        )
        if len(ohlvc_data) < 253 or not volatility_data:
            raise ValueError("Insufficient data for incremental computation")

        # Convert row dict/list into DataFrame for vectorized computation.
        ohlvc_df = pd.DataFrame(ohlvc_data).copy()
        volatility_df = pd.DataFrame(volatility_data).copy()

        # Convert object columns to float64 (commonly NUMERIC from DB).
        # CAUTION: This attempts conversion for all object columns, which may fail
        # if non-numeric string columns exist in result set.
        ohlvc_df = PandasUtil.cast_object_columns_to_float64(ohlvc_df)
        volatility_df = PandasUtil.cast_object_columns_to_float64(volatility_df)

        # Log returns
        ohlvc_df["log_return"] = self._calculate_log_return(ohlvc_df)

        # Rolling volatility
        vol_map = self._calculate_rolling_volatility(ohlvc_df["log_return"])
        for col_name, series in vol_map.items():
            ohlvc_df[col_name] = series

        # Volatility percentile 20d (annualized 252 days)
        ohlvc_df["vol_20d_pct_252"] = self._calculate_vol_20d_pct_252(ohlvc_df)

        # Volatility percentile 20d (annualized 252 days)
        ohlvc_df["vol_20d_pct_126"] = self._calculate_vol_20d_pct_126(ohlvc_df)

        parkinson_ohlvc_df = ohlvc_df.tail(20).copy().reset_index(drop=True)

        # Parkinson 20d
        parkinson_ohlvc_df["parkinson_20d"] = self._calculate_parkinson_20d(parkinson_ohlvc_df)

        # ATR 14 days standard rolling
        tr = self._calculate_true_range(parkinson_ohlvc_df)
        parkinson_ohlvc_df["atr_14_r"] = self._calculate_atr_14_r(tr)
        # ATR 14 days wilder smoothing
        atr_14_w = parkinson_ohlvc_df.tail(2).copy().reset_index(drop=True)
        final_df = self._calculate_wilder_atr_incremental(atr_14_w,volatility_df, 14)

        return final_df

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
        symbol = df[VolatilityConstant.SYMBOL_KEY.value].iloc[0]

        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:
                # Performance optimization:
                # allow faster commit behavior for this transaction scope.
                # Do not using this option for important data which can't be recovered
                cursor.execute(VolatilityConstant.SET_SYNC_COMMIT_OFF.value)

                # Remove existing rows before reloading.
                # For backfill: date is oldest bound -> broad cleanup.
                # For incremental: date is new boundary -> narrow cleanup.
                cursor.execute(
                    VolatilitySQLQueries.DELETE_VOLATILITY_DATA,
                    (symbol, date)
                )

                # Prepare CSV in-memory buffer for COPY command.
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # High-throughput insert into momentum target table.
                cursor.copy_expert(
                    VolatilitySQLQueries.COPY_VOLATILITY_DATA,
                    buffer
                )

                # Commit after delete + copy.
            conn.commit()

