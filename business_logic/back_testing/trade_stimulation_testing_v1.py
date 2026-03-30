import logging
import os

from pandas import DataFrame

from constant.constants.stock.stock_rsi_constant import RsiConstant
from constant.sql.back_testing.trade_simulation_testing_sql_queries import TradeSimulationTestingSQLQueries
from util.pandas_util import PandasUtil
from util.postgre_sql import PostgresSQLUtil
# Thread pool for running symbol jobs concurrently.
from concurrent.futures import ThreadPoolExecutor, as_completed


import pandas as pd
from io import StringIO

class TradeSimulationTesting:

    def __init__(
            self,
            max_workers: int = 5,
            symbol_queries: str = "",
            current_time: str = "",
            current_sector: str = ""
    ):
        # Logger is namespaced by concrete class for pipeline traceability.
        self.logger = logging.getLogger(self.__class__.__name__)

        # Worker count used in parallel symbol processing.
        self.max_workers = max_workers

        # SQL used to load symbol universe for `run_all_parallel`.
        self.symbol_queries = symbol_queries

        # Effective processing date/time used by incremental reads and delete scope.
        self.current_time = current_time

        self.current_sector = current_sector


    # =========================================================
    # Public API
    # =========================================================

    def run(self, symbol: str):

        try:
            self.logger.info(
                RsiConstant.LOG_START.value.format(symbol=symbol)
            )


            if self.current_sector == "OIL":
                self._oil_sector_simulator(symbol)
            elif self.current_sector == "":
                print("hihi")
            self.logger.info(
                RsiConstant.LOG_FINISH.value.format(symbol=symbol)
            )
        except Exception as e:
            # Error is logged, not re-raised -> job continues for other symbols.
            self.logger.error(
                RsiConstant.LOG_ERROR.value.format(symbol=symbol, error=e)
            )

    def run_all_parallel(self):
        """
        Run this pipeline across all symbols using a thread pool.

        Function-scale flow:
        - Load symbols via `self.symbol_queries`.
        - Submit one `run(symbol)` task per symbol.
        - Force worker exceptions to surface with `future.result()`.

        Operation-scale details:
        - Symbol universe query is expected to return list[dict]-like rows.
        - Each worker executes fully isolated read/compute/write for one symbol.
        - `future.result()` ensures thread exceptions are not silently ignored.
        - Parallelism is useful because workload is primarily DB I/O and lightweight math.
        """
        # NOTE: This string appears to have encoding artifacts in current source.
        self.logger.info(RsiConstant.LOG_PARALLEL_START.value)

        # Expected shape: list[dict], each dict includes at least symbol key.
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

    def _oil_sector_simulator(self, symbol: str, config=None):

        # Backfill path reads full symbol history.
        data = PostgresSQLUtil.run_sql(
            TradeSimulationTestingSQLQueries.GET_OIL_STOCK_DATA_BY_SYMBOL,
            (self.current_time,symbol)
        )

        # Convert DB result rows to DataFrame to leverage vectorized pandas operations.
        df = pd.DataFrame(data).reset_index(drop=True).copy()

        # Normalize numeric-like object columns (common from DB NUMERIC) to float64.
        # This avoids mixed dtype arithmetic and improves numerical consistency.
        df = PandasUtil.cast_object_columns_to_float64(df)

        if symbol == "BSR" :
            df = self._oil_BSR_entry(df)
            df = self._oil_BSR_exit(df)
        if symbol == "PVD" :
            df = self._oil_PVD_entry(df)
            df = self._oil_PVD_exit(df)

        df_uptrend = df[df["uptrend_start"]]
        df_downtrend = df[df["downtrend_start"]]

        symbol = df["symbol"].iloc[0]
        uptrend_file_name = f"{symbol}_up_trend.csv"
        downtrend_file_name = f"{symbol}_down_trend.csv"

        df_uptrend.to_csv(uptrend_file_name, index=False)
        df_downtrend.to_csv(downtrend_file_name, index=False)

        test_trade = self._trading_loop_test_v2(df)
        symbol = df["symbol"].iloc[0]
        test_trade_file_name = f"{symbol}_test_trade.csv"

        test_trade.to_csv(test_trade_file_name, index=False)

    def _oil_BSR_entry(self, df : DataFrame, config=None):

        # ======================
        # DEFAULT CONFIG
        # ======================
        if config is None:
            config = {
                "rsi_entry": 55
            }

        # ======================
        # SIGNALS
        # ======================

        df["rsi_cross_50"] = (df["rsi_14"] > config["rsi_entry"]) & \
                             (df["rsi_14"].shift(1) <= config["rsi_entry"])

        df["m1_flip_up"] = (df["m_1"] > 0) & (df["m_1"].shift(1) <= 0)

        df["price_cross_sma20"] = (
                (df["close"] > df["sma_20"]) &
                (df["close"].shift(1) <= df["sma_20"].shift(1))
        )



        # print(df.tail(50))

        # ======================
        # ENTRY LOGIC
        # ======================
        WINDOW = 5  # signal clustering window
        HOLD_DAYS = 3  # persistence check window
        df["rsi_recent"] = df["rsi_cross_50"].rolling(WINDOW).max()
        df["m1_recent"] = df["m1_flip_up"].rolling(WINDOW).max()
        df["price_recent"] = df["price_cross_sma20"].rolling(WINDOW).max()
        df["atr_expand"] = df["atr_14_w"].rolling(WINDOW).mean() > 0

        df["rsi_hold"] = (
                (df["rsi_14"] > config["rsi_entry"])
                .rolling(HOLD_DAYS)
                .sum() >= 2
        )

        df["rsi_zone"] = df["rsi_14"].between(50, 60)

        df["m1_persist"] = (
                (df["m_1"] > 0)
                .rolling(HOLD_DAYS)
                .sum() >= 2
        )

        df["price_hold"] = (
                (df["close"] > df["sma_20"])
                .rolling(HOLD_DAYS)
                .sum() >= 2
        )

        df["low_vol"] = df["vol_20d_pct_126"].between(0.3, 0.8)

        df["sequence_ok"] = (
                (df["m1_flip_up"].rolling(6).max() == 1) &
                (df["rsi_cross_50"].rolling(3).max() == 1)
        )

        df["score"] = (
                df["m1_recent"] +
                df["rsi_recent"] +
                df["price_recent"] +
                df["atr_expand"]
        )

        df["uptrend_start"] = (
                (df["m1_recent"] == 1) &
                (df["rsi_recent"] == 1) &
                (df["price_recent"] == 1) &
                (df["atr_expand"] == 1) &

                df["rsi_hold"] &
                df["rsi_zone"] &
                df["m1_persist"] &
                df["price_hold"] &
                df["low_vol"] &
                df["sequence_ok"]

        )

        return df

    def _oil_PVD_entry(self, df : DataFrame, config=None):

        # ======================
        # DEFAULT CONFIG
        # ======================
        if config is None:
            config = {
                "rsi_entry": 50,
                "rsi_entry_min": 50,
                "rsi_entry_max": 60 # 65 has more noise, 60 is  more stable threshold
            }
        # ======================
        # SIGNALS
        # ======================

        # ======================
        # ENTRY LOGIC
        # ======================
        WINDOW = 3 # signal clustering window
        HOLD_DAYS = 3  # persistence check window

        df["atr_expand"] = df["atr_14_w"].rolling(WINDOW).mean() > 0

        # df["rsi_pattern"] = (((df["rsi_14"].diff() > 0).rolling(WINDOW - 1).sum() == (WINDOW - 1)) &
        #                      (df["rsi_14"].between(config["rsi_entry_min"], config["rsi_entry_max"]).rolling(WINDOW).sum() == WINDOW))

        df["rsi_pattern"] = (df["rsi_14"].between(config["rsi_entry_min"], config["rsi_entry_max"])
                             .rolling(WINDOW,min_periods=WINDOW).sum() == HOLD_DAYS)

        df["price_sma20"] = (
                (df["close"] > df["sma_20"])
                .rolling(HOLD_DAYS)
                .sum() >= HOLD_DAYS
        )

        df["sma20_sma50"] = (
                (df["sma_20"] > df["sma_50"])
                .rolling(HOLD_DAYS)
                .sum() >= HOLD_DAYS
        )

        df["m_1"] = (
                (df["m_1"] > 0)
                .rolling(HOLD_DAYS)
                .sum() >= HOLD_DAYS
        )

        df["m_3"] = (
                (df["m_3"] > 0)
                .rolling(HOLD_DAYS)
                .sum() >= HOLD_DAYS
        )

        df["vni_m"] = (
                (df["vnim_1"] > 0)
                .rolling(5)
                .sum() ==5
        )

        df["uptrend_start"] = (
                (df["atr_expand"] == 1) &
                df["rsi_pattern"] &
                df["price_sma20"] &
                df["sma20_sma50"] &
                df["m_1"] &
                df["m_3"] &
                df["vni_m"]

        )

        return df

    def _oil_BSR_exit(self, df : DataFrame, config=None):

        # ======================
        # DEFAULT CONFIG
        # ======================
        if config is None:
            config = {
                "rsi_entry": 85
            }

        # ======================
        # SIGNALS
        # ======================

        df["rsi_cross_80"] = (df["rsi_14"] > config["rsi_entry"])
        df["price_cross_sma20"] = (df["close"] > df["sma_20"])


        # print(df.tail(50))

        # ======================
        # ENTRY LOGIC
        # ======================
        WINDOW = 3  # signal clustering window
        HOLD_DAYS = 3  # persistence check window
        df["rsi_recent"] = df["rsi_cross_80"].rolling(WINDOW).max()
        df["price_recent"] = df["price_cross_sma20"].rolling(WINDOW).max()

        df["rsi_hold"] = (
                (df["rsi_14"] > config["rsi_entry"])
                .rolling(HOLD_DAYS)
                .sum() == 3
        )

        df["price_hold"] = (
                (df["close"] > df["sma_20"])
                .rolling(HOLD_DAYS)
                .sum() == 3
        )


        df["score"] = (
                df["rsi_recent"] +
                df["price_recent"]
        )

        df["downtrend_start"] = (
                (df["rsi_recent"] == 1) &
                (df["price_recent"] == 1) &
                df["rsi_hold"] &
                df["price_hold"]

        )

        return df

    def _oil_PVD_exit(self, df : DataFrame, config=None):

        # ======================
        # DEFAULT CONFIG
        # ======================
        if config is None:
            config = {
                "rsi_max": 70,
                "rsi_min": 50
            }

        # ======================
        # SIGNALS
        # ======================

        df["rsi_cross_min"] = (df["rsi_14"] < config["rsi_min"]) & \
                             (df["rsi_14"].shift(1) >= config["rsi_min"])

        df["rsi_cross_max"] = (df["rsi_14"] > config["rsi_max"]) & \
                             (df["rsi_14"].shift(1) <= config["rsi_max"])


        # print(df.tail(50))

        # ======================
        # ENTRY LOGIC
        # ======================
        WINDOW = 2  # signal clustering window
        HOLD_DAYS = 2  # persistence check window
        df["rsi_max_recent"] = df["rsi_cross_max"].rolling(WINDOW).max()

        df["rsi_max_hold"] = (
                (df["rsi_14"] > config["rsi_max"])
                .rolling(HOLD_DAYS)
                .sum() == HOLD_DAYS
        )

        df["downtrend_start"] = (
            ((df["rsi_max_recent"] == 1) & df["rsi_max_hold"]) |
            df["rsi_cross_min"]
        )

        return df

    def _trading_loop_test_v1(self, df : DataFrame, config=None):

        trades = []

        exit_indices = df.index[df["downtrend_start"]].tolist()
        exit_ptr = 0

        for entry_idx in df.index[df["uptrend_start"]]:

            while exit_ptr < len(exit_indices) and exit_indices[exit_ptr] <= entry_idx:
                exit_ptr += 1

            if exit_ptr >= len(exit_indices):
                break

            exit_idx = exit_indices[exit_ptr]

            trades.append({
                "entry_idx": entry_idx,
                "exit_idx": exit_idx,
                "entry_time": df.loc[entry_idx, "time"],
                "exit_time": df.loc[exit_idx, "time"],
                "entry_price": df.loc[entry_idx, "close"],
                "exit_price": df.loc[exit_idx, "close"],
                "return": df.loc[exit_idx, "close"] / df.loc[entry_idx, "close"] - 1,
                "holding_days": exit_idx - entry_idx
            })

        columns = [
            "entry_time", "exit_time",
            "entry_price", "exit_price",
            "return", "holding_days"
        ]

        trades_df = pd.DataFrame(trades, columns=columns)

        return trades_df

    def _trading_loop_test_v2(self, df : DataFrame, config=None):

        MIN_HOLD = 15

        trades = []

        entry_indices = df.index[df["uptrend_start"]].tolist()
        exit_indices = df.index[df["downtrend_start"]].tolist()

        exit_ptr = 0

        for entry_idx in entry_indices:

            # move pointer until exit is AFTER entry + MIN_HOLD
            while (
                    exit_ptr < len(exit_indices) and
                    exit_indices[exit_ptr] < entry_idx + MIN_HOLD
            ):
                exit_ptr += 1

            if exit_ptr >= len(exit_indices):
                continue  # no valid exit

            exit_idx = exit_indices[exit_ptr]

            entry_price = df.loc[entry_idx, "close"]
            exit_price = df.loc[exit_idx, "close"]

            trades.append({
                "entry_idx": entry_idx,
                "exit_idx": exit_idx,
                "entry_time": df.loc[entry_idx, "time"],
                "exit_time": df.loc[exit_idx, "time"],
                "entry_price": entry_price,
                "exit_price": exit_price,
                "return": exit_price / entry_price - 1,
                "holding_days": exit_idx - entry_idx
            })

        columns = [
            "entry_time", "exit_time",
            "entry_price", "exit_price",
            "return", "holding_days"
        ]

        trades_df = pd.DataFrame(trades, columns=columns)

        return trades_df