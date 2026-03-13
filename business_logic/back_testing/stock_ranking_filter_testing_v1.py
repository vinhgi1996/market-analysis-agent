import logging

from config.postgre_manager import PostgresManager
from constant.constants.analysis.stock_ranking_filter_constant import StockRankingFilterConstant
from constant.sql.analysis.stock_ranking_filter_sql_queries import StockRankingFilterSQLQueries
from constant.sql.back_testing.stock_ranking_testing_sql_queries import StockRankingTestingSQLQueries

from util.pandas_util import PandasUtil
from util.postgre_sql import PostgresSQLUtil
# Thread pool for running symbol jobs concurrently.

import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
from io import StringIO

from util.time_util import TimeUtil

import matplotlib.pyplot as plt
from scipy.stats import spearmanr

import numpy as np

class StockRankingTesting:

    def __init__(
            self,
            current_time: str = "",
            start_date: str = "",
            end_date: str = "",
    ):
        # Logger is class-scoped for pipeline-specific tracing.
        self.logger = logging.getLogger(self.__class__.__name__)

        # Effective processing date/time; used by incremental read + scoped delete.
        self.current_time = TimeUtil.add_time_to_date(current_time)

        self.start_date = TimeUtil.add_time_to_date(start_date)

        self.end_date = TimeUtil.add_time_to_date(end_date)


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
                StockRankingFilterConstant.LOG_START.value
            )

            df = None

            self.logger.info(
                StockRankingFilterConstant.LOG_FINISH.value
            )
        except Exception as e:
            # Error is logged, not re-raised -> job continues for other symbols.
            self.logger.error(
                StockRankingFilterConstant.LOG_ERROR.value.format(symbol='VNINDEX', error=e)
            )

    def _compute_ic(self,group):
        if len(group) < 5:
            return None
        return spearmanr(group["alpha_score"], group["fwd_return_20d"]).correlation


    def ic_test(self):

        try:
            self.logger.info(
                StockRankingFilterConstant.LOG_START.value
            )

            # Latest feature snapshot up to boundary date (inclusive by SQL boundary logic).
            ohlvc_data = PostgresSQLUtil.run_sql(
                StockRankingTestingSQLQueries.GET_STOCK_OHLVC_DATA_BY_DATE_RANGE,
                (self.start_date, self.end_date,self.start_date, self.end_date)
            )

            stock_ranking_data = PostgresSQLUtil.run_sql(
                StockRankingTestingSQLQueries.GET_STOCK_RANKING_DATA_BY_DATE_RANGE,
                (self.start_date, self.end_date)
            )

            # Materialize SQL rows into DataFrame for vectorized logic.
            ohlvc_df = pd.DataFrame(ohlvc_data)
            stock_ranking_df = pd.DataFrame(stock_ranking_data)

            ohlvc_df["fwd_return_20d"] = ohlvc_df["close_20"]/ohlvc_df["close"] - 1

            df_merged = pd.merge(stock_ranking_df, ohlvc_df, on=['time', 'symbol'])
            df_merged = df_merged.dropna(subset=["alpha_score", "fwd_return_20d"])
            df_merged["fwd_return_20d"] = df_merged["fwd_return_20d"].clip(-0.5, 0.5)

            df_merged = df_merged.sort_values(
                by=['time', 'alpha_score'],
                ascending=[False, False]
            )
            print(f":{df_merged.head(100)}")

            ic_series = df_merged.groupby("time").apply(self._compute_ic)
            ic_mean = ic_series.mean()
            ic_std = ic_series.std()

            n = len(ic_series)

            ic_tstat = ic_mean / (ic_std / np.sqrt(n))

            positive_ic_pct = (ic_series > 0).mean()

            print("IC Mean:", ic_mean)
            print("IC Std:", ic_std)
            print("IC T-Stat:", ic_tstat)
            print("Positive IC %:", positive_ic_pct)

            ic_series.plot()
            plt.title("Information Coefficient Over Time")
            plt.show()

            self.logger.info(
                StockRankingFilterConstant.LOG_FINISH.value
            )
        except Exception as e:
            # Error is logged, not re-raised -> job continues for other symbols.
            self.logger.error(
                StockRankingFilterConstant.LOG_ERROR.value.format(symbol='VNINDEX', error=e)
            )

    # =========================================================
    # Feature Engineering Helper
    # =========================================================
    def _winsorize_zscore(self, series, lower_q=0.05, upper_q=0.95):
        lower = series.quantile(lower_q)
        upper = series.quantile(upper_q)

        clipped = series.clip(lower, upper)

        mean = clipped.mean()
        std = clipped.std()

        z = (clipped - mean) / std
        return z

    def _zscore(self, series, lower_q=0.05, upper_q=0.95):

        mean = series.mean()
        std = series.std()

        z = (series - mean) / std
        return z

    def _compute_momentum_features(self, df: pd.DataFrame) -> pd.DataFrame:
        # MOMENTUM handling
        # compute relative momentum metrics between stock momentum and vnindex momentum
        df["relative_m1"] = df["sm_1"] - df["vnim_1"]
        df["relative_m3"] = df["sm_3"] - df["vnim_3"]
        df["relative_m6"] = df["sm_6"] - df["vnim_6"]

        # computing z score for each momentum metric
        df["m1_z"] = self._winsorize_zscore(df["relative_m1"]).clip(-3, 3)
        df["m3_z"] = self._winsorize_zscore(df["relative_m3"]).clip(-3, 3)
        df["m6_z"] = self._winsorize_zscore(df["relative_m6"]).clip(-3, 3)

        # score for whole factor
        df["relative_momentum_score"] = 0.4 * df["m3_z"] + 0.35 * df["m6_z"] + 0.25 * df["m1_z"]

        # this code is used to plot relative momentum score, for better visualization
        # df_sorted = df.sort_values("relative_momentum_score")
        # plt.bar(df_sorted["symbol"], df_sorted["relative_momentum_score"])
        # plt.axhline(0)
        # plt.title("Momentum Score by Stock")
        # plt.ylabel("Z-score")
        # plt.show()

        df["momentum_adjusted"] = df["relative_momentum_score"] * (1 - df["vol_20d_pct_126"])
        return df

    def _compute_trend_features(self, df: pd.DataFrame) -> pd.DataFrame:
        # TREND handling
        # compute raw trend metrics
        df["close_sma50"] = df["close"] / df["sma_50"]
        df["sma20_sma50"] = df["sma_20"] / df["sma_50"]

        # compute z score
        df["close_sma50_z"] = self._winsorize_zscore(df["close_sma50"]).clip(-3, 3)
        df["sma20_sma50_z"] = self._winsorize_zscore(df["sma20_sma50"]).clip(-3, 3)

        # score for whole factor
        df["trend_score"] = 0.6 * df["close_sma50_z"] + 0.4 * df["sma20_sma50_z"]
        return df

    def _compute_volatility_features(self, df: pd.DataFrame) -> pd.DataFrame:
        # VOLATILITY factor
        # compute raw volatility metric
        df["atr14_close"] = df["atr_14_w"] / df["close"]

        # compute z score
        df["atr14_close_z"] = self._winsorize_zscore(df["atr14_close"]).clip(-3, 3)
        df["vol_20d_pct_126_z"] = self._winsorize_zscore(df["vol_20d_pct_126"]).clip(-3, 3)

        # score for whole factor
        df["volatility_score"] = -0.5 * df["atr14_close_z"] + -0.5 * df["vol_20d_pct_126_z"]
        return df

    def _compute_rsi_features(self, df: pd.DataFrame) -> pd.DataFrame:
        # RSI factor
        # compute raw RSI metric
        df["rsi_centered"] = abs(df["rsi_14"] - 55)
        df["rsi_score"] = -df["rsi_centered"]

        # compute z score
        df["rsi_14_z"] = self._zscore(df["rsi_score"]).clip(-3, 3)
        return df

    def _compute_alpha_score(self, df: pd.DataFrame) -> pd.DataFrame:
        # FINAL ALPHA SCORE construct
        df["alpha_score"] = (0.45 * df["momentum_adjusted"]
                             + 0.30 * df["trend_score"]
                             + 0.15 * df["volatility_score"]
                             + 0.10 * df["rsi_14_z"])
        return df

    def _select_persisted_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # Return single-row dataframe aligned with persistence contract.
        return df[
            [
                StockRankingFilterConstant.TIME_KEY.value,
                StockRankingFilterConstant.SYMBOL_KEY.value,
                StockRankingFilterConstant.ALPHA_SCORE_KEY.value,
            ]
        ]

    def _synthesis_logic(self, df: pd.DataFrame) -> pd.DataFrame:

        # Convert numeric-like object columns (common DB NUMERIC) to float64.
        # CAUTION: conversion fails if any object column contains non-numeric values.
        df = PandasUtil.cast_object_columns_to_float64(df)

        df = self._compute_momentum_features(df)
        df = self._compute_trend_features(df)
        df = self._compute_volatility_features(df)
        df = self._compute_rsi_features(df)

        df = self._compute_alpha_score(df)

        df = df.sort_values(by="alpha_score", ascending=False)

        return self._select_persisted_columns(df)

    # =========================================================
    # Feature Engineering
    # =========================================================

    def _backfill_synthesize(self) -> pd.DataFrame:

        # Latest feature snapshot up to boundary date (inclusive by SQL boundary logic).
        data = PostgresSQLUtil.run_sql(
            StockRankingFilterSQLQueries.GET_DATA_BY_DATE_RANGE,
            (self.start_date,self.end_date)
        )

        # Materialize SQL rows into DataFrame for vectorized logic.
        df = pd.DataFrame(data)

        if df.empty:
            return df

        frames = (
            self._synthesis_logic(df_day)
            for _, df_day in df.groupby("time")
        )

        return pd.concat(frames, ignore_index=True)


    def _incremental_synthesize(self) -> pd.DataFrame:

        # Latest feature snapshot up to boundary date (inclusive by SQL boundary logic).
        data = PostgresSQLUtil.run_sql(
            StockRankingFilterSQLQueries.GET_DATA_BY_DATE,
            (self.current_time,)
        )

        # Materialize SQL rows into DataFrame for vectorized logic.
        df = pd.DataFrame(data)

        if df.empty:
            return df

        return self._synthesis_logic(df)

