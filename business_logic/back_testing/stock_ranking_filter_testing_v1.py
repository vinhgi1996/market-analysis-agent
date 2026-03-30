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

import statsmodels.api as sm

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

    def _compute_ic_multiple_return(self,group, return_col):

        if len(group) < 5:
            return np.nan

        return spearmanr(
            group["alpha_score"],
            group[return_col]
        )[0]

    def _newey_west_tstat(self, ic_series, lag=20):

        ic_array = np.array(ic_series.dropna())

        X = np.ones(len(ic_array))

        model = sm.OLS(ic_array, X)

        results = model.fit(cov_type='HAC', cov_kwds={'maxlags': lag})

        mean_ic = results.params[0]
        t_stat = results.tvalues[0]

        return mean_ic, t_stat

    def factor_decay_curve(self):
        try:
            self.logger.info(
                StockRankingFilterConstant.LOG_START.value
            )

            # Latest feature snapshot up to boundary date (inclusive by SQL boundary logic).
            ohlvc_data = PostgresSQLUtil.run_sql(
                StockRankingTestingSQLQueries.GET_STOCK_OHLVC_DATA_BY_DATE_RANGE_V2,
                (self.start_date, self.end_date,self.start_date, self.end_date)
            )

            stock_ranking_data = PostgresSQLUtil.run_sql(
                StockRankingTestingSQLQueries.GET_STOCK_RANKING_DATA_BY_DATE_RANGE,
                (self.start_date, self.end_date)
            )

            # Materialize SQL rows into DataFrame for vectorized logic.
            ohlvc_df = pd.DataFrame(ohlvc_data)
            stock_ranking_df = pd.DataFrame(stock_ranking_data)

            ohlvc_df = PandasUtil.cast_object_columns_to_float64(ohlvc_df)
            stock_ranking_df = PandasUtil.cast_object_columns_to_float64(stock_ranking_df)

            ohlvc_df["fwd_return_5d"] = ohlvc_df["close_5"] / ohlvc_df["close"] - 1
            ohlvc_df["fwd_return_10d"] = ohlvc_df["close_10"] / ohlvc_df["close"] - 1
            ohlvc_df["fwd_return_15d"] = ohlvc_df["close_15"] / ohlvc_df["close"] - 1
            ohlvc_df["fwd_return_20d"] = ohlvc_df["close_20"]/ohlvc_df["close"] - 1
            ohlvc_df["fwd_return_25d"] = ohlvc_df["close_25"] / ohlvc_df["close"] - 1
            ohlvc_df["fwd_return_30d"] = ohlvc_df["close_30"] / ohlvc_df["close"] - 1
            ohlvc_df["fwd_return_35d"] = ohlvc_df["close_35"] / ohlvc_df["close"] - 1
            ohlvc_df["fwd_return_40d"] = ohlvc_df["close_40"] / ohlvc_df["close"] - 1
            ohlvc_df["fwd_return_45d"] = ohlvc_df["close_45"] / ohlvc_df["close"] - 1
            ohlvc_df["fwd_return_50d"] = ohlvc_df["close_50"] / ohlvc_df["close"] - 1
            ohlvc_df["fwd_return_55d"] = ohlvc_df["close_55"] / ohlvc_df["close"] - 1
            ohlvc_df["fwd_return_60d"] = ohlvc_df["close_60"] / ohlvc_df["close"] - 1

            df_merged = pd.merge(stock_ranking_df, ohlvc_df, on=['time', 'symbol'])
            df_merged = df_merged.dropna(subset=["alpha_score",
                                                 "fwd_return_5d",
                                                 "fwd_return_10d",
                                                 "fwd_return_15d",
                                                 "fwd_return_20d",
                                                 "fwd_return_25d",
                                                 "fwd_return_30d",
                                                 "fwd_return_35d",
                                                 "fwd_return_40d",
                                                 "fwd_return_45d",
                                                 "fwd_return_50d",
                                                 "fwd_return_55d",
                                                 "fwd_return_60d"])
            df_merged["fwd_return_5d"] = df_merged["fwd_return_5d"].clip(-0.5, 0.5)
            df_merged["fwd_return_10d"] = df_merged["fwd_return_10d"].clip(-0.5, 0.5)
            df_merged["fwd_return_15d"] = df_merged["fwd_return_15d"].clip(-0.5, 0.5)
            df_merged["fwd_return_20d"] = df_merged["fwd_return_20d"].clip(-0.5, 0.5)
            df_merged["fwd_return_25d"] = df_merged["fwd_return_25d"].clip(-0.5, 0.5)
            df_merged["fwd_return_30d"] = df_merged["fwd_return_30d"].clip(-0.5, 0.5)
            df_merged["fwd_return_35d"] = df_merged["fwd_return_35d"].clip(-0.5, 0.5)
            df_merged["fwd_return_40d"] = df_merged["fwd_return_40d"].clip(-0.5, 0.5)
            df_merged["fwd_return_45d"] = df_merged["fwd_return_45d"].clip(-0.5, 0.5)
            df_merged["fwd_return_50d"] = df_merged["fwd_return_50d"].clip(-0.5, 0.5)
            df_merged["fwd_return_55d"] = df_merged["fwd_return_55d"].clip(-0.5, 0.5)
            df_merged["fwd_return_60d"] = df_merged["fwd_return_60d"].clip(-0.5, 0.5)


            df_merged = df_merged.sort_values(
                by=['time', 'alpha_score'],
                ascending=[False, False]
            )
            print(f":{df_merged.head(100)}")

            horizons = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]

            ic_results = {}

            for h in horizons:
                col = f"fwd_return_{h}d"

                daily_ic = df_merged.groupby("time").apply(
                    lambda g: self._compute_ic_multiple_return(g, col)
                )

                ic_results[h] = daily_ic.mean()

            print(ic_results)

            horizons = list(ic_results.keys())
            values = list(ic_results.values())

            plt.plot(horizons, values, marker='o')

            plt.title("Factor Decay Curve")
            plt.xlabel("Forward Return Horizon (days)")
            plt.ylabel("Mean IC")

            plt.show()

            self.logger.info(
                StockRankingFilterConstant.LOG_FINISH.value
            )
        except Exception as e:
            # Error is logged, not re-raised -> job continues for other symbols.
            self.logger.error(
                StockRankingFilterConstant.LOG_ERROR.value.format(symbol='VNINDEX', error=e)
            )

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

            ohlvc_df = PandasUtil.cast_object_columns_to_float64(ohlvc_df)
            stock_ranking_df = PandasUtil.cast_object_columns_to_float64(stock_ranking_df)

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

            # NORMAL TSTAT
            n = len(ic_series)
            ic_tstat_naive = ic_mean / (ic_std / np.sqrt(n))

            # NEWEY-WEST TSTAT
            nw_mean_ic, ic_tstat_nw = self._newey_west_tstat(ic_series, lag=20)

            positive_ic_pct = (ic_series > 0).mean()

            print("IC Mean:", ic_mean)
            print("IC Std:", ic_std)
            print("Naive IC T-Stat:", ic_tstat_naive)
            print("Newey-West IC T-Stat:", ic_tstat_nw )
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

    def _compute_spread(self,group):

        if len(group) < 6:
            return None

        group = group.sort_values("alpha_score", ascending=False)

        n = len(group)
        k = max(1, int(n * 0.46))

        top = group.head(k)
        bottom = group.tail(k)

        top_return = top["fwd_return_20d"].mean()
        bottom_return = bottom["fwd_return_20d"].mean()

        spread = top_return - bottom_return

        return pd.Series({
            "top_return": top_return,
            "bottom_return": bottom_return,
            "spread": spread,
            "top_n": k,
            "universe_size": n
        })

    def _assign_quintiles(self,group):

        if len(group) < 10:
            return None

        group = group.sort_values("alpha_score")

        group["quintile"] = pd.qcut(
            group["alpha_score"],
            5,
            labels=[1, 2, 3, 4, 5]
        )

        return group

    def portfolio_spread_test(self):

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

            ohlvc_df = PandasUtil.cast_object_columns_to_float64(ohlvc_df)
            stock_ranking_df = PandasUtil.cast_object_columns_to_float64(stock_ranking_df)

            ohlvc_df["fwd_return_20d"] = ohlvc_df["close_20"]/ohlvc_df["close"] - 1

            df_merged = pd.merge(stock_ranking_df, ohlvc_df, on=['time', 'symbol'])
            df_merged = df_merged.dropna(subset=["alpha_score", "fwd_return_20d"])
            df_merged["fwd_return_20d"] = df_merged["fwd_return_20d"].clip(-0.5, 0.5)

            df_merged = df_merged.sort_values(
                by=['time', 'alpha_score'],
                ascending=[False, False]
            )
            print(f":{df_merged.head(100)}")

            spread_df  = df_merged.groupby("time").apply(self._compute_spread)
            spread_df = spread_df.dropna()

            spread_mean = spread_df["spread"].mean()
            spread_std = spread_df["spread"].std()
            N = len(spread_df)

            spread_tstat = spread_mean / (spread_std / np.sqrt(N))

            positive_pct = (spread_df["spread"] > 0).mean()

            spread_df["cum_spread"] = (1 + spread_df["spread"]).cumprod()

            print("Mean spread:", spread_mean)
            print("Mean Std:", spread_std)
            print("Spread T-Stat:", spread_tstat)
            print("Positive Spread %:", positive_pct)

            plt.figure(figsize=(10, 6))

            plt.plot(spread_df.index, spread_df["cum_spread"])

            plt.title("Long-Short Spread Equity Curve")
            plt.xlabel("Date")
            plt.ylabel("Cumulative Return")

            plt.grid(True)

            plt.show()

            print("Mean top_return:", spread_df["top_return"].mean())
            print("Mean bottom_return:", spread_df["bottom_return"].mean())
            print("Number of days:", len(spread_df))
            print("Average universe size:", spread_df["universe_size"].mean())
            print("Average top_n:", spread_df["top_n"].mean())

            # Factor monotonicity test (quintile portfolios)

            print(df_merged.head())

            df_q = df_merged.groupby("time", group_keys=True).apply(self._assign_quintiles)
            df_q = df_q.dropna()

            # print(type(df_q))
            # print(df_q.columns)
            # print(df_q.index)
            # print(df_q.head())

            quintile_returns = (
                df_q.groupby(["time", "quintile"])["fwd_return_20d"]
                .mean()
                .reset_index()
            )

            mean_quintile_return = (
                quintile_returns.groupby("quintile")["fwd_return_20d"]
                .mean()
            )
            print(mean_quintile_return)

            mean_quintile_return.plot(kind="bar")

            plt.title("Factor Monotonicity Test")
            plt.ylabel("Mean Forward Return")
            plt.xlabel("Quintile (Worst → Best)")
            plt.show()

            self.logger.info(
                StockRankingFilterConstant.LOG_FINISH.value
            )
        except Exception as e:
            # Error is logged, not re-raised -> job continues for other symbols.
            self.logger.error(
                StockRankingFilterConstant.LOG_ERROR.value.format(symbol='VNINDEX', error=e)
            )




