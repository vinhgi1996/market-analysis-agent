import logging

import numpy as np

from config.postgre_manager import PostgresManager

from constant.constants.vnindex.vnindex_volatility_constant import VolatilityConstant
from constant.sql.vnindex.vnindex_volatility_sql_queries import VolatilitySQLQueries
from util.pandas_util import PandasUtil
from util.postgre_sql import PostgresSQLUtil



import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
from io import StringIO

class VnIndexVolatilityPipeline:
    """
    VNINDEX volatility feature pipeline.

    Responsibility at class scale:
    - Orchestrate end-to-end volatility generation for VNINDEX history.
    - Support two execution modes:
      1) backfill: recompute across full available history.
      2) incremental: recompute only the latest effective date snapshot.
    - Persist output into `vnindex_volatility` using delete-then-copy semantics.

    Output contract:
    - Required columns: `time`, `vol_20d`
    - Volatility definition: annualized standard deviation of log returns
      over a 20-session rolling window (multiplied by sqrt(252)).
    """

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

    def run(self):
        """
        Entry point for pipeline execution.

        Function-scale behavior:
        - Decide synthesis path based on mode (`backfill` or `incremental`).
        - Persist resulting dataframe via `_store`.
        - Log lifecycle start/finish/error.
        """

        try:
            self.logger.info(
                VolatilityConstant.LOG_START.value
            )

            df = None

            if self.mode == VolatilityConstant.MODE_BACKFILL.value:
                df = self._backfill_synthesize()
            elif self.mode == VolatilityConstant.MODE_INCREMENTAL.value:
                df = self._incremental_synthesize(self.current_time)
            # Persist dataframe (delete existing range first, then bulk copy).
            self._store(df, self.current_time)

            self.logger.info(
                VolatilityConstant.LOG_FINISH.value
            )
        except Exception as e:
            # Error is logged, not re-raised -> job continues for other symbols.
            self.logger.error(
                VolatilityConstant.LOG_ERROR.value.format(symbol='VNINDEX', error=e)
            )

    # =========================================================
    # Feature Engineering
    # =========================================================

    def _calculate_log_return(self, df: pd.DataFrame) -> pd.Series:
        """
        Compute one-period log return from close prices.

        Formula:
        - log_return_t = ln(close_t / close_{t-1})
        """
        return np.log(df["close"] / df["close"].shift(1))

    def _calculate_rolling_volatility(self, log_return: pd.Series) -> dict[str, pd.Series]:
        """
        Build volatility feature series from log returns.

        Current implementation:
        - Only `vol_20d` is produced.
        - Uses rolling standard deviation and annualizes by sqrt(252).
        """
        volatility_map = {}
        for w in [20]:
            volatility_map[f"vol_{w}d"] = (
                    log_return
                    .rolling(w)
                    .std() * np.sqrt(252)
            )
        return volatility_map


    def _backfill_synthesize(self) -> pd.DataFrame:
        """
        Generate full-history volatility dataset.

        Function-scale steps:
        - Load historical OHLC data from source table.
        - Cast numeric-like object columns to float for stable math operations.
        - Compute log returns and rolling volatility features.
        - Return only target persistence columns in COPY order.
        """

        # Backfill: fetch broad historical set for symbol.
        data = PostgresSQLUtil.run_sql(
            VolatilitySQLQueries.GET_DATA,
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

        # Keep only persistence contract columns (order matters for COPY).
        return df[
            [
                VolatilityConstant.TIME_KEY.value,
                VolatilityConstant.VOL_20D.value,
            ]
        ]

    def _incremental_synthesize(self, date: str) -> pd.DataFrame:
        """
        Generate a single-date volatility snapshot for incremental processing.

        Function-scale steps:
        - Fetch bounded history ending at `date` (inclusive by query design).
        - Validate minimum rows required for 20-session window math.
        - Compute features on the bounded set.
        - Return only the latest row in target output shape.
        """
        # Pull the minimum bounded history needed to compute the latest 20-day vol.
        data = PostgresSQLUtil.run_sql(
            VolatilitySQLQueries.GET_DATA_BY_DATE,
            (date,)
        )

        # 20-day rolling std requires enough observations for a valid last window.
        if len(data) < 20 :
            raise ValueError("Insufficient data for incremental computation")

        # Convert row dict/list into DataFrame for vectorized computation.
        data_df = pd.DataFrame(data).copy()


        # Convert object columns to float64 (commonly NUMERIC from DB).
        # CAUTION: This attempts conversion for all object columns, which may fail
        # if non-numeric string columns exist in result set.
        data_df = PandasUtil.cast_object_columns_to_float64(data_df)

        # Log returns
        data_df["log_return"] = self._calculate_log_return(data_df)

        # Rolling volatility
        vol_map = self._calculate_rolling_volatility(data_df["log_return"])
        for col_name, series in vol_map.items():
            data_df[col_name] = series

        # Keep only the newest computed observation for incremental write.
        last_row = data_df.iloc[-1]

        # Keep only persistence contract columns (order matters for COPY).
        return pd.DataFrame([{
            VolatilityConstant.TIME_KEY.value: last_row[VolatilityConstant.TIME_KEY.value],
            VolatilityConstant.VOL_20D.value: last_row[VolatilityConstant.VOL_20D.value],
        }])

    # =========================================================
    # Persistence
    # =========================================================

    def _store(self, df: pd.DataFrame, date: str):
        """
        Persist one symbol dataframe:
        - Skip if empty
        - Delete existing target rows in scope
        - Bulk insert via COPY for speed

        Operation-scale notes:
        - Executes delete + copy in one DB transaction.
        - Uses `synchronous_commit = off` for faster ETL throughput in recoverable flows.
        - Delete scope is controlled by `date` and query predicate (`time >= %s`).
        """

        # No-op for empty synthesis output (avoids unnecessary DB roundtrip).
        if df.empty:
            return

        # Assumes dataframe contains a single symbol only.

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
                    (date,)
                )

                # Prepare CSV in-memory buffer for COPY command.
                # This avoids row-by-row INSERT overhead.
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

