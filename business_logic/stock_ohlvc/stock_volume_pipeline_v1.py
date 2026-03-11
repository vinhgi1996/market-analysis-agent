import logging

import numpy as np

from config.postgre_manager import PostgresManager
from constant.constants.stock.stock_moving_average_constant import MaConstant
from constant.constants.stock.stock_volume_constant import VolumeConstant
from constant.sql.stock.stock_moving_average_sql_queries import MovingAverageSQLQueries
from constant.sql.stock.stock_volume_sql_queries import VolumeSQLQueries
from util.postgre_sql import PostgresSQLUtil
# Thread pool for running symbol jobs concurrently.
from concurrent.futures import ThreadPoolExecutor, as_completed


import pandas as pd
from io import StringIO

class StockVolumePipeline:


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
                VolumeConstant.LOG_START.value.format(symbol=symbol)
            )

            df = None

            if self.mode == VolumeConstant.MODE_BACKFILL.value:
                df = self._backfill_synthesize(symbol)
            elif self.mode == VolumeConstant.MODE_INCREMENTAL.value:
                df = self._incremental_synthesize(symbol,self.current_time)

            # Persist with idempotent refresh pattern (delete scoped rows, then COPY).
            self._store(df, self.current_time)

            self.logger.info(
                VolumeConstant.LOG_FINISH.value.format(symbol=symbol)
            )
        except Exception as e:
            # Error is logged, not re-raised -> job continues for other symbols.
            self.logger.error(
                VolumeConstant.LOG_ERROR.value.format(symbol=symbol, error=e)
            )

    def run_all_parallel(self):

        # NOTE: This string appears to have encoding artifacts in current source.
        self.logger.info(VolumeConstant.LOG_PARALLEL_START.value)

        # Expected query shape: list[dict] with at least symbol field present.
        symbols = PostgresSQLUtil.run_sql(self.symbol_queries)
        symbol_list = [s[VolumeConstant.SYMBOL_KEY.value] for s in symbols]

        # Thread pool dispatch: each symbol runs independently.
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.run, symbol) for symbol in symbol_list]

            # Consume futures and re-surface unexpected thread exceptions.
            for future in as_completed(futures):
                future.result()

                # NOTE: This string appears to have encoding artifacts in current source.
        self.logger.info(VolumeConstant.LOG_PARALLEL_FINISH.value)

    # =========================================================
    # Feature Engineering
    # =========================================================

    def _backfill_synthesize(self, symbol: str) -> pd.DataFrame:


        # Backfill reads broad/full available history for the symbol.
        data = PostgresSQLUtil.run_sql(
            VolumeSQLQueries.GET_DATA_BY_SYMBOL,
            (symbol,)
        )

        # Convert DB rows into DataFrame for vectorized rolling computations.
        df = pd.DataFrame(data).copy()

        # Normalize numeric-like object columns (e.g., DB NUMERIC) to float64.
        # CAUTION: Any non-numeric object column included here would raise on cast.
        df = df.astype({
            col: VolumeConstant.FLOAT64_DTYPE.value
            for col in df.columns
            if df[col].dtype == VolumeConstant.OBJECT_DTYPE.value
        })

        # Average volume 20 day
        df[VolumeConstant.VOL_20D.value] = (
            df[VolumeConstant.VOLUME.value]
            .rolling(VolumeConstant.VOL_20_WINDOW.value)
            .mean()
            .shift(1)
        )
        # Average volume 60 day
        df[VolumeConstant.VOL_60D.value] = (
            df[VolumeConstant.VOLUME.value]
            .rolling(VolumeConstant.VOL_60_WINDOW.value)
            .mean()
            .shift(1)
        )

        df["volume_ratio"] = df[VolumeConstant.VOL_20D.value] / df[VolumeConstant.VOL_60D.value]
        df["volume_spike"] = df[VolumeConstant.VOLUME.value] / df[VolumeConstant.VOL_20D.value]

        # replace infinity value with NA to avoid error when save data into postgre
        df.replace([np.inf, -np.inf], np.nan, inplace=True)

        # Keep only persistence contract columns (order matters for COPY).
        return df[
            [
                VolumeConstant.SYMBOL_KEY.value,
                VolumeConstant.TIME_KEY.value,
                VolumeConstant.VOL_20D.value,
                VolumeConstant.VOL_60D.value,
                "volume_ratio",
                "volume_spike",
            ]
        ]

    def _incremental_synthesize(self, symbol: str, date: str) -> pd.DataFrame:

        # Query returns a recent historical slice for one symbol up to target date.
        data = PostgresSQLUtil.run_sql(
            VolumeSQLQueries.GET_DATA_BY_DATE_SYMBOL,
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

        # Average volume 20 day
        df[VolumeConstant.VOL_20D.value] = (
            df[VolumeConstant.VOLUME.value]
            .rolling(VolumeConstant.VOL_20_WINDOW.value)
            .mean()
            .shift(1)
        )
        # Average volume 60 day
        df[VolumeConstant.VOL_60D.value] = (
            df[VolumeConstant.VOLUME.value]
            .rolling(VolumeConstant.VOL_60_WINDOW.value)
            .mean()
            .shift(1)
        )

        df["volume_ratio"] = df[VolumeConstant.VOL_20D.value] / df[VolumeConstant.VOL_60D.value]
        df["volume_spike"] = df[VolumeConstant.VOLUME.value] / df[VolumeConstant.VOL_20D.value]

        # Latest row is the incremental target timestamp.
        last_row = df.iloc[-1]

        # Return exactly one output row aligned to persistence column contract.
        return pd.DataFrame([{
            VolumeConstant.SYMBOL_KEY.value: last_row[VolumeConstant.SYMBOL_KEY.value],
            VolumeConstant.TIME_KEY.value: last_row[VolumeConstant.TIME_KEY.value],
            VolumeConstant.VOL_20D.value: last_row[VolumeConstant.VOL_20D.value],
            VolumeConstant.VOL_60D.value: last_row[VolumeConstant.VOL_60D.value],
            "volume_ratio": last_row["volume_ratio"],
            "volume_spike": last_row["volume_spike"],
        }])


    # =========================================================
    # Persistence
    # =========================================================

    def _store(self, df: pd.DataFrame, date: str):

        # No output rows means no DB operation needed.
        if df.empty:
            return

        # Per-symbol run contract: dataframe should contain exactly one symbol.
        symbol = df[VolumeConstant.SYMBOL_KEY.value].iloc[0]

        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:
                # Performance optimization for recoverable ETL workloads:
                # relax commit durability guarantees for faster write throughput.
                cursor.execute(VolumeConstant.SET_SYNC_COMMIT_OFF.value)

                # Step 1: delete existing rows in symbol+date scope.
                cursor.execute(
                    VolumeSQLQueries.DELETE_VOLUME_DATA,
                    (symbol, date)
                )

                # Step 2: serialize dataframe into in-memory CSV buffer.
                # header=False because COPY query defines the destination column order.
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # Step 3: bulk insert into moving-average target table.
                cursor.copy_expert(
                    VolumeSQLQueries.COPY_VOLUME_DATA,
                    buffer
                )

                # Step 4: commit atomic refresh.
            conn.commit()
