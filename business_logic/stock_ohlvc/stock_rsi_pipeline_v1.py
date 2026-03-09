import logging

from config.postgre_manager import PostgresManager
from constant.constants.stock.stock_rsi_constant import RsiConstant
from constant.sql.stock.stock_rsi_sql_queries import RsiSQLQueries
from util.pandas_util import PandasUtil
from util.postgre_sql import PostgresSQLUtil
# Thread pool for running symbol jobs concurrently.
from concurrent.futures import ThreadPoolExecutor, as_completed


import pandas as pd
from io import StringIO

class RsiPipeline:
    """
    Stock RSI feature pipeline.

    Class-scale responsibilities:
    - Compute RSI(14) and its smoothing state (`average_gain_14`, `average_loss_14`)
      per symbol.
    - Support `backfill` (full-history recompute) and `incremental` (latest row update).
    - Persist output into `stock_relative_strength_index` via scoped delete + COPY.

    Output contract:
    - `symbol`, `time`, `rsi_14`, `average_gain_14`, `average_loss_14`

    Data-flow overview:
    1) Read source close prices from `ohlcv_prices` (full history or bounded window).
    2) Compute RSI(14) values:
       - Backfill path: vectorized series calculation over full dataset.
       - Incremental path: one-step Wilder recursion from previously stored state.
    3) Persist into `stock_relative_strength_index` using:
       - scoped DELETE (`symbol`, `time >= date`)
       - COPY bulk insert in the same transaction.

    Why store average gain/loss:
    - These are the state variables of Wilder RSI smoothing.
    - Persisting them avoids reprocessing long history in incremental mode.
    - Next run can continue exactly from prior state with stable numerical behavior.
    """

    def __init__(
            self,
            max_workers: int = 5,
            symbol_queries: str = "",
            current_time: str = "",
            mode: str = ""
    ):
        # Logger is namespaced by concrete class for pipeline traceability.
        self.logger = logging.getLogger(self.__class__.__name__)

        # Worker count used in parallel symbol processing.
        self.max_workers = max_workers

        # SQL used to load symbol universe for `run_all_parallel`.
        self.symbol_queries = symbol_queries

        # Effective processing date/time used by incremental reads and delete scope.
        self.current_time = current_time

        # Execution mode selector. Expected: "backfill" or "incremental".
        self.mode = mode

    # =========================================================
    # Public API
    # =========================================================

    def run(self, symbol: str):
        """
        Execute pipeline for one symbol.

        Function-scale flow:
        - Select synthesis path based on mode.
        - Compute RSI outputs.
        - Persist with delete-then-COPY idempotent write pattern.

        Mode behavior:
        - `backfill`:
          Recompute historical RSI rows for the symbol and refresh downstream table.
        - `incremental`:
          Compute only the newest row based on latest close + persisted RSI state.

        Operational expectations:
        - Caller supplies a valid `symbol`.
        - `self.current_time` should represent the processing boundary for incremental runs.
        - On failure, function logs and returns (exception is not re-thrown).
        """

        try:
            self.logger.info(
                RsiConstant.LOG_START.value.format(symbol=symbol)
            )

            df = None

            if self.mode == RsiConstant.MODE_BACKFILL.value:
                df = self._backfill_synthesize(symbol)
            elif self.mode == RsiConstant.MODE_INCREMENTAL.value:
                df = self._incremental_synthesize(symbol,self.current_time)
            # Persist computed data after scoped cleanup.
            self._store(df, self.current_time)

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

    def _backfill_synthesize(self, symbol: str) -> pd.DataFrame:
        """
        Build full-history RSI dataset for one symbol.

        Function-scale steps:
        - Load complete close history for symbol.
        - Cast numeric-like columns for reliable math operations.
        - Compute RSI(14) using Wilder EMA-style smoothing.
        - Return only persistence columns in COPY order.

        Input assumptions:
        - SQL query returns rows ordered by time ascending.
        - Source rows include at least: `symbol`, `time`, `close`.

        Numerical notes:
        - First diff is NaN (no previous close).
        - First valid RSI appears only after enough history (`period=14`).
        - If average loss is zero at any point, RSI tends toward 100.
        """

        # Backfill path reads full symbol history.
        data = PostgresSQLUtil.run_sql(
            RsiSQLQueries.GET_DATA_BY_SYMBOL_OHLVC,
            (symbol,)
        )

        # Convert DB result rows to DataFrame to leverage vectorized pandas operations.
        df = pd.DataFrame(data).copy()

        # Normalize numeric-like object columns (common from DB NUMERIC) to float64.
        # This avoids mixed dtype arithmetic and improves numerical consistency.
        df = PandasUtil.cast_object_columns_to_float64(df)

        # RSI period length (classic configuration).
        period = 14

        # Session-to-session close-price delta:
        # delta_t = close_t - close_{t-1}
        delta = df["close"].diff()

        # Decompose delta into directional components used by RSI:
        # gain_t = max(delta_t, 0)
        # loss_t = max(-delta_t, 0)
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)

        # Wilder smoothing (recursive EMA form with alpha=1/period):
        # avg_gain_t = (avg_gain_{t-1} * (period - 1) + gain_t) / period
        # avg_loss_t = (avg_loss_{t-1} * (period - 1) + loss_t) / period
        # `min_periods=period` delays non-null values until sufficient history exists.
        avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

        # Relative Strength and RSI transform:
        # RS_t = avg_gain_t / avg_loss_t
        # RSI_t = 100 - (100 / (1 + RS_t))
        # RSI is bounded in [0, 100].
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        df[RsiConstant.RSI_14.value] = rsi
        df[RsiConstant.AVG_GAIN_14.value] = avg_gain
        df[RsiConstant.AVG_LOSS_14.value] = avg_loss
        # Keep only persistence contract columns (order matters for COPY).
        return df[
            [
                RsiConstant.SYMBOL_KEY.value,
                RsiConstant.TIME_KEY.value,
                RsiConstant.RSI_14.value,
                RsiConstant.AVG_GAIN_14.value,
                RsiConstant.AVG_LOSS_14.value,
            ]
        ]

    def _incremental_synthesize(self, symbol: str, date: str) -> pd.DataFrame:
        """
        Build one-row RSI snapshot for incremental mode.

        Function-scale steps:
        - Read latest two closes up to target date.
        - Read previous stored RSI smoothing state.
        - Apply Wilder one-step update for avg gain/loss.
        - Compute RSI and return single-row output.

        Why this is efficient:
        - It avoids recomputing full RSI history.
        - Only newest close and previous smoothing state are needed.

        Required inputs:
        - Two close prices: previous and current.
        - Previous `average_gain_14` and `average_loss_14`.
        """
        # Need previous + current close to compute one-step gain/loss update.
        ohlvc_data = PostgresSQLUtil.run_sql(
            RsiSQLQueries.GET_DATA_BY_DATE_SYMBOL_OHLVC,
            (symbol, date)
        )
        # Need previous smoothing state for exact Wilder recursion continuity.
        rsi_data = PostgresSQLUtil.run_sql(
            RsiSQLQueries.GET_DATA_BY_DATE_SYMBOL_RSI,
            (symbol, date)
        )
        # Incremental RSI requires both price-step data and prior smoothed state.
        if len(ohlvc_data) < 2 or not rsi_data:
            raise ValueError("Insufficient data for incremental RSI computation")

        # Extract raw values directly (faster and simpler than temporary DataFrames).
        prev_close = float(ohlvc_data[-2]["close"])
        current_row = ohlvc_data[-1]
        current_close = float(current_row["close"])

        # Persisted averages from the latest prior RSI row.
        prev_avg_gain = float(rsi_data[-1]["average_gain_14"])
        prev_avg_loss = float(rsi_data[-1]["average_loss_14"])

        # Current session price delta and split into gain/loss legs.
        delta = current_close - prev_close
        gain = max(delta, 0)
        loss = max(-delta, 0)

        # Wilder one-step recursive update for period=14.
        # Equivalent to EMA(alpha=1/14) continuation from prior state.
        avg_gain = (prev_avg_gain * 13 + gain) / 14
        avg_loss = (prev_avg_loss * 13 + loss) / 14

        # Guard division-by-zero in RS when the smoothed loss is zero.
        # Convention: zero loss implies RSI saturates at upper bound (100).
        if avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

        # Return exactly one row (latest session snapshot) for persistence.
        result_df = pd.DataFrame([{
            RsiConstant.SYMBOL_KEY.value: symbol,
            RsiConstant.TIME_KEY.value: current_row[RsiConstant.TIME_KEY.value],
            RsiConstant.RSI_14.value: rsi,
            RsiConstant.AVG_GAIN_14.value: avg_gain,
            RsiConstant.AVG_LOSS_14.value: avg_loss
        }])
        return result_df


    # =========================================================
    # Persistence
    # =========================================================

    def _store(self, df: pd.DataFrame, date: str):
        """
        Persist synthesized RSI rows:
        - Skip if empty
        - Delete existing target rows in scope
        - Bulk insert via COPY for throughput

        Operation-scale notes:
        - Delete and COPY run in one transaction.
        - `synchronous_commit = off` is used for ETL speed in recoverable workflows.

        Idempotency behavior:
        - Re-running for the same `symbol` and `date` window yields stable final state
          because old rows in scope are deleted before COPY insert.

        Scope rules:
        - Delete filter is constrained by symbol + time boundary.
        - This avoids affecting other symbols and historical rows before boundary.
        """

        # No data produced -> no persistence work.
        if df.empty:
            return

        # Assumes one-symbol dataframe (true for per-symbol `run(symbol)` orchestration).
        symbol = df[RsiConstant.SYMBOL_KEY.value].iloc[0]

        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:
                # Performance optimization for ETL:
                # faster commit behavior by relaxing synchronous durability guarantees.
                # Use only when data can be recomputed/recovered if needed.
                cursor.execute(RsiConstant.SET_SYNC_COMMIT_OFF.value)

                # Step 1: delete target scope before insert (idempotent refresh pattern).
                cursor.execute(
                    RsiSQLQueries.DELETE_RSI_DATA,
                    (symbol, date)
                )

                # Step 2: serialize dataframe to in-memory CSV for COPY ingestion.
                # header=False because COPY query already defines destination columns.
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # Step 3: bulk insert rows into RSI table.
                cursor.copy_expert(
                    RsiSQLQueries.COPY_RSI_DATA,
                    buffer
                )

                # Step 4: commit delete + copy as one atomic write unit.
            conn.commit()
