from enum import Enum


class VolatilityConstant(Enum):
    MAX_WORKERS = 5
    MODE_BACKFILL = "backfill"
    MODE_INCREMENTAL = "incremental"

    LOG_START = "Start processing {symbol}"
    LOG_FINISH = "Finished processing {symbol}"
    LOG_ERROR = "Error processing {symbol}: {error}"
    LOG_PARALLEL_START = " Parallel moving average pipeline started"
    LOG_PARALLEL_FINISH = "Parallel moving average pipeline completed"

    SYMBOL_KEY = "symbol"
    TIME_KEY = "time"
    CLOSE_KEY = "close"
    HIGH_KEY = "high"
    LOW_KEY = "low"
    OBJECT_DTYPE = "object"
    FLOAT64_DTYPE = "float64"

    VOL_20D = "vol_20d"
    VOL_60D = "vol_60d"
    VOL_126D = "vol_126d"
    VOL_252D = "vol_252d"
    ATR_14_R = "atr_14_r"
    ATR_14_W = "atr_14_w"
    PARKINSON_20D = "parkinson_20d"
    VOL_20D_PCT = "vol_20d_pct"

    SET_SYNC_COMMIT_OFF = "SET LOCAL synchronous_commit = OFF;"
