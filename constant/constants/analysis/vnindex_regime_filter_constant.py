from enum import Enum


class VnIndexRegimeFilterConstant(Enum):
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

    M_60D_THRESHOLD = 'm_60d_threshold'
    DRAWDOWN_40D_THRESHOLD = 'drawdown_40d_threshold'
    VOL_20D_THRESHOLD = 'vol_20d_threshold'
    SUGGESTION= 'suggestion'

    SET_SYNC_COMMIT_OFF = "SET LOCAL synchronous_commit = OFF;"
