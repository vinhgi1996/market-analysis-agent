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
    POSITIVE_STREAK = 'positive_streak'

    SOURCE_SMA_150D = "sma_150d"
    SOURCE_M_60D = "m_60d"
    SOURCE_DRAWDOWN_40D = "drawdown_40d"
    SOURCE_VOL_20D = "vol_20d"

    M_60D_THRESHOLD_VALUE = 0.03
    DRAWDOWN_40D_THRESHOLD_VALUE = -0.08
    VOL_20D_THRESHOLD_VALUE = 0.25
    ZERO = 0
    ONE = 1

    SUGGESTION_SAFE = "SAFE"
    SUGGESTION_DEFENSIVE = "DEFENSIVE"

    ERROR_INSUFFICIENT_INCREMENTAL_DATA = "Insufficient data for incremental computation"

    SET_SYNC_COMMIT_OFF = "SET LOCAL synchronous_commit = OFF;"
