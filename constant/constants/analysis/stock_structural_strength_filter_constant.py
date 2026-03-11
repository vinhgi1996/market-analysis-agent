from enum import Enum


class StockStructuralStrengthFilterConstant(Enum):
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

    M_3_THRESHOLD = 'm_3_threshold'
    M_1_THRESHOLD = 'm_1_threshold'
    RSI_14_THRESHOLD = 'rsi_14_threshold'
    VOL_20D_PCT_126_THRESHOLD = 'vol_20d_pct_126_threshold'
    ATR_14_CLOSE_THRESHOLD = 'rsi_14_close_threshold'
    VOLUME_RATIO_THRESHOLD = 'volume_ratio_threshold'
    SUGGESTION= 'suggestion'
    POSITIVE_STREAK = 'positive_streak'

    SOURCE_SMA_20 = "sma_20"
    SOURCE_SMA_50 = "sma_50"
    SOURCE_M_1 = "m_1"
    SOURCE_M_3 = "m_3"
    SOURCE_RSI_14 = "rsi_14"
    SOURCE_ATR_14_W = "atr_14_w"
    SOURCE_VOL_20D_PCT_126 = "vol_20d_pct_126"
    SOURCE_VOLUME_RATIO = "volume_ratio"

    M_3_THRESHOLD_VALUE = 0
    M_1_THRESHOLD_VALUE = -0.05
    RSI_14_THRESHOLD_VALUE = 75
    VOL_20D_PCT_126_THRESHOLD_VALUE = 90
    ATR_14_CLOSE_THRESHOLD_VALUE = 0.08
    VOLUME_RATIO_THRESHOLD_VALUE = 0.7
    ZERO = 0
    ONE = 1

    SUGGESTION_POSSIBLE = "POSSIBLE"
    SUGGESTION_AVOID = "AVOID"

    ERROR_INSUFFICIENT_INCREMENTAL_DATA = "Insufficient data for incremental computation"

    SET_SYNC_COMMIT_OFF = "SET LOCAL synchronous_commit = OFF;"
