from enum import Enum


class MomentumConstant(Enum):
    MAX_WORKERS = 5

    MODE_BACKFILL = "backfill"
    MODE_INCREMENTAL = "incremental"

    LOG_START = "Start processing {symbol}"
    LOG_FINISH = "Finished processing {symbol}"
    LOG_ERROR = "Error processing {symbol}: {error}"
    LOG_PARALLEL_START = " Parallel momentum pipeline started"
    LOG_PARALLEL_FINISH = "Parallel momentum pipeline completed"

    SYMBOL_KEY = "symbol"
    TIME_KEY = "time"
    CLOSE_KEY = "close"
    CLOSE_63_KEY = "close_63"
    CLOSE_126_KEY = "close_126"
    CLOSE_252_KEY = "close_252"

    M_3_KEY = "m_3"
    M_6_KEY = "m_6"
    M_12_KEY = "m_12"
    M_COMPOSITE_KEY = "m_composite"

    OBJECT_DTYPE = "object"
    FLOAT64_DTYPE = "float64"

    WINDOW_3 = 63
    WINDOW_6 = 126
    WINDOW_12 = 252

    ONE = 1
    COMPOSITE_DIVISOR = 3

    SET_SYNC_COMMIT_OFF = "SET LOCAL synchronous_commit = OFF;"
