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
    CLOSE_60_KEY = "close_60"


    M_60D_KEY = "m_60d"


    OBJECT_DTYPE = "object"
    FLOAT64_DTYPE = "float64"

    ONE = 1
    COMPOSITE_DIVISOR = 3

    SET_SYNC_COMMIT_OFF = "SET LOCAL synchronous_commit = OFF;"
