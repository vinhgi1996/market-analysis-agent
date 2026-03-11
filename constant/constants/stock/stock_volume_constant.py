from enum import Enum


class VolumeConstant(Enum):
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
    VOLUME = "volume"
    OBJECT_DTYPE = "object"
    FLOAT64_DTYPE = "float64"

    VOL_20D = "vol_20d"
    VOL_60D = "vol_60d"

    VOL_20_WINDOW = 20
    VOL_60_WINDOW = 60


    SET_SYNC_COMMIT_OFF = "SET LOCAL synchronous_commit = OFF;"
