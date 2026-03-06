from enum import Enum


class MaConstant(Enum):
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
    OBJECT_DTYPE = "object"
    FLOAT64_DTYPE = "float64"

    SMA_20_KEY = "sma_20"
    SMA_50_KEY = "sma_50"
    SMA_200_KEY = "sma_200"
    SMA_20_WINDOW = 20
    SMA_50_WINDOW = 50
    SMA_200_WINDOW = 200
    SMA_21_WINDOW = 21
    SMA_51_WINDOW = 51
    SMA_201_WINDOW = 201

    SET_SYNC_COMMIT_OFF = "SET LOCAL synchronous_commit = OFF;"
