import pandas as pd
import logging
import time
from vnstock import Quote,Listing
from io import StringIO
from constant.sql_queries import SQLQueries
from util.postgre_sql import PostgresSQLUtil
import pandas as pd
logger = logging.getLogger(__name__)

from config.postgre_manager import PostgresManager

def vnindex():
    # Read CSV into DataFrame
    df = pd.read_csv("vnindex.csv")

    df["time"] = pd.to_datetime(df["time"], format="%d/%m/%Y")
    df["volume"] = (
        df["volume"]
        .str.upper()
        .str.extract(r"([\d.]+)([KMB])")
        .assign(mult=lambda x: x[1].map({"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}))
        .pipe(lambda x: (x[0].astype(float) * x["mult"]).round().astype("Int64"))
    )
    df["change_percentage"] = (
        df["change_percentage"]
        .str.strip()
        .str.replace("%", "", regex=False)
        .astype(float)
    )

    df["open"] = (
            df["open"]
            .str.strip()
            .str.replace(",", "", regex=False)
            .astype(float)
    )
    df["high"] = (
            df["high"]
            .str.strip()
            .str.replace(",", "", regex=False)
            .astype(float)
    )
    df["low"] = (
            df["low"]
            .str.strip()
            .str.replace(",", "", regex=False)
            .astype(float)
    )
    df["close"] = (
            df["close"]
            .str.strip()
            .str.replace(",", "", regex=False)
            .astype(float)
    )

    if df is None:
        logger.warning("⚠️ No data provided.")
        return

    if df.empty:
        logger.warning("⚠️ DataFrame is empty.")
        return

    required_columns = [
        "time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "change_percentage",
    ]

    df = df[required_columns]

    # Pandas 2/3 safe string cleaning
    string_columns = df.select_dtypes(include=["object", "string"]).columns
    for col in string_columns:
        df[col] = df[col].astype(str).str.strip()

    try:
        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:

                # 🔥 Speed boost for bulk ingest
                # this line of code turn of the disk flush in fsync process of Posgre into async
                # so the commit will return immediately and boost the insert performance
                # WARNING: do not use for the pipeline that the data is extremely important and
                # can't be recovered, only used to process data that can be recovered.
                cursor.execute("SET LOCAL synchronous_commit = OFF;")

                # Create temp staging table
                cursor.execute("""
                    CREATE TEMP TABLE tmp_vnindex_history 
                    (LIKE vnindex_history INCLUDING DEFAULTS)
                    ON COMMIT DROP;
                """)

                # Convert dataframe to CSV buffer
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # COPY to temp table
                cursor.copy_expert("""
                    COPY tmp_vnindex_history 
                    (time, open, high,
                     low, close, volume, change_percentage)
                    FROM STDIN WITH (FORMAT CSV)
                """, buffer)

                # UPSERT into main table
                cursor.execute("""
                    INSERT INTO vnindex_history 
                    (time, open, high,
                     low, close, volume, change_percentage)
                    SELECT  time, open, high,
                           low, close, volume, change_percentage
                    FROM tmp_vnindex_history
                    ON CONFLICT (time)
                    DO UPDATE SET
                        open   = EXCLUDED.open,
                        high   = EXCLUDED.high,
                        low    = EXCLUDED.low,
                        close  = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        change_percentage = EXCLUDED.change_percentage;
                """)

            conn.commit()

        logger.info(f"✅ Successfully inserted/updated {len(df)} records.")

    except Exception as e:
        logger.error(f"[ingest_symbol_master] Bulk upsert failed: {e}")
        raise

def ohlvc():
    logger.info("🔄 Ingesting symbol by exchange data...")

    symbols =  PostgresSQLUtil.run_sql(SQLQueries.GET_HOSE_ENERGY_COMPANY_SYMBOL)
    batch_size = 20
    delay = 120
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        for row in batch:
            ohlvc_insert(row["symbol"],'KBS','2023-01-01','2024-01-01',"1D")

        if i + batch_size < len(symbols):
            print(f"Processed {i + batch_size} rows. Sleeping {delay} seconds...")
            time.sleep(delay)

    print("Finished processing.")


def ohlvc_insert(symbol:str, source:str,start_date:str,end_date:str,interval:str,):

    quote = Quote(symbol=symbol, source=source)
    raw_data = quote.history(start=start_date, end=end_date, interval=interval)
    raw_data.insert(0, "symbol", symbol)
    print(f": {raw_data.shape}")

    if raw_data is None:
        logger.warning("⚠️ No data provided.")
        return

    # Ensure DataFrame
    if not isinstance(raw_data, pd.DataFrame):
        df = pd.DataFrame(raw_data)
    else:
        df = raw_data.copy()

    if df.empty:
        logger.warning("⚠️ DataFrame is empty.")
        return

    required_columns = [
        "symbol",
        "time",
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]

    df = df[required_columns]

    # Pandas 2/3 safe string cleaning
    string_columns = df.select_dtypes(include=["object", "string"]).columns
    for col in string_columns:
        df[col] = df[col].astype(str).str.strip()

    try:
        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:

                # 🔥 Speed boost for bulk ingest
                # this line of code turn the disk flush in fsync process of Postgre into async
                # so the commit will return immediately and boost the insert performance
                # WARNING: do not use for the pipeline that the data is extremely important and
                # can't be recovered, only used to process data that can be recovered.
                cursor.execute("SET LOCAL synchronous_commit = OFF;")

                # Create temp staging table
                cursor.execute("""
                    CREATE TEMP TABLE tmp_ohlcv_prices 
                    (LIKE ohlcv_prices INCLUDING DEFAULTS)
                    ON COMMIT DROP;
                """)

                # Convert dataframe to CSV buffer
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # COPY to temp table
                cursor.copy_expert("""
                    COPY tmp_ohlcv_prices 
                    (symbol, time, open, high,
                     low, close, volume)
                    FROM STDIN WITH (FORMAT CSV)
                """, buffer)

                # UPSERT into main table
                cursor.execute("""
                    INSERT INTO ohlcv_prices 
                    (symbol, time, open, high,
                     low, close, volume)
                    SELECT symbol, time, open, high,
                           low, close, volume
                    FROM tmp_ohlcv_prices
                    ON CONFLICT (symbol, time)
                    DO UPDATE SET
                        open   = EXCLUDED.open,
                        high   = EXCLUDED.high,
                        low    = EXCLUDED.low,
                        close  = EXCLUDED.close,
                        volume = EXCLUDED.volume;
                """)

            conn.commit()

        logger.info(f"✅ Successfully inserted/updated {len(df)} records.")

    except Exception as e:
        logger.error(f"[ingest_symbol_master] Bulk upsert failed: {e}")
        raise


def symbol_by_exchange():
    logger.info("🔄 Ingesting symbol by exchange data...")

    listing = Listing(source="VCI")
    raw_data = listing.symbols_by_exchange(exchange='HOSE')
    print(f": {raw_data.shape}")  # (2, 77)

    if raw_data is None:
        logger.warning("⚠️ No data provided.")
        return

    # Ensure DataFrame
    if not isinstance(raw_data, pd.DataFrame):
        df = pd.DataFrame(raw_data)
    else:
        df = raw_data.copy()

    if df.empty:
        logger.warning("⚠️ DataFrame is empty.")
        return

    required_columns = [
        "symbol",
        "exchange",
        "type",
        "organ_short_name",
        "organ_name",
        "product_grp_id",
        "icb_code2"
    ]

    df = df[required_columns]

    # Pandas 2/3 safe string cleaning
    string_columns = df.select_dtypes(include=["object", "string"]).columns
    for col in string_columns:
        df[col] = df[col].astype(str).str.strip()

    try:
        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:

                # Create temp staging table
                cursor.execute("""
                    CREATE TEMP TABLE tmp_symbol_by_exchange 
                    (LIKE symbol_by_exchange INCLUDING DEFAULTS)
                    ON COMMIT DROP;
                """)

                # Convert dataframe to CSV buffer
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # COPY to temp table
                cursor.copy_expert("""
                    COPY tmp_symbol_by_exchange
                    (symbol, exchange, type, organ_short_name,
                     organ_name, product_grp_id, icb_code2)
                    FROM STDIN WITH (FORMAT CSV)
                """, buffer)

                # UPSERT into main table
                cursor.execute("""
                    INSERT INTO symbol_by_exchange
                    (symbol, exchange, type, organ_short_name,
                     organ_name, product_grp_id, icb_code2)
                    SELECT symbol, exchange, type, organ_short_name,
                           organ_name, product_grp_id, icb_code2
                    FROM tmp_symbol_by_exchange
                    ON CONFLICT (symbol)
                    DO UPDATE SET
                        exchange = EXCLUDED.exchange,
                        type = EXCLUDED.type,
                        organ_short_name = EXCLUDED.organ_short_name,
                        organ_name = EXCLUDED.organ_name,
                        product_grp_id = EXCLUDED.product_grp_id,
                        icb_code2 = EXCLUDED.icb_code2;
                """)

            conn.commit()

        logger.info(f"✅ Successfully inserted/updated {len(df)} records.")

    except Exception as e:
        logger.error(f"[ingest_symbol_master] Bulk upsert failed: {e}")
        raise


def symbol_by_industry():
    logger.info("🔄 Loading industry symbols...")

    listing = Listing(source="VCI")
    raw_data = listing.symbols_by_industries()
    print(f": {raw_data.shape}")  # (2, 77)

    if raw_data is None:
        logger.warning("⚠️ No data returned from source.")
        return

    # --------------------------------------------------
    # Ensure pandas DataFrame
    # --------------------------------------------------
    if not isinstance(raw_data, pd.DataFrame):
        df = pd.DataFrame(raw_data)
    else:
        df = raw_data.copy()

    if df.empty:
        logger.warning("⚠️ DataFrame is empty.")
        return

    # --------------------------------------------------
    # Select required columns only
    # --------------------------------------------------
    required_columns = [
        "symbol",
        "organ_name",
        "icb_name2",
        "icb_name3",
        "icb_name4",
        "com_type_code",
        "icb_code1",
        "icb_code2",
        "icb_code3",
        "icb_code4"
    ]

    df = df[required_columns]

    # --------------------------------------------------
    # Clean string columns properly (no applymap)
    # --------------------------------------------------
    # Clean string columns (Pandas 2/3 safe)
    string_columns = df.select_dtypes(include=["object", "string"]).columns

    for col in string_columns:
        df[col] = df[col].astype(str).str.strip()

    # --------------------------------------------------
    # Bulk Insert with COPY + UPSERT
    # --------------------------------------------------
    try:
        with PostgresManager.get_sync_connection() as conn:
            with conn.cursor() as cursor:

                # Create temporary staging table
                cursor.execute("""
                    CREATE TEMP TABLE tmp_symbol_by_industry
                    (LIKE symbol_by_industry INCLUDING DEFAULTS)
                    ON COMMIT DROP;
                """)

                # Convert dataframe to CSV buffer
                buffer = StringIO()
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)

                # COPY into temp table
                cursor.copy_expert("""
                    COPY tmp_symbol_by_industry
                    (symbol, organ_name, icb_name2, icb_name3, icb_name4,
                     com_type_code, icb_code1, icb_code2, icb_code3, icb_code4)
                    FROM STDIN WITH (FORMAT CSV)
                """, buffer)

                # Upsert into main table
                cursor.execute("""
                    INSERT INTO symbol_by_industry
                    (symbol, organ_name, icb_name2, icb_name3, icb_name4,
                     com_type_code, icb_code1, icb_code2, icb_code3, icb_code4)
                    SELECT symbol, organ_name, icb_name2, icb_name3, icb_name4,
                           com_type_code, icb_code1, icb_code2, icb_code3, icb_code4
                    FROM tmp_symbol_by_industry
                    ON CONFLICT (symbol)
                    DO UPDATE SET
                        organ_name = EXCLUDED.organ_name,
                        icb_name2 = EXCLUDED.icb_name2,
                        icb_name3 = EXCLUDED.icb_name3,
                        icb_name4 = EXCLUDED.icb_name4,
                        com_type_code = EXCLUDED.com_type_code,
                        icb_code1 = EXCLUDED.icb_code1,
                        icb_code2 = EXCLUDED.icb_code2,
                        icb_code3 = EXCLUDED.icb_code3,
                        icb_code4 = EXCLUDED.icb_code4;
                """)

            conn.commit()

        logger.info(f"✅ Successfully inserted/updated {len(df)} records.")

    except Exception as e:
        logger.error(f"[symbol_by_industry] Bulk upsert failed: {e}")
        raise

def run_ingest():
    print("Ingestion started.")
    #symbol_by_industry()
    #symbol_by_exchange()
    #ohlvc()
    vnindex()


if __name__ == "__main__":
    run_ingest()