from vnstock import Quote,Listing,Company

from io import StringIO
import pandas as pd
import logging

logger = logging.getLogger(__name__)

from config.postgre_manager import PostgresManager

def ohlvc():
    quote = Quote(symbol='BSR', source='KBS')
    histories = quote.history(start='2025-01-01', end='2026-02-25', interval="1D")
    print(f": {histories}")  # (2, 77)


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
    ohlvc()


if __name__ == "__main__":
    run_ingest()