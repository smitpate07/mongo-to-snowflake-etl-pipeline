from mongo_to_snowflake.mongo_utils import fetch_from_mongo
from mongo_to_snowflake.snowflake_utils import get_snowflake_connection, load_to_staging, load_to_psa_if_counts_match

def run_pipeline():
    try:
        # Extract
        mongo_data, mongo_count = fetch_from_mongo()

        # Load to Snowflake
        conn = get_snowflake_connection()
        stg_count = load_to_staging(conn, mongo_data)

        # Validate + Load to PSA
        load_to_psa_if_counts_match(conn, mongo_count, stg_count)

    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("🔒 Snowflake connection closed.")
