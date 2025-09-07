import snowflake.connector
import json
from mongo_to_snowflake.config import (
    SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA, SNOWFLAKE_TABLE,
    SNOWFLAKE_PSA_SCHEMA, SNOWFLAKE_PSA_TABLE
)

def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    print("✅ Connected to Snowflake")
    return conn

def load_to_staging(conn, mongo_data):
    cursor = conn.cursor()
    rows_to_insert = [(json.dumps(doc),) for doc in mongo_data]

    cursor.executemany(f"""
        INSERT INTO {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (RAW_DATA)
        VALUES (%s)
    """, rows_to_insert)

    cursor.execute(f"SELECT COUNT(*) FROM {SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}")
    stg_count = cursor.fetchone()[0]
    return stg_count

def load_to_psa_if_counts_match(conn, mongo_count, stg_count):
    cursor = conn.cursor()
    print(f"📊 MongoDB count = {mongo_count}, Staging count = {stg_count}")

    if mongo_count == stg_count:
        print(f"✅ Counts match — loading into {SNOWFLAKE_PSA_SCHEMA}.{SNOWFLAKE_PSA_TABLE}...")
        cursor.execute(f"""
            INSERT INTO {SNOWFLAKE_PSA_SCHEMA}.{SNOWFLAKE_PSA_TABLE} (ID, STUDENT_ID, CLASS_ID, TYPE, SCORE)
            SELECT
                RAW_DATA:_id::STRING        AS ID,
                RAW_DATA:student_id::FLOAT  AS STUDENT_ID,
                RAW_DATA:class_id::FLOAT    AS CLASS_ID,
                s.value:type::STRING        AS TYPE,
                s.value:score::FLOAT        AS SCORE
            FROM (
                SELECT PARSE_JSON(RAW_DATA) AS RAW_DATA
                FROM STG.STUDENT_GRADES_1
            ),
            LATERAL FLATTEN(input => RAW_DATA:scores) s;
        """)
        conn.commit()
        print(f"🎉 Successfully loaded data into PSA....")
    else:
        print("⚠️ Counts do not match! Skipping PSA load.")
