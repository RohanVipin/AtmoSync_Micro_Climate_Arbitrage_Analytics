import json
import os
from pathlib import Path
import snowflake.connector

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "data" / "sensor_events.jsonl"

BATCH_SIZE = 1

def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    connection = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse="COMPUTE_WH",
        database="ATMOSYNC_DB",
        schema="RAW",
        role="ACCOUNTADMIN"
    )

    try:
        cursor = connection.cursor()

        cursor.execute("""
            SELECT COUNT(*)
            FROM ATMOSYNC_DB.RAW.CONTAINER_TELEMETRY_RAW
        """)
        existing_count = cursor.fetchone()[0]

        if existing_count > 0:
            raise RuntimeError(
                f"RAW table already contains {existing_count} rows. "
                "Stopping to prevent duplicate loading."
            )

        insert_sql = """
            INSERT INTO ATMOSYNC_DB.RAW.CONTAINER_TELEMETRY_RAW
                (RECORD_METADATA, RECORD_CONTENT)
            SELECT PARSE_JSON(%s), PARSE_JSON(%s)
        """

        batch = []
        total_loaded = 0

        with INPUT_FILE.open("r", encoding="utf-8") as file:
            for line in file:
                event = json.loads(line)
                batch.append(("{}", json.dumps(event)))

                if len(batch) >= BATCH_SIZE:
                    cursor.execute(insert_sql, batch[0])
                    total_loaded += len(batch)
                    batch.clear()

        if batch:
            cursor.execute(insert_sql, batch[0])
            total_loaded += len(batch)

        connection.commit()

        cursor.execute("""
            SELECT COUNT(*), COUNT(DISTINCT RECORD_CONTENT:event_id::VARCHAR)
            FROM ATMOSYNC_DB.RAW.CONTAINER_TELEMETRY_RAW
        """)
        total_rows, unique_events = cursor.fetchone()

        print("Snowflake RAW ingestion successful!")
        print("Rows loaded:", total_loaded)
        print("Total RAW rows:", total_rows)
        print("Unique event IDs:", unique_events)

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

if __name__ == "__main__":
    main()


