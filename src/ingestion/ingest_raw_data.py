"""
Bronze Layer Ingestion
Reads raw JSON event data and writes it to a Delta Lake Bronze table.
Simulates a streaming ingestion pattern using batch mode.
"""
import os
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from src.utils.spark_session import get_spark_session

# ── Paths ──────────────────────────────────────────────────────────────────────
RAW_DATA_PATH = "data/sample_events.json"
BRONZE_TABLE_PATH = "delta/bronze/events"

# ── Schema ─────────────────────────────────────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("event_id",   StringType(),   nullable=False),
    StructField("user_id",    StringType(),   nullable=False),
    StructField("event_type", StringType(),   nullable=True),
    StructField("product_id", StringType(),   nullable=True),
    StructField("amount",     DoubleType(),   nullable=True),
    StructField("currency",   StringType(),   nullable=True),
    StructField("timestamp",  StringType(),   nullable=True),
    StructField("country",    StringType(),   nullable=True),
    StructField("device",     StringType(),   nullable=True),
])


def ingest_bronze() -> None:
    """
    Reads raw JSON events and appends them to the Bronze Delta table.
    Adds ingestion metadata columns.
    """
    spark = get_spark_session("BronzeIngestion")

    print("[Bronze] Reading raw JSON events...")
    raw_df = (
        spark.read
        .schema(EVENT_SCHEMA)
        .option("multiLine", True)
        .json(RAW_DATA_PATH)
    )

    # Add ingestion metadata
    bronze_df = raw_df.withColumns({
        "ingested_at": F.current_timestamp(),
        "source_file": F.lit(RAW_DATA_PATH),
    })

    row_count = bronze_df.count()
    print(f"[Bronze] Ingested {row_count} raw records.")

    # Write to Delta Lake (append mode for idempotent re-runs)
    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .save(BRONZE_TABLE_PATH)
    )
    print(f"[Bronze] Data written to Delta table: {BRONZE_TABLE_PATH}")


if __name__ == "__main__":
    ingest_bronze()
