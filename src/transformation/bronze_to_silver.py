"""
Silver Layer Transformation
Cleans, deduplicates, and validates Bronze data using Delta Lake MERGE.
Produces a schema-enforced Silver table ready for aggregation.
"""
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from src.utils.spark_session import get_spark_session

# ── Paths ──────────────────────────────────────────────────────────────────────
BRONZE_TABLE_PATH = "delta/bronze/events"
SILVER_TABLE_PATH = "delta/silver/events"


def clean_bronze(df: DataFrame) -> DataFrame:
    """
    Applies data quality rules to the Bronze DataFrame:
    - Drops rows with null event_id or user_id
    - Casts timestamp string to proper TimestampType
    - Filters out non-positive amounts for purchase events
    - Normalises event_type to lowercase
    """
    return (
        df
        .dropna(subset=["event_id", "user_id"])
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        .withColumn("event_ts", F.to_timestamp(F.col("timestamp")))
        .drop("timestamp")
        .filter(
            (F.col("event_type") != "purchase") | (F.col("amount") > 0)
        )
        .withColumn("processed_at", F.current_timestamp())
    )


def upsert_to_silver(spark, cleaned_df: DataFrame) -> None:
    """
    Merges cleaned data into the Silver Delta table using event_id as key.
    Ensures no duplicate events are stored (idempotent upsert).
    """
    import os

    if os.path.exists(SILVER_TABLE_PATH):
        silver_table = DeltaTable.forPath(spark, SILVER_TABLE_PATH)
        (
            silver_table.alias("target")
            .merge(
                cleaned_df.alias("source"),
                condition="target.event_id = source.event_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("[Silver] MERGE (upsert) completed.")
    else:
        # First run — create the Silver table
        (
            cleaned_df.write
            .format("delta")
            .mode("overwrite")
            .save(SILVER_TABLE_PATH)
        )
        print(f"[Silver] Table created at: {SILVER_TABLE_PATH}")


def transform_bronze_to_silver() -> None:
    spark = get_spark_session("BronzeToSilver")

    print("[Silver] Reading Bronze Delta table...")
    bronze_df = spark.read.format("delta").load(BRONZE_TABLE_PATH)

    cleaned_df = clean_bronze(bronze_df)
    record_count = cleaned_df.count()
    print(f"[Silver] {record_count} clean records ready for Silver.")

    upsert_to_silver(spark, cleaned_df)


if __name__ == "__main__":
    transform_bronze_to_silver()
