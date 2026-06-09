"""
Gold Layer Aggregation
Produces business-level KPI tables from the Silver Delta table.
Gold tables are optimised for BI dashboards and Azure Synapse queries.
"""
from pyspark.sql import functions as F
from src.utils.spark_session import get_spark_session

# ── Paths ──────────────────────────────────────────────────────────────────────
SILVER_TABLE_PATH = "delta/silver/events"
GOLD_REVENUE_PATH = "delta/gold/revenue_by_product"
GOLD_USER_PATH    = "delta/gold/user_purchase_summary"


def build_revenue_by_product(silver_df):
    """
    Aggregates total revenue, order count, and average order value per product.
    Filters to purchase events only.
    """
    return (
        silver_df
        .filter(F.col("event_type") == "purchase")
        .groupBy("product_id", "country", "currency")
        .agg(
            F.sum("amount").alias("total_revenue"),
            F.count("event_id").alias("order_count"),
            F.avg("amount").alias("avg_order_value"),
            F.max("event_ts").alias("last_purchase_ts"),
        )
        .withColumn("report_date", F.current_date())
    )


def build_user_purchase_summary(silver_df):
    """
    Summarises purchase behaviour per user — total spend, order count, devices used.
    """
    return (
        silver_df
        .filter(F.col("event_type") == "purchase")
        .groupBy("user_id")
        .agg(
            F.sum("amount").alias("lifetime_value"),
            F.count("event_id").alias("total_orders"),
            F.collect_set("device").alias("devices_used"),
            F.min("event_ts").alias("first_purchase_ts"),
            F.max("event_ts").alias("last_purchase_ts"),
        )
    )


def transform_silver_to_gold() -> None:
    spark = get_spark_session("SilverToGold")

    print("[Gold] Reading Silver Delta table...")
    silver_df = spark.read.format("delta").load(SILVER_TABLE_PATH)

    # Revenue by product
    revenue_df = build_revenue_by_product(silver_df)
    print(f"[Gold] Revenue aggregation: {revenue_df.count()} product rows.")
    revenue_df.write.format("delta").mode("overwrite").save(GOLD_REVENUE_PATH)
    print(f"[Gold] Revenue table written to: {GOLD_REVENUE_PATH}")

    # User purchase summary
    user_df = build_user_purchase_summary(silver_df)
    print(f"[Gold] User summary: {user_df.count()} user rows.")
    user_df.write.format("delta").mode("overwrite").save(GOLD_USER_PATH)
    print(f"[Gold] User summary table written to: {GOLD_USER_PATH}")


if __name__ == "__main__":
    transform_silver_to_gold()
