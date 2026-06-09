"""
Spark Session Factory
Reusable utility to create a configured SparkSession with Delta Lake support.
"""
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "DataPipeline") -> SparkSession:
    """
    Creates and returns a SparkSession configured with Delta Lake.

    Args:
        app_name (str): Name for the Spark application.

    Returns:
        SparkSession: Configured Spark session.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")  # optimized for local dev
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
