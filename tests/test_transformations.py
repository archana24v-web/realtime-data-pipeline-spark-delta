"""
Unit Tests for Silver Layer Transformations
Uses pytest + PySpark local session to validate data quality logic.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.transformation.bronze_to_silver import clean_bronze


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("TestSuite")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def make_df(spark, rows):
    return spark.createDataFrame([Row(**r) for r in rows])


class TestCleanBronze:

    def test_drops_null_event_id(self, spark):
        rows = [
            {"event_id": None,  "user_id": "u1", "event_type": "purchase",
             "product_id": "p1", "amount": 10.0, "currency": "USD",
             "timestamp": "2024-06-01T08:00:00Z", "country": "US", "device": "mobile",
             "ingested_at": None, "source_file": "test"},
            {"event_id": "e1",   "user_id": "u2", "event_type": "purchase",
             "product_id": "p2", "amount": 20.0, "currency": "USD",
             "timestamp": "2024-06-01T09:00:00Z", "country": "US", "device": "desktop",
             "ingested_at": None, "source_file": "test"},
        ]
        df = make_df(spark, rows)
        result = clean_bronze(df)
        assert result.count() == 1
        assert result.first()["event_id"] == "e1"

    def test_normalises_event_type_to_lowercase(self, spark):
        rows = [
            {"event_id": "e2", "user_id": "u3", "event_type": "  PURCHASE  ",
             "product_id": "p3", "amount": 15.0, "currency": "USD",
             "timestamp": "2024-06-01T10:00:00Z", "country": "US", "device": "mobile",
             "ingested_at": None, "source_file": "test"},
        ]
        df = make_df(spark, rows)
        result = clean_bronze(df)
        assert result.first()["event_type"] == "purchase"

    def test_filters_zero_amount_purchases(self, spark):
        rows = [
            {"event_id": "e3", "user_id": "u4", "event_type": "purchase",
             "product_id": "p4", "amount": 0.0, "currency": "USD",
             "timestamp": "2024-06-01T11:00:00Z", "country": "US", "device": "desktop",
             "ingested_at": None, "source_file": "test"},
        ]
        df = make_df(spark, rows)
        result = clean_bronze(df)
        assert result.count() == 0
