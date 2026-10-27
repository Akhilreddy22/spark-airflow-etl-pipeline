"""
Unit Tests for PySpark Customer 360 Transformation
Uses pytest and PySpark local SparkSession.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType
)
import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts"))

from transform_data import clean_and_transform, validate_data_quality


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("PySparkUnitTests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_null_email_handling(spark):
    """Ensure null emails are cleaned to fallback address."""
    cust_data = [
        ("C001", "Alice", "Smith", None, "US", "Gold", "2025-01-10 10:00:00"),
        ("C002", "Bob", "Jones", "bob@test.com", "CA", "Silver", "2025-02-15 11:00:00")
    ]
    cust_cols = ["customer_id", "first_name", "last_name", "email", "country", "tier", "created_at"]
    cust_df = spark.createDataFrame(cust_data, cust_cols)

    click_df = spark.createDataFrame([], StructType([
        StructField("event_id", StringType()),
        StructField("session_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("event_type", StringType()),
        StructField("product_category", StringType()),
        StructField("dwell_time_seconds", IntegerType()),
        StructField("event_timestamp", StringType()),
    ]))

    tx_df = spark.createDataFrame([], StructType([
        StructField("transaction_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("status", StringType()),
        StructField("transaction_timestamp", StringType()),
    ]))

    c360, _ = clean_and_transform(cust_df, click_df, tx_df)
    results = {row["customer_id"]: row["email"] for row in c360.collect()}

    assert results["C001"] == "unspecified@domain.com"
    assert results["C002"] == "bob@test.com"


def test_transaction_completed_filter(spark):
    """Ensure only COMPLETED transactions contribute to customer spend."""
    cust_data = [("C001", "Alice", "Smith", "alice@test.com", "US", "Gold", "2025-01-10 10:00:00")]
    cust_cols = ["customer_id", "first_name", "last_name", "email", "country", "tier", "created_at"]
    cust_df = spark.createDataFrame(cust_data, cust_cols)

    click_df = spark.createDataFrame([], StructType([
        StructField("event_id", StringType()),
        StructField("session_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("event_type", StringType()),
        StructField("product_category", StringType()),
        StructField("dwell_time_seconds", IntegerType()),
        StructField("event_timestamp", StringType()),
    ]))

    tx_data = [
        ("T1", "C001", 100.0, "COMPLETED", "2026-10-20 10:00:00"),
        ("T2", "C001", 200.0, "COMPLETED", "2026-10-22 14:00:00"),
        ("T3", "C001", 500.0, "REFUNDED", "2026-10-23 16:00:00"),
        ("T4", "C001", 300.0, "FAILED", "2026-10-24 18:00:00"),
    ]
    tx_cols = ["transaction_id", "customer_id", "amount", "status", "transaction_timestamp"]
    tx_df = spark.createDataFrame(tx_data, tx_cols)

    c360, _ = clean_and_transform(cust_df, click_df, tx_df)
    row = c360.first()

    assert row["total_orders"] == 2
    assert row["total_lifetime_spend"] == 300.0


def test_customer_segment_assignment(spark):
    """Validate RFM customer segment classification rules."""
    cust_data = [
        ("C001", "Alice", "Champion", "a@test.com", "US", "Platinum", "2025-01-10 10:00:00"),
        ("C002", "Bob", "Prospect", "b@test.com", "CA", "Bronze", "2025-02-15 11:00:00"),
    ]
    cust_cols = ["customer_id", "first_name", "last_name", "email", "country", "tier", "created_at"]
    cust_df = spark.createDataFrame(cust_data, cust_cols)

    click_df = spark.createDataFrame([], StructType([
        StructField("event_id", StringType()),
        StructField("session_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("event_type", StringType()),
        StructField("product_category", StringType()),
        StructField("dwell_time_seconds", IntegerType()),
        StructField("event_timestamp", StringType()),
    ]))

    # Alice spent 600 recently -> Champion. Bob has 0 transactions -> Prospect.
    tx_data = [
        ("T1", "C001", 600.0, "COMPLETED", "2026-10-25 10:00:00"),
    ]
    tx_cols = ["transaction_id", "customer_id", "amount", "status", "transaction_timestamp"]
    tx_df = spark.createDataFrame(tx_data, tx_cols)

    c360, _ = clean_and_transform(cust_df, click_df, tx_df)
    segments = {row["customer_id"]: row["customer_segment"] for row in c360.collect()}

    assert segments["C001"] == "Champion"
    assert segments["C002"] == "Prospect / Browser"


def test_data_quality_assertion(spark):
    """Ensure data quality assertion raises on empty dataframe."""
    empty_df = spark.createDataFrame([], StructType([StructField("customer_id", StringType())]))
    with pytest.raises(ValueError, match="empty"):
        validate_data_quality(empty_df, "TestEmpty")
