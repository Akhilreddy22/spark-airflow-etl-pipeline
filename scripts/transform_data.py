#!/usr/bin/env python3
"""
PySpark Customer 360 Transformation Job
Orchestrated by Apache Airflow.
Cleans raw clickstream and transactional logs, computes RFM metrics,
and writes optimized partitioned Parquet files for analytics.
"""

import os
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, DateType
)
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("PySparkCustomerETL")


def create_spark_session(app_name="Customer360_ETL"):
    """Initialize a SparkSession with optimized configs."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def extract_data(spark, raw_dir):
    """Load raw datasets into Spark DataFrames with explicit schemas."""
    logger.info(f"Extracting raw data from: {raw_dir}")

    # 1. Customers
    customers_path = os.path.join(raw_dir, "customers.csv")
    customers_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(customers_path)
    )

    # 2. Clickstream JSON
    clickstream_path = os.path.join(raw_dir, "clickstream.json")
    clickstream_df = spark.read.json(clickstream_path)

    # 3. Transactions JSON
    transactions_path = os.path.join(raw_dir, "transactions.json")
    transactions_df = spark.read.json(transactions_path)

    logger.info(f"✓ Loaded: {customers_df.count()} customers, {clickstream_df.count()} events, {transactions_df.count()} transactions")
    return customers_df, clickstream_df, transactions_df


def clean_and_transform(customers_df, clickstream_df, transactions_df):
    """Clean data, handle nulls, and compute Customer 360 & RFM metrics."""
    logger.info("Starting data cleaning and metric aggregations...")

    # --- Step 1: Clean Customers ---
    cleaned_customers = (
        customers_df
        .filter(F.col("customer_id").isNotNull())
        .withColumn("email", F.coalesce(F.col("email"), F.lit("unspecified@domain.com")))
        .withColumn("created_date", F.to_date(F.col("created_at")))
        .dropDuplicates(["customer_id"])
    )

    # --- Step 2: Clean and Aggregate Clickstream ---
    # Filter anonymous guest events and compute per-customer engagement
    user_clickstream = (
        clickstream_df
        .filter(F.col("customer_id").isNotNull())
        .withColumn("event_time", F.to_timestamp(F.col("event_timestamp")))
        .withColumn("event_date", F.to_date(F.col("event_timestamp")))
    )

    # Calculate engagement per customer
    engagement_agg = (
        user_clickstream
        .groupBy("customer_id")
        .agg(
            F.count("event_id").alias("total_click_events"),
            F.countDistinct("session_id").alias("unique_sessions_count"),
            F.round(F.avg("dwell_time_seconds"), 1).alias("avg_dwell_time_sec"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_additions_count")
        )
    )

    # Find favorite category using window rank
    cat_window = Window.partitionBy("customer_id").orderBy(F.desc("cat_count"))
    favorite_category_df = (
        user_clickstream
        .filter(F.col("product_category").isNotNull())
        .groupBy("customer_id", "product_category")
        .agg(F.count("event_id").alias("cat_count"))
        .withColumn("rank", F.row_number().over(cat_window))
        .filter(F.col("rank") == 1)
        .select("customer_id", F.col("product_category").alias("favorite_category"))
    )

    clickstream_metrics = engagement_agg.join(favorite_category_df, on="customer_id", how="left")

    # --- Step 3: Clean and Aggregate Transactions (RFM) ---
    valid_transactions = (
        transactions_df
        .filter(F.col("status") == "COMPLETED")
        .withColumn("tx_timestamp", F.to_timestamp(F.col("transaction_timestamp")))
        .withColumn("tx_date", F.to_date(F.col("transaction_timestamp")))
    )

    reference_date = F.current_date()

    rfm_agg = (
        valid_transactions
        .groupBy("customer_id")
        .agg(
            F.count("transaction_id").alias("total_orders"),
            F.round(F.sum("amount"), 2).alias("total_lifetime_spend"),
            F.round(F.avg("amount"), 2).alias("average_order_value"),
            F.max("tx_date").alias("latest_order_date")
        )
        .withColumn("recency_days", F.datediff(reference_date, F.col("latest_order_date")))
    )

    # --- Step 4: Join into Customer 360 Master Record ---
    customer_360 = (
        cleaned_customers
        .join(clickstream_metrics, on="customer_id", how="left")
        .join(rfm_agg, on="customer_id", how="left")
        .na.fill({
            "total_click_events": 0,
            "unique_sessions_count": 0,
            "avg_dwell_time_sec": 0.0,
            "cart_additions_count": 0,
            "favorite_category": "None",
            "total_orders": 0,
            "total_lifetime_spend": 0.0,
            "average_order_value": 0.0,
            "recency_days": 999
        })
    )

    # --- Step 5: Assign Customer Segment via RFM rules ---
    customer_360 = customer_360.withColumn(
        "customer_segment",
        F.when((F.col("total_lifetime_spend") >= 500) & (F.col("recency_days") <= 30), "Champion")
        .when((F.col("total_orders") >= 3) & (F.col("recency_days") <= 60), "Loyal Regular")
        .when((F.col("recency_days") > 60) & (F.col("total_orders") > 0), "At Risk / Inactive")
        .when(F.col("total_orders") == 0, "Prospect / Browser")
        .otherwise("Standard")
    )

    # Add partition column (year)
    customer_360 = customer_360.withColumn("created_year", F.year(F.col("created_date")))

    return customer_360, user_clickstream


def validate_data_quality(df, df_name):
    """Run data quality assertions before writing output."""
    logger.info(f"Running data quality validation on {df_name}...")
    total_count = df.count()
    if total_count == 0:
        raise ValueError(f"Data quality error: {df_name} is empty (0 records)!")

    # Check for null primary keys
    null_pks = df.filter(F.col("customer_id").isNull()).count()
    if null_pks > 0:
        raise ValueError(f"Data quality error: Found {null_pks} null customer_ids in {df_name}!")

    logger.info(f"✓ Data Quality PASSED for {df_name} ({total_count} records validated, 0 null PKs).")


def load_data(customer_360_df, clickstream_df, output_dir):
    """Save datasets as optimized, partitioned Parquet files."""
    logger.info(f"Loading partitioned Parquet files to: {output_dir}")

    # 1. Save Customer 360 partitioned by created_year and tier
    c360_output = os.path.join(output_dir, "customer_360")
    (
        customer_360_df
        .write
        .mode("overwrite")
        .partitionBy("created_year", "tier")
        .parquet(c360_output)
    )
    logger.info(f"✓ Saved Customer 360 Parquet partitioned by (created_year, tier) -> {c360_output}")

    # 2. Save Daily Category Analytics Parquet
    cat_summary_output = os.path.join(output_dir, "category_daily_summary")
    category_summary = (
        clickstream_df
        .groupBy("event_date", "product_category")
        .agg(
            F.count("event_id").alias("event_count"),
            F.countDistinct("session_id").alias("unique_sessions"),
            F.round(F.avg("dwell_time_seconds"), 2).alias("avg_dwell_sec")
        )
    )
    (
        category_summary
        .write
        .mode("overwrite")
        .partitionBy("product_category")
        .parquet(cat_summary_output)
    )
    logger.info(f"✓ Saved Category Summary Parquet partitioned by (product_category) -> {cat_summary_output}")


def run_pipeline(raw_dir=None, output_dir=None):
    """Main pipeline execution function."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if not raw_dir:
        raw_dir = os.path.join(base_dir, "data", "raw")
    if not output_dir:
        output_dir = os.path.join(base_dir, "data", "curated")

    start_time = datetime.now()
    logger.info("==================================================")
    logger.info("🚀 Starting PySpark Customer 360 ETL Pipeline")
    logger.info("==================================================")

    spark = create_spark_session()
    try:
        # Extract
        customers, clickstream, transactions = extract_data(spark, raw_dir)

        # Transform
        c360, cleaned_clicks = clean_and_transform(customers, clickstream, transactions)

        # Validate
        validate_data_quality(c360, "Customer360_Gold_Layer")

        # Load
        load_data(c360, cleaned_clicks, output_dir)

        # Print Preview
        print("\n--- SAMPLE PROCESSED CUSTOMER 360 DATA ---")
        c360.select(
            "customer_id", "country", "tier", "total_orders",
            "total_lifetime_spend", "customer_segment", "favorite_category"
        ).show(10, truncate=False)

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"🎉 Pipeline successfully completed in {duration:.2f} seconds!")

    finally:
        spark.stop()


if __name__ == "__main__":
    raw_path = sys.argv[1] if len(sys.argv) > 1 else None
    output_path = sys.argv[2] if len(sys.argv) > 2 else None
    run_pipeline(raw_path, output_path)
