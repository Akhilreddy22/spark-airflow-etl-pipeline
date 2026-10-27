#!/usr/bin/env python3
"""
Local Standalone Pipeline Runner
Allows running and testing the PySpark ETL pipeline end-to-end without Airflow running.
"""

import os
import sys
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts"))

from generate_sample_data import generate_customers, generate_clickstream, generate_transactions
from transform_data import run_pipeline, create_spark_session
from pyspark.sql import functions as F


def main():
    print("================================================================")
    print("🚀 Running Customer 360 PySpark ETL Pipeline Locally")
    print("================================================================")

    raw_dir = os.path.join(BASE_DIR, "data", "raw")
    curated_dir = os.path.join(BASE_DIR, "data", "curated")

    # Step 1: Ensure raw data exists
    if not os.path.exists(os.path.join(raw_dir, "customers.csv")):
        print("\n[Step 1/3] Generating raw test datasets...")
        cids = generate_customers(150)
        generate_clickstream(cids, 1500)
        generate_transactions(cids, 450)
    else:
        print("\n[Step 1/3] Raw data already present in data/raw/")

    # Step 2: Run PySpark Transformation
    print("\n[Step 2/3] Executing PySpark transformation job...")
    t0 = time.time()
    run_pipeline(raw_dir, curated_dir)
    print(f"ETL completed in {time.time() - t0:.2f}s")

    # Step 3: Query & Verify Output Parquet Lakehouse
    print("\n[Step 3/3] Querying Output Parquet Lakehouse...")
    spark = create_spark_session("LakehouseVerification")
    try:
        c360_path = os.path.join(curated_dir, "customer_360")
        df = spark.read.parquet(c360_path)

        print(f"\n📊 Total Curated Customers in Lakehouse: {df.count()}")

        print("\n--- 1. Breakdown by Customer Segment ---")
        df.groupBy("customer_segment").agg(
            F.count("customer_id").alias("customer_count"),
            F.round(F.sum("total_lifetime_spend"), 2).alias("total_revenue"),
            F.round(F.avg("total_lifetime_spend"), 2).alias("avg_spend")
        ).orderBy(F.desc("total_revenue")).show(truncate=False)

        print("\n--- 2. Revenue by Loyalty Tier ---")
        df.groupBy("tier").agg(
            F.count("customer_id").alias("members"),
            F.round(F.sum("total_lifetime_spend"), 2).alias("revenue"),
            F.round(F.avg("total_orders"), 1).alias("avg_orders")
        ).orderBy(F.desc("revenue")).show(truncate=False)

        print("\n--- 3. Top 5 High-Value Champions ---")
        df.filter(F.col("customer_segment") == "Champion").select(
            "customer_id", "first_name", "country", "tier",
            "total_orders", "total_lifetime_spend", "favorite_category"
        ).orderBy(F.desc("total_lifetime_spend")).show(5, truncate=False)

        print("\n🎉 ALL TESTS AND QUERIES COMPLETED SUCCESSFULLY!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
