"""
Apache Airflow DAG: customer_360_pyspark_etl
Daily batch pipeline orchestrating PySpark transformation of customer clickstream
and transactional logs into partitioned Parquet files with data quality checks.
"""

from datetime import datetime, timedelta
import os
import glob
from airflow import DAG

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

# Pipeline Paths
BASE_DIR = os.getenv("AIRFLOW_HOME", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
CURATED_DIR = os.path.join(DATA_DIR, "curated")

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 10, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
}


def check_source_files_exist(**context):
    """Ensure all required input datasets exist before triggering PySpark."""
    required_files = [
        os.path.join(RAW_DIR, "customers.csv"),
        os.path.join(RAW_DIR, "clickstream.json"),
        os.path.join(RAW_DIR, "transactions.json"),
    ]
    missing = [f for f in required_files if not os.path.exists(f)]
    if missing:
        raise FileNotFoundError(f"Missing required input files: {missing}")
    print(f"✓ All source files present in {RAW_DIR}")


def validate_parquet_outputs(**context):
    """Verify that PySpark produced non-empty Parquet partition files."""
    c360_path = os.path.join(CURATED_DIR, "customer_360")

    if not os.path.exists(c360_path):
        raise FileNotFoundError(f"Target directory {c360_path} was not created!")

    parquet_files = glob.glob(f"{c360_path}/**/*.parquet", recursive=True)
    if not parquet_files:
        raise ValueError(f"No Parquet files found in {c360_path}!")

    print(f"✓ Data Quality Success: Found {len(parquet_files)} partitioned Parquet files in {c360_path}")


def generate_pipeline_summary(**context):
    """Log execution metrics and pipeline summary."""
    execution_date = context.get("ds", str(datetime.now().date()))
    print("==================================================")
    print(f"🎉 Pipeline Executed Successfully for Batch Date: {execution_date}")
    print(f"Output Location: {CURATED_DIR}")
    print("Target Schema: Customer 360 (RFM, Session Metrics, Tier Partitioning)")
    print("==================================================")


with DAG(
    dag_id="customer_360_pyspark_etl",
    default_args=default_args,
    description="Daily automated ingestion, PySpark transformation, and Parquet lakehouse loading",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["pyspark", "etl", "customer_360", "parquet", "lakehouse"],
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    check_sources = PythonOperator(
        task_id="check_source_files",
        python_callable=check_source_files_exist,
    )

    pyspark_transform = BashOperator(
        task_id="pyspark_transform_job",
        bash_command=f"python3 {SCRIPTS_DIR}/transform_data.py {RAW_DIR} {CURATED_DIR}",
    )

    validate_output = PythonOperator(
        task_id="validate_parquet_output",
        python_callable=validate_parquet_outputs,
    )

    publish_summary = PythonOperator(
        task_id="publish_pipeline_summary",
        python_callable=generate_pipeline_summary,
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # Task Orchestration Flow
    start_pipeline >> check_sources >> pyspark_transform >> validate_output >> publish_summary >> end_pipeline
