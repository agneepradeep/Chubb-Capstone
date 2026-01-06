from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import pipeline task
from scripts import ingest_raw_to_bronze, transform_bronze_to_silver, transform_silver_to_gold


# --------------------------------------------------
# Default DAG arguments
# --------------------------------------------------

default_args = {
    "owner": "capstone",
    "depends_on_past": False,
    "retries": 1
}

# --------------------------------------------------
# DAG definition
# --------------------------------------------------
with DAG (
    dag_id = "agriculture_crop_bronze_pipeline",
    description = "Agriculture Crop Production ETL Pipeline (Bronze -> Silver)",
    default_args = default_args,
    start_date = datetime(2026, 1, 5),
    tags=["capstone", "etl", "agriculture"]
) as dag:
    
    # --------------------------------------------------
    # Task: Raw -> Bronze ingestion
    # --------------------------------------------------
    ingest_raw_task = PythonOperator (
        task_id = "ingest_raw_to_bronze",
        python_callable = ingest_raw_to_bronze
    )

    # --------------------------------------------------
    # Task: Bronze -> Silver Transformation
    # --------------------------------------------------
    silver_transformation_task = PythonOperator (
        task_id = "transform_bronze_to_silver",
        python_callable = transform_bronze_to_silver
    )

    # --------------------------------------------------
    # Task: Silver -> Gold Transformation
    # --------------------------------------------------
    gold_transform_task = PythonOperator(
        task_id="transform_silver_to_gold",
        python_callable=transform_silver_to_gold
    )

    # --------------------------------------------------
    # Task Dependency
    # --------------------------------------------------
    ingest_raw_task >> silver_transformation_task >> gold_transform_task