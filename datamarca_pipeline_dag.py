"""
Cloud Composer (Airflow 2) DAG for the DATAMARCA Spark pipeline.

Runs the same ``pipeline.py`` used locally, as two Serverless for Apache
Spark batches (no cluster to manage), once a day:

    wait_for_raw_data  ->  ingest (+ quality gate)  ->  transform (+ reconciliation)

Each batch exits with a non-zero code when its quality gate fails, so Airflow
marks the task as failed and the gold tables are not refreshed.

Deployment:
    1. gsutil cp data_generator.py ingest.py transform.py pipeline.py gs://<bucket>/code/
    2. Raw files land in gs://<bucket>/raw/batch/... and dims in gs://<bucket>/raw/dims/
    3. Copy this file to the Composer environment's dags/ folder and set the
       Airflow variables below (or edit the defaults).

NOTE: reference implementation - it is not executed in the local tests.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

PROJECT_ID = Variable.get("datamarca_project_id", default_var="datamarca-dev")
REGION = Variable.get("datamarca_region", default_var="us-central1")
BUCKET = Variable.get("datamarca_bucket", default_var="datamarca-lake")

CODE = f"gs://{BUCKET}/code"
RAW = f"gs://{BUCKET}/raw/batch"
DIMS = f"gs://{BUCKET}/raw/dims"
LAKE = f"gs://{BUCKET}/lake"


def spark_batch(stage: str) -> dict:
    """Serverless batch running ``pipeline.py --stages <stage>``."""
    return {
        "pyspark_batch": {
            "main_python_file_uri": f"{CODE}/pipeline.py",
            "python_file_uris": [f"{CODE}/ingest.py", f"{CODE}/transform.py",
                                 f"{CODE}/data_generator.py"],
            "args": ["--stages", stage, "--input", RAW, "--dims", DIMS,
                     "--lake", LAKE, "--format", "parquet"],
        },
        "runtime_config": {
            "version": "2.2",
            "properties": {"spark.executor.instances": "2",
                           "spark.sql.shuffle.partitions": "16"},
        },
        "labels": {"app": "datamarca", "stage": stage},
    }


with DAG(
    dag_id="datamarca_spark_pipeline",
    description="Raw feeds -> bronze/silver/quarantine -> gold (DATAMARCA)",
    start_date=datetime(2026, 11, 1),
    schedule="0 6 * * *",           # 06:00 UTC = 01:00 Bogotá/Lima, 00:00 CDMX
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["datamarca", "spark", "unidad3"],
) as dag:

    wait_for_raw_data = GCSObjectExistenceSensor(
        task_id="wait_for_raw_data",
        bucket=BUCKET,
        object="raw/batch/_contract.json",   # written last by the producer
        mode="reschedule",
        poke_interval=300,
        timeout=6 * 3600,
    )

    ingest = DataprocCreateBatchOperator(
        task_id="ingest_and_validate",
        project_id=PROJECT_ID,
        region=REGION,
        batch=spark_batch("ingest"),
        batch_id="datamarca-ingest-{{ ts_nodash | lower }}",
    )

    transform = DataprocCreateBatchOperator(
        task_id="transform_gold",
        project_id=PROJECT_ID,
        region=REGION,
        batch=spark_batch("transform"),
        batch_id="datamarca-transform-{{ ts_nodash | lower }}",
    )

    wait_for_raw_data >> ingest >> transform
