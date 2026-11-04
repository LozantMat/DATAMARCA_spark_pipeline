"""
DATAMARCA S.A. - daily orchestration of the Databricks pipeline (Unidad 4).

    generar_datos -> ingestar -> validar_calidad -> transformar -> exportar
                                                                     |
                                               notificar_exito <-----+
    notificar_fallo  <- runs as soon as ANY upstream task fails

* Data tasks run on Databricks serverless through DatabricksRunNowOperator:
  one job (``datamarca-pipeline``, pipeline.py) is triggered once per stage
  with its own ``--stages``; a second job (``datamarca-export``) runs
  export_results.py. Airflow only waits (deferrable -> triggerer).
* ``validar_calidad`` is the quality gate: it queries the quarantine and bronze
  tables through the SQL warehouse and fails WITHOUT retries when the
  rejection rate is above the ``umbral_rechazo`` param (a data error is not
  transient). Trigger with umbral_rechazo=0.001 to force a controlled
  failure in this intermediate task.
* Transient failures (API, serverless start-up) are retried 3 times with
  exponential backoff: ~2, 4, 8 minutes, capped at 20.

Configuration (airflow/.env):
    AIRFLOW_CONN_DATABRICKS_DEFAULT        workspace URL + personal access token
    AIRFLOW_VAR_DATAMARCA_PIPELINE_JOB_ID  job with the pipeline.py task
    AIRFLOW_VAR_DATAMARCA_EXPORT_JOB_ID    job with the export_results.py task
    AIRFLOW_VAR_DATAMARCA_SQL_HTTP_PATH    SQL warehouse HTTP path
    AIRFLOW_VAR_DATAMARCA_WEBHOOK_URL      optional Slack/Discord/Teams webhook
"""

from __future__ import annotations

import json
import logging
import urllib.request
from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.sdk import DAG, Param, Variable, task

log = logging.getLogger(__name__)

CONN_ID = "databricks_default"
TABLES = "workspace.datamarca"
VOL = "/Volumes/workspace/datamarca"
COMMON_ARGS = ["--input", f"{VOL}/raw/batch", "--dims", f"{VOL}/raw/dims",
               "--lake", f"{VOL}/lake", "--tables", TABLES]
PIPELINE_JOB = "{{ var.value.datamarca_pipeline_job_id }}"
EXPORT_JOB = "{{ var.value.datamarca_export_job_id }}"

default_args = {
    "owner": "datamarca",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
}


def run_stage(task_id: str, params: list[str], timeout_min: int, job: str = PIPELINE_JOB):
    """One Databricks run of an existing job, waiting in the triggerer."""
    return DatabricksRunNowOperator(
        task_id=task_id,
        databricks_conn_id=CONN_ID,
        job_id=job,
        python_params=params,           # replaces the job's parameters for this run
        forward_dag_params=False,
        deferrable=True,
        polling_period_seconds=30,
        execution_timeout=timedelta(minutes=timeout_min),
    )


def _webhook(text: str) -> None:
    url = Variable.get("datamarca_webhook_url", default="")
    if not url:
        return
    body = json.dumps({"text": text, "content": text}).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req, timeout=10)


with DAG(
    dag_id="datamarca_pipeline_databricks",
    description="Raw feeds -> bronze/silver/quarantine -> gold -> exports (Databricks)",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Bogota"),
    schedule="0 1 * * *",                 # every day 01:00 Bogota
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    params={"umbral_rechazo": Param(0.05, type="number", minimum=0, maximum=1,
                                    description="Max share of quarantined lines")},
    tags=["datamarca", "databricks", "unidad4"],
) as dag:

    generar_datos = run_stage(
        "generar_datos", ["--stages", "generate", *COMMON_ARGS, "--overwrite-generated"], 20)

    # the pipeline's own gate is disabled here: validar_calidad is the gate
    ingestar = run_stage(
        "ingestar", ["--stages", "ingest", *COMMON_ARGS, "--max-reject-rate", "1"], 25)

    @task(retries=0, execution_timeout=timedelta(minutes=10))
    def validar_calidad(params=None) -> dict:
        hook = DatabricksSqlHook(databricks_conn_id=CONN_ID,
                                 http_path=Variable.get("datamarca_sql_http_path"))
        q, b = hook.get_first(
            f"SELECT (SELECT COUNT(*) FROM {TABLES}.quarantine_sales_lines), "
            f"(SELECT COUNT(*) FROM {TABLES}.bronze_sales_lines)")
        rate = q / b if b else 1.0
        limit = float(params["umbral_rechazo"])
        log.info("Lineas en cuarentena: %s de %s (%.2f %%), umbral %.2f %%",
                 q, b, rate * 100, limit * 100)
        if b == 0 or rate > limit:
            raise AirflowFailException(
                f"Tasa de rechazo {rate:.2%} supera el umbral {limit:.2%}: "
                "no se publican las tablas gold")
        return {"quarantine_lines": q, "bronze_lines": b, "reject_rate": round(rate, 4)}

    transformar = run_stage("transformar", ["--stages", "transform", *COMMON_ARGS], 20)

    exportar = run_stage(
        "exportar", ["--lake", f"{VOL}/lake", "--tables", TABLES, "--sep", ";"], 15,
        job=EXPORT_JOB)

    @task(retries=0, trigger_rule="all_success")
    def notificar_exito(ti=None, dag_run=None) -> None:
        dq = ti.xcom_pull(task_ids="validar_calidad")
        msg = (f"DATAMARCA OK - {dag_run.run_id}: pipeline completo, "
               f"rechazo {dq['reject_rate']:.2%}. Resultados en {VOL}/lake/exports")
        log.info(msg)
        _webhook(msg)

    @task(retries=0, trigger_rule="one_failed")
    def notificar_fallo(dag_run=None) -> None:
        msg = (f"DATAMARCA FALLO - {dag_run.run_id}: una tarea fallo; las tareas "
               "siguientes no se ejecutaron. Revise la vista Grid en Airflow.")
        log.error(msg)
        _webhook(msg)

    dq = validar_calidad()
    generar_datos >> ingestar >> dq >> transformar >> exportar >> notificar_exito()
    [generar_datos, ingestar, dq, transformar, exportar] >> notificar_fallo()
