"""
Smoke test: checks that Airflow can reach Databricks with the token in .env
and lists the workspace jobs with their IDs (to fill in .env).

Trigger it once by hand from the UI; it has no schedule.
"""

from __future__ import annotations

import logging

import pendulum
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.sdk import dag, task

log = logging.getLogger(__name__)


@dag(
    dag_id="datamarca_conexion_test",
    description="Comprueba la conexion con Databricks y lista los jobs",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Bogota"),
    catchup=False,
    tags=["datamarca", "setup"],
)
def datamarca_conexion_test():
    @task
    def listar_jobs() -> list[dict]:
        jobs = DatabricksHook(databricks_conn_id="databricks_default").list_jobs()
        found = [{"job_id": j["job_id"], "name": j.get("settings", {}).get("name")}
                 for j in jobs]
        if not found:
            raise RuntimeError("Connected, but the workspace has no jobs yet")
        for j in found:
            log.info("Databricks job %-30s id=%s", j["name"], j["job_id"])
        return found

    listar_jobs()


datamarca_conexion_test()
