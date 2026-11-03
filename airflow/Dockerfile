# Airflow image for DATAMARCA: official image + the Databricks provider.
# Pinning apache-airflow in the same pip call keeps pip from upgrading or
# downgrading Airflow while it resolves the provider's dependencies.
ARG AIRFLOW_VERSION=3.3.1
FROM apache/airflow:${AIRFLOW_VERSION}
ARG AIRFLOW_VERSION
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      "apache-airflow-providers-databricks"
