"""Airflow 2.10 DAG. Streaming jobs run independently; only bounded exports are scheduled."""
from datetime import datetime, timedelta, timezone
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

PROJECT = os.environ.get("CLICKSTREAM_PROJECT_DIR", "/opt/airflow/clickstream")
with DAG("clickstream_gold_to_analytics", start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
         schedule="*/5 * * * *", catchup=False, max_active_runs=1,
         default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
         tags=["clickstream", "lakehouse"]) as dag:
    export = DatabricksRunNowOperator(task_id="export_gold", databricks_conn_id="databricks_default",
        job_id="{{ var.value.clickstream_export_job_id }}", polling_period_seconds=20)
    build = BashOperator(task_id="dbt_build", cwd=f"{PROJECT}/dbt",
        bash_command="dbt build --profiles-dir . && dbt docs generate --profiles-dir .")
    freshness = BashOperator(task_id="source_freshness", cwd=f"{PROJECT}/dbt",
        bash_command="dbt source freshness --profiles-dir .")
    export >> build >> freshness
