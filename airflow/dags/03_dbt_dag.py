"""
DAG 03 — dbt
Runs dbt models (Silver → Gold) via dbt-trino.
Uses BashOperator so the full dbt CLI is available.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt"   # mounted via volume: ./dbt:/opt/airflow/dbt

# dbt env vars — profiles.yml reads these
DBT_ENV = {
    "DBT_TRINO_HOST":     "trino",
    "DBT_TRINO_PORT":     "8080",
    "DBT_TRINO_USER":     "admin",
    "DBT_TRINO_CATALOG":  "iceberg",
    "DBT_TRINO_SCHEMA":   "silver",
}

with DAG(
    dag_id="03_dbt_transformations",
    description="dbt-trino: Silver → Gold layer transformations",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "gold", "trino"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f"cd {DBT_DIR} && dbt run --select silver",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"cd {DBT_DIR} && dbt run --select gold",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test",
        env=DBT_ENV,
        append_env=True,
    )

    dbt_deps >> dbt_run_silver >> dbt_run_gold >> dbt_test
