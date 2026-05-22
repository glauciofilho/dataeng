"""
DAG 04 — Full Pipeline
Chains DAGs 01 → 02 → 03 using TriggerDagRunOperator.
Run this DAG to execute the entire end-to-end pipeline.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id="04_full_pipeline",
    description="End-to-end: Ingest → Spark → dbt",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["pipeline", "full"],
    default_args={"retries": 0},
) as dag:

    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_ingestion",
        trigger_dag_id="01_ingestion_nyc_taxi",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
    )

    trigger_processing = TriggerDagRunOperator(
        task_id="trigger_processing",
        trigger_dag_id="02_processing_spark",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="03_dbt_transformations",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
    )

    trigger_ingestion >> trigger_processing >> trigger_dbt
