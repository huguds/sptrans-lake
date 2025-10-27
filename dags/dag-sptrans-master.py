# dags/dag-sptrans-master.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="dag-sptrans-master",
    description="Orquestra: raw->trusted e depois latest-per-vehicle-append",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/3 * * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    run_raw_to_trusted = TriggerDagRunOperator(
        task_id="run_raw_to_trusted",
        trigger_dag_id="dag-sptrans-raw-to-trusted",
        conf={"triggered_by": "dag-sptrans-master"},
        wait_for_completion=True,            # espera terminar
        poke_interval=30,                    # verifica status a cada 30s
        allowed_states=["success"],          # considera concluído só em sucesso
        failed_states=["failed"], # falha se a DAG alvo falhar/pular
        deferrable=True,                  # (opcional) se seu Airflow suportar
    )

    run_latest_per_vehicle = TriggerDagRunOperator(
        task_id="run_latest_per_vehicle",
        trigger_dag_id="dag-sptrans-latest-per-vehicle-append",
        conf={"triggered_by": "dag-sptrans-master"},
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        deferrable=True,
    )

    run_raw_to_trusted >> run_latest_per_vehicle
