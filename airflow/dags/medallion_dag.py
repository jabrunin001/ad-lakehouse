# airflow/dags/medallion_dag.py
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _spark import spark_submit

with DAG(
    dag_id="medallion_build",
    description="Build the silver then gold Iceberg layers in dependency order",
    schedule="@hourly",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["ad-lakehouse", "silver", "gold"],
):
    build_silver = BashOperator(task_id="build_silver", bash_command=spark_submit("silver"))
    build_gold = BashOperator(task_id="build_gold", bash_command=spark_submit("gold"))
    build_silver >> build_gold
