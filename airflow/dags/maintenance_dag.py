# airflow/dags/maintenance_dag.py
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _spark import DEFAULT_ARGS, spark_submit

with DAG(
    dag_id="iceberg_maintenance",
    description="Compact small files and expire old snapshots across all Iceberg tables",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ad-lakehouse", "maintenance"],
) as dag:
    BashOperator(task_id="maintain_tables", bash_command=spark_submit("maintenance"))
