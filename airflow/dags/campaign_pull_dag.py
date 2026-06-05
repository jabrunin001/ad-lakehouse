# airflow/dags/campaign_pull_dag.py
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _spark import DEFAULT_ARGS, spark_submit

with DAG(
    dag_id="campaign_pull",
    description="Pull campaign metadata from the FastAPI service into silver.dim_campaign",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ad-lakehouse", "silver"],
) as dag:
    BashOperator(task_id="pull_dim_campaign", bash_command=spark_submit("dim_campaign"))
