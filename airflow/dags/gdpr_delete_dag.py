# airflow/dags/gdpr_delete_dag.py
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _spark import DEFAULT_ARGS, PACKAGES, SPARK_CONTAINER

# Triggered on demand with a user_id:
#   airflow dags trigger gdpr_delete -c '{"user_id": "usr-01234"}'
# shuffle.partitions=8 keeps the gold rebuild light enough for the single-node host.
FORGET_CMD = (
    f"docker exec -e PYTHONPATH=/opt/app {SPARK_CONTAINER} "
    f"/opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 "
    f"--conf spark.sql.shuffle.partitions=8 --packages {PACKAGES} "
    "/opt/app/gdpr/forget_user.py --user-id '{{ dag_run.conf[\"user_id\"] }}'"
)

with DAG(
    dag_id="gdpr_delete",
    description="Right-to-be-forgotten: erase a user_id across the lakehouse (trigger with conf user_id)",
    schedule=None,  # on-demand only
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ad-lakehouse", "gdpr", "governance"],
) as dag:
    BashOperator(task_id="forget_user", bash_command=FORGET_CMD)
