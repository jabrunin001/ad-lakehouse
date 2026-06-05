# airflow/dags/_spark.py
"""Helper: build the `docker exec spark-submit` command DAG tasks run.

Airflow does not run Spark itself — it execs into the long-lived `ad-lakehouse-spark`
container (jars already warm at /tmp/.ivy2) and runs the transform driver there.
"""

SPARK_CONTAINER = "ad-lakehouse-spark"
PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,"
    "org.apache.iceberg:iceberg-aws-bundle:1.8.1,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)


def spark_submit(target: str) -> str:
    """Return the bash command that runs `transform/run.py <target>` in the spark container."""
    return (
        f"docker exec -e PYTHONPATH=/opt/app {SPARK_CONTAINER} "
        f"/opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 "
        f"--packages {PACKAGES} /opt/app/transform/run.py {target}"
    )
