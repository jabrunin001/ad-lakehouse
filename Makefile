.PHONY: up down topic seed stream query build-silver silver-checks build-gold gold-queries test lint airflow-up airflow-password dags-list dag-medallion gdpr-efficiency gdpr-mor forget-user

up:
	docker compose up -d

down:
	docker compose down -v

topic:
	docker compose exec -T redpanda rpk topic create ad_events --topic-config retention.ms=-1 || true

seed:
	set -a && . ./.env && set +a && .venv/bin/python -m generator.produce --n 10000

stream:
	docker compose exec -d -e PYTHONPATH=/opt/app spark /opt/spark/bin/spark-submit \
	  --conf spark.jars.ivy=/tmp/.ivy2 \
	  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
	  /opt/app/streaming/ingest_bronze.py

query:
	docker compose exec -T trino trino --catalog iceberg < trino/00_bronze_smoke.sql

build-silver:
	docker compose exec -T -e PYTHONPATH=/opt/app spark /opt/spark/bin/spark-submit \
	  --conf spark.jars.ivy=/tmp/.ivy2 \
	  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
	  /opt/app/transform/run.py silver

silver-checks:
	docker compose exec -T trino trino --catalog iceberg < trino/01_silver_checks.sql

build-gold:
	docker compose exec -T -e PYTHONPATH=/opt/app spark /opt/spark/bin/spark-submit \
	  --conf spark.jars.ivy=/tmp/.ivy2 \
	  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
	  /opt/app/transform/run.py gold

gold-queries:
	docker compose exec -T trino trino --catalog iceberg < trino/02_gold_queries.sql

test:
	.venv/bin/pytest -v

lint:
	.venv/bin/ruff check .

airflow-up: ; docker compose up -d --build airflow
airflow-password: ; docker compose exec -T airflow cat /opt/airflow/standalone_admin_password.txt
dags-list: ; docker compose exec -T airflow airflow dags list
dag-medallion: ; docker compose exec -T airflow airflow dags test medallion_build 2026-06-05

# shuffle.partitions=8: small demo data — keeps the gold rebuild light enough to
# run on a memory-tight host (the default 200 partitions OOM-kill it).
gdpr-efficiency: ; docker exec -e PYTHONPATH=/opt/app ad-lakehouse-spark /opt/spark/bin/spark-submit \
	--conf spark.jars.ivy=/tmp/.ivy2 --conf spark.sql.shuffle.partitions=8 \
	--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
	/opt/app/gdpr/efficiency_demo.py
gdpr-mor: ; docker exec -e PYTHONPATH=/opt/app ad-lakehouse-spark /opt/spark/bin/spark-submit \
	--conf spark.jars.ivy=/tmp/.ivy2 --conf spark.sql.shuffle.partitions=8 \
	--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
	/opt/app/gdpr/mor_demo.py
forget-user: ; docker exec -e PYTHONPATH=/opt/app ad-lakehouse-spark /opt/spark/bin/spark-submit \
	--conf spark.jars.ivy=/tmp/.ivy2 --conf spark.sql.shuffle.partitions=8 \
	--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
	/opt/app/gdpr/forget_user.py --user-id "$(UID)"
