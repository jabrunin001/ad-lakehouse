.PHONY: up down topic seed stream query build-silver silver-checks build-gold gold-queries test lint

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
