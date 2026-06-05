# ad-lakehouse

Ad-serving event lakehouse: generator -> Kafka -> Spark Structured Streaming ->
Iceberg (bronze/silver/gold) -> Trino, orchestrated with Airflow. Targets an Ads
Data Engineering role. See `docs/superpowers/specs/` for the design.

## Quickstart (streaming spine)

    python3.11 -m venv .venv && . .venv/bin/activate && pip install -e '.[dev]'
    make up       # redpanda + minio + iceberg-rest + spark + trino
    make stream   # start Structured Streaming Kafka -> bronze
    make seed     # produce 10k ad events to Kafka
    make query    # count rows landed in bronze via Trino
    make test     # unit tests (add `-m integration` for the smoke test)

Consoles: Trino at http://localhost:8081 (host port 8081 maps to the
container's 8080), MinIO at http://localhost:9001.
