# docker/airflow/Dockerfile
FROM apache/airflow:2.9.3-python3.11
USER root
# docker CLI so DAG tasks can `docker exec` the spark container (Docker-out-of-Docker)
RUN apt-get update \
 && apt-get install -y --no-install-recommends docker.io \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow
