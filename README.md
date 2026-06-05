# ad-lakehouse

A miniature ad-serving event lakehouse, built to mirror what an Ads Data
Engineering team actually ships: ingest ad-serving events, land them in Apache
Iceberg, and turn them into clean, connected data products (delivery, inventory
fill, campaign pacing).

**What runs today (the streaming spine):**

```
generator ──▶ Redpanda (Kafka) ──▶ Spark Structured Streaming ──▶ Iceberg bronze ──▶ Trino
 (Python)      topic: ad_events        (checkpointed ingest)        (append-only)     (queries)
```

A synthetic generator emits ad events — ad requests, impressions, quartile
completions — and deliberately injects **duplicate** and **late-arriving** events
so downstream layers have something real to clean up. Spark Structured Streaming
reads the Kafka topic and appends every event, raw, to an Iceberg `bronze` table
on MinIO/S3, behind an Iceberg REST catalog. Trino queries the same tables Spark
writes.

Storage (MinIO), catalog (Iceberg REST), and compute (Spark + Trino) are
independent, swappable layers — the way modern lakehouses actually run.

## Quickstart

```bash
python3.11 -m venv .venv && . .venv/bin/activate && pip install -e '.[dev]'
make up       # start redpanda + minio + iceberg-rest + spark + trino
make stream   # start Structured Streaming Kafka -> bronze (see note below)
make seed     # produce 10k ad events to Kafka
make query    # count rows landed in bronze, via Trino
make test     # unit tests   (run `make` integration smoke separately, below)
```

> **First `make stream` is slow (~1–2 min):** Spark downloads the Iceberg + Kafka
> connector jars via Ivy on first launch, then the job initializes. It is not
> hung. Watch progress with `docker compose logs --tail 40 spark`. `make stream`
> must run *before* `make seed` so the consumer is live when events are produced
> (though `startingOffsets=earliest` will also pick up a topic seeded earlier).

Run the end-to-end smoke test (requires the stack up + bronze populated):

```bash
.venv/bin/pytest -m integration -v
```

Consoles: **Trino** at http://localhost:8081 (host 8081 → container 8080),
**MinIO** at http://localhost:9001.

## Design

The full design — data model, the bronze→silver→gold medallion, GDPR
right-to-be-forgotten on Iceberg, and the performance before/after study — lives in
[`docs/superpowers/specs/2026-06-04-ad-lakehouse-design.md`](docs/superpowers/specs/2026-06-04-ad-lakehouse-design.md).

## Roadmap

This repo is built in milestones; each produces working, testable software.

| Plan | Scope | Status |
|------|-------|--------|
| 1. Infra + streaming spine | generator → Kafka → Spark Structured Streaming → Iceberg bronze → Trino | ✅ **done** |
| 2. Medallion + campaign API + gold | FastAPI campaigns, bronze→silver (dedup/late-data), gold delivery/fill/**pacing** | planned |
| 3. Airflow orchestration | DAGs for the batch builds + Iceberg maintenance | planned |
| 4. GDPR right-to-be-forgotten | `bucket()`-efficient MERGE deletes + merge-on-read equality deletes | planned |
| 5. Performance before/after | deliberately-bad vs optimized pipeline, with measured query-time deltas | planned |

## Repo tour (current)

| Path | Responsibility |
|------|----------------|
| `generator/` | Synthetic ad-event model, batch generator (dupes + late events), Kafka producer |
| `streaming/` | Spark session builder + Structured Streaming ingest to Iceberg bronze |
| `docker-compose.yml`, `docker/` | Redpanda, MinIO, Iceberg REST catalog, Spark, Trino |
| `trino/` | Analyst SQL against the lakehouse |
| `tests/` | pytest: generator unit tests + an integration smoke test |
| `docs/` | Design spec + implementation plans |
