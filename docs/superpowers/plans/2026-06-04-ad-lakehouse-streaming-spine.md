# Ad-Lakehouse — Plan 1: Infra + Streaming Spine — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the Dockerized lakehouse stack and a live spine where a synthetic ad-event generator produces to Kafka and Spark Structured Streaming lands those events in an Iceberg `bronze` table, queryable from Trino.

**Architecture:** All services run under one `docker-compose.yml` — Redpanda (Kafka API), MinIO (S3 storage), Iceberg REST catalog, Spark, Trino. A pure-Python generator builds valid ad events (with injected duplicate + late-arriving events) and a thin Kafka producer publishes them to the `ad_events` topic. A Spark Structured Streaming job reads the topic, parses JSON, and appends to `bronze.ad_events_raw` with checkpointing to MinIO. Generator event-construction logic is unit-tested with TDD; infra and the streaming job are verified with smoke checks against Trino.

**Tech Stack:** Python 3.11, `confluent-kafka` (producer), `pydantic`/`dataclasses` for the event, pytest + ruff; Spark 3.5 with the Iceberg 1.5 Spark runtime; Redpanda, MinIO, Iceberg REST catalog, Trino 4xx; Docker Compose; Make.

**Spec:** `docs/superpowers/specs/2026-06-04-ad-lakehouse-design.md` (§3 architecture, §4.1–4.2 schema/bronze, §5 streaming).

---

## File structure (created in this plan)

- `pyproject.toml` — project metadata, ruff + pytest config, deps
- `Makefile` — `up`, `down`, `seed`, `stream`, `query`, `test`, `lint` targets
- `generator/__init__.py`
- `generator/event.py` — the `AdEvent` schema + `make_event()` factory
- `generator/stream.py` — `event_batch()` generator that injects dupes + late events
- `generator/produce.py` — Kafka producer entrypoint (generator → `ad_events`)
- `streaming/ingest_bronze.py` — Spark Structured Streaming Kafka → `bronze.ad_events_raw`
- `streaming/spark_session.py` — shared Spark session builder (Iceberg REST + S3 config)
- `docker-compose.yml` — redpanda, minio, minio-init, iceberg-rest, spark, trino
- `docker/trino/catalog/iceberg.properties` — Trino Iceberg connector → REST catalog
- `trino/00_bronze_smoke.sql` — count + sample bronze rows
- `tests/test_event.py`, `tests/test_stream.py` — generator unit tests
- `tests/test_smoke.py` — optional end-to-end smoke (marked `integration`)
- `.env` — shared ports/creds for compose + clients

---

## Task 1: Project scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `generator/__init__.py` (empty)
- Create: `tests/__init__.py` (empty)
- Create: `.env`

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[project]
name = "ad-lakehouse"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
  "confluent-kafka>=2.3",
  "pydantic>=2.6",
]

[project.optional-dependencies]
dev = ["pytest>=8.0", "ruff>=0.5", "pyspark==3.5.1"]

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.pytest.ini_options]
markers = ["integration: requires the docker stack running"]
addopts = "-m 'not integration'"
```

- [ ] **Step 2: Create `.env`**

```env
# Kafka / Redpanda
KAFKA_BOOTSTRAP=localhost:19092
KAFKA_TOPIC=ad_events
# MinIO
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=password
AWS_REGION=us-east-1
S3_ENDPOINT=http://localhost:9000
WAREHOUSE_BUCKET=warehouse
# Iceberg REST
ICEBERG_REST_URI=http://localhost:8181
```

- [ ] **Step 3: Create empty package files**

Run: `touch generator/__init__.py tests/__init__.py`

- [ ] **Step 4: Create the venv and install dev deps**

Run: `python3.11 -m venv .venv && . .venv/bin/activate && pip install -e '.[dev]'`
Expected: installs confluent-kafka, pydantic, pytest, ruff, pyspark with no errors.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml .env generator/__init__.py tests/__init__.py
git commit -m "chore: project scaffolding for ad-lakehouse streaming spine"
```

---

## Task 2: The `AdEvent` schema + factory (TDD)

**Files:**
- Create: `generator/event.py`
- Test: `tests/test_event.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_event.py
from datetime import datetime, timezone
from generator.event import AdEvent, make_event, EVENT_TYPES

def test_make_event_has_all_required_fields():
    ev = make_event(seed=1, now=datetime(2026, 6, 1, tzinfo=timezone.utc))
    d = ev.model_dump()
    for field in ["event_id", "event_type", "event_ts", "campaign_id",
                  "creative_id", "request_id", "user_id", "device", "geo", "placement"]:
        assert field in d and d[field] is not None

def test_event_type_is_valid():
    ev = make_event(seed=2, now=datetime(2026, 6, 1, tzinfo=timezone.utc))
    assert ev.event_type in EVENT_TYPES

def test_seed_is_deterministic():
    now = datetime(2026, 6, 1, tzinfo=timezone.utc)
    assert make_event(seed=7, now=now).model_dump() == make_event(seed=7, now=now).model_dump()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_event.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'generator.event'`

- [ ] **Step 3: Write minimal implementation**

```python
# generator/event.py
from __future__ import annotations
import random
from datetime import datetime, timezone
from pydantic import BaseModel

EVENT_TYPES = ("ad_request", "impression", "q25", "q50", "q75", "q100")
DEVICES = ("mobile", "desktop", "ctv")
GEOS = ("US-CA", "US-NY", "GB-LND", "DE-BE", "JP-13")
PLACEMENTS = ("preroll", "midroll", "banner_top", "banner_side")

class AdEvent(BaseModel):
    event_id: str
    event_type: str
    event_ts: datetime
    campaign_id: str
    creative_id: str
    request_id: str
    user_id: str
    device: str
    geo: str
    placement: str

def make_event(seed: int, now: datetime) -> AdEvent:
    r = random.Random(seed)
    return AdEvent(
        event_id=f"evt-{r.getrandbits(64):016x}",
        event_type=r.choice(EVENT_TYPES),
        event_ts=now,
        campaign_id=f"cmp-{r.randint(1, 20):03d}",
        creative_id=f"crv-{r.randint(1, 60):03d}",
        request_id=f"req-{r.getrandbits(48):012x}",
        user_id=f"usr-{r.randint(1, 5000):05d}",
        device=r.choice(DEVICES),
        geo=r.choice(GEOS),
        placement=r.choice(PLACEMENTS),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_event.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add generator/event.py tests/test_event.py
git commit -m "feat(generator): AdEvent schema and deterministic factory"
```

---

## Task 3: Event stream with injected duplicates + late events (TDD)

**Files:**
- Create: `generator/stream.py`
- Test: `tests/test_stream.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_stream.py
from datetime import datetime, timezone, timedelta
from generator.stream import event_batch

NOW = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)

def test_batch_size_includes_injected_duplicates():
    events = list(event_batch(n=1000, now=NOW, dup_rate=0.02, late_rate=0.05, seed=0))
    # duplicates are extra emissions on top of n unique base events
    ids = [e.event_id for e in events]
    assert len(ids) > 1000
    assert len(set(ids)) == 1000  # exactly n unique base events

def test_duplicate_rate_in_tolerance():
    events = list(event_batch(n=5000, now=NOW, dup_rate=0.02, late_rate=0.0, seed=1))
    extra = len(events) - 5000
    assert 0.01 <= extra / 5000 <= 0.03

def test_late_events_are_backdated():
    events = list(event_batch(n=5000, now=NOW, dup_rate=0.0, late_rate=0.05, seed=2))
    late = [e for e in events if e.event_ts < NOW - timedelta(minutes=1)]
    assert 0.03 <= len(late) / 5000 <= 0.07
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_stream.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'generator.stream'`

- [ ] **Step 3: Write minimal implementation**

```python
# generator/stream.py
from __future__ import annotations
import random
from collections.abc import Iterator
from datetime import datetime, timedelta
from generator.event import make_event, AdEvent

def event_batch(n: int, now: datetime, dup_rate: float, late_rate: float,
                seed: int = 0) -> Iterator[AdEvent]:
    """Yield n unique base events, plus duplicate re-emissions (~dup_rate),
    with ~late_rate of all events backdated to simulate late arrival."""
    r = random.Random(seed)
    for i in range(n):
        ev = make_event(seed=seed * 1_000_003 + i, now=now)
        if r.random() < late_rate:
            ev = ev.model_copy(update={
                "event_ts": now - timedelta(minutes=r.randint(2, 240))
            })
        yield ev
        if r.random() < dup_rate:
            yield ev  # same event_id → duplicate
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_stream.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add generator/stream.py tests/test_stream.py
git commit -m "feat(generator): event_batch with injected duplicates and late events"
```

---

## Task 4: Docker Compose infra (Redpanda, MinIO, Iceberg REST, Spark, Trino)

**Files:**
- Create: `docker-compose.yml`
- Create: `docker/trino/catalog/iceberg.properties`

- [ ] **Step 1: Create `docker-compose.yml`**

```yaml
services:
  redpanda:
    image: redpandadata/redpanda:v24.1.7
    command:
      - redpanda start
      - --smp 1
      - --overprovisioned
      - --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092
      - --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092
    ports: ["19092:19092"]

  minio:
    image: minio/minio:RELEASE.2024-06-13T22-53-53Z
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: admin
      MINIO_ROOT_PASSWORD: password
    ports: ["9000:9000", "9001:9001"]

  minio-init:
    image: minio/mc:RELEASE.2024-06-12T14-34-03Z
    depends_on: [minio]
    entrypoint: >
      /bin/sh -c "
      until (mc alias set m http://minio:9000 admin password) do sleep 1; done;
      mc mb --ignore-existing m/warehouse;
      exit 0;"

  iceberg-rest:
    image: apache/iceberg-rest-fixture:1.5.2
    depends_on: [minio, minio-init]
    environment:
      AWS_ACCESS_KEY_ID: admin
      AWS_SECRET_ACCESS_KEY: password
      AWS_REGION: us-east-1
      CATALOG_WAREHOUSE: s3://warehouse/
      CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
      CATALOG_S3_ENDPOINT: http://minio:9000
      CATALOG_S3_PATH__STYLE__ACCESS: "true"
    ports: ["8181:8181"]

  spark:
    image: apache/spark:3.5.1-python3
    depends_on: [iceberg-rest, redpanda, minio]
    environment:
      AWS_ACCESS_KEY_ID: admin
      AWS_SECRET_ACCESS_KEY: password
      AWS_REGION: us-east-1
    volumes:
      - ./:/opt/app
    working_dir: /opt/app
    command: ["tail", "-f", "/dev/null"]   # long-lived; we exec spark-submit into it

  trino:
    image: trinodb/trino:451
    depends_on: [iceberg-rest, minio]
    ports: ["8080:8080"]
    volumes:
      - ./docker/trino/catalog:/etc/trino/catalog
```

- [ ] **Step 2: Create the Trino Iceberg catalog**

```properties
# docker/trino/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
iceberg.rest-catalog.warehouse=s3://warehouse/
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=admin
s3.aws-secret-key=password
```

- [ ] **Step 3: Bring the stack up**

Run: `docker compose up -d`
Expected: all six services start; `docker compose ps` shows redpanda, minio, iceberg-rest, spark, trino as running (minio-init exits 0).

- [ ] **Step 4: Verify Trino is alive**

Run: `docker compose exec trino trino --execute "SHOW CATALOGS"`
Expected: output includes `iceberg` and `system`.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml docker/trino/catalog/iceberg.properties
git commit -m "feat(infra): docker-compose stack with redpanda, minio, iceberg-rest, spark, trino"
```

---

## Task 5: Shared Spark session builder

**Files:**
- Create: `streaming/__init__.py` (empty)
- Create: `streaming/spark_session.py`

- [ ] **Step 1: Create the package file**

Run: `touch streaming/__init__.py`

- [ ] **Step 2: Write `streaming/spark_session.py`**

```python
# streaming/spark_session.py
import os
from pyspark.sql import SparkSession

ICEBERG_PKGS = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,"
    "org.apache.iceberg:iceberg-aws-bundle:1.8.1,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)

def build_spark(app_name: str) -> SparkSession:
    rest_uri = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    s3_endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", ICEBERG_PKGS)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lh", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lh.type", "rest")
        .config("spark.sql.catalog.lh.uri", rest_uri)
        .config("spark.sql.catalog.lh.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.lh.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lh.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.lh.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", "lh")
        .getOrCreate()
    )
```

- [ ] **Step 3: Smoke-test the session against the catalog**

Run:
```bash
docker compose exec -e ICEBERG_REST_URI=http://iceberg-rest:8181 -e S3_ENDPOINT=http://minio:9000 \
  spark /opt/spark/bin/spark-submit /opt/app/streaming/spark_session.py 2>/dev/null || \
docker compose exec spark python3 -c "import sys; sys.path.insert(0,'/opt/app'); \
from streaming.spark_session import build_spark; \
s=build_spark('smoke'); s.sql('CREATE NAMESPACE IF NOT EXISTS lh.bronze'); \
print('namespaces:', [r.namespace for r in s.sql('SHOW NAMESPACES IN lh').collect()])"
```
Expected: prints `namespaces: ['bronze']` (jars download on first run — allow a minute).

- [ ] **Step 4: Commit**

```bash
git add streaming/__init__.py streaming/spark_session.py
git commit -m "feat(streaming): shared Spark session builder for Iceberg REST + S3"
```

---

## Task 6: Kafka producer entrypoint

**Files:**
- Create: `generator/produce.py`

- [ ] **Step 1: Write `generator/produce.py`**

```python
# generator/produce.py
import argparse
import json
import os
from datetime import datetime, timezone
from confluent_kafka import Producer
from generator.stream import event_batch

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=10_000)
    ap.add_argument("--dup-rate", type=float, default=0.02)
    ap.add_argument("--late-rate", type=float, default=0.05)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
    topic = os.environ.get("KAFKA_TOPIC", "ad_events")
    producer = Producer({"bootstrap.servers": bootstrap})

    now = datetime.now(timezone.utc)
    count = 0
    for ev in event_batch(args.n, now, args.dup_rate, args.late_rate, args.seed):
        payload = ev.model_dump()
        payload["event_ts"] = payload["event_ts"].isoformat()
        producer.produce(topic, key=ev.user_id, value=json.dumps(payload))
        count += 1
        if count % 2000 == 0:
            producer.poll(0)
    producer.flush()
    print(f"produced {count} events to {topic}")

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Produce a batch to Kafka**

Run: `. .venv/bin/activate && set -a && . ./.env && set +a && python -m generator.produce --n 5000`
Expected: prints `produced ~5100 events to ad_events`.

- [ ] **Step 3: Verify the topic has records**

Run: `docker compose exec redpanda rpk topic consume ad_events --num 1`
Expected: one JSON event record printed (has `event_id`, `event_type`, `user_id`, etc.).

- [ ] **Step 4: Commit**

```bash
git add generator/produce.py
git commit -m "feat(generator): Kafka producer entrypoint for ad_events"
```

---

## Task 7: Spark Structured Streaming → bronze

**Files:**
- Create: `streaming/ingest_bronze.py`

- [ ] **Step 1: Write `streaming/ingest_bronze.py`**

```python
# streaming/ingest_bronze.py
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, TimestampType)
from streaming.spark_session import build_spark

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("campaign_id", StringType()),
    StructField("creative_id", StringType()),
    StructField("request_id", StringType()),
    StructField("user_id", StringType()),
    StructField("device", StringType()),
    StructField("geo", StringType()),
    StructField("placement", StringType()),
])

def main() -> None:
    spark = build_spark("ingest-bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.bronze")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lh.bronze.ad_events_raw (
          event_id string, event_type string, event_ts timestamp,
          campaign_id string, creative_id string, request_id string,
          user_id string, device string, geo string, placement string,
          kafka_ts timestamp, ingest_ts timestamp
        ) USING iceberg
        PARTITIONED BY (days(ingest_ts))
    """)

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", "redpanda:9092")
           .option("subscribe", "ad_events")
           .option("startingOffsets", "earliest")
           .load())

    parsed = (raw.select(
                F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
                F.col("timestamp").alias("kafka_ts"))
              .select("e.*", "kafka_ts")
              .withColumn("ingest_ts", F.current_timestamp()))

    query = (parsed.writeStream
             .format("iceberg")
             .outputMode("append")
             .option("checkpointLocation", "/opt/app/.checkpoints/bronze_ad_events")
             .trigger(processingTime="10 seconds")
             .toTable("lh.bronze.ad_events_raw"))
    query.awaitTermination()

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Start the streaming job (inside the spark container)**

Run:
```bash
docker compose exec -d -e PYTHONPATH=/opt/app spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/app/streaming/ingest_bronze.py
```
Note: `-e PYTHONPATH=/opt/app` is required so the `streaming` package is importable (spark-submit puts the script's dir on sys.path, not cwd). `--conf spark.jars.ivy=/tmp/.ivy2` is required because the `spark` container user has no writable home for Ivy's default cache.
Expected: job starts; after ~30s it has processed the earliest offsets. (Check `docker compose logs spark | tail`.)

- [ ] **Step 3: Produce more events while streaming runs**

Run: `set -a && . ./.env && set +a && .venv/bin/python -m generator.produce --n 5000 --seed 9`
Expected: `produced ~5100 events to ad_events`. (Use `.venv/bin/python` explicitly — a shell alias may shadow the venv's `python`.)

- [ ] **Step 4: Verify rows landed in bronze via Trino**

Run: `docker compose exec trino trino --execute "SELECT count(*) FROM iceberg.bronze.ad_events_raw"`
Expected: a non-zero count that grows on repeat (≈10000+ after both produce runs are consumed).

- [ ] **Step 5: Commit**

```bash
git add streaming/ingest_bronze.py
git commit -m "feat(streaming): Structured Streaming Kafka -> Iceberg bronze with checkpointing"
```

---

## Task 8: Makefile, smoke SQL, and README quickstart

**Files:**
- Create: `Makefile`
- Create: `trino/00_bronze_smoke.sql`
- Create: `tests/test_smoke.py`
- Create: `README.md`

- [ ] **Step 1: Write `trino/00_bronze_smoke.sql`**

```sql
SELECT count(*) AS rows,
       count(DISTINCT event_id) AS distinct_ids,
       count(DISTINCT user_id) AS distinct_users
FROM iceberg.bronze.ad_events_raw;
```

- [ ] **Step 2: Write `Makefile`**

```makefile
.PHONY: up down seed stream query test lint
up:        ; docker compose up -d
down:      ; docker compose down -v
seed:      ; set -a && . ./.env && set +a && .venv/bin/python -m generator.produce --n 10000
stream:    ; docker compose exec -d -e PYTHONPATH=/opt/app spark /opt/spark/bin/spark-submit \
             --conf spark.jars.ivy=/tmp/.ivy2 \
             --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
             /opt/app/streaming/ingest_bronze.py
query:     ; docker compose exec -T trino trino --catalog iceberg < trino/00_bronze_smoke.sql
test:      ; . .venv/bin/activate && pytest -v
lint:      ; . .venv/bin/activate && ruff check .
```

- [ ] **Step 3: Write `tests/test_smoke.py` (integration-marked)**

```python
# tests/test_smoke.py
import subprocess
import pytest

@pytest.mark.integration
def test_bronze_has_rows():
    out = subprocess.check_output(
        ["docker", "compose", "exec", "-T", "trino", "trino",
         "--execute", "SELECT count(*) FROM iceberg.bronze.ad_events_raw"],
        text=True,
    )
    assert int(out.strip().strip('"')) > 0
```

- [ ] **Step 4: Write `README.md` quickstart**

```markdown
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
```

- [ ] **Step 5: Run the full unit suite + lint**

Run: `make test && make lint`
Expected: all unit tests pass (event + stream), ruff clean.

- [ ] **Step 6: Commit**

```bash
git add Makefile trino/00_bronze_smoke.sql tests/test_smoke.py README.md
git commit -m "feat: Makefile, bronze smoke query, smoke test, README quickstart"
```

---

## Self-review notes

- **Spec coverage (Plan 1 scope):** §3 architecture stack (Task 4), §4.1 event schema (Tasks 2–3, incl. injected dupes + late events), §4.2 bronze table partitioned by `days(ingest_ts)` (Task 7), §5 streaming ingest with checkpointing + 10s trigger (Task 7). Silver/gold/GDPR/perf/Airflow/API are out of scope here by design — Plans 2–5.
- **Type consistency:** the 10 event fields are identical across `event.py`, `produce.py` JSON, `EVENT_SCHEMA` in `ingest_bronze.py`, and the bronze DDL. Catalog name `lh` (Spark) maps to Trino catalog `iceberg` — both point at the same Iceberg REST warehouse `s3://warehouse/`.
- **Known first-run cost:** Spark downloads Iceberg/Kafka jars on first `spark-submit`; allow ~1–2 min. Subsequent runs are cached in the container layer.
```
