# Ad-Lakehouse — Plan 4: Airflow Orchestration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put the batch layers and table maintenance under real Airflow — DAGs that pull campaign metadata, build the silver→gold medallion in dependency order, and run Iceberg maintenance (compaction + snapshot expiry) — so the lakehouse is operated like a production platform, not a sequence of `make` calls.

**Architecture:** A single Airflow container (`airflow standalone`, SequentialExecutor + SQLite — lightest footprint for this memory-constrained stack) orchestrates the existing `transform/run.py` driver. Airflow does NOT re-implement Spark: each task is a `BashOperator` that `docker exec`s into the already-running, jar-warmed `spark` container and runs `spark-submit transform/run.py <target>` (Docker-out-of-Docker via the mounted socket). The streaming ingest stays a long-lived service outside Airflow; Airflow owns the batch + maintenance lifecycle. A new `transform/maintenance.py` adds Iceberg `rewrite_data_files` + `expire_snapshots` as a driver target.

**Tech Stack:** Apache Airflow 2.9 (standalone, SQLite); Docker-out-of-Docker (`docker exec`); the existing Spark 3.5 + Iceberg 1.8.1 + Trino + MinIO stack; Docker Compose; pytest + ruff.

**Spec:** `docs/superpowers/specs/2026-06-04-ad-lakehouse-design.md` §9 (Airflow DAGs: campaign_pull, medallion_build, iceberg_maintenance; streaming stays a service). **Builds on:** Plans 1–3 (merged). The `gdpr_delete` DAG from §9 is deferred to the GDPR plan (it needs that plan's delete logic).

## Realized state this plan builds on

- `transform/run.py` driver: `python -m transform.run <target>` where target ∈ `silver|gold|all` (groups) or a single builder name; builds in one Spark session, `try/finally: spark.stop()`.
- Spark runs via: `docker exec -e PYTHONPATH=/opt/app ad-lakehouse-spark-1 /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/app/transform/run.py <target>`. The spark container is long-lived (`tail -f /dev/null`) with the repo bind-mounted at `/opt/app` and the Ivy cache warm at `/tmp/.ivy2`.
- Iceberg tables: `bronze.ad_events_raw`, `silver.fact_event`, `silver.dim_campaign`, `gold.fact_impression_delivery`, `gold.inventory_fill`, `gold.campaign_pacing` (catalog `lh` in Spark, `iceberg` in Trino). Trino host port 8081, api 8000, MinIO console 9001.

## ⚠️ Memory & socket notes (read before executing)

- **Memory:** Airflow adds a container to a stack that already OOM-kills Trino/Redpanda (exit 137) under Spark load. Before executing, raise Docker Desktop memory (Settings → Resources) to ≥ 8 GB. Redpanda can stay stopped during Airflow work (DAGs touch only Iceberg/Spark). If Trino dies during a DAG run, `docker compose up -d trino` — it isn't needed by the DAGs themselves, only for your verification queries.
- **Docker-out-of-Docker socket:** the Airflow container runs `docker exec` against `ad-lakehouse-spark-1` via the mounted `/var/run/docker.sock`. On Docker Desktop for macOS the mounted socket is generally usable from a container that has the docker CLI. The #1 integration risk is a socket **permission** error inside the Airflow container. Resolution order if it happens: (a) confirm the docker CLI is installed in the image and the socket is mounted; (b) run the Airflow service as `user: "0:0"` (root) — acceptable for a local demo; (c) `group_add` the socket's GID. The executor should try (a)→(b) and report which worked.

## File structure (created/modified)

- `transform/maintenance.py` *(new)* — `build(spark)` running Iceberg compaction + snapshot expiry
- `transform/run.py` *(modify)* — register `maintenance` builder + group
- `airflow/dags/_spark.py` *(new)* — shared helper building the `docker exec spark-submit` command (DRY across DAGs)
- `airflow/dags/campaign_pull_dag.py`, `airflow/dags/medallion_dag.py`, `airflow/dags/maintenance_dag.py` *(new)*
- `docker/airflow/Dockerfile` *(new)* — Airflow + docker CLI
- `docker-compose.yml` *(modify)* — `airflow` service + `container_name: ad-lakehouse-spark` on the spark service
- `tests/test_dags.py` *(new)* — DAG-parse integrity test (no Airflow runtime needed beyond import)
- `Makefile` *(modify)* — `airflow-up`, `airflow-password`, `dags-list`, `dag-medallion` targets
- `README.md` *(modify)* — roadmap + orchestration section
- `pyproject.toml` *(modify)* — add `apache-airflow==2.9.3` to a new `airflow` extra (for the DAG-parse test on the host)

---

## Task 1: Iceberg maintenance as a driver target (integration)

**Files:**
- Create: `transform/maintenance.py`
- Modify: `transform/run.py`

- [ ] **Step 1: Implement `transform/maintenance.py`**

```python
# transform/maintenance.py
from pyspark.sql import SparkSession

# Tables to maintain (catalog 'lh' is the default in build_spark).
TABLES = [
    "bronze.ad_events_raw",
    "silver.fact_event",
    "silver.dim_campaign",
    "gold.fact_impression_delivery",
    "gold.inventory_fill",
    "gold.campaign_pacing",
]


def build(spark: SparkSession) -> None:
    """Iceberg table maintenance: compact small files (rewrite_data_files) and
    expire old snapshots (retain only the newest), per table. Idempotent and safe
    to run repeatedly. Named build(spark) for uniformity with the other driver
    targets even though it runs maintenance, not a table build.
    """
    now = spark.sql(
        "SELECT date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS t"
    ).collect()[0]["t"]
    for table in TABLES:
        before = spark.sql(f"SELECT count(*) AS c FROM lh.{table}.snapshots").collect()[0]["c"]
        spark.sql(f"CALL lh.system.rewrite_data_files(table => '{table}')")
        # older_than = now makes every existing snapshot eligible; retain_last keeps the newest.
        spark.sql(
            f"CALL lh.system.expire_snapshots("
            f"table => '{table}', older_than => TIMESTAMP '{now}', retain_last => 1)"
        )
        after = spark.sql(f"SELECT count(*) AS c FROM lh.{table}.snapshots").collect()[0]["c"]
        print(f"[maintenance] {table}: snapshots {before} -> {after}")
```

- [ ] **Step 2: Register it in `transform/run.py`**

Add the import and entries (keep everything else):
```python
from transform import dim_campaign, fact_event, gold_delivery, gold_fill, gold_pacing, maintenance
```
In `BUILDERS` add: `"maintenance": maintenance.build,`
In `GROUPS` add: `"maintenance": ["maintenance"],`

- [ ] **Step 3: Run it standalone in the spark container**

```bash
docker exec -e PYTHONPATH=/opt/app ad-lakehouse-spark-1 /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/app/transform/run.py maintenance 2>&1 | grep -E "\[maintenance\]|Error|Exception" | tail -10
```
Expected: a `[maintenance] <table>: snapshots N -> M` line per table (M <= N; after a fresh build there may be only 1 snapshot already, so N==M==1 is fine — the point is it runs without error). Report the lines.

- [ ] **Step 4: Lint + commit**

```bash
.venv/bin/ruff check transform/maintenance.py transform/run.py
git add transform/maintenance.py transform/run.py
git commit -m "feat(transform): iceberg maintenance (compaction + snapshot expiry) as a driver target"
```

## Context
`CALL lh.system.rewrite_data_files` compacts small files; `CALL lh.system.expire_snapshots` drops old snapshots so storage and metadata don't grow unbounded. Both are standard Iceberg table-maintenance procedures Airflow will schedule. The `.snapshots` metadata table gives a before/after to prove it ran.

## IMPORTANT gotchas
- Procedure signatures are Iceberg 1.8.1 / Spark 3.5: `CALL <catalog>.system.rewrite_data_files(table => 'db.tbl')` and `CALL <catalog>.system.expire_snapshots(table => 'db.tbl', older_than => TIMESTAMP '...', retain_last => 1)`. If a signature errors, capture the exact AnalysisException and adjust (don't blind-guess).
- The `lh.<table>.snapshots` metadata table is how you read snapshot count in Spark.
- Run via `docker exec ad-lakehouse-spark-1 ...` (the container exists now).

## Report Format
- **Status:** DONE | DONE_WITH_CONCERNS | BLOCKED | NEEDS_CONTEXT
- The per-table `[maintenance]` lines
- ruff result + commit SHA
- Any deviations/errors

---

## Task 2: Airflow container (integration — riskiest task)

**Files:**
- Create: `docker/airflow/Dockerfile`, `airflow/dags/.gitkeep`
- Modify: `docker-compose.yml`

- [ ] **Step 1: Give the spark service a stable container name**

In `docker-compose.yml`, add to the `spark:` service (sibling of `image:`):
```yaml
    container_name: ad-lakehouse-spark
```
This makes the `docker exec` target deterministic regardless of compose project naming. (The DAGs in later tasks reference `ad-lakehouse-spark`.)

- [ ] **Step 2: Create `docker/airflow/Dockerfile`**

```dockerfile
# docker/airflow/Dockerfile
FROM apache/airflow:2.9.3-python3.11
USER root
# docker CLI so DAG tasks can `docker exec` the spark container (Docker-out-of-Docker)
RUN apt-get update \
 && apt-get install -y --no-install-recommends docker.io \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow
```

- [ ] **Step 3: Add the `airflow` service to `docker-compose.yml`**

```yaml
  airflow:
    build:
      context: .
      dockerfile: docker/airflow/Dockerfile
    user: "0:0"   # run as root so the mounted docker socket is accessible (local demo)
    environment:
      AIRFLOW__CORE__EXECUTOR: SequentialExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"
      AIRFLOW__WEBSERVER__EXPOSE_CONFIG: "true"
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - /var/run/docker.sock:/var/run/docker.sock
    ports: ["8082:8080"]
    command: standalone
```

- [ ] **Step 4: Create the dags dir placeholder**

Run: `mkdir -p airflow/dags && touch airflow/dags/.gitkeep`

- [ ] **Step 5: Build + start Airflow**

```bash
docker compose up -d --build airflow
# standalone takes ~60-90s to init the DB and start. Watch:
for i in $(seq 1 30); do docker compose exec -T airflow airflow db check >/dev/null 2>&1 && { echo "airflow db ready"; break; }; sleep 5; done
docker compose exec -T airflow airflow dags list 2>&1 | tail -5
```
Expected: `airflow db ready`, then `airflow dags list` runs without error (likely "No data found" / empty since no DAGs yet — that's success; the command working is the signal).

- [ ] **Step 6: Verify the Docker-out-of-Docker socket works from Airflow**

```bash
docker compose exec -T airflow docker ps --format '{{.Names}}' 2>&1 | grep ad-lakehouse-spark
```
Expected: prints `ad-lakehouse-spark` — proving the Airflow container can see and will be able to `docker exec` the spark container. **If this fails with a permission error**, the `user: "0:0"` should resolve it; if still failing, report the exact error (this is the known socket risk).

- [ ] **Step 7: Commit**

```bash
git add docker/airflow/Dockerfile docker-compose.yml airflow/dags/.gitkeep
git commit -m "feat(airflow): standalone Airflow container with docker-out-of-docker access to spark"
```

## IMPORTANT gotchas
- `airflow standalone` runs webserver + scheduler + a SQLite metadata DB in one process — lightest footprint. First boot is slow (DB migration). Be patient (use the db-check loop).
- The admin password is written to `/opt/airflow/standalone_admin_password.txt` inside the container (retrieved in Task 6). UI at http://localhost:8082.
- `user: "0:0"` runs Airflow as root so the docker socket is accessible. Airflow's image tolerates this for standalone. If file-permission warnings appear but the DB inits and `dags list` works, proceed.
- Container port 8080 inside airflow does NOT conflict with trino's internal 8080 (separate network namespaces); only host ports must differ — airflow uses host 8082.
- If the image build fails installing `docker.io`, an alternative is the official docker-ce-cli apt repo; report the failure and the fix used.

## Report Format
- **Status / what happened at each step**
- The output of Step 6 (socket check) — MUST show `ad-lakehouse-spark`, or report the permission error + resolution
- commit SHA
- Any deviations (esp. socket/permission), OOM events

---

## Task 3: Shared spark helper + campaign_pull DAG (integration)

**Files:**
- Create: `airflow/dags/_spark.py`, `airflow/dags/campaign_pull_dag.py`

- [ ] **Step 1: Create the shared helper `airflow/dags/_spark.py`**

```python
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
```

- [ ] **Step 2: Create `airflow/dags/campaign_pull_dag.py`**

```python
# airflow/dags/campaign_pull_dag.py
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _spark import spark_submit

with DAG(
    dag_id="campaign_pull",
    description="Pull campaign metadata from the FastAPI service into silver.dim_campaign",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["ad-lakehouse", "silver"],
):
    BashOperator(task_id="pull_dim_campaign", bash_command=spark_submit("dim_campaign"))
```

- [ ] **Step 3: Verify the DAG parses and runs**

```bash
docker compose exec -T airflow airflow dags list 2>&1 | grep campaign_pull
docker compose exec -T airflow airflow dags test campaign_pull 2026-06-05 2>&1 | tail -15
```
Expected: `campaign_pull` appears in the list; `dags test` runs the single task, which docker-execs spark-submit and rebuilds `silver.dim_campaign`, ending with the task in `success` state. Then confirm via Trino: `docker compose exec -T trino trino --execute "SELECT count(*) FROM iceberg.silver.dim_campaign"` → 20. Report the task state + the count.

- [ ] **Step 4: Commit**

```bash
git add airflow/dags/_spark.py airflow/dags/campaign_pull_dag.py
git commit -m "feat(airflow): campaign_pull DAG -> silver.dim_campaign"
```

## IMPORTANT gotchas
- DAG files import `from _spark import spark_submit` — Airflow puts the dags folder on sys.path, so a sibling module import works. (Confirm: if the import fails in `dags list`, the dags-folder-on-path assumption is wrong; fall back to `from dags._spark import ...` is NOT correct — instead keep `_spark.py` in the same dags dir, which is on the path.)
- `airflow dags test <id> <logical_date>` runs the DAG synchronously without the scheduler — ideal for verifying. It executes the real `docker exec` task.
- If the task fails on `docker: command not found` or socket permission, that's the Task-2 socket setup — report it.

## Report Format
- **Status**, the `dags test` final task state, the dim_campaign count (20), ruff/commit, any issues

---

## Task 4: medallion_build DAG (integration)

**Files:**
- Create: `airflow/dags/medallion_dag.py`

- [ ] **Step 1: Create `airflow/dags/medallion_dag.py`**

```python
# airflow/dags/medallion_dag.py
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _spark import spark_submit

with DAG(
    dag_id="medallion_build",
    description="Build the silver then gold Iceberg layers in dependency order",
    schedule="@hourly",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["ad-lakehouse", "silver", "gold"],
):
    build_silver = BashOperator(task_id="build_silver", bash_command=spark_submit("silver"))
    build_gold = BashOperator(task_id="build_gold", bash_command=spark_submit("gold"))
    build_silver >> build_gold
```

- [ ] **Step 2: Verify the DAG parses and runs end-to-end**

```bash
docker compose exec -T airflow airflow dags list 2>&1 | grep medallion_build
docker compose exec -T airflow airflow dags test medallion_build 2026-06-05 2>&1 | tail -25
```
Expected: `build_silver` runs (rebuilds dim_campaign + fact_event) THEN `build_gold` runs (rebuilds the 3 gold tables), both `success`, in that order. Then verify gold is fresh via Trino: `docker compose exec -T trino trino --execute "SELECT count(*) FROM iceberg.gold.campaign_pacing"` → 169 (or current). Report both task states + the count.

- [ ] **Step 3: Commit**

```bash
git add airflow/dags/medallion_dag.py
git commit -m "feat(airflow): medallion_build DAG (silver -> gold, dependency-ordered)"
```

## IMPORTANT gotchas
- `build_silver >> build_gold` enforces order: gold only runs after silver succeeds. This is the core orchestration value — demonstrate it in the `dags test` output (silver task completes before gold starts).
- Each task is a separate spark-submit (separate Spark session) — that's fine; the dependency is what matters. (Running both in one session would defeat the per-step retry/visibility Airflow gives.)
- Watch for Trino/Spark OOM during the gold step; if the gold task fails with a Spark error (not a SQL error), it may be memory — note it and retry after freeing memory.

## Report Format
- **Status**, both task states + ordering proof, the gold count, ruff/commit, any OOM/issues

---

## Task 5: iceberg_maintenance DAG (integration)

**Files:**
- Create: `airflow/dags/maintenance_dag.py`

- [ ] **Step 1: Create `airflow/dags/maintenance_dag.py`**

```python
# airflow/dags/maintenance_dag.py
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _spark import spark_submit

with DAG(
    dag_id="iceberg_maintenance",
    description="Compact small files and expire old snapshots across all Iceberg tables",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["ad-lakehouse", "maintenance"],
):
    BashOperator(task_id="maintain_tables", bash_command=spark_submit("maintenance"))
```

- [ ] **Step 2: Verify the DAG parses and runs**

```bash
docker compose exec -T airflow airflow dags list 2>&1 | grep iceberg_maintenance
docker compose exec -T airflow airflow dags test iceberg_maintenance 2026-06-05 2>&1 | tail -20
```
Expected: the task runs `transform/run.py maintenance`, printing the per-table `[maintenance] <table>: snapshots N -> M` lines, task `success`. Report the task state + a couple of the maintenance lines.

- [ ] **Step 3: Commit**

```bash
git add airflow/dags/maintenance_dag.py
git commit -m "feat(airflow): iceberg_maintenance DAG (compaction + snapshot expiry)"
```

## Report Format
- **Status**, task state, sample maintenance lines, commit, any issues

---

## Task 6: DAG-parse test, Makefile, README (integration + unit)

**Files:**
- Create: `tests/test_dags.py`
- Modify: `pyproject.toml`, `Makefile`, `README.md`

- [ ] **Step 1: Add an `airflow` extra to `pyproject.toml`**

In `[project.optional-dependencies]`, add a new extra (keep `dev` as-is):
```toml
airflow = ["apache-airflow==2.9.3"]
```
Then install it into the venv for the parse test: `.venv/bin/pip install -e '.[airflow]'` (this is a large install; allow a few minutes). If the resolver conflicts with `dev` deps, install into the venv anyway — the parse test only imports airflow + the dag modules.

- [ ] **Step 2: Write `tests/test_dags.py`**

```python
# tests/test_dags.py
import importlib.util
import sys
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "airflow" / "dags"
DAG_FILES = sorted(DAGS_DIR.glob("*_dag.py"))


@pytest.fixture(autouse=True)
def _dags_on_path():
    # DAG modules import their sibling `_spark` helper; Airflow puts the dags dir
    # on sys.path at runtime, so replicate that for the test.
    sys.path.insert(0, str(DAGS_DIR))
    yield
    sys.path.remove(str(DAGS_DIR))


@pytest.mark.parametrize("dag_file", DAG_FILES, ids=lambda p: p.stem)
def test_dag_file_imports_and_defines_a_dag(dag_file):
    pytest.importorskip("airflow")  # only runs where the airflow extra is installed
    from airflow.models import DAG

    spec = importlib.util.spec_from_file_location(dag_file.stem, dag_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    dags = [v for v in vars(module).values() if isinstance(v, DAG)]
    assert len(dags) == 1, f"{dag_file.name} should define exactly one DAG"
    assert dags[0].dag_id  # non-empty id


def test_three_dag_files_present():
    assert {p.stem for p in DAG_FILES} == {
        "campaign_pull_dag", "medallion_dag", "maintenance_dag",
    }
```

- [ ] **Step 3: Run the DAG-parse test + full suite**

```bash
.venv/bin/pytest tests/test_dags.py -v
.venv/bin/pytest -q
.venv/bin/ruff check .
```
Expected: the 3 parametrized parse tests pass (or skip if airflow isn't importable on host — but Step 1 installed it, so they should PASS) + `test_three_dag_files_present` passes; full suite green; ruff clean. Report counts.

- [ ] **Step 4: Add Makefile targets** (tab-indented; add to `.PHONY`)

```makefile
airflow-up: ; docker compose up -d --build airflow
airflow-password: ; docker compose exec -T airflow cat /opt/airflow/standalone_admin_password.txt
dags-list: ; docker compose exec -T airflow airflow dags list
dag-medallion: ; docker compose exec -T airflow airflow dags test medallion_build 2026-06-05
```
Run `make -n dag-medallion` to confirm it assembles.

- [ ] **Step 5: Update `README.md`**

- Flip the roadmap "3. Airflow orchestration" row to `✅ **done**`.
- Add an "Orchestration" section: Airflow (standalone) runs three DAGs — `campaign_pull` (→ silver.dim_campaign), `medallion_build` (silver → gold, dependency-ordered), `iceberg_maintenance` (compaction + snapshot expiry). Note that Airflow orchestrates by `docker exec`-ing the spark container (it does not run Spark itself) and that streaming stays a long-lived service outside Airflow.
- Add to the quickstart:
```
make airflow-up        # start Airflow (standalone) — UI at http://localhost:8082
make airflow-password  # print the admin password
make dags-list         # list the DAGs
make dag-medallion     # run the silver->gold DAG end to end
```
- Note the Docker memory recommendation (≥ 8 GB) and that the Airflow UI is on http://localhost:8082.
- Add `airflow/` to the repo tour.

- [ ] **Step 6: Commit**

```bash
git add tests/test_dags.py pyproject.toml Makefile README.md
git commit -m "feat(airflow): DAG-parse test, Makefile targets, README orchestration section"
```

## Self-review notes

- **Spec coverage (§9):** campaign_pull DAG (Task 3), medallion_build DAG silver→gold in dependency order (Task 4), iceberg_maintenance DAG with compaction + snapshot expiry (Tasks 1 + 5). Streaming stays a service (untouched). `gdpr_delete` DAG is explicitly deferred to the GDPR plan.
- **Type/contract consistency:** every DAG task uses `spark_submit(<target>)` from `_spark.py`; targets (`dim_campaign`, `silver`, `gold`, `maintenance`) all exist in `transform/run.py`'s BUILDERS/GROUPS after Task 1. The spark container name `ad-lakehouse-spark` is set in compose (Task 2) and referenced in `_spark.py` (Task 3).
- **Lightest-footprint Airflow:** standalone + SequentialExecutor + SQLite, single container, DooD into the warm spark container (no second Spark JVM) — deliberate given the stack's memory pressure.
- **Honest risks flagged:** memory (raise Docker to ≥8GB) and the docker-socket permission path are documented up front, not discovered silently.
- **No placeholders:** every step has full code or exact commands + expected output.
```
