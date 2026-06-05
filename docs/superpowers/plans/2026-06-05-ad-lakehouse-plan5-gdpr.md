# Ad-Lakehouse — Plan 5: GDPR Right-to-be-Forgotten — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement GDPR right-to-be-forgotten on the Iceberg lakehouse — given a `user_id`, remove that user from every layer, efficiently, and make the data unrecoverable — and *prove* the two techniques that make it the headline governance flex: (1) `bucket(16, user_id)` co-partitioning so the analytical delete rewrites a fraction of the data, with a measured bucketed-vs-unbucketed comparison; (2) merge-on-read deletes that write a small delete file instead of rewriting data.

**Architecture:** `gdpr/forget_user.py` does the operational delete: row-level `DELETE` from the two PII base tables (`bronze.ad_events_raw`, `silver.fact_event`), a rebuild of the gold layer from the now-cleaned silver (so the per-impression fact and the aggregates exclude the user), and `expire_snapshots` so the pre-delete snapshots — which still contain the user — are physically gone. Two separate demo modules *measure* the techniques on throwaway copies without touching production tables: `efficiency_demo.py` (bucketed silver vs an unbucketed control) and `mor_demo.py` (merge-on-read delete files). A parameterized `gdpr_delete` Airflow DAG runs the forget on demand.

**Tech Stack:** Spark 3.5 + Iceberg 1.8.1 (row-level `DELETE` copy-on-write + merge-on-read, `expire_snapshots`, snapshot `summary` metadata); Trino; Airflow; pytest + ruff.

**Spec:** `docs/superpowers/specs/2026-06-04-ad-lakehouse-design.md` §7 (RTBF: headline `bucket()` MERGE/COW delete; second technique MoR equality deletes; verification + snapshot expiry for unrecoverability). **Builds on:** Plans 1–4 (merged). Adds the `gdpr_delete` DAG that §9 deferred to this plan.

## Realized state this plan builds on

- **PII tables (carry `user_id`):** `bronze.ad_events_raw` (partitioned `days(ingest_ts)` — no user partitioning → a user-delete scatters), `silver.fact_event` (partitioned `days(event_ts), bucket(16, user_id)` → a user hashes to ONE bucket, so a user-delete prunes to that bucket = the efficient case), `gold.fact_impression_delivery` (per-impression, carries user_id).
- **Aggregate gold (no `user_id`):** `gold.inventory_fill`, `gold.campaign_pacing` — they *count* the user's impressions, so they're corrected by rebuilding from cleaned silver, not by row-delete.
- `transform/` has `dim_campaign|fact_event|gold_delivery|gold_fill|gold_pacing|maintenance` modules (each `build(spark)`) and `run.py` (targets silver/gold/all/maintenance). `streaming/spark_session.py` → `build_spark`. Spark runs via `docker exec -e PYTHONPATH=/opt/app ad-lakehouse-spark ...`. Airflow DAGs use `airflow/dags/_spark.py`'s `spark_submit(target)` + `DEFAULT_ARGS`.

## Key design decisions

- **Why row-level DELETE (not rebuild-from-bronze) for silver:** the whole flex is that `bucket(user_id)` makes the analytical delete cheap. So forget() does `DELETE FROM silver.fact_event WHERE user_id = X` (prunes to one bucket) rather than rebuilding silver. Bronze is also deleted (row-level) — bronze has no user partitioning so it's the scattered/expensive case, AND deleting it is required so a future silver rebuild can't resurrect the user.
- **Gold:** the aggregates can't be row-deleted (no user_id), so after silver is clean, forget() rebuilds the whole gold layer from silver — `fact_impression_delivery` loses the user's rows and `inventory_fill`/`campaign_pacing` recompute without them.
- **Unrecoverability:** `expire_snapshots(older_than => now, retain_last => 1)` on every touched table drops the pre-delete snapshots so time-travel can't recover the user — the actual GDPR requirement, not just a logical delete.
- **Out of scope (noted):** suppressing *future* events for a forgotten user (a streaming-side suppression list) — RTBF here is a point-in-time erasure. dim_campaign's `target_geo`/`target_device` are campaign attributes, not user PII.

## File structure

- `gdpr/__init__.py`, `gdpr/forget_user.py` *(new)* — `forget(spark, user_id)` + CLI
- `gdpr/efficiency_demo.py` *(new)* — bucketed-vs-unbucketed measured comparison
- `gdpr/mor_demo.py` *(new)* — merge-on-read delete demonstration
- `tests/test_gdpr.py` *(new, integration)* — forget removes user across layers; bucketed << unbucketed; MoR writes delete files
- `airflow/dags/gdpr_delete_dag.py` *(new)* — parameterized forget DAG
- `pyproject.toml` *(modify)* — add `gdpr*` to packages.find include
- `Makefile` *(modify)* — `forget-user`, `gdpr-efficiency`, `gdpr-mor` targets
- `docs/gdpr-right-to-be-forgotten.md` *(new)* — the writeup + measured numbers
- `README.md` *(modify)* — roadmap + governance section

---

## Task 1: forget(user_id) — the operational erasure (integration)

**Files:**
- Create: `gdpr/__init__.py` (empty), `gdpr/forget_user.py`
- Modify: `pyproject.toml`

- [ ] **Step 1: Add `gdpr*` to packages.find in `pyproject.toml`**

Change the include line to:
```toml
include = ["generator*", "api*", "transform*", "streaming*", "gdpr*"]
```

- [ ] **Step 2: Implement `gdpr/forget_user.py`**

```python
# gdpr/forget_user.py
import argparse

from pyspark.sql import SparkSession

from streaming.spark_session import build_spark
from transform import gold_delivery, gold_fill, gold_pacing

# Base tables that physically store the user's PII rows.
PII_TABLES = ["bronze.ad_events_raw", "silver.fact_event"]
# All tables whose snapshots must be expired so the user is unrecoverable
# (the two PII bases plus the gold tables that get rebuilt below).
TOUCHED_TABLES = PII_TABLES + [
    "gold.fact_impression_delivery",
    "gold.inventory_fill",
    "gold.campaign_pacing",
]


def forget(spark: SparkSession, user_id: str) -> None:
    """GDPR right-to-be-forgotten for one user_id.

    1. Row-level DELETE from the PII base tables. silver.fact_event is
       bucket(16, user_id)-partitioned, so the predicate prunes to one bucket and
       the copy-on-write rewrite touches a fraction of the data. bronze has no user
       partitioning (scattered rewrite) but must be cleaned so a later silver
       rebuild can't resurrect the user.
    2. Rebuild the gold layer from the now-cleaned silver: fact_impression_delivery
       drops the user's rows; inventory_fill / campaign_pacing recompute without
       the user's impressions (they can't be row-deleted — no user_id column).
    3. expire_snapshots on every touched table so the pre-delete snapshots, which
       still contain the user, are physically removed (true erasure, not logical).
    """
    for table in PII_TABLES:
        spark.sql(f"DELETE FROM lh.{table} WHERE user_id = '{user_id}'")
        print(f"[forget] deleted {user_id} from {table}")

    # Rebuild gold from cleaned silver.
    for mod in (gold_delivery, gold_fill, gold_pacing):
        mod.build(spark)

    now = spark.sql(
        "SELECT date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS t"
    ).collect()[0]["t"]
    for table in TOUCHED_TABLES:
        spark.sql(
            f"CALL lh.system.expire_snapshots("
            f"table => '{table}', older_than => TIMESTAMP '{now}', retain_last => 1)"
        )
    print(f"[forget] expired snapshots; {user_id} is unrecoverable")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--user-id", required=True)
    args = ap.parse_args()
    spark = build_spark("gdpr-forget")
    try:
        forget(spark, args.user_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Run it against a real user and verify erasure**

```bash
# pick a user that has impressions
UID=$(docker compose exec -T trino trino --execute \
  "SELECT user_id FROM iceberg.gold.fact_impression_delivery GROUP BY user_id ORDER BY count(*) DESC LIMIT 1" 2>/dev/null | tr -d '"')
echo "forgetting $UID"
docker exec -e PYTHONPATH=/opt/app ad-lakehouse-spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/app/gdpr/forget_user.py --user-id "$UID" 2>&1 | grep -E "\[forget\]|Error|Exception" | tail -8
# verify zero rows across the PII layers
docker compose exec -T trino trino --execute "
SELECT
  (SELECT count(*) FROM iceberg.bronze.ad_events_raw WHERE user_id='$UID') AS bronze,
  (SELECT count(*) FROM iceberg.silver.fact_event WHERE user_id='$UID') AS silver,
  (SELECT count(*) FROM iceberg.gold.fact_impression_delivery WHERE user_id='$UID') AS gold"
```
Expected: the `[forget]` lines print; the final query returns `0, 0, 0`. Report the user_id and the three counts.

- [ ] **Step 4: Lint + commit**

```bash
touch gdpr/__init__.py
.venv/bin/ruff check gdpr/forget_user.py
git add gdpr/__init__.py gdpr/forget_user.py pyproject.toml
git commit -m "feat(gdpr): forget(user_id) — row-level delete across PII tables, gold rebuild, snapshot expiry"
```

## IMPORTANT gotchas
- Iceberg 1.8.1 + Spark 3.5 support `DELETE FROM` on copy-on-write tables. The `user_id = 'X'` predicate on `silver.fact_event` prunes to the user's bucket via the `bucket(16, user_id)` transform — that pruning IS the efficiency this plan sells (measured in Task 2).
- `user_id` is a synthetic id like `usr-01234` — safe to string-interpolate, but it comes from a controlled query, not external input.
- Rebuilding gold reuses the existing `build(spark)` functions — no duplicated SQL.
- After `expire_snapshots`, the deleted user genuinely cannot be recovered via time-travel — that's the point. Don't be alarmed that older snapshots vanish.
- Trino may OOM (exit 137) — restart with `docker compose up -d trino` and retry the verification query.
- Forgetting a user is permanent on the demo data (correct RTBF behavior); pick the top-impression user as shown.

## Report Format
- **Status / the user_id chosen / the `[forget]` lines / the 0,0,0 verification / ruff + commit SHA / any issues**

---

## Task 2: Efficiency proof — bucketed vs unbucketed delete (integration, HEADLINE)

**Files:**
- Create: `gdpr/efficiency_demo.py`

- [ ] **Step 1: Implement `gdpr/efficiency_demo.py`**

```python
# gdpr/efficiency_demo.py
"""Measure why bucket(user_id) makes a GDPR delete cheap.

Builds two throwaway copies of silver.fact_event — one bucketed by user_id (like
production silver), one partitioned only by date (the control) — deletes the SAME
user from each, and reads each delete's snapshot summary to compare the data
actually rewritten. Touches only the gdpr_demo namespace, never production.
"""
from pyspark.sql import SparkSession

from streaming.spark_session import build_spark

BUCKETED = "lh.gdpr_demo.fact_event_bucketed"
UNBUCKETED = "lh.gdpr_demo.fact_event_unbucketed"


def _delete_metrics(spark: SparkSession, table: str, user_id: str) -> dict:
    spark.sql(f"DELETE FROM {table} WHERE user_id = '{user_id}'")
    row = spark.sql(
        f"SELECT summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()[0]
    s = row["summary"]
    return {
        "data_files_rewritten": int(s.get("removed-data-files", "0")),
        "records_rewritten": int(s.get("removed-records", "0")),
        "bytes_rewritten": int(s.get("removed-files-size", "0")),
        "records_actually_deleted": int(s.get("deleted-records", s.get("removed-records", "0"))),
    }


def run(spark: SparkSession) -> dict:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.gdpr_demo")
    spark.sql(
        f"CREATE OR REPLACE TABLE {BUCKETED} USING iceberg "
        f"PARTITIONED BY (days(event_ts), bucket(16, user_id)) "
        f"AS SELECT * FROM lh.silver.fact_event"
    )
    spark.sql(
        f"CREATE OR REPLACE TABLE {UNBUCKETED} USING iceberg "
        f"PARTITIONED BY (days(event_ts)) "
        f"AS SELECT * FROM lh.silver.fact_event"
    )
    user_id = spark.sql(
        f"SELECT user_id FROM {BUCKETED} GROUP BY user_id ORDER BY count(*) DESC LIMIT 1"
    ).collect()[0]["user_id"]

    bucketed = _delete_metrics(spark, BUCKETED, user_id)
    unbucketed = _delete_metrics(spark, UNBUCKETED, user_id)
    ratio = (unbucketed["records_rewritten"] / bucketed["records_rewritten"]
             if bucketed["records_rewritten"] else float("nan"))

    print(f"[efficiency] user={user_id} deleted_rows={bucketed['records_actually_deleted']}")
    print(f"[efficiency] bucketed:   {bucketed}")
    print(f"[efficiency] unbucketed: {unbucketed}")
    print(f"[efficiency] records_rewritten ratio (unbucketed / bucketed) = {ratio:.1f}x")
    return {"user_id": user_id, "bucketed": bucketed, "unbucketed": unbucketed, "ratio": ratio}


def main() -> None:
    spark = build_spark("gdpr-efficiency")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run it**

```bash
docker exec -e PYTHONPATH=/opt/app ad-lakehouse-spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/app/gdpr/efficiency_demo.py 2>&1 | grep -E "\[efficiency\]|Error|Exception" | tail -6
```
Expected: both deletes remove the SAME small number of `deleted` records, but the unbucketed table's `records_rewritten` / `bytes_rewritten` is substantially larger (roughly up to ~16x, since one user hashes to 1 of 16 buckets) — the bucketing payoff. Report the two metric dicts + the ratio. (On this small demo data the absolute numbers are small; the RATIO is the point. If the ratio is ~1x, investigate — the bucketed table may not have created distinct per-bucket files; report it, don't fake it.)

- [ ] **Step 3: Commit**

```bash
.venv/bin/ruff check gdpr/efficiency_demo.py
git add gdpr/efficiency_demo.py
git commit -m "feat(gdpr): efficiency demo — measured bucketed-vs-unbucketed delete rewrite"
```

## IMPORTANT gotchas
- The snapshot `summary` is a `map<string,string>`; keys used: `removed-data-files`, `removed-records`, `removed-files-size`, `deleted-records`. If a key is absent on a given engine version, `_delete_metrics` defaults it to "0" — but verify the bucketed/unbucketed numbers actually differ; if both are identical, the bucket pruning isn't engaging (report it).
- `removed-records` = records in the files that were rewritten (the whole file, not just the deleted rows). That's the metric that exposes the cost: unbucketed rewrites whole date-partition files (~16x more records) to remove the same handful of user rows.
- This touches only `lh.gdpr_demo.*` — production tables are untouched.
- Trino not needed (all measured via Spark snapshot metadata).

## Report Format
- **Status / the bucketed + unbucketed metric dicts / the ratio / ruff + commit SHA / any issues**

---

## Task 3: Merge-on-read deletes — the second technique (integration)

**Files:**
- Create: `gdpr/mor_demo.py`

- [ ] **Step 1: Implement `gdpr/mor_demo.py`**

```python
# gdpr/mor_demo.py
"""Contrast copy-on-write vs merge-on-read deletes.

A MoR-configured table answers a DELETE by writing a small delete file instead of
rewriting data files — the delete is near-instant; reads transparently exclude the
rows; a later compaction reconciles. Demonstrated on a throwaway gdpr_demo table.
"""
from pyspark.sql import SparkSession

from streaming.spark_session import build_spark

MOR = "lh.gdpr_demo.fact_event_mor"


def run(spark: SparkSession) -> dict:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.gdpr_demo")
    spark.sql(
        f"CREATE OR REPLACE TABLE {MOR} USING iceberg "
        f"TBLPROPERTIES ('write.delete.mode'='merge-on-read') "
        f"AS SELECT * FROM lh.silver.fact_event"
    )
    user_id = spark.sql(
        f"SELECT user_id FROM {MOR} GROUP BY user_id ORDER BY count(*) DESC LIMIT 1"
    ).collect()[0]["user_id"]
    data_files_before = spark.sql(f"SELECT count(*) AS c FROM {MOR}.data_files").collect()[0]["c"]

    spark.sql(f"DELETE FROM {MOR} WHERE user_id = '{user_id}'")
    summary = spark.sql(
        f"SELECT summary FROM {MOR}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()[0]["summary"]
    delete_files = spark.sql(f"SELECT count(*) AS c FROM {MOR}.delete_files").collect()[0]["c"]
    data_files_after = spark.sql(f"SELECT count(*) AS c FROM {MOR}.data_files").collect()[0]["c"]
    remaining = spark.sql(f"SELECT count(*) AS c FROM {MOR} WHERE user_id = '{user_id}'").collect()[0]["c"]

    print(f"[mor] user={user_id}")
    print(f"[mor] delete wrote delete_files={delete_files}, "
          f"added-delete-files={summary.get('added-delete-files', '0')}, "
          f"data_files unchanged: {data_files_before} -> {data_files_after}")
    print(f"[mor] rows for user after delete (reads exclude them): {remaining}")

    # Compaction reconciles: rewrites data without the deleted rows, drops delete files.
    spark.sql(f"CALL lh.system.rewrite_data_files(table => 'gdpr_demo.fact_event_mor')")
    delete_files_after_compact = spark.sql(
        f"SELECT count(*) AS c FROM {MOR}.delete_files"
    ).collect()[0]["c"]
    print(f"[mor] after compaction: delete_files={delete_files_after_compact} (reconciled)")
    return {
        "user_id": user_id,
        "delete_files": delete_files,
        "data_files_unchanged": data_files_before == data_files_after,
        "rows_remaining": remaining,
        "delete_files_after_compact": delete_files_after_compact,
    }


def main() -> None:
    spark = build_spark("gdpr-mor")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run it**

```bash
docker exec -e PYTHONPATH=/opt/app ad-lakehouse-spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/app/gdpr/mor_demo.py 2>&1 | grep -E "\[mor\]|Error|Exception" | tail -6
```
Expected: the delete writes `delete_files >= 1` with `data_files` UNCHANGED (no data rewrite — the MoR contrast); `rows for user after delete = 0` (reads exclude them); after compaction `delete_files = 0` (reconciled into rewritten data). Report the printed lines. (If Spark wrote position deletes vs equality deletes, that's expected — Spark SQL DELETE on a MoR table produces position deletes; the delete-file-not-data-rewrite behavior is the point. Note it.)

- [ ] **Step 3: Commit**

```bash
.venv/bin/ruff check gdpr/mor_demo.py
git add gdpr/mor_demo.py
git commit -m "feat(gdpr): merge-on-read delete demo (delete file, no data rewrite, compaction reconciles)"
```

## IMPORTANT gotchas
- `write.delete.mode=merge-on-read` makes `DELETE` write delete files rather than rewriting data. Spark SQL emits POSITION deletes (equality deletes are the Flink/streaming-upsert form); the demonstrated contrast — delete-file write vs data rewrite — is the spec's point. The docstring/print should be accurate about position-vs-equality (note it in the report).
- `.data_files` and `.delete_files` are Iceberg metadata tables; `.snapshots.summary` carries `added-delete-files` / `added-position-deletes`.
- Throwaway `lh.gdpr_demo.*` only.

## Report Format
- **Status / the [mor] lines (delete_files, data_files unchanged, rows after=0, delete_files after compaction=0) / position-vs-equality note / ruff + commit SHA**

---

## Task 4: GDPR integration tests + Makefile (integration)

**Files:**
- Create: `tests/test_gdpr.py`
- Modify: `Makefile`

- [ ] **Step 1: Add Makefile targets** (tab-indented; add to `.PHONY`). Use a shared spark-submit; `forget-user` takes `UID=...`.

```makefile
gdpr-efficiency: ; docker exec -e PYTHONPATH=/opt/app ad-lakehouse-spark /opt/spark/bin/spark-submit \
	--conf spark.jars.ivy=/tmp/.ivy2 \
	--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
	/opt/app/gdpr/efficiency_demo.py
gdpr-mor: ; docker exec -e PYTHONPATH=/opt/app ad-lakehouse-spark /opt/spark/bin/spark-submit \
	--conf spark.jars.ivy=/tmp/.ivy2 \
	--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
	/opt/app/gdpr/mor_demo.py
forget-user: ; docker exec -e PYTHONPATH=/opt/app ad-lakehouse-spark /opt/spark/bin/spark-submit \
	--conf spark.jars.ivy=/tmp/.ivy2 \
	--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
	/opt/app/gdpr/forget_user.py --user-id "$(UID)"
```
Run `make -n gdpr-efficiency` to confirm assembly.

- [ ] **Step 2: Write `tests/test_gdpr.py` (integration-marked)**

```python
# tests/test_gdpr.py
import subprocess

import pytest

PKGS = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,"
    "org.apache.iceberg:iceberg-aws-bundle:1.8.1,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)


def _spark(script: str, *args: str) -> str:
    return subprocess.check_output(
        ["docker", "exec", "-e", "PYTHONPATH=/opt/app", "ad-lakehouse-spark",
         "/opt/spark/bin/spark-submit", "--conf", "spark.jars.ivy=/tmp/.ivy2",
         "--packages", PKGS, f"/opt/app/{script}", *args],
        text=True, stderr=subprocess.STDOUT,
    )


def _trino(sql: str) -> list[str]:
    out = subprocess.check_output(
        ["docker", "compose", "exec", "-T", "trino", "trino", "--execute", sql],
        text=True,
    )
    return out.strip().strip('"').split('","')


@pytest.mark.integration
def test_forget_removes_user_across_layers():
    (uid,) = _trino(
        "SELECT user_id FROM iceberg.gold.fact_impression_delivery "
        "GROUP BY user_id ORDER BY count(*) DESC LIMIT 1"
    )
    _spark("gdpr/forget_user.py", "--user-id", uid)
    bronze, silver, gold = _trino(
        f"SELECT (SELECT count(*) FROM iceberg.bronze.ad_events_raw WHERE user_id='{uid}'), "
        f"(SELECT count(*) FROM iceberg.silver.fact_event WHERE user_id='{uid}'), "
        f"(SELECT count(*) FROM iceberg.gold.fact_impression_delivery WHERE user_id='{uid}')"
    )
    assert (int(bronze), int(silver), int(gold)) == (0, 0, 0)


@pytest.mark.integration
def test_bucketing_makes_delete_cheaper():
    out = _spark("gdpr/efficiency_demo.py")
    # parse the ratio line: "[efficiency] records_rewritten ratio (...) = N.Nx"
    line = [ln for ln in out.splitlines() if "ratio (unbucketed / bucketed)" in ln][-1]
    ratio = float(line.rsplit("=", 1)[1].strip().rstrip("x"))
    assert ratio > 1.5  # bucketed rewrites materially less than unbucketed


@pytest.mark.integration
def test_mor_delete_writes_delete_file_not_data_rewrite():
    out = _spark("gdpr/mor_demo.py")
    assert "rows for user after delete (reads exclude them): 0" in out
    line = [ln for ln in out.splitlines() if "delete wrote delete_files=" in ln][-1]
    assert "data_files unchanged" in line
```

- [ ] **Step 3: Run the GDPR integration tests**

```bash
.venv/bin/pytest tests/test_gdpr.py -m integration -v
```
Expected: all 3 pass (forget erases across layers; efficiency ratio > 1.5; MoR delete writes a delete file with data files unchanged and reads excluding the user). Report results. (These mutate production tables — `test_forget_removes_user_across_layers` permanently forgets a user; that's the correct behavior.)

- [ ] **Step 4: Run full unit suite + lint + commit**

```bash
.venv/bin/pytest -q
.venv/bin/ruff check .
git add tests/test_gdpr.py Makefile
git commit -m "feat(gdpr): integration tests + Makefile targets (forget-user, efficiency, mor)"
```

## Report Format
- **Status / 3 gdpr integration test results / unit-suite count / make -n gdpr-efficiency / ruff + commit SHA**

---

## Task 5: gdpr_delete DAG, docs writeup, README (integration)

**Files:**
- Create: `airflow/dags/gdpr_delete_dag.py`, `docs/gdpr-right-to-be-forgotten.md`
- Modify: `README.md`

- [ ] **Step 1: Create `airflow/dags/gdpr_delete_dag.py`**

```python
# airflow/dags/gdpr_delete_dag.py
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _spark import DEFAULT_ARGS, PACKAGES, SPARK_CONTAINER

# Triggered on demand with a user_id: `airflow dags trigger gdpr_delete -c '{"user_id": "usr-01234"}'`.
# The forget script is parameterized, so we build the command from the run conf.
FORGET_CMD = (
    f"docker exec -e PYTHONPATH=/opt/app {SPARK_CONTAINER} "
    f"/opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages {PACKAGES} "
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
```

- [ ] **Step 2: Verify the DAG parses (it's on-demand; don't trigger a destructive run in the test)**

```bash
docker compose exec -T airflow airflow dags reserialize 2>&1 | tail -1 || true
sleep 5
docker compose exec -T airflow airflow dags list 2>&1 | grep gdpr_delete
docker compose exec -T airflow airflow dags list-import-errors 2>&1 | tail -3
```
Expected: `gdpr_delete` listed, no import errors. (The DAG uses `PACKAGES`/`SPARK_CONTAINER` from `_spark.py` — confirm those are importable. Do NOT trigger it here; it permanently forgets a user. The destructive path is already covered by Task 4's test.) Report the list + import-errors result.

- [ ] **Step 3: Write `docs/gdpr-right-to-be-forgotten.md`**

Write the governance writeup. Include: the requirement (RTBF / erasure, not logical delete); the design (row-level DELETE on PII bases, gold rebuild, snapshot expiry for unrecoverability; bronze-must-be-purged-or-rebuild-resurrects); **the two techniques** — (1) `bucket(16, user_id)` co-partitioning with the MEASURED bucketed-vs-unbucketed numbers from `make gdpr-efficiency` (paste the actual ratio + the two metric dicts you observed), (2) merge-on-read deletes (delete file, no data rewrite, compaction reconciles — paste the `make gdpr-mor` output); the verification (zero rows across layers) and unrecoverability (snapshot expiry); and the on-demand `gdpr_delete` DAG. Keep numbers honest (demo-scale data → small absolutes, the ratio is the point).

- [ ] **Step 4: Update `README.md`**

- Flip the roadmap "4. GDPR right-to-be-forgotten" row to `✅ **done**`.
- Add a **"Data governance (GDPR)"** section: right-to-be-forgotten erases a user across bronze→silver→gold; the `bucket(16, user_id)` layout makes the analytical delete rewrite a fraction of the data (link `docs/gdpr-right-to-be-forgotten.md` with the measured ratio); merge-on-read deletes as the second technique; snapshot expiry makes it unrecoverable; on-demand via the `gdpr_delete` Airflow DAG (`airflow dags trigger gdpr_delete -c '{"user_id":"usr-XXXXX"}'`).
- Quickstart: add `make gdpr-efficiency` and `make gdpr-mor` (the demos) and `make forget-user UID=usr-XXXXX`.
- Repo tour: add `gdpr/`.

- [ ] **Step 5: Run full suite + lint + commit**

```bash
.venv/bin/pytest -q
.venv/bin/ruff check .
git add airflow/dags/gdpr_delete_dag.py docs/gdpr-right-to-be-forgotten.md README.md
git commit -m "feat(gdpr): gdpr_delete DAG, governance writeup, README"
```

## IMPORTANT gotchas
- The DAG references `PACKAGES` and `SPARK_CONTAINER` from `_spark.py` — they're already exported there. Confirm the import line works (`from _spark import DEFAULT_ARGS, PACKAGES, SPARK_CONTAINER`).
- The Jinja `{{ dag_run.conf["user_id"] }}` is templated by Airflow at run time — `airflow dags list`/parse won't evaluate it, so parsing succeeds without a conf. Do NOT actually trigger it in verification (destructive).
- README/docs honesty: GDPR done now; performance before/after is the last planned milestone. Demo-scale numbers — report the real ratio, don't inflate.

## Report Format
- **Status / gdpr_delete parses (no import errors) / docs written with real numbers / unit-suite count / ruff + commit SHA**

---

## Self-review notes

- **Spec coverage (§7):** headline `bucket()` efficient row-level delete (Task 1 forget + Task 2 measured proof); second technique merge-on-read deletes (Task 3); verification of zero rows across layers (Task 1 + Task 4 test); unrecoverability via snapshot expiry (Task 1); the `gdpr_delete` DAG §9 deferred here (Task 5). The bronze-resurrection cross-plan note (from Plan 2) is resolved: forget deletes bronze too.
- **Type/contract consistency:** `forget(spark, user_id)` and the demo `run(spark)` functions; `forget_user.py` reuses `gold_delivery/gold_fill/gold_pacing.build`; the DAG imports `DEFAULT_ARGS, PACKAGES, SPARK_CONTAINER` from `_spark.py` (all already exported); `gdpr*` added to packages.find.
- **Honesty:** demo-scale data means small absolute byte/record counts — the plan measures and reports the RATIO (and flags investigating if it's ~1x rather than faking it). MoR via Spark writes position (not equality) deletes — documented accurately.
- **No placeholders:** every step has full code or exact commands + expected output.
```
