# Ad-Lakehouse — Plan 6: Performance Before/After — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a deliberately-bad table layout and a tuned one over the *same* data, run a fixed set of representative queries against both, and write a before/after table with real measured numbers (wall-clock, data scanned, file counts) to `docs/`. This is the most-emphasized line on the target JD ("expert at building performant data pipelines and optimizing existing workflows") and the project's signature result.

**Architecture:** `perf/build_tables.py` amplifies `silver.fact_event` ~20x (so timings are meaningful, not sub-millisecond noise) into a shared source, then writes two Iceberg tables from it: `perf.events_bad` (unpartitioned, ~1000 tiny files, never compacted — the small-files / full-scan problem) and `perf.events_optimized` (hidden-partitioned by `days(event_ts), bucket(16, user_id)`, then compacted and sorted via `rewrite_data_files`). `perf/benchmark.py` runs three representative queries against each table through Trino, captures median wall-clock and bytes scanned, reads each table's file-count and average file size from Iceberg metadata, and writes `docs/performance.md` with the measured comparison. A small integration test asserts the robust invariants (optimized has fewer/larger files; both tables hold the same rows; the pruned query scans less).

**Tech Stack:** Spark 3.5 + Iceberg 1.8.1 (`rewrite_data_files` sort compaction, `.files` metadata); Trino (the analytical engine + `EXPLAIN ANALYZE`); Python timing; pytest + ruff.

**Spec:** `docs/superpowers/specs/2026-06-04-ad-lakehouse-design.md` §8 (deliberately-bad vs optimized; measure pacing rollup / user-filtered scan / date-range query; capture wall-clock + files + bytes scanned; document the delta with numbers). **Builds on:** Plans 1–5 (merged). This is the final roadmap milestone.

## Realized state this plan builds on

- `silver.fact_event` (~31k rows, 11 days, ~4,290 users), columns: event_id, event_type, event_ts (timestamp), campaign_id, creative_id, request_id, user_id, device, geo, placement. Partitioned `days(event_ts), bucket(16, user_id)`.
- `streaming/spark_session.py` → `build_spark` (catalog `lh`). Spark runs via `docker exec -e PYTHONPATH=/opt/app ad-lakehouse-spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --conf spark.sql.shuffle.partitions=8 --packages <iceberg 1.8.1 + kafka> <script>`. Trino host catalog `iceberg`; host port 8081 (queries go through `docker compose exec -T trino trino`).
- **Memory:** the host is tight (Trino/Redpanda get OOM-killed). Keep `--conf spark.sql.shuffle.partitions=8` on every Spark job. If Trino dies (exit 137), `docker compose up -d trino`, wait, retry. Redpanda is not needed.

## Honesty note

This is a laptop-scale benchmark on ~625k synthetic rows. The absolute times are small. The *ratios* (data scanned, wall-clock) and the file-layout differences are the point, exactly as a real optimization review would present them, and the doc must say so. Report whatever the numbers actually are — if a query doesn't speed up, say why, don't massage it.

## File structure

- `perf/__init__.py`, `perf/build_tables.py` *(new)* — amplify + build the bad/optimized tables
- `perf/benchmark.py` *(new)* — run the queries, measure, write `docs/performance.md`
- `docs/performance.md` *(new, written by the benchmark)* — the before/after results
- `tests/test_perf.py` *(new, integration)* — layout + correctness invariants
- `pyproject.toml` *(modify)* — add `perf*` to packages.find include
- `Makefile` *(modify)* — `perf-build`, `perf-bench` targets
- `README.md` *(modify)* — roadmap flip + a performance section

---

## Task 1: Build the bad and optimized tables (integration)

**Files:**
- Create: `perf/__init__.py` (empty), `perf/build_tables.py`
- Modify: `pyproject.toml`

- [ ] **Step 1: Add `perf*` to packages.find in `pyproject.toml`**

Change the include line to:
```toml
include = ["generator*", "api*", "transform*", "streaming*", "gdpr*", "perf*"]
```

- [ ] **Step 2: Implement `perf/build_tables.py`**

```python
# perf/build_tables.py
"""Build two Iceberg tables over the SAME amplified data:

  perf.events_bad        — unpartitioned, ~1000 tiny files, never compacted
                           (the small-files / full-scan problem).
  perf.events_optimized  — hidden-partitioned by days(event_ts), bucket(16, user_id),
                           then compacted + sorted via rewrite_data_files.

silver.fact_event is only ~31k rows, so it is amplified ~20x first (event_id made
unique per copy, every other column kept) so query times are measurable rather than
noise. Both tables are written from one shared source table, so they hold identical rows.
"""
from pyspark.sql import SparkSession

from streaming.spark_session import build_spark

SOURCE = "lh.perf.events_source"
BAD = "lh.perf.events_bad"
OPTIMIZED = "lh.perf.events_optimized"
AMPLIFY = 20      # ~31k rows -> ~625k
BAD_FILES = 1000  # force many tiny files


def _layout(spark: SparkSession, table: str) -> dict:
    rows = spark.sql(f"SELECT count(*) AS c FROM {table}").collect()[0]["c"]
    f = spark.sql(
        f"SELECT count(*) AS files, sum(file_size_in_bytes) AS bytes, "
        f"avg(file_size_in_bytes) AS avg_bytes FROM {table}.files"
    ).collect()[0]
    return {"rows": rows, "files": f["files"], "total_bytes": int(f["bytes"] or 0),
            "avg_file_bytes": int(f["avg_bytes"] or 0)}


def build(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.perf")
    # Amplify: AMPLIFY distinct copies of every event (unique event_id, same dims/ts).
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {SOURCE} USING iceberg AS
        SELECT concat(s.event_id, '-', r.rep) AS event_id,
               s.event_type, s.event_ts, s.campaign_id, s.creative_id, s.request_id,
               s.user_id, s.device, s.geo, s.placement
        FROM lh.silver.fact_event s
        CROSS JOIN (SELECT explode(sequence(0, {AMPLIFY - 1})) AS rep) r
        """
    )

    # BAD: unpartitioned; force many tiny files via repartition; never compacted.
    (spark.table(SOURCE).repartition(BAD_FILES)
        .writeTo(BAD).using("iceberg").createOrReplace())

    # OPTIMIZED: hidden partitioning, then sort-compaction.
    spark.sql(
        f"CREATE OR REPLACE TABLE {OPTIMIZED} USING iceberg "
        f"PARTITIONED BY (days(event_ts), bucket(16, user_id)) "
        f"AS SELECT * FROM {SOURCE}"
    )
    spark.sql(
        "CALL lh.system.rewrite_data_files("
        "table => 'perf.events_optimized', strategy => 'sort', "
        "sort_order => 'campaign_id, event_ts')"
    )

    bad, opt = _layout(spark, BAD), _layout(spark, OPTIMIZED)
    print(f"[build] events_bad:       {bad}")
    print(f"[build] events_optimized: {opt}")
    assert bad["rows"] == opt["rows"], "bad/optimized row counts differ — not the same data"


def main() -> None:
    spark = build_spark("perf-build")
    try:
        build(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Run it**

```bash
docker exec -e PYTHONPATH=/opt/app ad-lakehouse-spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 --conf spark.sql.shuffle.partitions=8 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/app/perf/build_tables.py 2>&1 | grep -E "\[build\]|Error|Exception|AnalysisException" | tail -5
```
Expected: two `[build]` lines. `events_bad` has ~1000 files with a small `avg_file_bytes`; `events_optimized` has far fewer files with a much larger `avg_file_bytes` (sort-compaction merged them), and the SAME `rows` (~625k). Report both dicts. (If `rewrite_data_files` with `strategy => 'sort'` errors on the arg form, capture the exact AnalysisException — the 1.8.1 signature is `rewrite_data_files(table => '...', strategy => 'sort', sort_order => 'col1, col2')`; adjust minimally and report.)

- [ ] **Step 4: Lint + commit**

```bash
touch perf/__init__.py
.venv/bin/ruff check perf/build_tables.py
git add perf/__init__.py perf/build_tables.py pyproject.toml
git commit -m "feat(perf): build deliberately-bad vs optimized Iceberg tables over the same amplified data"
```

## IMPORTANT gotchas
- `repartition(1000)` forces 1000 output files (tiny). That is the small-files problem on purpose.
- `rewrite_data_files(strategy => 'sort', ...)` compacts AND clusters by the sort order — fewer, larger, sorted files within each partition. The optimized table will still have one-or-more files *per partition* (11 days x 16 buckets), but each is far larger than a bad-table file, and partition pruning (Task 2) reads only the relevant ones.
- Same-data guarantee: both tables are CTAS/written from `perf.events_source`, so row counts must match (the assert checks it).
- Keep `--conf spark.sql.shuffle.partitions=8` (memory). If the amplify/compaction OOMs (exit 137), lower `AMPLIFY` to 10 and report.
- Touches only `lh.perf.*`.

## Report Format
- **Status / the two `[build]` dicts (files, avg_file_bytes, rows) / any rewrite signature change / ruff + commit SHA**

---

## Task 2: Benchmark + write the results (integration)

**Files:**
- Create: `perf/benchmark.py`

- [ ] **Step 1: Implement `perf/benchmark.py`**

```python
# perf/benchmark.py
"""Run three representative queries against perf.events_bad and perf.events_optimized
through Trino, capture median wall-clock and bytes scanned, read each table's file
layout from Iceberg metadata, and write docs/performance.md with the comparison.

Run on the host (it shells out to the trino container). The optimized table wins by
partition pruning (user/date queries read only matching files) and sort-compaction
(the rollup reads fewer, larger, ordered files).
"""
import re
import statistics
import subprocess
import time
from pathlib import Path

TABLES = {"bad": "iceberg.perf.events_bad", "optimized": "iceberg.perf.events_optimized"}
RUNS = 5


def _trino(sql: str) -> str:
    return subprocess.check_output(
        ["docker", "compose", "exec", "-T", "trino", "trino", "--execute", sql],
        text=True, stderr=subprocess.STDOUT,
    )


def _median_wall_ms(sql: str) -> float:
    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        _trino(sql)
        times.append((time.perf_counter() - t0) * 1000.0)
    return round(statistics.median(times), 1)


def _bytes_scanned(sql: str) -> int:
    """Best-effort: parse the largest physical-input byte figure from EXPLAIN ANALYZE.
    Trino prints scan stats like 'Input: 625000 rows (12.3MB)' / 'Physical input: 8.1MB'.
    If the format differs on this Trino version, returns -1 (wall-clock still stands)."""
    out = _trino(f"EXPLAIN ANALYZE {sql}")
    sizes = []
    for m in re.finditer(r"(?:Physical input|Input):[^\n]*?([\d.]+)\s*([kKmMgG]?)B", out):
        val, unit = float(m.group(1)), m.group(2).lower()
        mult = {"": 1, "k": 1e3, "m": 1e6, "g": 1e9}[unit]
        sizes.append(int(val * mult))
    return max(sizes) if sizes else -1


def _layout(table: str) -> dict:
    out = _trino(
        f'SELECT count(*), sum(file_size_in_bytes), avg(file_size_in_bytes) '
        f'FROM {table}.files'
    ).strip().strip('"').split('","')
    files, total, avg = (out + ["0", "0", "0"])[:3]
    return {"files": int(files), "total_bytes": int(float(total)),
            "avg_file_bytes": int(float(avg))}


def queries(table: str) -> dict:
    uid = _trino(
        f"SELECT user_id FROM {table} GROUP BY user_id ORDER BY count(*) DESC LIMIT 1"
    ).strip().strip('"')
    day = _trino(
        f"SELECT CAST(min(event_ts) AS DATE) FROM {table}"
    ).strip().strip('"')
    return {
        "user-filtered scan": f"SELECT count(*) FROM {table} WHERE user_id = '{uid}'",
        "date-range scan": (
            f"SELECT count(*) FROM {table} "
            f"WHERE event_ts >= TIMESTAMP '{day} 00:00:00' "
            f"AND event_ts < TIMESTAMP '{day} 00:00:00' + INTERVAL '2' DAY"
        ),
        "campaign rollup": (
            f"SELECT campaign_id, count(*) FROM {table} "
            f"WHERE event_type = 'impression' GROUP BY campaign_id"
        ),
    }


def run() -> dict:
    results = {}
    layouts = {name: _layout(t) for name, t in TABLES.items()}
    for qname in queries(TABLES["bad"]):
        results[qname] = {}
        for name, table in TABLES.items():
            sql = queries(table)[qname]
            results[qname][name] = {
                "wall_ms": _median_wall_ms(sql),
                "bytes_scanned": _bytes_scanned(sql),
            }
            print(f"[bench] {qname:20s} {name:10s} "
                  f"{results[qname][name]['wall_ms']:8.1f} ms  "
                  f"scanned={results[qname][name]['bytes_scanned']}")
    _write_report(layouts, results)
    return {"layouts": layouts, "results": results}


def _mb(n: int) -> str:
    return f"{n/1e6:.2f} MB" if n >= 0 else "n/a"


def _write_report(layouts: dict, results: dict) -> None:
    lines = ["# Performance: deliberately-bad vs optimized layout", ""]
    lines += [
        "Same ~625k-row dataset, two Iceberg layouts. `events_bad` is unpartitioned with",
        "many tiny files; `events_optimized` is hidden-partitioned by `days(event_ts)` and",
        "`bucket(16, user_id)`, then sort-compacted. Laptop-scale data, so the absolute",
        "times are small — the ratios and the layout difference are the point.", "",
        "## Table layout", "",
        "| table | files | avg file size | total |",
        "|---|--:|--:|--:|",
    ]
    for name in ("bad", "optimized"):
        lo = layouts[name]
        lines.append(f"| events_{name} | {lo['files']} | {_mb(lo['avg_file_bytes'])} "
                     f"| {_mb(lo['total_bytes'])} |")
    lines += ["", "## Query before/after", "",
              "| query | bad (ms) | optimized (ms) | speedup | bad scanned | optimized scanned |",
              "|---|--:|--:|--:|--:|--:|"]
    for qname, r in results.items():
        b, o = r["bad"], r["optimized"]
        speed = f"{b['wall_ms']/o['wall_ms']:.1f}x" if o["wall_ms"] else "n/a"
        lines.append(f"| {qname} | {b['wall_ms']} | {o['wall_ms']} | {speed} "
                     f"| {_mb(b['bytes_scanned'])} | {_mb(o['bytes_scanned'])} |")
    lines += ["", "Wall-clock is the median of 5 runs through the Trino CLI (the fixed",
              "docker-exec overhead is the same for both tables, so the comparison holds).",
              "Bytes scanned is the physical input reported by `EXPLAIN ANALYZE`; the",
              "optimized table reads less because partition pruning skips non-matching files.",
              ""]
    Path("docs/performance.md").write_text("\n".join(lines) + "\n")
    print("[bench] wrote docs/performance.md")


def main() -> None:
    run()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run it (from the host venv)**

```bash
.venv/bin/python -m perf.benchmark 2>&1 | grep -E "\[bench\]" | tail -12
```
Expected: a `[bench]` line per query×table, then `wrote docs/performance.md`. The optimized table should show lower (or equal) wall-clock and clearly smaller `bytes_scanned` on the user/date queries (pruning). **Verify the bytes parse worked** — if `bytes_scanned` is `-1` for everything, the `EXPLAIN ANALYZE` byte format differs on this Trino version: capture one raw `EXPLAIN ANALYZE` output (`docker compose exec -T trino trino --execute "EXPLAIN ANALYZE SELECT count(*) FROM iceberg.perf.events_bad WHERE user_id='usr-00001'"`), find the actual scan-bytes line, and fix the `_bytes_scanned` regex to match it (same lesson as the GDPR snapshot-key fixes). Report the real numbers you observed.

- [ ] **Step 3: Inspect `docs/performance.md`**

```bash
cat docs/performance.md
```
Confirm it has the layout table (bad: many tiny files; optimized: fewer, larger) and the query table with real speedups + scanned bytes. Report the rendered table.

- [ ] **Step 4: Lint + commit**

```bash
.venv/bin/ruff check perf/benchmark.py
git add perf/benchmark.py docs/performance.md
git commit -m "feat(perf): benchmark harness + measured before/after report"
```

## IMPORTANT gotchas
- The benchmark runs on the HOST and shells into the trino container. Trino may OOM (exit 137) under the query load; if `_trino` raises `CalledProcessError` mid-run, `docker compose up -d trino`, wait ~20s, and re-run the benchmark.
- `EXPLAIN ANALYZE` byte-stat format varies by Trino version — VERIFY the regex against live output (do not trust the parse blindly; this is the §key-format lesson from Plans 2/5). Wall-clock and file-layout are robust regardless.
- 5 runs × 3 queries × 2 tables = 30 Trino queries + 6 EXPLAIN ANALYZEs. A couple of minutes. Be patient.
- If the optimized wall-clock isn't lower on the rollup (it scans most data anyway), that's expected and honest — its win there is fewer/larger files, and the pruning wins show on the user/date queries. Report the truth.

## Report Format
- **Status / the per-query [bench] numbers (wall-clock + scanned) / any EXPLAIN-ANALYZE regex fix / the rendered docs/performance.md table / ruff + commit SHA**

---

## Task 3: Makefile, invariant test, README (integration)

**Files:**
- Create: `tests/test_perf.py`
- Modify: `Makefile`, `README.md`

- [ ] **Step 1: Add Makefile targets** (tab-indented; add `perf-build`, `perf-bench` to `.PHONY`)

```makefile
perf-build: ; docker exec -e PYTHONPATH=/opt/app ad-lakehouse-spark /opt/spark/bin/spark-submit \
	--conf spark.jars.ivy=/tmp/.ivy2 --conf spark.sql.shuffle.partitions=8 \
	--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
	/opt/app/perf/build_tables.py
perf-bench: ; .venv/bin/python -m perf.benchmark
```
Run `make -n perf-build` to confirm it assembles.

- [ ] **Step 2: Write `tests/test_perf.py` (integration-marked)**

```python
# tests/test_perf.py
import subprocess

import pytest


def _trino(sql: str) -> list[str]:
    out = subprocess.check_output(
        ["docker", "compose", "exec", "-T", "trino", "trino", "--execute", sql],
        text=True,
    )
    return out.strip().strip('"').split('","')


@pytest.mark.integration
def test_same_data_both_tables():
    bad, opt = _trino(
        "SELECT (SELECT count(*) FROM iceberg.perf.events_bad), "
        "(SELECT count(*) FROM iceberg.perf.events_optimized)"
    )
    assert int(bad) == int(opt) and int(bad) > 0


@pytest.mark.integration
def test_optimized_has_fewer_larger_files():
    bad_files, bad_avg = _trino(
        "SELECT count(*), avg(file_size_in_bytes) FROM iceberg.perf.\"events_bad$files\""
    )
    opt_files, opt_avg = _trino(
        "SELECT count(*), avg(file_size_in_bytes) FROM iceberg.perf.\"events_optimized$files\""
    )
    assert int(opt_files) < int(bad_files)            # compaction reduced file count
    assert float(opt_avg) > float(bad_avg)            # and produced larger files


@pytest.mark.integration
def test_pruning_scans_less_on_a_user_query():
    # the optimized table prunes a user_id= predicate to one bucket; the bad table
    # (unpartitioned) cannot prune, so it reads strictly more files for the same query.
    (uid,) = _trino(
        "SELECT user_id FROM iceberg.perf.events_optimized GROUP BY user_id "
        "ORDER BY count(*) DESC LIMIT 1"
    )
    # count distinct data files each table would read for this user (Iceberg metadata):
    # bad has no user partitioning -> all files; optimized -> only the user's bucket files.
    (bad_files,) = _trino("SELECT count(*) FROM iceberg.perf.\"events_bad$files\"")
    (opt_user_files,) = _trino(
        "SELECT count(DISTINCT f.file_path) FROM iceberg.perf.events_optimized o "
        f"JOIN iceberg.perf.\"events_optimized$files\" f ON true WHERE o.user_id='{uid}' LIMIT 1"
    ) if False else ("0",)  # placeholder; see note
    assert int(bad_files) > 0
```

NOTE on `test_pruning_scans_less_on_a_user_query`: the file-level pruning count is awkward to express in pure SQL. REPLACE the placeholder body with the robust check actually available: assert that `EXPLAIN ANALYZE` (or the benchmark's recorded `bytes_scanned`) shows the optimized user-query scans fewer bytes than the bad one. Simplest implementation: shell `EXPLAIN ANALYZE SELECT count(*) FROM <table> WHERE user_id='<uid>'` for both tables and assert the optimized physical-input bytes < bad. If the byte parse is unavailable on this Trino version, fall back to asserting `events_optimized` file count < `events_bad` file count (already covered by the previous test) and DELETE this test rather than ship a fake one. Use your judgment; do not assert something you can't measure.

- [ ] **Step 3: Run unit + integration + lint**

```bash
.venv/bin/pytest -q
.venv/bin/pytest tests/test_perf.py -m integration -v
.venv/bin/ruff check .
```
Expected: unit green; the perf integration tests pass (same-data; optimized fewer/larger files; and the pruning check if you kept it). Report counts. (Restart Trino if it OOMs.)

- [ ] **Step 4: Update `README.md`**

- Flip the roadmap "5. Performance before/after" row to `✅ **done**` (the roadmap is now complete — note that).
- Add a **"Performance"** section: a deliberately-bad layout (unpartitioned, ~1000 tiny files) vs an optimized one (hidden partitioning + sort-compaction); link `docs/performance.md` and quote the headline result you actually measured (e.g., the user/date query speedup and the bytes-scanned reduction). Note `make perf-build` then `make perf-bench` to reproduce.
- Repo tour: add `perf/`.

- [ ] **Step 5: Lint + commit**

```bash
.venv/bin/pytest -q
.venv/bin/ruff check .
git add tests/test_perf.py Makefile README.md
git commit -m "feat(perf): invariant tests, Makefile targets, README performance section"
```

## Report Format
- **Status / make -n perf-build / perf integration test results / unit-suite count / ruff + commit SHA / any deviations**

---

## Self-review notes

- **Spec coverage (§8):** deliberately-bad table (unpartitioned, tiny files — Task 1) vs optimized (hidden partitioning + compaction + sort — Task 1); the three representative queries (user-filtered scan, date-range, campaign rollup — Task 2); measured wall-clock + bytes scanned + file layout written to `docs/` (Task 2); invariants tested (Task 3). The pacing/35→7 narrative this reproduces is the JD's headline line.
- **Type/contract consistency:** `build(spark)` and `run()` mirror the other modules; `perf*` added to packages.find; tables `lh.perf.events_bad` / `events_optimized` referenced consistently across build, benchmark, and tests; Trino reads them as `iceberg.perf.*`.
- **Honesty built in:** the data is amplified only to make timings measurable; the doc states the scale; the benchmark reports real numbers and the plan tells the implementer NOT to fake a speedup or a metric whose format they couldn't verify (the recurring snapshot-key lesson).
- **Memory:** every Spark job carries `shuffle.partitions=8`; Trino-OOM recovery is documented; only `lh.perf.*` is touched.
- **No placeholders:** every code step is complete except the one clearly-flagged pruning-test body, which has explicit instructions to implement-or-delete based on what's measurable (never ship a fake assertion).
```
