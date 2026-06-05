# Ad-Lakehouse — Plan 3: Gold Data Products — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the three gold data products on top of silver — an impression-delivery fact (with quartile completion folded in), an inventory/fill rollup, and the headline **campaign pacing** product (delivered vs expected over each flight, labelled ahead/on_track/behind).

**Architecture:** Two small data-calibration tweaks first make the headline pacing product meaningful on synthetic data: the generator spreads request timestamps over a multi-day window (so delivery accumulates over a flight, not in one spike), and campaign budgets are scaled to the demo's delivery volume (so pace labels vary instead of everyone reading "behind"). Then three idempotent Spark SQL builds (`CREATE OR REPLACE TABLE … AS`) turn `silver.fact_event` + `silver.dim_campaign` into `gold.fact_impression_delivery`, `gold.inventory_fill`, and `gold.campaign_pacing`, wired into the existing `transform.run` driver.

**Tech Stack:** Python 3.11, pydantic, confluent-kafka; Spark 3.5 + Iceberg 1.8.1 (Spark SQL window functions, `sequence`/`explode`, `bool_or`); Trino; Docker Compose; pytest + ruff.

**Spec:** `docs/superpowers/specs/2026-06-04-ad-lakehouse-design.md` §4.4 (the three gold products). **Builds on:** Plan 1 (streaming spine) + Plan 2 (correlated events, campaign API, silver) — both merged to `main`.

---

## Realized state this plan builds on

- `silver.fact_event` (10 cols: event_id, event_type, event_ts `timestamp`, campaign_id, creative_id, request_id, user_id, device, geo, placement) — deduped, correlated (impressions share `request_id` with an ad_request; quartiles share it with the impression), partitioned `days(event_ts), bucket(16, user_id)`.
- `silver.dim_campaign` (campaign_id, budget `bigint`, flight_start `date`, flight_end `date`, daily_budget `double`, target_geo, target_device).
- `generator/stream.py` `event_batch(n_requests, now, dup_rate, late_rate, seed=0, fill_prob=0.7)`; `generator/produce.py` (`--n` requests, `--dup-rate/--late-rate/--fill-prob`); `api/campaigns.py` `build_campaigns(reference)` with `budget = r.randint(50, 500) * 1000`.
- `transform/run.py` driver: `GROUPS = {"silver": ["dim_campaign", "fact_event"]}`, `BUILDERS` maps names to `build(spark)` functions. `make build-silver` runs it. Spark jobs run with `docker compose exec [-d] -e PYTHONPATH=/opt/app spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 <script>`. Producer runs from host as `.venv/bin/python -m generator.produce`. `make topic` creates `ad_events` (Redpanda won't for a consumer). Trino host port 8081.

## Why calibration (honest note)

These are synthetic-data calibrations, documented as such:
- **Spread:** without it, every event timestamp is ~`now`, so a campaign's whole delivery lands on one day and the pacing curve is a single step. Spreading request times over a window makes cumulative delivery accumulate across the flight.
- **Budget scale:** the demo seeds ~10k requests → ~7k impressions → ~350 impressions/campaign. A realistic 50k–500k-impression budget would make every campaign trivially "behind." Scaling budget to `randint(300, 1500)` (a few hundred impressions, commensurate with demo volume) makes pace labels vary. Real budgets would scale with real traffic — noted in the docstring + README.

## File structure (created/modified)

- `generator/stream.py` *(modify)* — add `spread_days` to `event_batch`
- `generator/produce.py` *(modify)* — add `--spread-days`
- `tests/test_stream.py` *(modify)* — add a spread test
- `api/campaigns.py` *(modify)* — recalibrate `budget`
- `tests/test_campaigns.py` *(modify)* — update the pinned snapshot
- `transform/gold_delivery.py`, `transform/gold_fill.py`, `transform/gold_pacing.py` *(new)* — one `build(spark)` each
- `transform/run.py` *(modify)* — register the gold builders + a `gold` group
- `trino/02_gold_queries.sql` *(new)* — analyst queries over gold
- `tests/test_gold.py` *(new, integration)* — gold invariants
- `Makefile` *(modify)* — `build-gold`, `gold-queries` targets
- `README.md` *(modify)* — roadmap + gold quickstart

---

## Task 1: Spread request timestamps over a window (TDD)

**Files:**
- Modify: `generator/stream.py`
- Modify: `generator/produce.py`
- Modify: `tests/test_stream.py`

- [ ] **Step 1: Add a spread test to `tests/test_stream.py`** (append; keep the existing 4 tests unchanged)

```python
def test_spread_distributes_events_over_multiple_days():
    events = list(event_batch(n_requests=3000, now=NOW, dup_rate=0.0, late_rate=0.0,
                              seed=4, fill_prob=0.5, spread_days=10))
    days = {e.event_ts.date() for e in events}
    assert len(days) >= 5  # events span many distinct days, not one spike

def test_spread_zero_keeps_single_day():
    events = list(event_batch(n_requests=500, now=NOW, dup_rate=0.0, late_rate=0.0,
                              seed=5, fill_prob=0.0, spread_days=0))
    assert {e.event_ts.date() for e in events} == {NOW.date()}
```

- [ ] **Step 2: Run to verify the spread test fails**

Run: `.venv/bin/pytest tests/test_stream.py -v`
Expected: FAIL — `TypeError: event_batch() got an unexpected keyword argument 'spread_days'`.

- [ ] **Step 3: Update `generator/stream.py`** (full file)

```python
# generator/stream.py
from __future__ import annotations

import random
from collections.abc import Iterator
from datetime import datetime, timedelta

from generator.event import AdEvent
from generator.session import request_session


def event_batch(
    n_requests: int,
    now: datetime,
    dup_rate: float,
    late_rate: float,
    seed: int = 0,
    fill_prob: float = 0.7,
    spread_days: float = 0.0,
) -> Iterator[AdEvent]:
    """Yield the events of n_requests correlated ad requests.

    Each request contributes an ad_request plus its causal impression/quartile
    events (sharing request_id) via request_session(). The request's base time is
    drawn uniformly from the last `spread_days` days (0 = all at `now`) so delivery
    accumulates over a flight rather than in a single spike. On top of that, ~late_rate
    of events are backdated a further 2-240 min (late arrival), and ~dup_rate are
    re-emitted as exact duplicates (same event_id). Cleaning both up is silver's job.
    """
    r = random.Random(seed)
    for i in range(n_requests):
        base = now - timedelta(seconds=r.random() * spread_days * 86_400)
        for ev in request_session(seed=seed * 1_000_003 + i, now=base, fill_prob=fill_prob):
            if r.random() < late_rate:
                ev = ev.model_copy(
                    update={"event_ts": ev.event_ts - timedelta(minutes=r.randint(2, 240))}
                )
            yield ev
            if r.random() < dup_rate:
                yield ev  # same event_id -> duplicate
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/pytest tests/test_stream.py -v`
Expected: PASS (6 passed — the 4 existing + 2 new). The existing tests use the default `spread_days=0`, so they are unaffected.

- [ ] **Step 5: Add `--spread-days` to `generator/produce.py`**

Add the argument and thread it into the call. The argparse block gains:
```python
    ap.add_argument("--spread-days", type=float, default=10.0,
                    help="spread request timestamps uniformly over the last N days")
```
and the `event_batch(...)` call becomes:
```python
    for ev in event_batch(args.n, now, args.dup_rate, args.late_rate,
                          args.seed, args.fill_prob, args.spread_days):
```

- [ ] **Step 6: Lint + commit**

```bash
.venv/bin/ruff check generator/stream.py generator/produce.py tests/test_stream.py
git add generator/stream.py generator/produce.py tests/test_stream.py
git commit -m "feat(generator): spread request timestamps over a window for pacing"
```

---

## Task 2: Recalibrate campaign budgets (TDD)

**Files:**
- Modify: `api/campaigns.py`
- Modify: `tests/test_campaigns.py`

- [ ] **Step 1: Update the pinned snapshot in `tests/test_campaigns.py`**

Replace the `test_pinned_values_cmp001` body with the recalibrated expectations (flight_days is unchanged at 17; budget/geo/device shift because the new `randint` range changes the RNG draw stream):

```python
def test_pinned_values_cmp001():
    # Pin a known-seed campaign so a future change to the draw order or RNG
    # (which would silently shift the campaign metadata events join against)
    # fails loudly instead of corrupting downstream pacing.
    c = build_campaigns(REF)[0]
    assert c.campaign_id == "cmp-001"
    assert c.budget == 429
    assert c.target_geo == "GB-LND"
    assert c.target_device == "mobile"
    assert (c.flight_end - c.flight_start).days == 17

def test_budget_is_calibrated_to_demo_volume():
    for c in build_campaigns(REF):
        assert 300 <= c.budget <= 1500
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest tests/test_campaigns.py -v`
Expected: FAIL — `test_pinned_values_cmp001` asserts `budget == 429` but the code still returns `483000`, and `test_budget_is_calibrated_to_demo_volume` fails too.

- [ ] **Step 3: Recalibrate the budget in `api/campaigns.py`**

Change the budget line inside `build_campaigns` and its comment:
```python
        # Budget is in impressions. Scaled to the demo's synthetic delivery volume
        # (~350 impressions/campaign at the default 10k-request seed) so pace labels
        # vary. Real campaigns would scale budget with real traffic.
        budget = r.randint(300, 1500)
```
(Replace the existing `budget = r.randint(50, 500) * 1000` line. Also update the `budget: int` field comment in the `Campaign` model from "a count of impressions" wording if it still says "over the flight" — keep it accurate: "total impression budget over the flight, sized to demo volume".)

- [ ] **Step 4: Run to verify it passes + full suite**

Run: `.venv/bin/pytest tests/test_campaigns.py -v` → PASS
Run: `.venv/bin/pytest -q` → all green
Run: `.venv/bin/ruff check api/campaigns.py tests/test_campaigns.py` → clean

- [ ] **Step 5: Commit**

```bash
git add api/campaigns.py tests/test_campaigns.py
git commit -m "feat(api): scale campaign budgets to demo delivery volume for meaningful pacing"
```

---

## Task 3: Re-stream spread data + rebuild silver (integration)

**Files:** none (operational + rebuild)

- [ ] **Step 1: Rebuild the API image so the new budgets are served**

```bash
docker compose up -d --build api
sleep 4
curl -s http://localhost:8000/campaigns | python3 -c "import sys,json; d=json.load(sys.stdin); print('cmp-001 budget', d[0]['budget'])"
```
Expected: `cmp-001 budget 429`.

- [ ] **Step 2: Reset bronze + topic + checkpoint, then re-stream with spread**

```bash
# stop the running stream
docker compose exec -T spark bash -lc "pkill -f ingest_bronze || true"
# drop bronze (throwaway script; do NOT commit)
cat > scripts/_reset_bronze.py <<'PY'
import sys; sys.path.insert(0, "/opt/app")
from streaming.spark_session import build_spark
s = build_spark("reset"); s.sql("DROP TABLE IF EXISTS lh.bronze.ad_events_raw"); print("DROPPED"); s.stop()
PY
docker compose exec -T -e PYTHONPATH=/opt/app spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/app/scripts/_reset_bronze.py 2>&1 | grep -E "DROPPED|Error" | tail -2
rm -f scripts/_reset_bronze.py
docker compose exec -T redpanda rpk topic delete ad_events || true
docker compose exec -T redpanda rpk topic create ad_events
rm -rf .checkpoints/bronze_ad_events
# restart stream + seed spread data
make stream
sleep 35
set -a && . ./.env && set +a && .venv/bin/python -m generator.produce --n 10000 --spread-days 10
```
Expected: `produced N events from 10000 requests to ad_events`. Wait ~60s for the stream to consume.

- [ ] **Step 3: Rebuild silver + verify multi-day spread**

```bash
make build-silver
docker compose exec -T trino trino --execute "
SELECT count(DISTINCT CAST(event_ts AS DATE)) AS distinct_days,
       min(CAST(event_ts AS DATE)) AS earliest,
       max(CAST(event_ts AS DATE)) AS latest
FROM iceberg.silver.fact_event"
```
Expected: `distinct_days >= 5` (events now span ~10 days), confirming the spread. Report the row. Also re-confirm dedup with `make silver-checks` (events == distinct_events, campaigns == 20).

- [ ] **Step 4: Commit** (nothing to commit — this is operational; confirm `git status` is clean except no throwaway files remain).

Run: `git status --short` → should be empty. If `scripts/_reset_bronze.py` lingers, `rm -f` it.

---

## Task 4: gold.fact_impression_delivery (Spark, integration)

**Files:**
- Create: `transform/gold_delivery.py`

- [ ] **Step 1: Implement `transform/gold_delivery.py`**

```python
# transform/gold_delivery.py
from pyspark.sql import SparkSession


def build(spark: SparkSession) -> None:
    """gold.fact_impression_delivery: one row per impression, with quartile
    completion flags folded in from quartile events sharing the request_id.
    Each filled request has exactly one impression in silver, so request_id is a
    safe grain. CREATE OR REPLACE makes it idempotent.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.gold")
    spark.sql(
        """
        CREATE OR REPLACE TABLE lh.gold.fact_impression_delivery
        USING iceberg
        PARTITIONED BY (days(impression_ts))
        AS
        SELECT
          i.request_id, i.event_ts AS impression_ts, i.campaign_id, i.creative_id,
          i.user_id, i.device, i.geo, i.placement,
          coalesce(bool_or(q.event_type = 'q25'),  false) AS completed_q25,
          coalesce(bool_or(q.event_type = 'q50'),  false) AS completed_q50,
          coalesce(bool_or(q.event_type = 'q75'),  false) AS completed_q75,
          coalesce(bool_or(q.event_type = 'q100'), false) AS completed_q100
        FROM (SELECT * FROM lh.silver.fact_event WHERE event_type = 'impression') i
        LEFT JOIN (
          SELECT request_id, event_type FROM lh.silver.fact_event
          WHERE event_type IN ('q25', 'q50', 'q75', 'q100')
        ) q ON i.request_id = q.request_id
        GROUP BY i.request_id, i.event_ts, i.campaign_id, i.creative_id,
                 i.user_id, i.device, i.geo, i.placement
        """
    )
    n = spark.sql("SELECT count(*) AS c FROM lh.gold.fact_impression_delivery").collect()[0]["c"]
    print(f"[gold_delivery] wrote {n} impression rows")
```

- [ ] **Step 2: Run it standalone in the spark container**

Create a TEMPORARY runner `transform/_run.py` (do NOT commit it):
```python
import sys
from streaming.spark_session import build_spark
from transform import gold_delivery, gold_fill, gold_pacing
MODS = {"gold_delivery": gold_delivery, "gold_fill": gold_fill, "gold_pacing": gold_pacing}
s = build_spark("gold-adhoc")
MODS[sys.argv[1]].build(s)
s.stop()
```
(You'll create gold_fill/gold_pacing in Tasks 5-6; for this task, temporarily import only gold_delivery, or guard imports. Simplest: for Task 4 make the temp runner import only gold_delivery. You will delete it after Task 6.)

Run:
```bash
docker compose exec -T -e PYTHONPATH=/opt/app spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/app/transform/_run.py gold_delivery 2>&1 | grep -E "gold_delivery]|Error|Exception" | tail -5
```
Expected: `[gold_delivery] wrote N impression rows`.

- [ ] **Step 3: Verify via Trino**

```bash
docker compose exec -T trino trino --execute "
SELECT
  (SELECT count(*) FROM iceberg.gold.fact_impression_delivery) AS delivery_rows,
  (SELECT count(*) FROM iceberg.silver.fact_event WHERE event_type='impression') AS silver_impressions,
  (SELECT count(*) FROM iceberg.gold.fact_impression_delivery
     WHERE completed_q25 AND NOT completed_q50 IS NULL) AS sanity"
docker compose exec -T trino trino --execute "
SELECT count(*) AS monotonic_violations FROM iceberg.gold.fact_impression_delivery
WHERE (completed_q50 AND NOT completed_q25)
   OR (completed_q75 AND NOT completed_q50)
   OR (completed_q100 AND NOT completed_q75)"
```
Expected: `delivery_rows == silver_impressions` (one row per impression) and `monotonic_violations == 0` (quartile completion is nested: no q50 without q25, etc.). Report both.

- [ ] **Step 4: Commit**

```bash
git add transform/gold_delivery.py
git commit -m "feat(transform): gold.fact_impression_delivery with quartile completion flags"
```

---

## Task 5: gold.inventory_fill (Spark, integration)

**Files:**
- Create: `transform/gold_fill.py`

- [ ] **Step 1: Implement `transform/gold_fill.py`**

```python
# transform/gold_fill.py
from pyspark.sql import SparkSession


def build(spark: SparkSession) -> None:
    """gold.inventory_fill: per campaign x placement x hour, the request count,
    impression count, and fill_rate = impressions / requests. Computed from the
    ad_request vs impression rows in silver. CREATE OR REPLACE = idempotent.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.gold")
    spark.sql(
        """
        CREATE OR REPLACE TABLE lh.gold.inventory_fill
        USING iceberg
        PARTITIONED BY (days(event_hour))
        AS
        SELECT
          campaign_id, placement, date_trunc('HOUR', event_ts) AS event_hour,
          sum(CASE WHEN event_type = 'ad_request' THEN 1 ELSE 0 END) AS requests,
          sum(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) AS impressions,
          CASE WHEN sum(CASE WHEN event_type = 'ad_request' THEN 1 ELSE 0 END) > 0
               THEN sum(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) * 1.0
                    / sum(CASE WHEN event_type = 'ad_request' THEN 1 ELSE 0 END)
               ELSE NULL END AS fill_rate
        FROM lh.silver.fact_event
        WHERE event_type IN ('ad_request', 'impression')
        GROUP BY campaign_id, placement, date_trunc('HOUR', event_ts)
        """
    )
    n = spark.sql("SELECT count(*) AS c FROM lh.gold.inventory_fill").collect()[0]["c"]
    print(f"[gold_fill] wrote {n} campaign x placement x hour rows")
```

- [ ] **Step 2: Run it standalone** (extend the temp `transform/_run.py` to import gold_fill, then:)

```bash
docker compose exec -T -e PYTHONPATH=/opt/app spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/app/transform/_run.py gold_fill 2>&1 | grep -E "gold_fill]|Error|Exception" | tail -5
```
Expected: `[gold_fill] wrote N campaign x placement x hour rows`.

- [ ] **Step 3: Verify via Trino**

```bash
docker compose exec -T trino trino --execute "
SELECT
  round(sum(impressions) * 1.0 / sum(requests), 3) AS overall_fill_rate,
  min(fill_rate) AS min_fr, max(fill_rate) AS max_fr,
  count(*) AS rows
FROM iceberg.gold.inventory_fill"
```
Expected: `overall_fill_rate` is roughly the generator's `fill_prob` (~0.7, allow 0.6-0.8 given hour-bucketing edge effects); `rows` > 1. Report the row.

- [ ] **Step 4: Commit**

```bash
git add transform/gold_fill.py
git commit -m "feat(transform): gold.inventory_fill (requests/impressions/fill_rate per campaign x placement x hour)"
```

---

## Task 6: gold.campaign_pacing — the headline (Spark, integration)

**Files:**
- Create: `transform/gold_pacing.py`

- [ ] **Step 1: Implement `transform/gold_pacing.py`**

```python
# transform/gold_pacing.py
from pyspark.sql import SparkSession


def build(spark: SparkSession) -> None:
    """gold.campaign_pacing: per campaign x day over its flight (up to the latest
    delivery date), delivered impressions, cumulative delivered, the linearly
    expected pace (budget * elapsed-flight-fraction), pace_index = cumulative /
    expected, and an ahead|on_track|behind label. The dim_campaign join is what
    makes this product exist. CREATE OR REPLACE = idempotent.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.gold")
    spark.sql(
        """
        CREATE OR REPLACE TABLE lh.gold.campaign_pacing
        USING iceberg
        PARTITIONED BY (campaign_id)
        AS
        WITH as_of AS (
          SELECT max(CAST(event_ts AS DATE)) AS as_of_date FROM lh.silver.fact_event
        ),
        daily AS (
          SELECT campaign_id, CAST(event_ts AS DATE) AS pacing_date,
                 count(*) AS delivered_impressions
          FROM lh.silver.fact_event
          WHERE event_type = 'impression'
          GROUP BY campaign_id, CAST(event_ts AS DATE)
        ),
        calendar AS (
          SELECT c.campaign_id, c.budget, c.flight_start, c.flight_end,
                 explode(sequence(c.flight_start, c.flight_end, interval 1 day)) AS pacing_date
          FROM lh.silver.dim_campaign c
        ),
        windowed AS (
          SELECT cal.campaign_id, cal.pacing_date, cal.budget,
                 cal.flight_start, cal.flight_end,
                 coalesce(d.delivered_impressions, 0) AS delivered_impressions
          FROM calendar cal
          CROSS JOIN as_of a
          LEFT JOIN daily d
            ON cal.campaign_id = d.campaign_id AND cal.pacing_date = d.pacing_date
          WHERE cal.pacing_date <= a.as_of_date
        ),
        cum AS (
          SELECT *,
            sum(delivered_impressions) OVER (
              PARTITION BY campaign_id ORDER BY pacing_date
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_delivered,
            datediff(pacing_date, flight_start) + 1 AS days_elapsed,
            datediff(flight_end, flight_start) + 1 AS flight_days
          FROM windowed
        )
        SELECT
          campaign_id, pacing_date, delivered_impressions, cumulative_delivered, budget,
          budget * (days_elapsed / CAST(flight_days AS DOUBLE)) AS expected_pace,
          cumulative_delivered
            / nullif(budget * (days_elapsed / CAST(flight_days AS DOUBLE)), 0) AS pace_index,
          CASE
            WHEN cumulative_delivered
                 / nullif(budget * (days_elapsed / CAST(flight_days AS DOUBLE)), 0) >= 1.05
              THEN 'ahead'
            WHEN cumulative_delivered
                 / nullif(budget * (days_elapsed / CAST(flight_days AS DOUBLE)), 0) <= 0.95
              THEN 'behind'
            ELSE 'on_track'
          END AS pace_label
        FROM cum
        """
    )
    n = spark.sql("SELECT count(*) AS c FROM lh.gold.campaign_pacing").collect()[0]["c"]
    print(f"[gold_pacing] wrote {n} campaign-day rows")
```

- [ ] **Step 2: Run it standalone** (extend temp `transform/_run.py` to import gold_pacing, then:)

```bash
docker compose exec -T -e PYTHONPATH=/opt/app spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/app/transform/_run.py gold_pacing 2>&1 | grep -E "gold_pacing]|Error|Exception" | tail -5
```
Expected: `[gold_pacing] wrote N campaign-day rows` (N ≈ 20 campaigns × several flight days each).

- [ ] **Step 3: Verify via Trino — the money query**

```bash
docker compose exec -T trino trino --execute "
SELECT pace_label, count(*) AS rows, count(DISTINCT campaign_id) AS campaigns
FROM iceberg.gold.campaign_pacing
GROUP BY pace_label ORDER BY 1"
docker compose exec -T trino trino --execute "
SELECT campaign_id, pacing_date, cumulative_delivered, round(expected_pace,1) AS expected,
       round(pace_index,3) AS pace_index, pace_label
FROM iceberg.gold.campaign_pacing
WHERE campaign_id IN ('cmp-001','cmp-003')
ORDER BY campaign_id, pacing_date"
```
Expected: the label breakdown shows a MIX (not all 'behind') — at least two distinct labels appear across campaigns; cumulative_delivered is monotonically non-decreasing per campaign over pacing_date; pace_index is finite. Report the label breakdown and a sample campaign's trajectory. (If everything is one label, the budget calibration or spread didn't take — investigate, don't paper over.)

- [ ] **Step 4: Commit**

```bash
git add transform/gold_pacing.py
git commit -m "feat(transform): gold.campaign_pacing (delivered vs expected, ahead/on_track/behind)"
```

---

## Task 7: Driver wiring, Makefile, gold queries, tests, README (integration)

**Files:**
- Modify: `transform/run.py`
- Create: `trino/02_gold_queries.sql`, `tests/test_gold.py`
- Modify: `Makefile`, `README.md`

- [ ] **Step 1: Register the gold builders in `transform/run.py`** (full file)

```python
# transform/run.py
import sys

from streaming.spark_session import build_spark
from transform import dim_campaign, fact_event, gold_delivery, gold_fill, gold_pacing

BUILDERS = {
    "dim_campaign": dim_campaign.build,
    "fact_event": fact_event.build,
    "gold_delivery": gold_delivery.build,
    "gold_fill": gold_fill.build,
    "gold_pacing": gold_pacing.build,
}
GROUPS = {
    "silver": ["dim_campaign", "fact_event"],
    "gold": ["gold_delivery", "gold_fill", "gold_pacing"],
    "all": ["dim_campaign", "fact_event", "gold_delivery", "gold_fill", "gold_pacing"],
}


def main() -> None:
    target = sys.argv[1] if len(sys.argv) > 1 else "all"
    names = GROUPS.get(target, [target])
    spark = build_spark(f"transform-{target}")
    try:
        for name in names:
            print(f"[transform] building {name}")
            BUILDERS[name](spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Delete the temporary ad-hoc runner**

Run: `rm -f transform/_run.py` (the real driver replaces it). Confirm `git status --short` shows no `_run.py`.

- [ ] **Step 3: Write `trino/02_gold_queries.sql`**

```sql
-- Headline pacing: which campaigns are ahead / behind, latest snapshot per campaign
WITH latest AS (
  SELECT campaign_id, max(pacing_date) AS d FROM iceberg.gold.campaign_pacing GROUP BY campaign_id
)
SELECT p.campaign_id, p.pacing_date, p.cumulative_delivered,
       round(p.expected_pace, 1) AS expected, round(p.pace_index, 3) AS pace_index, p.pace_label
FROM iceberg.gold.campaign_pacing p
JOIN latest l ON p.campaign_id = l.campaign_id AND p.pacing_date = l.d
ORDER BY p.pace_index DESC;

-- Fill rate by placement
SELECT placement, sum(requests) AS requests, sum(impressions) AS impressions,
       round(sum(impressions) * 1.0 / sum(requests), 3) AS fill_rate
FROM iceberg.gold.inventory_fill
GROUP BY placement ORDER BY fill_rate DESC;

-- Completion funnel from the delivery fact
SELECT count(*) AS impressions,
       round(avg(CAST(completed_q25 AS int)), 3)  AS q25_rate,
       round(avg(CAST(completed_q50 AS int)), 3)  AS q50_rate,
       round(avg(CAST(completed_q75 AS int)), 3)  AS q75_rate,
       round(avg(CAST(completed_q100 AS int)), 3) AS q100_rate
FROM iceberg.gold.fact_impression_delivery;
```

- [ ] **Step 4: Add Makefile targets** (tab-indented bodies; add `build-gold`, `gold-queries` to `.PHONY`)

```makefile
build-gold: ; docker compose exec -T -e PYTHONPATH=/opt/app spark /opt/spark/bin/spark-submit \
	--conf spark.jars.ivy=/tmp/.ivy2 \
	--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
	/opt/app/transform/run.py gold
gold-queries: ; docker compose exec -T trino trino --catalog iceberg < trino/02_gold_queries.sql
```
Run `make -n build-gold` to confirm the recipe assembles with the `gold` arg.

- [ ] **Step 5: Build gold via the driver + verify end-to-end**

```bash
make build-gold
make gold-queries
```
Expected: the three gold tables build in one Spark session; `gold-queries` prints the pacing leaderboard (mixed labels), fill-by-placement, and the completion funnel (q25 > q50 > q75 > q100 rates). Report the pacing leaderboard.

- [ ] **Step 6: Write `tests/test_gold.py` (integration-marked)**

```python
# tests/test_gold.py
import subprocess

import pytest


def _trino(sql: str) -> list[str]:
    out = subprocess.check_output(
        ["docker", "compose", "exec", "-T", "trino", "trino", "--execute", sql],
        text=True,
    )
    return out.strip().strip('"').split('","')


@pytest.mark.integration
def test_delivery_one_row_per_impression():
    delivery, impressions = _trino(
        "SELECT (SELECT count(*) FROM iceberg.gold.fact_impression_delivery), "
        "(SELECT count(*) FROM iceberg.silver.fact_event WHERE event_type='impression')"
    )
    assert int(delivery) == int(impressions) and int(delivery) > 0


@pytest.mark.integration
def test_quartile_completion_is_nested():
    (violations,) = _trino(
        "SELECT count(*) FROM iceberg.gold.fact_impression_delivery "
        "WHERE (completed_q50 AND NOT completed_q25) "
        "OR (completed_q75 AND NOT completed_q50) "
        "OR (completed_q100 AND NOT completed_q75)"
    )
    assert int(violations) == 0


@pytest.mark.integration
def test_fill_rate_is_a_fraction():
    (bad,) = _trino(
        "SELECT count(*) FROM iceberg.gold.inventory_fill "
        "WHERE fill_rate IS NOT NULL AND (fill_rate < 0 OR fill_rate > 1.0000001)"
    )
    assert int(bad) == 0


@pytest.mark.integration
def test_pacing_cumulative_is_monotonic():
    # within each campaign, cumulative_delivered must never decrease as pacing_date advances
    (decreases,) = _trino(
        "SELECT count(*) FROM ("
        "  SELECT cumulative_delivered - lag(cumulative_delivered) OVER "
        "  (PARTITION BY campaign_id ORDER BY pacing_date) AS delta "
        "  FROM iceberg.gold.campaign_pacing"
        ") WHERE delta < 0"
    )
    assert int(decreases) == 0


@pytest.mark.integration
def test_pacing_labels_vary():
    # the calibration should produce more than one pace label across campaigns
    labels = _trino(
        "SELECT count(DISTINCT pace_label) FROM iceberg.gold.campaign_pacing"
    )
    assert int(labels[0]) >= 2
```

- [ ] **Step 7: Run unit + integration suites + lint**

```bash
.venv/bin/pytest -q                 # unit: all green
.venv/bin/pytest -m integration -q  # bronze + silver + gold
.venv/bin/ruff check .
```
Report counts. All pass; ruff clean.

- [ ] **Step 8: Update `README.md`**

- Flip the roadmap "2b. Gold data products" row to `✅ **done**`.
- In "what runs today", extend the diagram/prose to mention the gold layer: `gold.fact_impression_delivery`, `gold.inventory_fill`, `gold.campaign_pacing`.
- Add to the quickstart after `make build-silver`/`make silver-checks`:
```
make build-gold    # gold delivery + inventory fill + campaign pacing
make gold-queries  # pacing leaderboard, fill by placement, completion funnel
```
- Add a one-paragraph "Campaign pacing" highlight describing the headline product (delivered vs expected over each flight, ahead/on_track/behind), and note that budgets + event spread are calibrated to the demo's synthetic volume.
- Add `transform/gold_*.py` and the `gold.*` tables to the repo tour.

- [ ] **Step 9: Commit**

```bash
git add transform/run.py trino/02_gold_queries.sql tests/test_gold.py Makefile README.md
git commit -m "feat(transform): gold build group, queries, integration tests, README"
```

---

## Self-review notes

- **Spec coverage (§4.4):** `gold.fact_impression_delivery` with q25–q100 folded in (Task 4); `gold.inventory_fill` per campaign×placement×hour with fill_rate via request/impression rows (Task 5); `gold.campaign_pacing` per campaign×day with delivered/cumulative/budget/expected_pace/pace_index/label, depending on the dim_campaign join (Task 6). The calibrations (Tasks 1-3) exist to make §4.4's pacing product demonstrable.
- **Type consistency:** `event_batch(..., spread_days=0.0)` defined in Task 1, called with `args.spread_days` in produce.py (Task 1). `build(spark)` signature for all three gold modules matches the existing dim_campaign/fact_event contract and is registered in `run.py` BUILDERS (Task 7). Pacing columns referenced in `trino/02_gold_queries.sql` and `tests/test_gold.py` (campaign_id, pacing_date, cumulative_delivered, expected_pace, pace_index, pace_label) match the SELECT in Task 6. Delivery columns (completed_q25..q100, request_id, impression_ts) match between Task 4 and the tests.
- **Idempotency:** every gold build is `CREATE OR REPLACE TABLE … AS`; re-running `make build-gold` is safe. Same Plan-4 caveat as silver applies (rebuild from silver/bronze), already documented.
- **Calibration honesty:** budgets `randint(300,1500)` and `--spread-days 10` are sized to the default 10k-request seed; documented in docstrings + README so a reviewer understands the numbers are demo-scaled, not real ad budgets.
- **No placeholders:** every step has full code or an exact command + expected output.
```
