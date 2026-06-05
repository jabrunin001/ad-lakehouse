# Ad-Lakehouse — Plan 2: Realistic Events + Campaign API + Silver — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enrich the generator to emit causally-correlated ad-request sessions, source campaign metadata from a FastAPI service, and build the Iceberg **silver** layer — a deduped, late-data-correct `fact_event` (partitioned for GDPR efficiency) plus a `dim_campaign` from the API.

**Architecture:** The Plan 1 generator emitted independent random events; Plan 2 replaces that generation path with `request_session()` — each ad request yields an `ad_request`, an optional `impression` (same `request_id`), and nested quartile completions, so the `request_id` linkage the gold layer needs actually exists. A FastAPI `api` service serves deterministic campaign metadata (budget, flight dates, targeting). Spark SQL transforms (orchestrated by a `transform.run` driver, Airflow comes in a later plan) build `silver.dim_campaign` (from the API) and `silver.fact_event` (bronze → deduped on `event_id`, partitioned by `days(event_ts), bucket(16, user_id)`).

**Tech Stack:** Python 3.11, pydantic, FastAPI + uvicorn, confluent-kafka; Spark 3.5 + Iceberg 1.8.1 (Spark SQL); Trino; Redpanda, MinIO, Iceberg REST; Docker Compose; pytest + ruff.

**Spec:** `docs/superpowers/specs/2026-06-04-ad-lakehouse-design.md` (§4.3 silver, §6 transforms, §4.4 references the campaign join). **Builds on:** Plan 1 (`docs/superpowers/plans/2026-06-04-ad-lakehouse-streaming-spine.md`), already merged.

---

## Realized state this plan builds on

- `bronze.ad_events_raw` exists (12 cols: 10 event fields + `kafka_ts`, `ingest_ts`), partitioned `days(ingest_ts)`, append-only. Currently holds Plan 1's *independent* events — Task 3 resets it with correlated data.
- `streaming/spark_session.py` exports `build_spark(app_name)` wiring the Iceberg REST catalog `lh` + MinIO. Reuse it everywhere.
- `generator/event.py` defines `AdEvent` (10 fields) + `make_event(seed, now)` + vocab constants (`EVENT_TYPES`, `DEVICES`, `GEOS`, `PLACEMENTS`). Campaign ids are `cmp-001..cmp-020`.
- Stack runs via `make up`; Spark jobs run with `docker compose exec -d -e PYTHONPATH=/opt/app spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages <iceberg 1.8.1 + kafka> <script>`. Producer runs from host as `.venv/bin/python -m ...`. Trino host port is **8081**.

## Cross-plan note (for Plan 4 / GDPR)

`silver.fact_event` is rebuilt from bronze each run (`CREATE OR REPLACE TABLE … AS`). That means a GDPR delete applied only to silver would be **resurrected** on the next rebuild from bronze. Plan 4 must therefore purge bronze (or maintain a suppression list the silver build filters against). This is intentionally deferred — noted here so it isn't forgotten.

## File structure (created/modified in this plan)

- `generator/session.py` *(new)* — `request_session()`: one correlated request's events
- `generator/stream.py` *(modify)* — `event_batch()` becomes session-based
- `generator/produce.py` *(modify)* — `--n` now means request count; pass `fill_prob`
- `tests/test_session.py` *(new)*, `tests/test_stream.py` *(modify)*
- `api/__init__.py`, `api/campaigns.py` *(new)* — `Campaign` model + `build_campaigns(reference)`
- `api/main.py` *(new)* — FastAPI app: `GET /campaigns`, `GET /health`
- `docker/api/Dockerfile` *(new)*, `docker-compose.yml` *(modify — add `api` service)*
- `tests/test_campaigns.py`, `tests/test_api.py` *(new)*
- `transform/__init__.py`, `transform/dim_campaign.py`, `transform/fact_event.py`, `transform/run.py` *(new)*
- `trino/01_silver_checks.sql` *(new)*, `tests/test_silver.py` *(new, integration)*
- `Makefile` *(modify — add `seed`, `build-silver` adjustments)*, `pyproject.toml` *(modify — add api deps)*, `README.md` *(modify — roadmap)*

---

## Task 1: Correlated request session generator (TDD)

**Files:**
- Create: `generator/session.py`
- Test: `tests/test_session.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_session.py
from datetime import datetime, timezone
from generator.session import request_session, QUARTILES

NOW = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)

def test_always_starts_with_one_ad_request():
    evs = request_session(seed=1, now=NOW, fill_prob=0.0)
    assert [e.event_type for e in evs] == ["ad_request"]

def test_all_events_share_request_and_dims():
    evs = request_session(seed=3, now=NOW, fill_prob=1.0, quartile_probs=(1, 1, 1, 1))
    rid = {e.request_id for e in evs}
    cid = {e.campaign_id for e in evs}
    uid = {e.user_id for e in evs}
    assert len(rid) == 1 and len(cid) == 1 and len(uid) == 1

def test_full_fill_yields_request_impression_and_four_quartiles():
    evs = request_session(seed=3, now=NOW, fill_prob=1.0, quartile_probs=(1, 1, 1, 1))
    assert [e.event_type for e in evs] == ["ad_request", "impression", *QUARTILES]

def test_quartiles_are_nested_no_gap():
    # with q25 prob 1 but q50 prob 0, we get q25 and then stop
    evs = request_session(seed=5, now=NOW, fill_prob=1.0, quartile_probs=(1, 0, 1, 1))
    assert [e.event_type for e in evs] == ["ad_request", "impression", "q25"]

def test_quartiles_only_after_impression():
    evs = request_session(seed=7, now=NOW, fill_prob=0.0, quartile_probs=(1, 1, 1, 1))
    assert all(e.event_type == "ad_request" for e in evs)

def test_event_ids_unique_within_session():
    evs = request_session(seed=9, now=NOW, fill_prob=1.0, quartile_probs=(1, 1, 1, 1))
    assert len({e.event_id for e in evs}) == len(evs)

def test_deterministic():
    a = request_session(seed=11, now=NOW)
    b = request_session(seed=11, now=NOW)
    assert [e.model_dump() for e in a] == [e.model_dump() for e in b]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_session.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'generator.session'`

- [ ] **Step 3: Write minimal implementation**

```python
# generator/session.py
from __future__ import annotations

import random
from datetime import datetime, timedelta

from generator.event import AdEvent, make_event

QUARTILES = ("q25", "q50", "q75", "q100")


def request_session(
    seed: int,
    now: datetime,
    fill_prob: float = 0.7,
    quartile_probs: tuple[float, ...] = (0.9, 0.75, 0.55, 0.35),
) -> list[AdEvent]:
    """One ad request and its causal follow-ons, all sharing one request_id.

    Always emits an ad_request. With probability fill_prob the request is
    filled (an impression follows, same request_id/campaign/creative/user/
    device/geo/placement). If filled, quartile completions follow in nested
    order — q50 only after q25, etc. — each gated by its quartile_probs entry.
    Reuses make_event() purely as the source of the shared request dimensions.
    """
    base = make_event(seed=seed, now=now)
    r = random.Random((seed * 2_654_435_761) % (2**64))
    shared = dict(
        campaign_id=base.campaign_id,
        creative_id=base.creative_id,
        request_id=base.request_id,
        user_id=base.user_id,
        device=base.device,
        geo=base.geo,
        placement=base.placement,
    )

    def evt(event_type: str, ts: datetime) -> AdEvent:
        return AdEvent(
            event_id=f"evt-{r.getrandbits(64):016x}",
            event_type=event_type,
            event_ts=ts,
            **shared,
        )

    events = [evt("ad_request", now)]
    if r.random() < fill_prob:
        t = now + timedelta(seconds=1)
        events.append(evt("impression", t))
        for i, q in enumerate(QUARTILES):
            if r.random() < quartile_probs[i]:
                t += timedelta(seconds=2)
                events.append(evt(q, t))
            else:
                break
    return events
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_session.py -v`
Expected: PASS (7 passed)

- [ ] **Step 5: Lint + commit**

```bash
.venv/bin/ruff check generator/session.py tests/test_session.py
git add generator/session.py tests/test_session.py
git commit -m "feat(generator): correlated request_session (request -> impression -> nested quartiles)"
```

---

## Task 2: Session-based event batch (TDD, modifies stream.py)

**Files:**
- Modify: `generator/stream.py`
- Modify: `tests/test_stream.py`

- [ ] **Step 1: Replace `tests/test_stream.py` with the session-aware tests**

```python
# tests/test_stream.py
from datetime import datetime, timezone, timedelta
from generator.stream import event_batch

NOW = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)

def test_yields_at_least_one_event_per_request():
    events = list(event_batch(n_requests=1000, now=NOW, dup_rate=0.0, late_rate=0.0,
                              seed=0, fill_prob=0.0))
    # fill_prob 0 -> exactly one ad_request per request, no dups, no late
    assert len(events) == 1000
    assert all(e.event_type == "ad_request" for e in events)

def test_impressions_have_a_matching_ad_request():
    events = list(event_batch(n_requests=2000, now=NOW, dup_rate=0.0, late_rate=0.0,
                              seed=1, fill_prob=0.8))
    request_ids = {e.request_id for e in events if e.event_type == "ad_request"}
    impressions = [e for e in events if e.event_type == "impression"]
    assert impressions  # some fills happened
    assert all(e.request_id in request_ids for e in impressions)

def test_duplicates_inflate_total_but_not_distinct():
    events = list(event_batch(n_requests=2000, now=NOW, dup_rate=0.05, late_rate=0.0,
                              seed=2, fill_prob=0.7))
    ids = [e.event_id for e in events]
    assert len(ids) > len(set(ids))  # duplicates present

def test_late_events_are_backdated():
    events = list(event_batch(n_requests=3000, now=NOW, dup_rate=0.0, late_rate=0.05,
                              seed=3, fill_prob=0.7))
    late = [e for e in events if e.event_ts < NOW - timedelta(minutes=1)]
    assert 0.03 <= len(late) / len(events) <= 0.07
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest tests/test_stream.py -v`
Expected: FAIL — `TypeError` (old `event_batch` has no `n_requests`/`fill_prob`) or assertion failures.

- [ ] **Step 3: Rewrite `generator/stream.py`**

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
) -> Iterator[AdEvent]:
    """Yield the events of n_requests correlated ad requests.

    Each request contributes an ad_request plus its causal impression/quartile
    events (sharing request_id) via request_session(). On top of that raw
    stream, ~late_rate of events are backdated to simulate late arrival, and
    ~dup_rate are re-emitted as exact duplicates (same event_id). Cleaning both
    up is the silver layer's job — bronze keeps them.
    """
    r = random.Random(seed)
    for i in range(n_requests):
        for ev in request_session(seed=seed * 1_000_003 + i, now=now, fill_prob=fill_prob):
            if r.random() < late_rate:
                ev = ev.model_copy(update={"event_ts": now - timedelta(minutes=r.randint(2, 240))})
            yield ev
            if r.random() < dup_rate:
                yield ev  # same event_id -> duplicate
```

- [ ] **Step 4: Run to verify it passes**

Run: `.venv/bin/pytest tests/test_stream.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Lint + commit**

```bash
.venv/bin/ruff check generator/stream.py tests/test_stream.py
git add generator/stream.py tests/test_stream.py
git commit -m "feat(generator): event_batch emits correlated request sessions"
```

---

## Task 3: Update producer, reset bronze, re-stream correlated data (integration)

**Files:**
- Modify: `generator/produce.py`

- [ ] **Step 1: Update `generator/produce.py` arg semantics**

Replace the argparse block and the `event_batch` call so `--n` means *requests* and a `--fill-prob` is exposed. The full file becomes:

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
    ap.add_argument("--n", type=int, default=10_000, help="number of ad requests")
    ap.add_argument("--dup-rate", type=float, default=0.02)
    ap.add_argument("--late-rate", type=float, default=0.05)
    ap.add_argument("--fill-prob", type=float, default=0.7)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
    topic = os.environ.get("KAFKA_TOPIC", "ad_events")
    producer = Producer({"bootstrap.servers": bootstrap})

    now = datetime.now(timezone.utc)
    count = 0
    for ev in event_batch(args.n, now, args.dup_rate, args.late_rate, args.seed, args.fill_prob):
        payload = ev.model_dump()
        payload["event_ts"] = payload["event_ts"].isoformat()
        producer.produce(topic, key=ev.user_id, value=json.dumps(payload))
        count += 1
        if count % 2000 == 0:
            producer.poll(0)
    producer.flush()
    print(f"produced {count} events from {args.n} requests to {topic}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Lint**

Run: `.venv/bin/ruff check generator/produce.py`
Expected: clean.

- [ ] **Step 3: Reset bronze so the medallion is built on clean correlated data**

The running stream, the existing bronze table, AND the Kafka topic all hold Plan 1's
independent events — purge all three or old events leak back into the rebuilt bronze.

```bash
# 1. stop the running streaming job
docker compose exec -T spark bash -lc "pkill -f ingest_bronze || true"

# 2. drop the bronze table (throwaway script; do NOT commit it)
cat > scripts/_reset_bronze.py <<'PY'
import sys; sys.path.insert(0, "/opt/app")
from streaming.spark_session import build_spark
s = build_spark("reset"); s.sql("DROP TABLE IF EXISTS lh.bronze.ad_events_raw"); print("DROPPED"); s.stop()
PY
docker compose exec -T -e PYTHONPATH=/opt/app spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/app/scripts/_reset_bronze.py 2>&1 | tail -2   # expect: DROPPED

# 3. delete AND recreate the Kafka topic (empty). RECREATE IS REQUIRED:
#    Redpanda does NOT auto-create a topic for a *consumer*, so if the stream
#    starts against a missing topic it crashes with UnknownTopicOrPartition.
docker compose exec -T redpanda rpk topic delete ad_events || true
docker compose exec -T redpanda rpk topic create ad_events

# 4. clear the streaming checkpoint
rm -rf .checkpoints/bronze_ad_events
```

- [ ] **Step 4: Restart the stream and seed correlated events**

```bash
make stream
sleep 35              # let the streaming job initialize on the (empty) topic
make seed            # 10000 requests -> ~3x correlated events
```

- [ ] **Step 5: Verify correlation landed in bronze (via Trino)**

```bash
docker compose exec -T trino trino --execute "
SELECT
  (SELECT count(*) FROM iceberg.bronze.ad_events_raw WHERE event_type='impression') AS impressions,
  (SELECT count(*) FROM iceberg.bronze.ad_events_raw i
     WHERE i.event_type='impression'
       AND EXISTS (SELECT 1 FROM iceberg.bronze.ad_events_raw r
                   WHERE r.event_type='ad_request' AND r.request_id = i.request_id)) AS impressions_with_request"
```
Expected: both numbers are non-zero and **equal** — every impression has a matching ad_request (proof the correlation survived generation → Kafka → bronze). Report the two numbers.

- [ ] **Step 6: Commit**

```bash
git add generator/produce.py
git commit -m "feat(generator): producer emits correlated sessions (--n = requests, --fill-prob)"
```

---

## Task 4: Campaign metadata model + builder (TDD)

**Files:**
- Create: `api/__init__.py` (empty), `api/campaigns.py`
- Modify: `pyproject.toml` (add api/test deps)
- Test: `tests/test_campaigns.py`

- [ ] **Step 1: Add API deps to `pyproject.toml`**

In the `[project.optional-dependencies]` `dev` list, add `fastapi>=0.115`, `uvicorn>=0.32`, `httpx>=0.27` (httpx is needed by FastAPI's TestClient). The `dev` list becomes:

```toml
dev = ["pytest>=8.0", "ruff>=0.5", "pyspark==3.5.1", "fastapi>=0.115", "uvicorn>=0.32", "httpx>=0.27"]
```

Then install: `.venv/bin/pip install -e '.[dev]'`

- [ ] **Step 2: Write the failing test**

```python
# tests/test_campaigns.py
from datetime import date
from api.campaigns import build_campaigns, Campaign, N_CAMPAIGNS

REF = date(2026, 6, 4)

def test_builds_expected_count_and_ids():
    cs = build_campaigns(REF)
    assert len(cs) == N_CAMPAIGNS
    assert cs[0].campaign_id == "cmp-001"
    assert cs[-1].campaign_id == f"cmp-{N_CAMPAIGNS:03d}"

def test_flights_bracket_the_reference_date():
    for c in build_campaigns(REF):
        assert c.flight_start < REF < c.flight_end

def test_daily_budget_matches_budget_over_flight_days():
    for c in build_campaigns(REF):
        days = (c.flight_end - c.flight_start).days
        assert abs(c.daily_budget - c.budget / days) < 0.01

def test_deterministic():
    assert [c.model_dump() for c in build_campaigns(REF)] == \
           [c.model_dump() for c in build_campaigns(REF)]

def test_is_campaign_instances():
    assert all(isinstance(c, Campaign) for c in build_campaigns(REF))
```

- [ ] **Step 3: Run to verify it fails**

Run: `.venv/bin/pytest tests/test_campaigns.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'api.campaigns'`

- [ ] **Step 4: Implement**

```python
# api/campaigns.py
from __future__ import annotations

import random
from datetime import date, timedelta

from pydantic import BaseModel

from generator.event import DEVICES, GEOS

N_CAMPAIGNS = 20


class Campaign(BaseModel):
    campaign_id: str
    budget: int          # total impression budget over the flight
    flight_start: date
    flight_end: date
    daily_budget: float
    target_geo: str
    target_device: str


def build_campaigns(reference: date) -> list[Campaign]:
    """Deterministic metadata for cmp-001..cmp-020. Flights bracket `reference`
    so freshly-generated events (timestamped ~now) fall inside each flight."""
    campaigns: list[Campaign] = []
    for i in range(1, N_CAMPAIGNS + 1):
        r = random.Random(i)
        start = reference - timedelta(days=r.randint(3, 12))
        end = reference + timedelta(days=r.randint(3, 12))
        budget = r.randint(50, 500) * 1000
        days = (end - start).days
        campaigns.append(
            Campaign(
                campaign_id=f"cmp-{i:03d}",
                budget=budget,
                flight_start=start,
                flight_end=end,
                daily_budget=round(budget / days, 2),
                target_geo=r.choice(GEOS),
                target_device=r.choice(DEVICES),
            )
        )
    return campaigns
```

- [ ] **Step 5: Run to verify it passes + lint**

Run: `.venv/bin/pytest tests/test_campaigns.py -v` → PASS (5 passed)
Run: `.venv/bin/ruff check api/campaigns.py tests/test_campaigns.py` → clean

- [ ] **Step 6: Commit**

```bash
touch api/__init__.py
git add api/__init__.py api/campaigns.py tests/test_campaigns.py pyproject.toml
git commit -m "feat(api): deterministic campaign metadata model and builder"
```

---

## Task 5: FastAPI app + container (TDD for the app, integration for the container)

**Files:**
- Create: `api/main.py`, `docker/api/Dockerfile`
- Modify: `docker-compose.yml`
- Test: `tests/test_api.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_api.py
from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)

def test_health():
    assert client.get("/health").json() == {"status": "ok"}

def test_campaigns_returns_twenty_with_fields():
    r = client.get("/campaigns")
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 20
    first = body[0]
    for field in ["campaign_id", "budget", "flight_start", "flight_end",
                  "daily_budget", "target_geo", "target_device"]:
        assert field in first
    assert first["campaign_id"] == "cmp-001"
```

- [ ] **Step 2: Run to verify it fails**

Run: `.venv/bin/pytest tests/test_api.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'api.main'`

- [ ] **Step 3: Implement `api/main.py`**

```python
# api/main.py
from datetime import datetime, timezone

from fastapi import FastAPI

from api.campaigns import build_campaigns

app = FastAPI(title="ad-lakehouse campaigns API")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/campaigns")
def list_campaigns() -> list[dict]:
    today = datetime.now(timezone.utc).date()
    return [c.model_dump(mode="json") for c in build_campaigns(today)]
```

- [ ] **Step 4: Run to verify it passes + lint**

Run: `.venv/bin/pytest tests/test_api.py -v` → PASS (2 passed)
Run: `.venv/bin/ruff check api/main.py tests/test_api.py` → clean

- [ ] **Step 5: Create `docker/api/Dockerfile`**

```dockerfile
# docker/api/Dockerfile
FROM python:3.11-slim
RUN pip install --no-cache-dir "fastapi>=0.115" "uvicorn>=0.32" "pydantic>=2.6"
WORKDIR /opt/app
EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 6: Add the `api` service to `docker-compose.yml`**

Append this service (sibling of the others):

```yaml
  api:
    build:
      context: .
      dockerfile: docker/api/Dockerfile
    volumes:
      - ./:/opt/app
    ports: ["8000:8000"]
```

- [ ] **Step 7: Build + verify the live endpoint**

```bash
docker compose up -d --build api
sleep 5
curl -s http://localhost:8000/campaigns | python3 -c "import sys,json; d=json.load(sys.stdin); print('count', len(d)); print(d[0])"
```
Expected: `count 20` and the first campaign dict (cmp-001 with budget/flight/targeting). Report the output.

- [ ] **Step 8: Commit**

```bash
git add api/main.py tests/test_api.py docker/api/Dockerfile docker-compose.yml
git commit -m "feat(api): FastAPI campaigns service + container"
```

---

## Task 6: Campaign pull → silver.dim_campaign (Spark, integration)

**Files:**
- Create: `transform/__init__.py` (empty), `transform/dim_campaign.py`

- [ ] **Step 1: Implement `transform/dim_campaign.py`**

```python
# transform/dim_campaign.py
import json
import os
import urllib.request

from pyspark.sql import Row, SparkSession

API_URL = os.environ.get("CAMPAIGNS_API_URL", "http://api:8000/campaigns")


def build(spark: SparkSession) -> None:
    """Pull campaign metadata from the FastAPI service and (re)write
    lh.silver.dim_campaign. Idempotent: createOrReplace each run."""
    with urllib.request.urlopen(API_URL, timeout=30) as resp:
        campaigns = json.load(resp)

    rows = [
        Row(
            campaign_id=c["campaign_id"],
            budget=int(c["budget"]),
            flight_start=c["flight_start"],
            flight_end=c["flight_end"],
            daily_budget=float(c["daily_budget"]),
            target_geo=c["target_geo"],
            target_device=c["target_device"],
        )
        for c in campaigns
    ]

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
    df = spark.createDataFrame(rows).selectExpr(
        "campaign_id",
        "budget",
        "to_date(flight_start) AS flight_start",
        "to_date(flight_end) AS flight_end",
        "daily_budget",
        "target_geo",
        "target_device",
    )
    df.writeTo("lh.silver.dim_campaign").using("iceberg").createOrReplace()
    print(f"[dim_campaign] wrote {df.count()} campaigns")
```

- [ ] **Step 2: Run it standalone inside the spark container**

```bash
touch transform/__init__.py
docker compose exec -T -e PYTHONPATH=/opt/app spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/app/transform/_run_dim_campaign.py 2>&1 | tail -5
```
where you first create a temporary `transform/_run_dim_campaign.py`:
```python
from streaming.spark_session import build_spark
from transform.dim_campaign import build
s = build_spark("dim-campaign"); build(s); s.stop()
```
Expected: `[dim_campaign] wrote 20 campaigns`. (You'll delete `_run_dim_campaign.py` — Task 8 provides the real `transform/run.py` driver. Do NOT commit `_run_dim_campaign.py`.)

- [ ] **Step 3: Verify via Trino**

Run: `docker compose exec -T trino trino --execute "SELECT count(*), min(flight_start), max(flight_end) FROM iceberg.silver.dim_campaign"`
Expected: `20` campaigns with sensible flight bounds. Report it.

- [ ] **Step 4: Commit (only the real module)**

```bash
rm -f transform/_run_dim_campaign.py
git add transform/__init__.py transform/dim_campaign.py
git commit -m "feat(transform): pull campaign metadata into silver.dim_campaign"
```

---

## Task 7: bronze → silver.fact_event (dedup + GDPR-efficient partitioning) (Spark, integration)

**Files:**
- Create: `transform/fact_event.py`

- [ ] **Step 1: Implement `transform/fact_event.py`**

```python
# transform/fact_event.py
from pyspark.sql import SparkSession


def build(spark: SparkSession) -> None:
    """Rebuild lh.silver.fact_event from bronze: dedup on event_id (earliest
    ingest_ts wins), partitioned by days(event_ts) and bucket(16, user_id).

    The bucket(user_id) partitioning clusters each user's rows into one bucket
    per day so a GDPR right-to-be-forgotten delete (Plan 4) rewrites a few files
    rather than the whole table. CREATE OR REPLACE makes the build idempotent;
    late events land in their true event_ts partition automatically.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
    spark.sql(
        """
        CREATE OR REPLACE TABLE lh.silver.fact_event
        USING iceberg
        PARTITIONED BY (days(event_ts), bucket(16, user_id))
        AS
        SELECT event_id, event_type, event_ts, campaign_id, creative_id,
               request_id, user_id, device, geo, placement
        FROM (
          SELECT *,
                 row_number() OVER (PARTITION BY event_id ORDER BY ingest_ts ASC) AS rn
          FROM lh.bronze.ad_events_raw
          WHERE event_id IS NOT NULL
        ) WHERE rn = 1
        """
    )
    n = spark.sql("SELECT count(*) AS c FROM lh.silver.fact_event").collect()[0]["c"]
    print(f"[fact_event] wrote {n} deduped events")
```

- [ ] **Step 2: Run it standalone in the spark container**

```bash
docker compose exec -T -e PYTHONPATH=/opt/app spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/app/transform/_run_fact_event.py 2>&1 | tail -5
```
with a temporary `transform/_run_fact_event.py`:
```python
from streaming.spark_session import build_spark
from transform.fact_event import build
s = build_spark("fact-event"); build(s); s.stop()
```
Expected: `[fact_event] wrote N deduped events`. Do NOT commit `_run_fact_event.py`.

- [ ] **Step 3: Verify dedup + partitioning via Trino**

```bash
docker compose exec -T trino trino --execute "
SELECT
  (SELECT count(*) FROM iceberg.silver.fact_event) AS rows,
  (SELECT count(DISTINCT event_id) FROM iceberg.silver.fact_event) AS distinct_ids,
  (SELECT count(*) FROM iceberg.bronze.ad_events_raw) AS bronze_rows"
docker compose exec -T trino trino --execute "SELECT count(*) FROM iceberg.silver.\"fact_event\$partitions\""
```
Expected: `rows == distinct_ids` (dedup worked) and `rows <= bronze_rows` (duplicates removed). The `$partitions` count is > 1 (multiple date×bucket partitions exist). Report the numbers.

- [ ] **Step 4: Commit**

```bash
rm -f transform/_run_fact_event.py
git add transform/fact_event.py
git commit -m "feat(transform): bronze -> silver.fact_event with dedup and bucket(user_id) partitioning"
```

---

## Task 8: transform.run driver, Makefile, silver integration tests, README (integration)

**Files:**
- Create: `transform/run.py`, `trino/01_silver_checks.sql`, `tests/test_silver.py`
- Modify: `Makefile`, `README.md`

- [ ] **Step 1: Implement `transform/run.py`**

```python
# transform/run.py
import sys

from streaming.spark_session import build_spark
from transform import dim_campaign, fact_event

BUILDERS = {
    "dim_campaign": dim_campaign.build,
    "fact_event": fact_event.build,
}
GROUPS = {"silver": ["dim_campaign", "fact_event"]}


def main() -> None:
    target = sys.argv[1] if len(sys.argv) > 1 else "silver"
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

- [ ] **Step 2: Write `trino/01_silver_checks.sql`**

```sql
-- dedup + correlation sanity for the silver layer
SELECT
  (SELECT count(*) FROM iceberg.silver.fact_event) AS events,
  (SELECT count(DISTINCT event_id) FROM iceberg.silver.fact_event) AS distinct_events,
  (SELECT count(*) FROM iceberg.silver.dim_campaign) AS campaigns,
  (SELECT count(*) FROM iceberg.silver.fact_event f
     JOIN iceberg.silver.dim_campaign d ON f.campaign_id = d.campaign_id) AS joinable_events;
```

- [ ] **Step 3: Add Makefile targets**

Add a `build-silver` target (and keep existing ones). The new recipe (tab-indented like `stream`):

```makefile
topic: ; docker compose exec -T redpanda rpk topic create ad_events --topic-config retention.ms=-1 || true
build-silver: ; docker compose exec -T -e PYTHONPATH=/opt/app spark /opt/spark/bin/spark-submit \
	--conf spark.jars.ivy=/tmp/.ivy2 \
	--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
	/opt/app/transform/run.py silver
silver-checks: ; docker compose exec -T trino trino --catalog iceberg < trino/01_silver_checks.sql
```
Also add `topic`, `build-silver`, `silver-checks` to the `.PHONY` line. The `topic` target exists because Redpanda does not auto-create a topic for a consumer — a fresh `make up` then `make stream` would otherwise crash the stream on a missing topic. Run `make -n build-silver` to confirm the recipe assembles with all flags + the `silver` arg.

- [ ] **Step 4: Run the full silver build end-to-end via the driver**

```bash
make build-silver
make silver-checks
```
Expected: `silver-checks` shows `events == distinct_events`, `campaigns == 20`, and `joinable_events == events` (every event's campaign_id matches a campaign — proves the API join is sound). Report the row.

- [ ] **Step 5: Write `tests/test_silver.py` (integration-marked)**

```python
# tests/test_silver.py
import subprocess

import pytest


def _trino(sql: str) -> list[str]:
    out = subprocess.check_output(
        ["docker", "compose", "exec", "-T", "trino", "trino", "--execute", sql],
        text=True,
    )
    return out.strip().strip('"').split('","')


@pytest.mark.integration
def test_fact_event_is_deduped():
    rows, distinct = _trino(
        "SELECT count(*), count(DISTINCT event_id) FROM iceberg.silver.fact_event"
    )
    assert int(rows) == int(distinct)


@pytest.mark.integration
def test_dim_campaign_has_twenty():
    (n,) = _trino("SELECT count(*) FROM iceberg.silver.dim_campaign")
    assert int(n) == 20


@pytest.mark.integration
def test_every_event_joins_to_a_campaign():
    events, joinable = _trino(
        "SELECT (SELECT count(*) FROM iceberg.silver.fact_event), "
        "(SELECT count(*) FROM iceberg.silver.fact_event f "
        "JOIN iceberg.silver.dim_campaign d ON f.campaign_id = d.campaign_id)"
    )
    assert int(events) == int(joinable) and int(events) > 0


@pytest.mark.integration
def test_impressions_link_to_ad_requests_in_silver():
    (n,) = _trino(
        "SELECT count(*) FROM iceberg.silver.fact_event i "
        "WHERE i.event_type='impression' AND NOT EXISTS "
        "(SELECT 1 FROM iceberg.silver.fact_event r "
        "WHERE r.event_type='ad_request' AND r.request_id = i.request_id)"
    )
    assert int(n) == 0  # no orphan impressions
```

- [ ] **Step 6: Run unit + integration suites + lint**

```bash
.venv/bin/pytest -q                 # unit: session, stream, campaigns, api, event (all green)
.venv/bin/pytest -m integration -q  # silver + bronze smoke (stack must be up + built)
.venv/bin/ruff check .
```
Expected: all green, ruff clean. Report counts.

- [ ] **Step 7: Update `README.md` roadmap**

In the Roadmap table, change Plan 2 status to `✅ done` and add silver/API to the "what runs today" description and repo tour (mention `api/`, `transform/`, `silver.*`). Update the quickstart so it includes `make topic` BEFORE `make stream` (Redpanda doesn't auto-create the topic for a consumer), and add the silver steps after `make seed`:
```
make up
make topic          # create the ad_events topic (required before the consumer starts)
make stream         # Structured Streaming Kafka -> bronze
make seed           # produce correlated ad events
make build-silver   # build silver.dim_campaign (from the API) + deduped silver.fact_event
make silver-checks  # dedup + campaign-join sanity via Trino
```
(Also note the `api` service is on http://localhost:8000.)

- [ ] **Step 8: Commit**

```bash
git add transform/run.py trino/01_silver_checks.sql tests/test_silver.py Makefile README.md
git commit -m "feat(transform): silver build driver, Makefile targets, integration tests, README"
```

---

## Self-review notes

- **Spec coverage (Plan 2 scope):** §4.3 silver — `fact_event` deduped on `event_id`, partitioned `days(event_ts), bucket(16, user_id)` (Task 7); `dim_campaign` from the API (Task 6). §6 transforms — bronze→silver dedup + late-data correctness (late events route to their true `event_ts` partition via the CTAS) and the campaign pull (Tasks 6–8). Campaign API / "API sourcing" (Tasks 4–5). The `request_id` linkage that makes gold computable is established at generation (Tasks 1–3). Gold products (§4.4) are explicitly **Plan 3**, not here.
- **Type consistency:** `event_batch(n_requests, now, dup_rate, late_rate, seed, fill_prob)` is defined in Task 2 and called identically in Task 3's `produce.py`. `request_session(seed, now, fill_prob, quartile_probs)` defined Task 1, used Task 2. `Campaign` fields (Task 4) match the `dim_campaign` Row/DDL columns (Task 6) and the `test_api` field list (Task 5). Catalog `lh` (Spark) ↔ `iceberg` (Trino) as in Plan 1.
- **Known gotchas carried from Plan 1:** every spark-submit uses `-e PYTHONPATH=/opt/app` + `--conf spark.jars.ivy=/tmp/.ivy2` + the 1.8.1 packages; the producer runs as `.venv/bin/python`; Trino host port is 8081, the new `api` is 8000.
- **Deferred / flagged:** silver is a full rebuild from bronze, so Plan 4 (GDPR) must also purge bronze or filter via a suppression list (see Cross-plan note). Incremental MERGE is a possible later enhancement, out of scope here.
```
