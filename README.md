# ad-lakehouse

A miniature ad-serving event lakehouse, built to mirror what an Ads Data
Engineering team actually ships: ingest ad-serving events, land them in Apache
Iceberg, and turn them into clean, connected data products (delivery, inventory
fill, campaign pacing).

> New here? **[Why this matters (the business value)](docs/business-value.md)** explains
> what each piece is worth to an ads business. This README is the how; that doc is the why.

**What runs today (streaming spine + silver + gold):**

```
generator ──▶ Redpanda (Kafka) ──▶ Spark Structured Streaming ──▶ Iceberg bronze ──┐
 (Python)      topic: ad_events        (checkpointed ingest)        (append-only)  │
                                                                                   ▼
   FastAPI campaigns ──▶ Spark SQL ──▶ Iceberg silver (dim_campaign + fact_event) ──┐
   (:8000)                                (dedup / late-data, idempotent builds)     │
                                                                                    ▼
              Spark SQL ──▶ Iceberg gold (fact_impression_delivery,                Trino
                            inventory_fill, campaign_pacing) ──────────────────▶ (queries)
```

A synthetic generator emits ad events — ad requests, impressions, quartile
completions — and deliberately injects **duplicate** and **late-arriving** events
so downstream layers have something real to clean up. Spark Structured Streaming
reads the Kafka topic and appends every event, raw, to an Iceberg `bronze` table
on MinIO/S3, behind an Iceberg REST catalog. Trino queries the same tables Spark
writes.

A FastAPI service (`api/`, http://localhost:8000) serves campaign metadata, and a
batch transform layer (`transform/`) runs Spark SQL builds that turn bronze into a
clean **silver** layer: `silver.dim_campaign` (campaigns from the API) and
`silver.fact_event` (events deduped on `event_id`, late events landed in their true
`event_ts` partition). The builds are idempotent (`CREATE OR REPLACE`).

A **gold** layer then reshapes silver into analyst-ready data products, all built in
one Spark session: `gold.fact_impression_delivery` (one row per impression with its
quartile-completion flags), `gold.inventory_fill` (requests vs impressions per
placement/day → fill rate), and `gold.campaign_pacing` (the headline product, below).

### Campaign pacing

`gold.campaign_pacing` is the headline data product: one row per campaign per day,
tracking `cumulative_delivered` impressions against an `expected_pace` derived by
prorating the campaign's budget across its flight. The ratio of the two is a
`pace_index`, bucketed into a `pace_label` — `ahead`, `on_track`, or `behind` — so a
trader can scan a leaderboard and see at a glance which campaigns need attention.
Event spread and budgets are calibrated to the demo's synthetic volume so the labels
genuinely vary (most campaigns land `behind`, a few `ahead` or `on_track`); it is a
realistic shape, not a hand-tuned happy path.

Storage (MinIO), catalog (Iceberg REST), and compute (Spark + Trino) are
independent, swappable layers — the way modern lakehouses actually run.

## Quickstart

```bash
python3.11 -m venv .venv && . .venv/bin/activate && pip install -e '.[dev]'
make up            # start redpanda + minio + iceberg-rest + spark + trino + api
make topic         # create the ad_events topic (required before the consumer starts)
make stream        # start Structured Streaming Kafka -> bronze (see note below)
make seed          # produce 10k ad events to Kafka
make build-silver  # silver.dim_campaign (from the API) + deduped silver.fact_event
make silver-checks # dedup + campaign-join sanity via Trino
make build-gold    # gold delivery + inventory fill + campaign pacing
make gold-queries  # pacing leaderboard, fill by placement, completion funnel
make query         # count rows landed in bronze, via Trino
make test          # unit tests   (run `make` integration smoke separately, below)

make airflow-up        # start Airflow (standalone) — UI at http://localhost:8082
make airflow-password  # print the generated admin password
make dags-list         # list the DAGs
make dag-medallion     # run the silver->gold DAG end to end

make gdpr-efficiency       # demo: bucketed vs unbucketed delete (the ~14.5x win)
make gdpr-mor              # demo: merge-on-read delete (delete files, data unchanged)
make forget-user UID=usr-XXXXX  # erase a user across the lakehouse (destructive)
```

> **Docker memory:** give Docker **≥ 8 GB** — Airflow plus Spark, Trino, MinIO,
> Redpanda, and the Iceberg REST catalog all run as containers. The Airflow UI is
> at **http://localhost:8082**.

> **First `make stream` is slow (~1–2 min):** Spark downloads the Iceberg + Kafka
> connector jars via Ivy on first launch, then the job initializes. It is not
> hung. Watch progress with `docker compose logs --tail 40 spark`. `make stream`
> must run *before* `make seed` so the consumer is live when events are produced
> (though `startingOffsets=earliest` will also pick up a topic seeded earlier).
> Run `make topic` first — Redpanda does not auto-create `ad_events` for a
> consumer, so a fresh `make stream` would otherwise crash.

`make build-silver` pulls campaign metadata over HTTP, so the `api` service must be
up (it is, after `make up`); a failed pull surfaces as a Spark error.

**What "good" looks like** — `make silver-checks` proves the medallion boundary held:

```
 events | distinct_events | campaigns | joinable_events
 31525  |     31525       |    20     |     31525
```

`events == distinct_events` means silver deduped what bronze deliberately duplicated;
`joinable_events == events` means every event's `campaign_id` resolves to a campaign;
and the integration suite's orphan-impression check confirms each impression still
links to its `ad_request` after the bronze→silver round trip.

Run the end-to-end smoke + silver tests (requires the stack up + built):

```bash
.venv/bin/pytest -m integration -v
```

Consoles: **Trino** at http://localhost:8081 (host 8081 → container 8080),
**MinIO** at http://localhost:9001.

## Orchestration

Airflow (running in **standalone** mode) orchestrates the batch side of the
lakehouse with three DAGs (`airflow/dags/`):

- **`campaign_pull`** (`@daily`) — pulls campaign metadata from the FastAPI service
  into `silver.dim_campaign`.
- **`medallion_build`** (`@hourly`) — builds **silver → gold** in dependency order
  (`build_silver >> build_gold`).
- **`iceberg_maintenance`** (`@daily`) — compacts small files and expires old
  snapshots across the Iceberg tables.

All three carry **retries**, so a transient Spark OOM on a single task self-heals on
the next attempt instead of failing the run.

A deliberate design choice: **Airflow does not run Spark itself.** Each task
`docker exec`s into the long-lived `spark` container — Docker-out-of-Docker — so the
exact same `spark-submit` invocations `make` uses are what the DAGs run, with one
Spark runtime shared across the stack. **Streaming stays a long-lived service outside
Airflow** (continuous Kafka → bronze ingest is not a scheduled batch job).

### Local-demo security note

To `docker exec` the Spark container, the Airflow container **runs as root** and
**mounts the host Docker socket**, and `AIRFLOW__WEBSERVER__EXPOSE_CONFIG` is **on**.
These are acceptable tradeoffs for a **local, single-node demo** and **never for
production**. In a real deployment you would run Airflow as a **non-root user**, reach
Docker through a **socket proxy** (not the raw socket), and **disable config
exposure** — plus an executor (Kubernetes/Celery) that schedules real workers rather
than `docker exec`.

## Data governance (GDPR)

Right-to-be-forgotten (GDPR Art. 17) erases a user across **bronze → silver →
gold** and makes them **unrecoverable** — not just hidden. A row-level `DELETE`
clears every PII table (`bronze.ad_events_raw`, `silver.fact_event`,
`gold.fact_impression_delivery`), the aggregate gold tables are rebuilt from the
cleaned silver, and `expire_snapshots` drops the pre-delete snapshots so
time-travel can't bring the user back. Bronze *must* be deleted, or a later
silver rebuild resurrects the user.

Two Iceberg techniques make it efficient and verifiable:

- **`bucket(16, user_id)` layout** co-locates a user's rows in one bucket, so the
  analytical delete prunes to that bucket — measured **~14.5x fewer records** (and
  8.7x fewer bytes) rewritten than an unbucketed control.
- **Merge-on-read deletes** write small delete files instead of rewriting data;
  reads exclude the user immediately and compaction reconciles later.

An audit row in `lh.gdpr.erasure_log` records each erasure for Art. 5(2)
accountability. Run it on demand via the **`gdpr_delete`** Airflow DAG. Full
writeup with the measured numbers:
[`docs/gdpr-right-to-be-forgotten.md`](docs/gdpr-right-to-be-forgotten.md).

## Design

The full design — data model, the bronze→silver→gold medallion, GDPR
right-to-be-forgotten on Iceberg, and the performance before/after study — lives in
[`docs/superpowers/specs/2026-06-04-ad-lakehouse-design.md`](docs/superpowers/specs/2026-06-04-ad-lakehouse-design.md).

## Roadmap

This repo is built in milestones; each produces working, testable software.

| Plan | Scope | Status |
|------|-------|--------|
| 1. Infra + streaming spine | generator → Kafka → Spark Structured Streaming → Iceberg bronze → Trino | ✅ **done** |
| 2. Medallion silver + campaign API | FastAPI campaigns, bronze→silver (dedup/late-data, idempotent builds) | ✅ **done** |
| 2b. Gold data products | gold delivery / inventory fill / campaign **pacing** on top of silver | ✅ **done** |
| 3. Airflow orchestration | DAGs for the batch builds + Iceberg maintenance | ✅ **done** |
| 4. GDPR right-to-be-forgotten | `bucket()`-efficient deletes + merge-on-read deletes, snapshot expiry, audit log | ✅ **done** |
| 5. Performance before/after | deliberately-bad vs optimized pipeline, with measured query-time deltas | planned |

## Repo tour (current)

| Path | Responsibility |
|------|----------------|
| `generator/` | Synthetic ad-event model, batch generator (dupes + late events), Kafka producer |
| `streaming/` | Spark session builder + Structured Streaming ingest to Iceberg bronze |
| `api/` | FastAPI campaign-metadata service (http://localhost:8000) |
| `transform/` | Spark SQL silver builds (`dim_campaign`, `fact_event`), gold builds (`gold_delivery`, `gold_fill`, `gold_pacing` → `gold.fact_impression_delivery`, `gold.inventory_fill`, `gold.campaign_pacing`) + `run.py` driver |
| `airflow/` | Airflow DAGs orchestrating the batch builds + Iceberg maintenance (`campaign_pull`, `medallion_build`, `iceberg_maintenance`) and the on-demand `gdpr_delete`; each `docker exec`s the Spark container |
| `gdpr/` | Right-to-be-forgotten: `forget_user.py` (erase a user across bronze→silver→gold + snapshot expiry + audit log) and the `efficiency_demo` / `mor_demo` technique demos |
| `docker-compose.yml`, `docker/` | Redpanda, MinIO, Iceberg REST catalog, Spark, Trino |
| `trino/` | Analyst SQL against the lakehouse |
| `tests/` | pytest: generator unit tests + an integration smoke test |
| `docs/` | Design spec + implementation plans |
