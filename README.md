# ad-lakehouse

A miniature ad-serving event lakehouse, built to mirror what an Ads Data
Engineering team actually ships: ingest ad-serving events, land them in Apache
Iceberg, and turn them into clean, connected data products (delivery, inventory
fill, campaign pacing).

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
```

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
| 3. Airflow orchestration | DAGs for the batch builds + Iceberg maintenance | planned |
| 4. GDPR right-to-be-forgotten | `bucket()`-efficient MERGE deletes + merge-on-read equality deletes | planned |
| 5. Performance before/after | deliberately-bad vs optimized pipeline, with measured query-time deltas | planned |

## Repo tour (current)

| Path | Responsibility |
|------|----------------|
| `generator/` | Synthetic ad-event model, batch generator (dupes + late events), Kafka producer |
| `streaming/` | Spark session builder + Structured Streaming ingest to Iceberg bronze |
| `api/` | FastAPI campaign-metadata service (http://localhost:8000) |
| `transform/` | Spark SQL silver builds (`dim_campaign`, `fact_event`), gold builds (`gold_delivery`, `gold_fill`, `gold_pacing` → `gold.fact_impression_delivery`, `gold.inventory_fill`, `gold.campaign_pacing`) + `run.py` driver |
| `docker-compose.yml`, `docker/` | Redpanda, MinIO, Iceberg REST catalog, Spark, Trino |
| `trino/` | Analyst SQL against the lakehouse |
| `tests/` | pytest: generator unit tests + an integration smoke test |
| `docs/` | Design spec + implementation plans |
