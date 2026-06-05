# Ad-Serving Event Lakehouse — Design

**Date:** 2026-06-04
**Status:** Approved design, pre-implementation
**Repo:** `ad-lakehouse` (new, standalone)

## 1. Purpose

A miniature version of exactly what an Ads Data Engineering team builds: ingest
ad-serving events, land them in Apache Iceberg, and turn them into the clean,
connected data products an Ads DE JD lists — *ad inventories, forecasting,
targeting, ad serving, pacing*. The repo is the argument: nearly every line of the
JD has a matching, runnable piece here.

Three pieces are the differentiators and must land with high polish:

1. **Streaming front end** — Spark Structured Streaming reading from Kafka into
   Iceberg. Converts a résumé gap (batch-heavy) into a demonstrated capability.
2. **GDPR right-to-be-forgotten on Iceberg** — efficient row-level deletes by
   `user_id`. Max signal-per-effort; almost no portfolio handles it.
3. **Performance before/after** — a deliberately bad pipeline vs an optimized one,
   with documented query-time deltas. Reproduces the proven 35→7 min narrative.

## 2. Decisions (locked)

| Decision | Choice |
|---|---|
| Repo strategy | New standalone repo `ad-lakehouse` |
| Stream source | Real Kafka (Redpanda) in Docker |
| Orchestration | Full Airflow in Docker |
| Transform engine | Spark SQL, Airflow-driven (no dbt) |
| Campaign metadata | Mock REST API (FastAPI) |
| Scope | Comprehensive — spec the whole thing now |
| GDPR strategy | **Headline:** `bucket(N, user_id)` co-partitioning + MERGE delete. **Second technique:** merge-on-read (MoR) equality deletes |

Storage/catalog/compute reuse the lab's proven separation: **MinIO (S3)** storage,
**Iceberg REST** catalog, **Spark + Trino** as independent compute.

## 3. Architecture

```
                ┌─────────────────────────────────────────────────────┐
                │                  Docker Compose                      │
  generator ───▶│  Redpanda (Kafka)  ──▶  Spark Structured Streaming   │
  (Python)      │   topic: ad_events          (streaming ingest job)   │
                │                                   │                   │
  FastAPI ◀─────│── Airflow (campaign pull DAG)     ▼                   │
  campaigns API │                            Iceberg BRONZE  (append)   │
                │   Airflow (batch DAG) ─────────── │                   │
                │     · build silver (Spark SQL)    ▼                   │
                │     · build gold   (Spark SQL)   SILVER (dedup/clean) │
                │     · iceberg maintenance         │                   │
                │     · gdpr deletes                ▼                   │
                │                                  GOLD (3 products)    │
                │   storage: MinIO(S3)   catalog: Iceberg REST          │
                │              Trino  ◀─────────────┘  (analyst queries)│
                └─────────────────────────────────────────────────────┘
```

**Key boundary:** Spark Structured Streaming only ever lands **bronze**. Everything
downstream (silver, gold, GDPR, maintenance) is batch-orchestrated by Airflow and
**idempotent** — silver/gold can be rebuilt from bronze without touching the stream.

### Components

| Component | Responsibility | Interface |
|---|---|---|
| `generator/` | Emit synthetic ad events to Kafka, with injected late + duplicate events | produces JSON to `ad_events` |
| `api/` (FastAPI) | Serve `/campaigns` — budget, flight dates, targeting | REST GET, JSON |
| `streaming/` | Spark Structured Streaming: Kafka → Iceberg bronze, checkpointed, event-time aware | reads Kafka, writes Iceberg |
| `transform/` | Spark SQL bronze→silver→gold; dedup, late-data, campaign join | Iceberg tables |
| `gdpr/` | RTBF: `bucket()` MERGE delete (headline) + MoR equality delete (technique 2) | takes `user_id` |
| `perf/` | Bad-vs-optimized benchmark harness + results table | writes `docs/` numbers |
| `airflow/` | DAGs: campaign-pull, medallion-build, iceberg-maintenance, gdpr-delete | orchestrates the above |
| `trino/` | Analyst SQL: pacing, fill rate, delivery | reads gold |

## 4. Data model

### 4.1 Event schema (generator → Kafka)

| Field | Type | Notes |
|---|---|---|
| `event_id` | string (uuid) | dedup key; duplicates injected by re-emitting same id |
| `event_type` | enum | `ad_request`, `impression`, `q25`, `q50`, `q75`, `q100` |
| `event_ts` | timestamp | event time; late events backdated |
| `campaign_id` | string | FK → campaigns API |
| `creative_id` | string | served creative |
| `request_id` | string | ties an `ad_request` to its resulting `impression` (fill linkage) |
| `user_id` | string | **PII** — RTBF target |
| `device` | string | **PII-ish** — mobile/desktop/ctv |
| `geo` | string | **PII-ish** — country/region |
| `placement` | string | inventory slot |

Generator injects, with tunable rates: **duplicates** (~2%, same `event_id`),
**late arrivals** (~5%, `event_ts` minutes–hours behind wall clock).

### 4.2 Bronze

`bronze.ad_events_raw` — exact Kafka payload + `kafka_ts`, `ingest_ts`.
Append-only, partitioned by `days(ingest_ts)`. No dedup, no cleaning. The
replayable source of truth.

### 4.3 Silver

- `silver.fact_event` — deduped on `event_id` (keep earliest `ingest_ts`), typed,
  late events routed to the correct `event_ts` date partition.
  **Partitioning: `days(event_ts)`, `bucket(16, user_id)`** — the GDPR-efficient layout.
  PII columns (`user_id`, `device`, `geo`) live here.
- `silver.dim_campaign` — from the FastAPI pull: `campaign_id`, `budget`,
  `flight_start`, `flight_end`, `daily_budget`, targeting fields.

### 4.4 Gold (three data products)

1. `gold.fact_impression_delivery` — one row per impression with
   campaign/creative/geo/device + completion flags (q25–q100) folded in from
   quartile events. The delivery fact.
2. `gold.inventory_fill` — per campaign × placement × hour: `requests`,
   `impressions`, `fill_rate = impressions/requests`. Computed via `request_id`
   linkage (request rows vs impression rows). The inventory/fill product.
3. `gold.campaign_pacing` — **headline product.** Per campaign × day over its
   flight: `delivered_impressions`, `cumulative_delivered`, `budget`,
   `expected_pace` (budget × elapsed-flight-fraction), `pace_index =
   actual/expected`, and an `ahead | on_track | behind` label. Requires the
   campaign-API join to exist — the join is load-bearing, not bolted on.

### 4.5 Partitioning rationale

- `days(event_ts)` → analytics prune by date (pacing windows, daily fill).
- `bucket(16, user_id)` → a single user's rows cluster into one bucket per day →
  RTBF MERGE rewrites a few files, not the table. The `perf/` module measures this.

## 5. Streaming ingest (`streaming/`)

- Spark Structured Streaming job reads the `ad_events` Kafka topic.
- Parses JSON, attaches `kafka_ts` + `ingest_ts`, writes append-only to
  `bronze.ad_events_raw` using the Iceberg sink.
- **Checkpointing** to a durable location (MinIO) for exactly-once-ish bronze
  append and restart safety.
- **Trigger:** micro-batch (e.g. `processingTime=10s`) — demonstrable, not so fast
  it floods local disk.
- Event-time awareness lives downstream: bronze captures everything as-is
  (including late/dupe), silver does the event-time correction and dedup. This
  keeps the stream simple and the replayable truth intact.

## 6. Medallion transforms (`transform/`, Spark SQL)

- **bronze → silver:** dedup on `event_id` (window: earliest `ingest_ts` wins),
  cast types, derive `event_date = days(event_ts)`, write to `silver.fact_event`
  with the two-dimension partitioning. Late events naturally land in their true
  `event_ts` partition — demonstrate by querying a past partition after a late batch.
- **campaign pull → silver:** Airflow task calls FastAPI `/campaigns`, writes
  `silver.dim_campaign` (overwrite/merge).
- **silver → gold:** three idempotent Spark SQL builds for the products in §4.4.
  Pacing joins `fact_event` (impressions) to `dim_campaign` on `campaign_id`.

All builds are **idempotent** (overwrite-by-partition or MERGE) so Airflow retries
are safe.

## 7. GDPR right-to-be-forgotten (`gdpr/`)

Input: a `user_id`. Two techniques, both runnable, contrasted in docs.

### 7.1 Headline — `bucket()` co-partitioning + MERGE (copy-on-write)

- Because `silver.fact_event` is partitioned by `bucket(16, user_id)`, all of a
  user's rows live in one bucket per date partition.
- `DELETE FROM silver.fact_event WHERE user_id = :id` (or MERGE) rewrites only the
  affected bucket files — a small, bounded rewrite. Cascade to gold products that
  carry PII.
- **Demonstrate efficiency:** log the number of data files rewritten and bytes
  touched; compare against an unbucketed control table where the same delete
  rewrites the whole partition/table.

### 7.2 Second technique — merge-on-read equality deletes

- On a MoR-configured table, issue an **equality delete** on `user_id`: Iceberg
  writes a small delete file instead of rewriting data — the delete is near-instant
  to apply.
- Reads transparently exclude deleted rows; a later compaction
  (`rewrite_data_files`) reconciles and drops the delete files.
- **Contrast in docs:** copy-on-write (immediate rewrite, clean reads) vs
  merge-on-read (instant delete-file write, deferred reconciliation, read
  amplification until compaction). This is the real Iceberg governance trade-off.

### 7.3 Verification

After deletion, assert zero rows for the `user_id` across silver + gold (via Trino),
and confirm the row is gone in time-travel-forward snapshots while older snapshots
are then expired (so the data is truly unrecoverable — the actual GDPR requirement).

## 8. Performance before/after (`perf/`)

Required, not optional — this is the most-emphasized JD line and James's signature.

- **Bad version:** no partitioning, tiny files (many small streaming commits, no
  compaction), no sort.
- **Optimized version:** hidden partitioning (`days(event_ts)`,
  `bucket(user_id)`), `rewrite_data_files` compaction, sort/Z-order on common
  predicates.
- **Measure** a fixed set of representative queries (a pacing rollup, a
  user-filtered scan, a date-range fill query) on both, capture wall-clock + files
  scanned + bytes scanned, and write a **before/after table with real numbers** to
  `docs/`. The GDPR delete file-count comparison (§7.1) is a second before/after.

## 9. Orchestration (`airflow/`)

Full Airflow in Docker. DAGs:

1. `campaign_pull` — GET FastAPI `/campaigns` → `silver.dim_campaign` (scheduled).
2. `medallion_build` — bronze→silver→gold Spark SQL tasks, in dependency order.
3. `iceberg_maintenance` — `rewrite_data_files` compaction + `expire_snapshots` +
   orphan-file cleanup.
4. `gdpr_delete` — parameterized by `user_id`; runs §7 and verification.

Streaming ingest runs as its own long-lived service (not an Airflow task), since it
is continuous; Airflow owns the batch + maintenance + governance lifecycle.

## 10. Query layer (`trino/`)

Sample analyst SQL against gold: which campaigns are ahead/behind pace, fill rate by
placement, delivery by geo/device. Trino reads the same Iceberg tables Spark writes —
two engines, one table set.

## 11. Repo layout

```
ad-lakehouse/
  docker-compose.yml          # redpanda, minio, iceberg-rest, spark, trino, airflow, api
  docker/                     # per-service Dockerfiles/config
  generator/                  # synthetic event producer → Kafka
  api/                        # FastAPI campaigns service
  streaming/                  # Spark Structured Streaming → bronze
  transform/                  # Spark SQL bronze→silver→gold
  gdpr/                       # RTBF: bucket-MERGE + MoR equality deletes
  perf/                       # bad-vs-optimized benchmark harness
  airflow/dags/               # campaign_pull, medallion_build, iceberg_maintenance, gdpr_delete
  trino/                      # analyst queries
  tests/                      # pytest: generator, dedup/late-data, gdpr, stack smoke
  docs/                       # architecture, perf before/after table, blog post
  Makefile                    # up / seed / stream / build / demo / query / test
  README.md
```

## 12. Testing

- **Unit (pytest):** generator emits valid schema + injects the configured
  dupe/late rates; dedup keeps exactly one row per `event_id`; late events land in
  the correct partition; pacing math (`pace_index`, labels) correct on fixtures.
- **GDPR:** after delete, zero rows for the `user_id` across silver + gold; file
  rewrite count is bounded for the bucketed table and full for the control.
- **Stack smoke:** `make up` → produce N events → stream lands bronze → build →
  Trino returns expected gold counts.
- **CI:** ruff + pytest in GitHub Actions, mirroring the lab's hygiene.

## 13. Success criteria

- `make up && make seed && make stream && make build && make demo && make query`
  produces a working end-to-end run.
- Streaming ingest visibly lands live Kafka events into bronze.
- GDPR delete removes a user across the lakehouse and the docs show the
  bucketed-vs-control file-rewrite delta.
- `docs/` contains a before/after performance table with real measured numbers.
- All three gold products queryable in Trino; pacing labels campaigns
  ahead/on-track/behind.
- README maps each JD line to the piece that demonstrates it.

## 14. Out of scope (YAGNI)

- Flink (Spark Structured Streaming is sufficient and in-wheelhouse).
- GraphQL (REST is the common DE ingestion shape).
- Real PII/crypto-shredding vault (Approach C) — deferred; bucket-MERGE + MoR cover
  the JD's "row-level deletes / MERGE" ask without key-management overhead.
- Production auth, multi-tenant catalogs, cloud deploy.
