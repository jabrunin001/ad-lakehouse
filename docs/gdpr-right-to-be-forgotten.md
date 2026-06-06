# GDPR right-to-be-forgotten on the lakehouse

How this lakehouse erases a user, why "erase" means *physically gone* and not
just *hidden*, and the two Iceberg techniques that make the delete cheap and
verifiable.

## The requirement

**GDPR Art. 17 (right to erasure).** When a data subject requests erasure, their
personal data must be removed — *unrecoverable*, not merely filtered out of the
default view. A lakehouse with snapshot history makes this non-trivial: a plain
`DELETE` writes a *new* snapshot without the rows, but every *prior* snapshot
still holds them, and time-travel (or a rollback) brings the user straight back.
Logical deletion is not erasure.

**GDPR Art. 5(2) (accountability).** It is not enough to do the deletion — you
must be able to *demonstrate* it happened: which subject, across which tables,
and when. That requires a durable audit trail that itself survives the erasure.

## The design

A single driver — `gdpr/forget_user.py`, run by the `gdpr_delete` Airflow DAG —
does four things, in order:

1. **Row-level `DELETE` on every PII table.** Three tables carry a `user_id`
   column, so each gets a targeted delete:
   - `bronze.ad_events_raw` — the raw landed events.
   - `silver.fact_event` — the deduped event fact.
   - `gold.fact_impression_delivery` — one row per impression (per-user grain).
2. **Rebuild the aggregate gold tables.** `gold.inventory_fill` and
   `gold.campaign_pacing` have *no* `user_id` column — they are counts per
   placement/day and per campaign/day. You cannot row-delete a user from a
   count, so instead their builds are re-run against the now-cleaned silver,
   recomputing the aggregates so the user's contribution is dropped.
3. **Expire snapshots** on every touched table, so the pre-delete snapshots
   (which still hold the user) are physically removed — true erasure.
4. **Write an audit row** to `lh.gdpr.erasure_log` (`user_id`, `tables`,
   `erased_at`) for Art. 5(2) accountability. The log is a legally-retained
   record and is never itself a `forget()` target.

**The cross-layer subtlety:** bronze *must* be deleted. If you cleaned only
silver and gold, the next routine `build-silver` re-derives `silver.fact_event`
from bronze — and resurrects the user. Erasure has to run at the source of every
derivation, not just the serving layer.

## Technique 1 — `bucket(16, user_id)` co-partitioning (the headline)

`silver.fact_event` is partitioned by `bucket(16, user_id)`. Iceberg hashes each
`user_id` to one of 16 buckets and co-locates all of a user's rows in that one
bucket's files. A `DELETE ... WHERE user_id = 'usr-XXXXX'` can then *prune* to the
single bucket the user hashes into: the copy-on-write rewrite only touches that
bucket's data files, not the whole table.

To measure it, we deleted the same user (34 rows) from the bucketed
`silver.fact_event` and from an unbucketed control table:

| Layout      | Records rewritten | Bytes rewritten |
|-------------|-------------------|-----------------|
| bucketed    | 1,066             | 43,114          |
| unbucketed  | 15,484            | 374,814         |
| **ratio**   | **14.5x fewer**   | **8.7x fewer**  |

So the bucketed delete rewrote **14.5x fewer records** and **8.7x fewer bytes**.
The theoretical ceiling for `bucket(16, ...)` is 16x (one of sixteen buckets
touched); we land near it, reduced by real data skew (buckets are not perfectly
even). The absolute numbers are demo-scale — the *point* is the ratio, which holds
as the table grows: it is the difference between rewriting one partition and
rewriting the whole table on every erasure request.

## Technique 2 — merge-on-read deletes

The default copy-on-write `DELETE` rewrites data files. Iceberg also supports
**merge-on-read (MoR)**: the delete writes a small *delete file* that records
which rows are gone, and leaves the data files untouched until a later
compaction reconciles them. This trades cheap writes now for slightly more read
work until compaction.

Measured on the per-impression fact, deleting one user via MoR:

- wrote **4 position-delete files**;
- left the **data files unchanged (44 → 44)** — nothing was rewritten;
- reads immediately **excluded the user (0 rows)** — the delete files are
  applied at scan time;
- after **compaction**, the data is physically rewritten without the rows.

Spark writes **position deletes** (file + row-position pairs). The equality-delete
form — delete-by-predicate, applied without first finding row positions — is the
Flink/streaming-side technique; Spark's batch path uses position deletes.

**Honest Iceberg 1.8.1 detail:** after compaction, the delete-file manifest
entries persist as *dangling* references. They no longer apply to any live data
(0 of them match a row in the compacted files), but they linger in metadata
until expiry/cleanup prunes them. The data is correctly gone; the bookkeeping
just carries a harmless tail in this version.

## Verification & unrecoverability

- **Zero rows across layers.** The Task 4 integration test forgets a seeded user
  and asserts the post-delete counts are `(0, 0, 0)` across
  `bronze.ad_events_raw`, `silver.fact_event`, and `gold.fact_impression_delivery`.
- **Unrecoverable.** `expire_snapshots(retain_last => 1)` on every touched table
  drops the pre-delete snapshots, so there is no snapshot to time-travel or roll
  back to that still contains the user. If any table's expiry fails, the driver
  raises so the operator (or the DAG's retry) re-runs — it never silently leaves
  recoverable PII behind.
- **Accountable.** The audit row in `lh.gdpr.erasure_log` records the erasure —
  and only the tables that were *fully* erased and snapshot-expired, so a partial
  run records the truth rather than the intent.
- **Scope boundary.** Erasure is complete within the Iceberg catalog. Object-store
  versioning (MinIO/S3), filesystem backups, and downstream copies are a separate
  control — a real deployment pairs this with a backup/retention policy and
  `remove_orphan_files` so the expired data files are physically purged from S3.

## Operations

On demand, via the `gdpr_delete` Airflow DAG (the operator path):

```bash
airflow dags trigger gdpr_delete -c '{"user_id":"usr-XXXXX"}'
```

The DAG is `schedule=None` (on-demand only) and `docker exec`s the Spark
container to run `gdpr/forget_user.py`, the same driver `make` uses. Equivalently
from the CLI:

```bash
make forget-user UID=usr-XXXXX
```

Both paths are **destructive and permanent** — the user is unrecoverable after
snapshot expiry.

To *see* the two techniques without forgetting anyone (non-destructive demos):

```bash
make gdpr-efficiency   # bucketed vs unbucketed delete — the 14.5x / 8.7x numbers
make gdpr-mor          # merge-on-read: delete files written, data files unchanged
```
