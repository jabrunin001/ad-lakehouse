# gdpr/forget_user.py
import argparse
import re

from pyspark.sql import SparkSession

from streaming.spark_session import build_spark
from transform import gold_delivery, gold_fill, gold_pacing

# user_ids are synthetic ids like "usr-04897". Allowlist their character set so an
# operator- or DAG-supplied value can never break out of the DELETE predicate
# (this code interpolates it into Spark SQL).
_USER_ID_RE = re.compile(r"\A[A-Za-z0-9_-]+\Z")

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
    if not _USER_ID_RE.match(user_id):
        raise ValueError(f"refusing unsafe user_id {user_id!r} (expected [A-Za-z0-9_-]+)")
    for table in PII_TABLES:
        spark.sql(f"DELETE FROM lh.{table} WHERE user_id = '{user_id}'")
        print(f"[forget] deleted {user_id} from {table}")

    # Rebuild gold from cleaned silver.
    for mod in (gold_delivery, gold_fill, gold_pacing):
        mod.build(spark)

    now = spark.sql(
        "SELECT date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS t"
    ).collect()[0]["t"]
    # Attempt every table even if one fails — a mid-loop error must not silently
    # leave the user's pre-delete snapshots (and thus recoverable PII) on the
    # remaining tables. Collect failures and raise after attempting all.
    failed = []
    for table in TOUCHED_TABLES:
        try:
            spark.sql(
                f"CALL lh.system.expire_snapshots("
                f"table => '{table}', older_than => TIMESTAMP '{now}', retain_last => 1)"
            )
        except Exception as exc:  # noqa: BLE001 — record and continue
            failed.append(table)
            print(f"[forget] WARNING expire_snapshots failed for {table}: {exc}")

    _record_erasure(spark, user_id)
    if failed:
        raise RuntimeError(
            f"[forget] expire_snapshots failed for {failed}; PII may remain recoverable "
            f"there — re-run forget for {user_id}"
        )
    print(f"[forget] expired snapshots; {user_id} is unrecoverable")


def _record_erasure(spark: SparkSession, user_id: str) -> None:
    """Append an audit row — GDPR accountability (Art. 5(2)/17) means we must be
    able to demonstrate which user was erased, across which tables, and when. The
    log itself is a legally-retained record, so it is never a forget() target."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.gdpr")
    spark.sql(
        "CREATE TABLE IF NOT EXISTS lh.gdpr.erasure_log "
        "(user_id string, tables string, erased_at timestamp) USING iceberg"
    )
    spark.sql(
        f"INSERT INTO lh.gdpr.erasure_log "
        f"VALUES ('{user_id}', '{','.join(TOUCHED_TABLES)}', current_timestamp())"
    )
    print(f"[forget] audit: logged erasure of {user_id} to gdpr.erasure_log")


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
