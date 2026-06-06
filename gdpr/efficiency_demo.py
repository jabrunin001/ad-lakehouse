"""Measure why bucket(user_id) makes a GDPR delete cheap.

Builds two throwaway copies of silver.fact_event — one bucketed by user_id (like
production silver), one partitioned only by date (the control) — deletes the SAME
user from each, and reads each delete's snapshot summary to compare the data
actually rewritten. Touches only the gdpr_demo namespace, never production.
"""
from pyspark.sql import SparkSession

from gdpr.forget_user import _USER_ID_RE
from streaming.spark_session import build_spark

BUCKETED = "lh.gdpr_demo.fact_event_bucketed"
UNBUCKETED = "lh.gdpr_demo.fact_event_unbucketed"


def _delete_metrics(spark: SparkSession, table: str, user_id: str) -> dict:
    """Copy-on-write DELETE, then read the rewrite cost from the snapshot summary.

    Iceberg 1.8.1's overwrite summary reports rewritten files via `deleted-data-files`
    / `removed-files-size`, the records that lived in those rewritten files via
    `deleted-records`, and the survivors rewritten back via `added-records`. The data
    actually rewritten by the copy-on-write delete is `deleted-records`; the user rows
    truly removed is `deleted-records` - `added-records`. `bytes_touched` is
    `removed-files-size` — the size of the files read and discarded by the rewrite
    (the read-side cost), not the smaller set of bytes written back.
    """
    spark.sql(f"DELETE FROM {table} WHERE user_id = '{user_id}'")
    row = spark.sql(
        f"SELECT summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()[0]
    s = row["summary"]
    records_rewritten = int(s.get("deleted-records", "0"))
    records_kept = int(s.get("added-records", "0"))
    return {
        "data_files_rewritten": int(s.get("deleted-data-files", "0")),
        "records_rewritten": records_rewritten,
        "bytes_touched": int(s.get("removed-files-size", "0")),
        "records_actually_deleted": records_rewritten - records_kept,
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
    if not _USER_ID_RE.match(user_id):  # consistency with forget_user.py
        raise ValueError(f"refusing unsafe user_id {user_id!r}")

    bucketed = _delete_metrics(spark, BUCKETED, user_id)
    unbucketed = _delete_metrics(spark, UNBUCKETED, user_id)
    # Both tables are identical copies, so the SAME user must have the SAME true row
    # count deleted — if not, the two measurements aren't comparing the same operation.
    assert bucketed["records_actually_deleted"] == unbucketed["records_actually_deleted"], (
        "bucketed/unbucketed deleted different row counts — measurements not comparable"
    )
    ratio = (unbucketed["records_rewritten"] / bucketed["records_rewritten"]
             if bucketed["records_rewritten"] else float("nan"))
    bytes_ratio = (unbucketed["bytes_touched"] / bucketed["bytes_touched"]
                   if bucketed["bytes_touched"] else float("nan"))

    print(f"[efficiency] user={user_id} deleted_rows={bucketed['records_actually_deleted']}")
    print(f"[efficiency] bucketed:   {bucketed}")
    print(f"[efficiency] unbucketed: {unbucketed}")
    print(f"[efficiency] records_rewritten ratio (unbucketed / bucketed) = {ratio:.1f}x")
    print(f"[efficiency] bytes_touched ratio     (unbucketed / bucketed) = {bytes_ratio:.1f}x")
    return {"user_id": user_id, "bucketed": bucketed, "unbucketed": unbucketed,
            "ratio": ratio, "bytes_ratio": bytes_ratio}


def main() -> None:
    spark = build_spark("gdpr-efficiency")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
