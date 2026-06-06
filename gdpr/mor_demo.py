# gdpr/mor_demo.py
"""Contrast copy-on-write vs merge-on-read deletes.

A MoR-configured table answers a DELETE by writing a small delete file instead of
rewriting data files — the delete is near-instant; reads transparently exclude the
rows; a later compaction rewrites the data without the deleted rows, leaving the
delete files dangling (no longer applied). Demonstrated on a throwaway gdpr_demo table.

Spark SQL DELETE on a MoR table writes POSITION deletes (equality deletes are the
Flink/streaming-upsert form). The contrast that matters is delete-file write vs
data rewrite, not the delete encoding.
"""
from pyspark.sql import SparkSession

from gdpr.forget_user import _USER_ID_RE
from streaming.spark_session import build_spark

MOR = "lh.gdpr_demo.fact_event_mor"


def run(spark: SparkSession) -> dict:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.gdpr_demo")
    # Drop first so reruns start clean (CREATE OR REPLACE keeps prior snapshots,
    # which would carry stale delete files into this run's metadata counts).
    spark.sql(f"DROP TABLE IF EXISTS {MOR} PURGE")
    # format-version=2 is required for delete files; merge-on-read selects the
    # delete-file (no data rewrite) path for DELETE.
    spark.sql(
        f"CREATE OR REPLACE TABLE {MOR} USING iceberg "
        f"TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read') "
        f"AS SELECT * FROM lh.silver.fact_event"
    )
    user_id = spark.sql(
        f"SELECT user_id FROM {MOR} GROUP BY user_id ORDER BY count(*) DESC LIMIT 1"
    ).collect()[0]["user_id"]
    if not _USER_ID_RE.match(user_id):
        raise ValueError(f"refusing unsafe user_id {user_id!r}")
    data_files_before = spark.sql(f"SELECT count(*) AS c FROM {MOR}.data_files").collect()[0]["c"]

    spark.sql(f"DELETE FROM {MOR} WHERE user_id = '{user_id}'")
    summary = spark.sql(
        f"SELECT summary FROM {MOR}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()[0]["summary"]
    delete_files = spark.sql(f"SELECT count(*) AS c FROM {MOR}.delete_files").collect()[0]["c"]
    data_files_after = spark.sql(f"SELECT count(*) AS c FROM {MOR}.data_files").collect()[0]["c"]
    remaining = spark.sql(f"SELECT count(*) AS c FROM {MOR} WHERE user_id = '{user_id}'").collect()[0]["c"]

    added_pos = summary.get("added-position-delete-files", summary.get("added-delete-files", "0"))
    print(f"[mor] user={user_id}")
    print(f"[mor] delete wrote delete_files={delete_files} (position deletes), "
          f"added-position-delete-files={added_pos}, "
          f"data_files unchanged: {data_files_before} -> {data_files_after}")
    print(f"[mor] rows for user after delete (reads exclude them): {remaining}")

    # Compaction reconciles: rewrite_data_files reads through the position deletes and
    # writes fresh data files without the deleted rows (the old data files are
    # replaced). After this the position-delete files are DANGLING — their
    # referenced_data_file is no longer a live data file, so reads never consult them.
    # NOTE: in Iceberg 1.8.1 the rewrite swaps the data but does NOT prune the now-
    # dangling delete-file manifest entries, so `.delete_files` still *counts* them;
    # the honest reconciliation signal is that none of them still references live data.
    spark.sql("CALL lh.system.rewrite_data_files(table => 'gdpr_demo.fact_event_mor')")
    live_data = {r["file_path"] for r in spark.sql(f"SELECT file_path FROM {MOR}.data_files").collect()}
    delete_refs = [
        r["referenced_data_file"]
        for r in spark.sql(f"SELECT referenced_data_file FROM {MOR}.delete_files").collect()
    ]
    delete_files_after_compact = len(delete_refs)
    deletes_still_applied = sum(1 for ref in delete_refs if ref in live_data)
    rows_total = spark.sql(f"SELECT count(*) AS c FROM {MOR}").collect()[0]["c"]
    print(f"[mor] after compaction: data rewritten -> {len(live_data)} data file(s), "
          f"{rows_total} rows (deleted user's rows physically gone)")
    print(f"[mor] after compaction: delete_files still counted={delete_files_after_compact}, "
          f"but applying to live data={deletes_still_applied} (dangling -> reconciled)")
    return {
        "user_id": user_id,
        "delete_files": delete_files,
        "data_files_unchanged": data_files_before == data_files_after,
        "rows_remaining": remaining,
        "delete_files_after_compact": delete_files_after_compact,
        "deletes_still_applied": deletes_still_applied,
    }


def main() -> None:
    spark = build_spark("gdpr-mor")
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
