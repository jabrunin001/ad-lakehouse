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
AMPLIFY = 10      # ~31k rows -> ~310k
BAD_FILES = 500   # force many tiny files


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

    # OPTIMIZED: hidden partitioning, then sort-compaction. rewrite_data_files
    # with min-input-files => 2 compacts any partition holding more than one
    # fragment and (re)sorts every partition by (campaign_id, event_ts), so the
    # optimized table is both pruned (day + user bucket) and clustered.
    spark.sql(
        f"CREATE OR REPLACE TABLE {OPTIMIZED} USING iceberg "
        f"PARTITIONED BY (days(event_ts), bucket(16, user_id)) "
        f"AS SELECT * FROM {SOURCE}"
    )
    spark.sql(
        "CALL lh.system.rewrite_data_files("
        "table => 'perf.events_optimized', strategy => 'sort', "
        "sort_order => 'campaign_id, event_ts', "
        "options => map('min-input-files', '2'))"
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
