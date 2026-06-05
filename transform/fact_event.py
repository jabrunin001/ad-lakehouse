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
