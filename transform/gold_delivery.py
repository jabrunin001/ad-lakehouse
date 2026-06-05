# transform/gold_delivery.py
from pyspark.sql import SparkSession


def build(spark: SparkSession) -> None:
    """gold.fact_impression_delivery: one row per impression, with quartile
    completion flags folded in from quartile events sharing the request_id.

    Grain = request_id. This is safe because silver.fact_event is deduped on
    event_id and request_session emits at most one impression per request, so a
    request_id maps to a single impression row. If that upstream guarantee ever
    changed, the GROUP BY would silently merge impressions — guard then.
    CREATE OR REPLACE makes it idempotent.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.gold")
    spark.sql(
        """
        CREATE OR REPLACE TABLE lh.gold.fact_impression_delivery
        USING iceberg
        PARTITIONED BY (days(impression_ts))
        AS
        SELECT
          i.request_id, i.event_ts AS impression_ts, i.campaign_id, i.creative_id,
          i.user_id, i.device, i.geo, i.placement,
          coalesce(bool_or(q.event_type = 'q25'),  false) AS completed_q25,
          coalesce(bool_or(q.event_type = 'q50'),  false) AS completed_q50,
          coalesce(bool_or(q.event_type = 'q75'),  false) AS completed_q75,
          coalesce(bool_or(q.event_type = 'q100'), false) AS completed_q100
        FROM (SELECT * FROM lh.silver.fact_event WHERE event_type = 'impression') i
        LEFT JOIN (
          SELECT request_id, event_type FROM lh.silver.fact_event
          WHERE event_type IN ('q25', 'q50', 'q75', 'q100')
        ) q ON i.request_id = q.request_id
        GROUP BY i.request_id, i.event_ts, i.campaign_id, i.creative_id,
                 i.user_id, i.device, i.geo, i.placement
        """
    )
    # Read-back count: this is a CTAS, so query the persisted table to log what landed.
    n = spark.sql("SELECT count(*) AS c FROM lh.gold.fact_impression_delivery").collect()[0]["c"]
    print(f"[gold_delivery] wrote {n} impression rows")
