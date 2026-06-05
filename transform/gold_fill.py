# transform/gold_fill.py
from pyspark.sql import SparkSession


def build(spark: SparkSession) -> None:
    """gold.inventory_fill: per campaign x placement x hour, the request count,
    impression count, and fill_rate = impressions / requests. Computed from the
    ad_request vs impression rows in silver. CREATE OR REPLACE = idempotent.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.gold")
    spark.sql(
        """
        CREATE OR REPLACE TABLE lh.gold.inventory_fill
        USING iceberg
        PARTITIONED BY (days(event_hour))
        AS
        SELECT
          campaign_id, placement, date_trunc('HOUR', event_ts) AS event_hour,
          sum(CASE WHEN event_type = 'ad_request' THEN 1 ELSE 0 END) AS requests,
          sum(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) AS impressions,
          CASE WHEN sum(CASE WHEN event_type = 'ad_request' THEN 1 ELSE 0 END) > 0
               THEN sum(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) * 1.0
                    / sum(CASE WHEN event_type = 'ad_request' THEN 1 ELSE 0 END)
               ELSE NULL END AS fill_rate
        FROM lh.silver.fact_event
        WHERE event_type IN ('ad_request', 'impression')
        GROUP BY campaign_id, placement, date_trunc('HOUR', event_ts)
        """
    )
    n = spark.sql("SELECT count(*) AS c FROM lh.gold.inventory_fill").collect()[0]["c"]
    print(f"[gold_fill] wrote {n} campaign x placement x hour rows")
