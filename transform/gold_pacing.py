# transform/gold_pacing.py
from pyspark.sql import SparkSession


def build(spark: SparkSession) -> None:
    """gold.campaign_pacing: per campaign x day over its flight (up to the latest
    delivery date), delivered impressions, cumulative delivered, the linearly
    expected pace (budget * elapsed-flight-fraction), pace_index = cumulative /
    expected, and an ahead|on_track|behind label. The dim_campaign join is what
    makes this product exist. CREATE OR REPLACE = idempotent.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.gold")
    spark.sql(
        """
        CREATE OR REPLACE TABLE lh.gold.campaign_pacing
        USING iceberg
        PARTITIONED BY (campaign_id)
        AS
        WITH as_of AS (
          SELECT max(CAST(event_ts AS DATE)) AS as_of_date FROM lh.silver.fact_event
        ),
        daily AS (
          SELECT campaign_id, CAST(event_ts AS DATE) AS pacing_date,
                 count(*) AS delivered_impressions
          FROM lh.silver.fact_event
          WHERE event_type = 'impression'
          GROUP BY campaign_id, CAST(event_ts AS DATE)
        ),
        calendar AS (
          SELECT c.campaign_id, c.budget, c.flight_start, c.flight_end,
                 explode(sequence(c.flight_start, c.flight_end, interval 1 day)) AS pacing_date
          FROM lh.silver.dim_campaign c
        ),
        windowed AS (
          SELECT cal.campaign_id, cal.pacing_date, cal.budget,
                 cal.flight_start, cal.flight_end,
                 coalesce(d.delivered_impressions, 0) AS delivered_impressions
          FROM calendar cal
          CROSS JOIN as_of a
          LEFT JOIN daily d
            ON cal.campaign_id = d.campaign_id AND cal.pacing_date = d.pacing_date
          WHERE cal.pacing_date <= a.as_of_date
        ),
        cum AS (
          SELECT *,
            sum(delivered_impressions) OVER (
              PARTITION BY campaign_id ORDER BY pacing_date
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cumulative_delivered,
            datediff(pacing_date, flight_start) + 1 AS days_elapsed,
            datediff(flight_end, flight_start) + 1 AS flight_days
          FROM windowed
        )
        SELECT
          campaign_id, pacing_date, delivered_impressions, cumulative_delivered, budget,
          budget * (days_elapsed / CAST(flight_days AS DOUBLE)) AS expected_pace,
          cumulative_delivered
            / nullif(budget * (days_elapsed / CAST(flight_days AS DOUBLE)), 0) AS pace_index,
          CASE
            WHEN cumulative_delivered
                 / nullif(budget * (days_elapsed / CAST(flight_days AS DOUBLE)), 0) >= 1.05
              THEN 'ahead'
            WHEN cumulative_delivered
                 / nullif(budget * (days_elapsed / CAST(flight_days AS DOUBLE)), 0) <= 0.95
              THEN 'behind'
            ELSE 'on_track'
          END AS pace_label
        FROM cum
        """
    )
    n = spark.sql("SELECT count(*) AS c FROM lh.gold.campaign_pacing").collect()[0]["c"]
    print(f"[gold_pacing] wrote {n} campaign-day rows")
