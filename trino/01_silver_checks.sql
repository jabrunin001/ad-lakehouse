-- dedup + correlation sanity for the silver layer
SELECT
  (SELECT count(*) FROM iceberg.silver.fact_event) AS events,
  (SELECT count(DISTINCT event_id) FROM iceberg.silver.fact_event) AS distinct_events,
  (SELECT count(*) FROM iceberg.silver.dim_campaign) AS campaigns,
  (SELECT count(*) FROM iceberg.silver.fact_event f
     JOIN iceberg.silver.dim_campaign d ON f.campaign_id = d.campaign_id) AS joinable_events;
