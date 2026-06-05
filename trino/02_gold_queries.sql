-- Headline pacing: latest snapshot per campaign, ranked by pace_index
WITH latest AS (
  SELECT campaign_id, max(pacing_date) AS d FROM iceberg.gold.campaign_pacing GROUP BY campaign_id
)
SELECT p.campaign_id, p.pacing_date, p.cumulative_delivered,
       round(p.expected_pace, 1) AS expected, round(p.pace_index, 3) AS pace_index, p.pace_label
FROM iceberg.gold.campaign_pacing p
JOIN latest l ON p.campaign_id = l.campaign_id AND p.pacing_date = l.d
ORDER BY p.pace_index DESC;

-- Fill rate by placement (nullif guard mirrors gold_fill.py's requests=0 -> NULL)
SELECT placement, sum(requests) AS requests, sum(impressions) AS impressions,
       round(sum(impressions) * 1.0 / nullif(sum(requests), 0), 3) AS fill_rate
FROM iceberg.gold.inventory_fill
GROUP BY placement ORDER BY fill_rate DESC;

-- Completion funnel from the delivery fact
SELECT count(*) AS impressions,
       round(avg(CAST(completed_q25 AS int)), 3)  AS q25_rate,
       round(avg(CAST(completed_q50 AS int)), 3)  AS q50_rate,
       round(avg(CAST(completed_q75 AS int)), 3)  AS q75_rate,
       round(avg(CAST(completed_q100 AS int)), 3) AS q100_rate
FROM iceberg.gold.fact_impression_delivery;
