SELECT count(*) AS rows,
       count(DISTINCT event_id) AS distinct_ids,
       count(DISTINCT user_id) AS distinct_users
FROM iceberg.bronze.ad_events_raw;
