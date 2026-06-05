# transform/maintenance.py
from pyspark.sql import SparkSession

# Tables to maintain (catalog 'lh' is the default in build_spark).
TABLES = [
    "bronze.ad_events_raw",
    "silver.fact_event",
    "silver.dim_campaign",
    "gold.fact_impression_delivery",
    "gold.inventory_fill",
    "gold.campaign_pacing",
]


def build(spark: SparkSession) -> None:
    """Iceberg table maintenance: compact small files (rewrite_data_files) and
    expire old snapshots (retain only the newest), per table. Idempotent and safe
    to run repeatedly. Named build(spark) for uniformity with the other driver
    targets even though it runs maintenance, not a table build.
    """
    now = spark.sql(
        "SELECT date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS t"
    ).collect()[0]["t"]
    for table in TABLES:
        before = spark.sql(f"SELECT count(*) AS c FROM lh.{table}.snapshots").collect()[0]["c"]
        spark.sql(f"CALL lh.system.rewrite_data_files(table => '{table}')")
        # older_than = now makes every existing snapshot eligible; retain_last keeps the newest.
        spark.sql(
            f"CALL lh.system.expire_snapshots("
            f"table => '{table}', older_than => TIMESTAMP '{now}', retain_last => 1)"
        )
        after = spark.sql(f"SELECT count(*) AS c FROM lh.{table}.snapshots").collect()[0]["c"]
        print(f"[maintenance] {table}: snapshots {before} -> {after}")
