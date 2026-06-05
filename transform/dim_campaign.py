# transform/dim_campaign.py
import json
import os
import urllib.request

from pyspark.sql import Row, SparkSession

API_URL = os.environ.get("CAMPAIGNS_API_URL", "http://api:8000/campaigns")


def build(spark: SparkSession) -> None:
    """Pull campaign metadata from the FastAPI service and (re)write
    lh.silver.dim_campaign. Idempotent: createOrReplace each run."""
    with urllib.request.urlopen(API_URL, timeout=30) as resp:
        campaigns = json.load(resp)

    rows = [
        Row(
            campaign_id=c["campaign_id"],
            budget=int(c["budget"]),
            flight_start=c["flight_start"],
            flight_end=c["flight_end"],
            daily_budget=float(c["daily_budget"]),
            target_geo=c["target_geo"],
            target_device=c["target_device"],
        )
        for c in campaigns
    ]

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
    df = spark.createDataFrame(rows).selectExpr(
        "campaign_id",
        "budget",
        "to_date(flight_start) AS flight_start",
        "to_date(flight_end) AS flight_end",
        "daily_budget",
        "target_geo",
        "target_device",
    )
    df.writeTo("lh.silver.dim_campaign").using("iceberg").createOrReplace()
    print(f"[dim_campaign] wrote {len(rows)} campaigns")  # len(rows): avoid a redundant count() Spark job
