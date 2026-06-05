# streaming/ingest_bronze.py
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, TimestampType)
from streaming.spark_session import build_spark

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("campaign_id", StringType()),
    StructField("creative_id", StringType()),
    StructField("request_id", StringType()),
    StructField("user_id", StringType()),
    StructField("device", StringType()),
    StructField("geo", StringType()),
    StructField("placement", StringType()),
])


def main() -> None:
    spark = build_spark("ingest-bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.bronze")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lh.bronze.ad_events_raw (
          event_id string, event_type string, event_ts timestamp,
          campaign_id string, creative_id string, request_id string,
          user_id string, device string, geo string, placement string,
          kafka_ts timestamp, ingest_ts timestamp
        ) USING iceberg
        PARTITIONED BY (days(ingest_ts))
    """)

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", "redpanda:9092")
           .option("subscribe", "ad_events")
           .option("startingOffsets", "earliest")
           .load())

    parsed = (raw.select(
                F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
                F.col("timestamp").alias("kafka_ts"))
              .select("e.*", "kafka_ts")
              .withColumn("ingest_ts", F.current_timestamp()))

    query = (parsed.writeStream
             .format("iceberg")
             .outputMode("append")
             .option("checkpointLocation", "/opt/app/.checkpoints/bronze_ad_events")
             .trigger(processingTime="10 seconds")
             .toTable("lh.bronze.ad_events_raw"))
    query.awaitTermination()


if __name__ == "__main__":
    main()
