# streaming/spark_session.py
import os
from pyspark.sql import SparkSession

ICEBERG_PKGS = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,"
    "org.apache.iceberg:iceberg-aws-bundle:1.8.1,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)


def build_spark(app_name: str) -> SparkSession:
    rest_uri = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    s3_endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", ICEBERG_PKGS)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lh", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lh.type", "rest")
        .config("spark.sql.catalog.lh.uri", rest_uri)
        .config("spark.sql.catalog.lh.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.lh.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lh.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.lh.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", "lh")
        .getOrCreate()
    )
