# tests/test_gdpr.py
import subprocess

import pytest

PKGS = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,"
    "org.apache.iceberg:iceberg-aws-bundle:1.8.1,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)


def _spark(script: str, *args: str) -> str:
    # shuffle.partitions=8: the default 200 is wasteful on this ~31k-row demo and
    # the extra task overhead OOM-kills the gold rebuild on a memory-tight host.
    return subprocess.check_output(
        ["docker", "exec", "-e", "PYTHONPATH=/opt/app", "ad-lakehouse-spark",
         "/opt/spark/bin/spark-submit", "--conf", "spark.jars.ivy=/tmp/.ivy2",
         "--conf", "spark.sql.shuffle.partitions=8",
         "--packages", PKGS, f"/opt/app/{script}", *args],
        text=True, stderr=subprocess.STDOUT,
    )


def _trino(sql: str) -> list[str]:
    out = subprocess.check_output(
        ["docker", "compose", "exec", "-T", "trino", "trino", "--execute", sql],
        text=True,
    )
    return out.strip().strip('"').split('","')


@pytest.mark.integration
def test_forget_removes_user_across_layers():
    (uid,) = _trino(
        "SELECT user_id FROM iceberg.gold.fact_impression_delivery "
        "GROUP BY user_id ORDER BY count(*) DESC LIMIT 1"
    )
    _spark("gdpr/forget_user.py", "--user-id", uid)
    bronze, silver, gold = _trino(
        f"SELECT (SELECT count(*) FROM iceberg.bronze.ad_events_raw WHERE user_id='{uid}'), "
        f"(SELECT count(*) FROM iceberg.silver.fact_event WHERE user_id='{uid}'), "
        f"(SELECT count(*) FROM iceberg.gold.fact_impression_delivery WHERE user_id='{uid}')"
    )
    assert (int(bronze), int(silver), int(gold)) == (0, 0, 0)


@pytest.mark.integration
def test_bucketing_makes_delete_cheaper():
    out = _spark("gdpr/efficiency_demo.py")
    # the records_rewritten ratio line specifically (not the bytes line)
    line = [ln for ln in out.splitlines() if "records_rewritten ratio (unbucketed / bucketed)" in ln][-1]
    ratio = float(line.rsplit("=", 1)[1].strip().rstrip("x"))
    assert ratio > 1.5  # bucketed rewrites materially less than unbucketed


@pytest.mark.integration
def test_mor_delete_writes_delete_file_not_data_rewrite():
    out = _spark("gdpr/mor_demo.py")
    assert "rows for user after delete (reads exclude them): 0" in out
    line = [ln for ln in out.splitlines() if "delete wrote delete_files=" in ln][-1]
    assert "data_files unchanged" in line
