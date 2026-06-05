# tests/test_smoke.py
import subprocess
import pytest


@pytest.mark.integration
def test_bronze_has_rows():
    out = subprocess.check_output(
        ["docker", "compose", "exec", "-T", "trino", "trino",
         "--execute", "SELECT count(*) FROM iceberg.bronze.ad_events_raw"],
        text=True,
    )
    assert int(out.strip().strip('"')) > 0
