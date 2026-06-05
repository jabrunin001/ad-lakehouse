# tests/test_smoke.py
import subprocess

import pytest


def _trino(sql: str) -> str:
    out = subprocess.check_output(
        ["docker", "compose", "exec", "-T", "trino", "trino", "--execute", sql],
        text=True,
    )
    return out.strip().strip('"')


@pytest.mark.integration
def test_bronze_has_rows():
    assert int(_trino("SELECT count(*) FROM iceberg.bronze.ad_events_raw")) > 0


@pytest.mark.integration
def test_bronze_preserves_duplicates():
    # The generator injects duplicate events (same event_id). Bronze is
    # append-only and must NOT dedup, so total rows exceed distinct event_ids.
    # This is the bronze/silver boundary the design sells: dedup happens later.
    rows, distinct = _trino(
        "SELECT count(*), count(DISTINCT event_id) FROM iceberg.bronze.ad_events_raw"
    ).split('","')
    assert int(rows) > int(distinct)
