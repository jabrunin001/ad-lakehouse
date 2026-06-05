# tests/test_silver.py
import subprocess

import pytest


def _trino(sql: str) -> list[str]:
    out = subprocess.check_output(
        ["docker", "compose", "exec", "-T", "trino", "trino", "--execute", sql],
        text=True,
    )
    return out.strip().strip('"').split('","')


@pytest.mark.integration
def test_fact_event_is_deduped():
    rows, distinct = _trino(
        "SELECT count(*), count(DISTINCT event_id) FROM iceberg.silver.fact_event"
    )
    assert int(rows) == int(distinct)


@pytest.mark.integration
def test_dim_campaign_has_twenty():
    (n,) = _trino("SELECT count(*) FROM iceberg.silver.dim_campaign")
    assert int(n) == 20


@pytest.mark.integration
def test_every_event_joins_to_a_campaign():
    events, joinable = _trino(
        "SELECT (SELECT count(*) FROM iceberg.silver.fact_event), "
        "(SELECT count(*) FROM iceberg.silver.fact_event f "
        "JOIN iceberg.silver.dim_campaign d ON f.campaign_id = d.campaign_id)"
    )
    assert int(events) == int(joinable) and int(events) > 0


@pytest.mark.integration
def test_impressions_link_to_ad_requests_in_silver():
    (n,) = _trino(
        "SELECT count(*) FROM iceberg.silver.fact_event i "
        "WHERE i.event_type='impression' AND NOT EXISTS "
        "(SELECT 1 FROM iceberg.silver.fact_event r "
        "WHERE r.event_type='ad_request' AND r.request_id = i.request_id)"
    )
    assert int(n) == 0  # no orphan impressions
