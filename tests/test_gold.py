# tests/test_gold.py
import subprocess

import pytest


def _trino(sql: str) -> list[str]:
    out = subprocess.check_output(
        ["docker", "compose", "exec", "-T", "trino", "trino", "--execute", sql],
        text=True,
    )
    return out.strip().strip('"').split('","')


@pytest.mark.integration
def test_delivery_one_row_per_impression():
    delivery, impressions = _trino(
        "SELECT (SELECT count(*) FROM iceberg.gold.fact_impression_delivery), "
        "(SELECT count(*) FROM iceberg.silver.fact_event WHERE event_type='impression')"
    )
    assert int(delivery) == int(impressions) and int(delivery) > 0


@pytest.mark.integration
def test_quartile_completion_is_nested():
    (violations,) = _trino(
        "SELECT count(*) FROM iceberg.gold.fact_impression_delivery "
        "WHERE (completed_q50 AND NOT completed_q25) "
        "OR (completed_q75 AND NOT completed_q50) "
        "OR (completed_q100 AND NOT completed_q75)"
    )
    assert int(violations) == 0


@pytest.mark.integration
def test_fill_rate_is_non_negative():
    # per-bucket fill_rate can exceed 1 (impression in a different hour than its
    # ad_request), so we only assert non-negativity; the overall rate ~fill_prob.
    (bad,) = _trino(
        "SELECT count(*) FROM iceberg.gold.inventory_fill "
        "WHERE fill_rate IS NOT NULL AND fill_rate < 0"
    )
    assert int(bad) == 0


@pytest.mark.integration
def test_pacing_cumulative_is_monotonic():
    (decreases,) = _trino(
        "SELECT count(*) FROM ("
        "  SELECT cumulative_delivered - lag(cumulative_delivered) OVER "
        "  (PARTITION BY campaign_id ORDER BY pacing_date) AS delta "
        "  FROM iceberg.gold.campaign_pacing"
        ") WHERE delta < 0"
    )
    assert int(decreases) == 0


@pytest.mark.integration
def test_pacing_labels_vary():
    labels = _trino("SELECT count(DISTINCT pace_label) FROM iceberg.gold.campaign_pacing")
    assert int(labels[0]) >= 2
