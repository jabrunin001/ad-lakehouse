from datetime import date
from api.campaigns import build_campaigns, Campaign, N_CAMPAIGNS

REF = date(2026, 6, 4)

def test_builds_expected_count_and_ids():
    cs = build_campaigns(REF)
    assert len(cs) == N_CAMPAIGNS
    assert cs[0].campaign_id == "cmp-001"
    assert cs[-1].campaign_id == f"cmp-{N_CAMPAIGNS:03d}"

def test_flights_bracket_the_reference_date():
    for c in build_campaigns(REF):
        assert c.flight_start < REF < c.flight_end

def test_daily_budget_matches_budget_over_flight_days():
    for c in build_campaigns(REF):
        days = (c.flight_end - c.flight_start).days
        assert abs(c.daily_budget - c.budget / days) < 0.01

def test_deterministic():
    assert [c.model_dump() for c in build_campaigns(REF)] == \
           [c.model_dump() for c in build_campaigns(REF)]

def test_is_campaign_instances():
    assert all(isinstance(c, Campaign) for c in build_campaigns(REF))

def test_pinned_values_cmp001():
    # Pin a known-seed campaign so a future change to the draw order or RNG
    # (which would silently shift the campaign metadata events join against)
    # fails loudly instead of corrupting downstream pacing.
    c = build_campaigns(REF)[0]
    assert c.campaign_id == "cmp-001"
    assert c.budget == 429
    assert c.target_geo == "GB-LND"
    assert c.target_device == "mobile"
    assert (c.flight_end - c.flight_start).days == 17

def test_budget_is_calibrated_to_demo_volume():
    for c in build_campaigns(REF):
        assert 300 <= c.budget <= 1500
