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
