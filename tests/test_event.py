from datetime import datetime, timezone
from generator.event import make_event, EVENT_TYPES

def test_make_event_has_all_required_fields():
    ev = make_event(seed=1, now=datetime(2026, 6, 1, tzinfo=timezone.utc))
    d = ev.model_dump()
    for field in ["event_id", "event_type", "event_ts", "campaign_id",
                  "creative_id", "request_id", "user_id", "device", "geo", "placement"]:
        assert field in d and d[field] is not None

def test_event_type_is_valid():
    ev = make_event(seed=2, now=datetime(2026, 6, 1, tzinfo=timezone.utc))
    assert ev.event_type in EVENT_TYPES

def test_seed_is_deterministic():
    now = datetime(2026, 6, 1, tzinfo=timezone.utc)
    assert make_event(seed=7, now=now).model_dump() == make_event(seed=7, now=now).model_dump()
