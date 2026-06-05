# tests/test_stream.py
from datetime import datetime, timezone, timedelta
from generator.stream import event_batch

NOW = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)

def test_yields_at_least_one_event_per_request():
    events = list(event_batch(n_requests=1000, now=NOW, dup_rate=0.0, late_rate=0.0,
                              seed=0, fill_prob=0.0))
    # fill_prob 0 -> exactly one ad_request per request, no dups, no late
    assert len(events) == 1000
    assert all(e.event_type == "ad_request" for e in events)

def test_impressions_have_a_matching_ad_request():
    events = list(event_batch(n_requests=2000, now=NOW, dup_rate=0.0, late_rate=0.0,
                              seed=1, fill_prob=0.8))
    request_ids = {e.request_id for e in events if e.event_type == "ad_request"}
    impressions = [e for e in events if e.event_type == "impression"]
    assert impressions  # some fills happened
    assert all(e.request_id in request_ids for e in impressions)

def test_duplicates_inflate_total_but_not_distinct():
    events = list(event_batch(n_requests=2000, now=NOW, dup_rate=0.05, late_rate=0.0,
                              seed=2, fill_prob=0.7))
    ids = [e.event_id for e in events]
    assert len(ids) > len(set(ids))  # duplicates present

def test_late_events_are_backdated():
    events = list(event_batch(n_requests=3000, now=NOW, dup_rate=0.0, late_rate=0.05,
                              seed=3, fill_prob=0.7))
    late = [e for e in events if e.event_ts < NOW - timedelta(minutes=1)]
    assert 0.03 <= len(late) / len(events) <= 0.07
