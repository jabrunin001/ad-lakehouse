from datetime import datetime, timezone, timedelta
from generator.stream import event_batch

NOW = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)


def test_batch_size_includes_injected_duplicates():
    events = list(event_batch(n=1000, now=NOW, dup_rate=0.02, late_rate=0.05, seed=0))
    # duplicates are extra emissions on top of n unique base events
    ids = [e.event_id for e in events]
    assert len(ids) > 1000
    assert len(set(ids)) == 1000  # exactly n unique base events


def test_duplicate_rate_in_tolerance():
    events = list(event_batch(n=5000, now=NOW, dup_rate=0.02, late_rate=0.0, seed=1))
    extra = len(events) - 5000
    assert 0.01 <= extra / 5000 <= 0.03


def test_late_events_are_backdated():
    events = list(event_batch(n=5000, now=NOW, dup_rate=0.0, late_rate=0.05, seed=2))
    late = [e for e in events if e.event_ts < NOW - timedelta(minutes=1)]
    assert 0.03 <= len(late) / 5000 <= 0.07
