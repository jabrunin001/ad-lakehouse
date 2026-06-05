from datetime import datetime, timezone
from generator.session import request_session, QUARTILES

NOW = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)

def test_always_starts_with_one_ad_request():
    evs = request_session(seed=1, now=NOW, fill_prob=0.0)
    assert [e.event_type for e in evs] == ["ad_request"]

def test_all_events_share_request_and_dims():
    evs = request_session(seed=3, now=NOW, fill_prob=1.0, quartile_probs=(1, 1, 1, 1))
    rid = {e.request_id for e in evs}
    cid = {e.campaign_id for e in evs}
    uid = {e.user_id for e in evs}
    assert len(rid) == 1 and len(cid) == 1 and len(uid) == 1

def test_full_fill_yields_request_impression_and_four_quartiles():
    evs = request_session(seed=3, now=NOW, fill_prob=1.0, quartile_probs=(1, 1, 1, 1))
    assert [e.event_type for e in evs] == ["ad_request", "impression", *QUARTILES]

def test_quartiles_are_nested_no_gap():
    # with q25 prob 1 but q50 prob 0, we get q25 and then stop
    evs = request_session(seed=5, now=NOW, fill_prob=1.0, quartile_probs=(1, 0, 1, 1))
    assert [e.event_type for e in evs] == ["ad_request", "impression", "q25"]

def test_quartiles_only_after_impression():
    evs = request_session(seed=7, now=NOW, fill_prob=0.0, quartile_probs=(1, 1, 1, 1))
    assert all(e.event_type == "ad_request" for e in evs)

def test_event_ids_unique_within_session():
    evs = request_session(seed=9, now=NOW, fill_prob=1.0, quartile_probs=(1, 1, 1, 1))
    assert len({e.event_id for e in evs}) == len(evs)

def test_deterministic():
    a = request_session(seed=11, now=NOW)
    b = request_session(seed=11, now=NOW)
    assert [e.model_dump() for e in a] == [e.model_dump() for e in b]
