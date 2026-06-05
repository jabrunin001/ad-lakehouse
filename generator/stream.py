# generator/stream.py
from __future__ import annotations

import random
from collections.abc import Iterator
from datetime import datetime, timedelta

from generator.event import AdEvent
from generator.session import request_session


def event_batch(
    n_requests: int,
    now: datetime,
    dup_rate: float,
    late_rate: float,
    seed: int = 0,
    fill_prob: float = 0.7,
) -> Iterator[AdEvent]:
    """Yield the events of n_requests correlated ad requests.

    Each request contributes an ad_request plus its causal impression/quartile
    events (sharing request_id) via request_session(). On top of that raw
    stream, ~late_rate of events are backdated to simulate late arrival, and
    ~dup_rate are re-emitted as exact duplicates (same event_id). Cleaning both
    up is the silver layer's job — bronze keeps them.
    """
    r = random.Random(seed)
    for i in range(n_requests):
        for ev in request_session(seed=seed * 1_000_003 + i, now=now, fill_prob=fill_prob):
            if r.random() < late_rate:
                ev = ev.model_copy(update={"event_ts": now - timedelta(minutes=r.randint(2, 240))})
            yield ev
            if r.random() < dup_rate:
                yield ev  # same event_id -> duplicate
