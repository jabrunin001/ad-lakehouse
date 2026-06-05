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
    spread_days: float = 0.0,
) -> Iterator[AdEvent]:
    """Yield the events of n_requests correlated ad requests.

    Each request contributes an ad_request plus its causal impression/quartile
    events (sharing request_id) via request_session(). The request's base time is
    drawn uniformly from the last `spread_days` days (0 = all at `now`) so delivery
    accumulates over a flight rather than in a single spike. On top of that, ~late_rate
    of events are backdated a further 2-240 min (late arrival), and ~dup_rate are
    re-emitted as exact duplicates (same event_id). Cleaning both up is silver's job.
    """
    r = random.Random(seed)
    for i in range(n_requests):
        base = now - timedelta(seconds=r.random() * spread_days * 86_400)
        for ev in request_session(seed=seed * 1_000_003 + i, now=base, fill_prob=fill_prob):
            if r.random() < late_rate:
                ev = ev.model_copy(
                    update={"event_ts": ev.event_ts - timedelta(minutes=r.randint(2, 240))}
                )
            yield ev
            if r.random() < dup_rate:
                yield ev  # same event_id -> duplicate
