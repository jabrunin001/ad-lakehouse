from __future__ import annotations

import random
from collections.abc import Iterator
from datetime import datetime, timedelta

from generator.event import AdEvent, make_event


def event_batch(
    n: int,
    now: datetime,
    dup_rate: float,
    late_rate: float,
    seed: int = 0,
) -> Iterator[AdEvent]:
    """Yield n unique base events, plus duplicate re-emissions (~dup_rate).

    ~late_rate of base events are backdated to simulate late arrival;
    duplicates inherit the (possibly backdated) timestamp of their base event."""
    r = random.Random(seed)
    for i in range(n):
        ev = make_event(seed=seed * 1_000_003 + i, now=now)
        if r.random() < late_rate:
            ev = ev.model_copy(update={
                "event_ts": now - timedelta(minutes=r.randint(2, 240))
            })
        yield ev
        if r.random() < dup_rate:
            yield ev  # same event_id → duplicate
