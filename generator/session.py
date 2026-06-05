from __future__ import annotations

import random
from datetime import datetime, timedelta

from generator.event import AdEvent, make_event

QUARTILES = ("q25", "q50", "q75", "q100")


def request_session(
    seed: int,
    now: datetime,
    fill_prob: float = 0.7,
    quartile_probs: tuple[float, ...] = (0.9, 0.75, 0.55, 0.35),
) -> list[AdEvent]:
    """One ad request and its causal follow-ons, all sharing one request_id.

    Always emits an ad_request. With probability fill_prob the request is
    filled (an impression follows, same request_id/campaign/creative/user/
    device/geo/placement). If filled, quartile completions follow in nested
    order — q50 only after q25, etc. — each gated by its quartile_probs entry.
    Reuses make_event() purely as the source of the shared request dimensions.
    """
    base = make_event(seed=seed, now=now)
    r = random.Random((seed * 2_654_435_761) % (2**64))
    shared = dict(
        campaign_id=base.campaign_id,
        creative_id=base.creative_id,
        request_id=base.request_id,
        user_id=base.user_id,
        device=base.device,
        geo=base.geo,
        placement=base.placement,
    )

    def evt(event_type: str, ts: datetime) -> AdEvent:
        return AdEvent(
            event_id=f"evt-{r.getrandbits(64):016x}",
            event_type=event_type,
            event_ts=ts,
            **shared,
        )

    events = [evt("ad_request", now)]
    if r.random() < fill_prob:
        t = now + timedelta(seconds=1)
        events.append(evt("impression", t))
        for i, q in enumerate(QUARTILES):
            if r.random() < quartile_probs[i]:
                t += timedelta(seconds=2)
                events.append(evt(q, t))
            else:
                break
    return events
