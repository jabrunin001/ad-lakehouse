from __future__ import annotations
import random
from datetime import datetime
from pydantic import BaseModel

EVENT_TYPES = ("ad_request", "impression", "q25", "q50", "q75", "q100")
DEVICES = ("mobile", "desktop", "ctv")
GEOS = ("US-CA", "US-NY", "GB-LND", "DE-BE", "JP-13")
PLACEMENTS = ("preroll", "midroll", "banner_top", "banner_side")


class AdEvent(BaseModel):
    event_id: str
    event_type: str
    event_ts: datetime
    campaign_id: str
    creative_id: str
    request_id: str
    user_id: str
    device: str
    geo: str
    placement: str


def make_event(seed: int, now: datetime) -> AdEvent:
    r = random.Random(seed)
    return AdEvent(
        event_id=f"evt-{r.getrandbits(64):016x}",
        event_type=r.choice(EVENT_TYPES),
        event_ts=now,
        campaign_id=f"cmp-{r.randint(1, 20):03d}",
        creative_id=f"crv-{r.randint(1, 60):03d}",
        request_id=f"req-{r.getrandbits(48):012x}",
        user_id=f"usr-{r.randint(1, 5000):05d}",
        device=r.choice(DEVICES),
        geo=r.choice(GEOS),
        placement=r.choice(PLACEMENTS),
    )
