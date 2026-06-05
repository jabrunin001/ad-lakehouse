from __future__ import annotations

import random
from datetime import date, timedelta

from pydantic import BaseModel

from generator.event import DEVICES, GEOS

N_CAMPAIGNS = 20


class Campaign(BaseModel):
    # The silver/gold join to events is on campaign_id only. target_geo and
    # target_device describe what the campaign *targets* (campaign attributes),
    # they are NOT join keys to AdEvent.geo / AdEvent.device.
    campaign_id: str
    budget: int          # total impression budget over the flight (a count of impressions)
    flight_start: date
    flight_end: date
    daily_budget: float  # budget spread evenly over the flight days
    target_geo: str
    target_device: str


def build_campaigns(reference: date) -> list[Campaign]:
    """Deterministic metadata for cmp-001..cmp-020. Flights bracket `reference`
    so freshly-generated events (timestamped ~now) fall inside each flight."""
    campaigns: list[Campaign] = []
    for i in range(1, N_CAMPAIGNS + 1):
        r = random.Random(i)
        start = reference - timedelta(days=r.randint(3, 12))
        end = reference + timedelta(days=r.randint(3, 12))
        budget = r.randint(50, 500) * 1000
        days = (end - start).days
        campaigns.append(
            Campaign(
                campaign_id=f"cmp-{i:03d}",
                budget=budget,
                flight_start=start,
                flight_end=end,
                daily_budget=round(budget / days, 2),
                target_geo=r.choice(GEOS),
                target_device=r.choice(DEVICES),
            )
        )
    return campaigns
