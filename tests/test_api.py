from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)


def test_health():
    assert client.get("/health").json() == {"status": "ok"}


def test_campaigns_returns_twenty_with_fields():
    r = client.get("/campaigns")
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 20
    first = body[0]
    for field in ["campaign_id", "budget", "flight_start", "flight_end",
                  "daily_budget", "target_geo", "target_device"]:
        assert field in first
    assert first["campaign_id"] == "cmp-001"
