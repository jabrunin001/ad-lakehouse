from datetime import datetime, timezone

from fastapi import FastAPI

from api.campaigns import build_campaigns

app = FastAPI(title="ad-lakehouse campaigns API")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/campaigns")
def list_campaigns() -> list[dict]:
    today = datetime.now(timezone.utc).date()
    return [c.model_dump(mode="json") for c in build_campaigns(today)]
