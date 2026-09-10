"""Демо-приёмник: умеет отвечать с ошибкой, но учитывает каждое событие один раз."""

import asyncio
import hashlib
import json
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import text

from .config import settings
from .db import engine
from .security import verify_signature

app = FastAPI(title="HookRelay demonstration receiver")
scenarios = {}


class Scenario(BaseModel):
    delay_first: int = Field(default=0, ge=0, le=10)
    delay_after_commit_seconds: float = Field(default=0, ge=0, le=10)
    secret: str = Field(min_length=20, max_length=200)
    fail_first: int = Field(default=0, ge=0, le=100)
    fail_status: int = Field(default=503, ge=400, le=599)
    success_status: int = Field(default=200, ge=200, le=599)


def authorized(token):
    import hmac

    if not hmac.compare_digest(token or "", settings.demo_receiver_token):
        raise HTTPException(401, "Invalid demo control token")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.put("/control/{scenario_id}")
async def control(
    scenario_id: UUID, data: Scenario, x_demo_token: str | None = Header(default=None)
):
    authorized(x_demo_token)
    scenarios[scenario_id] = {**data.model_dump(), "calls": 0}
    return {"id": scenario_id, "configured": True}


@app.get("/control/{scenario_id}")
async def stats(scenario_id: UUID, x_demo_token: str | None = Header(default=None)):
    authorized(x_demo_token)
    async with engine.connect() as conn:
        result = (
            (
                await conn.execute(
                    text(
                        "SELECT count(*) AS unique_effects,COALESCE(sum(received_count),0) AS accepted_requests FROM demo_received_events WHERE scenario_id=:id"
                    ),
                    {"id": scenario_id},
                )
            )
            .mappings()
            .one()
        )
    return {"calls": scenarios.get(scenario_id, {}).get("calls", 0), **dict(result)}


@app.post("/hooks/{scenario_id}")
async def receive(scenario_id: UUID, request: Request):
    scenario = scenarios.get(scenario_id)
    if scenario is None:
        raise HTTPException(404, "Unknown scenario")
    body = await request.body()
    if len(body) > 70000:
        raise HTTPException(413, "Payload too large")
    if not verify_signature(
        scenario["secret"],
        request.headers.get("X-Webhook-Timestamp", ""),
        body,
        request.headers.get("X-Webhook-Signature", ""),
    ):
        raise HTTPException(401, "Invalid webhook signature")
    try:
        data = json.loads(body)
        event_id = UUID(data["id"])
        if str(event_id) != request.headers.get("X-Webhook-Id"):
            raise ValueError("Event ID mismatch")
    except (ValueError, KeyError, TypeError):
        raise HTTPException(400, "Invalid webhook envelope") from None
    scenario["calls"] += 1
    call_number = scenario["calls"]
    if scenario["calls"] <= scenario["fail_first"]:
        raise HTTPException(scenario["fail_status"], "Injected temporary receiver failure")
    if not 200 <= scenario["success_status"] < 300:
        raise HTTPException(scenario["success_status"], "Injected permanent receiver failure")
    digest = hashlib.sha256(body).hexdigest()
    async with engine.begin() as conn:
        stored = await conn.scalar(
            text("""INSERT INTO demo_received_events(scenario_id,event_id,body_hash)
            VALUES (:scenario,:event,:hash) ON CONFLICT(scenario_id,event_id)
            DO UPDATE SET received_count=demo_received_events.received_count+1
            RETURNING body_hash"""),
            {"scenario": scenario_id, "event": event_id, "hash": digest},
        )
        if stored != digest:
            raise HTTPException(409, "Event ID reused with another body")
    if call_number <= scenario["delay_first"]:
        await asyncio.sleep(scenario["delay_after_commit_seconds"])
    return {"accepted": True, "event_id": event_id}
