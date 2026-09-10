import json
import secrets
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from pydantic import BaseModel, Field
from sqlalchemy import text

from .auth import User, current_user
from .auth import router as auth_router
from .db import engine
from .observability import instrument
from .security import cipher, validate_url
from .service import publish_event


@asynccontextmanager
async def lifespan(app):
    cipher()  # Проверяем ключ при запуске, чтобы ошибка не всплыла при первой доставке.
    yield
    await engine.dispose()


app = FastAPI(
    title="HookRelay",
    version="0.1.0",
    lifespan=lifespan,
    description="Signed webhook delivery with durable scheduling, bounded retries and replay.",
)
app.include_router(auth_router)
instrument(app)


class EndpointInput(BaseModel):
    url: str = Field(min_length=8, max_length=2048)
    event_types: list[Annotated[str, Field(pattern=r"^[a-z][a-z0-9_.-]{1,99}$")]] = Field(
        min_length=1, max_length=30
    )


class EndpointUpdate(BaseModel):
    enabled: bool


class EventInput(BaseModel):
    type: str = Field(pattern=r"^[a-z][a-z0-9_.-]{1,99}$")
    data: dict


@app.get("/health", tags=["Operations"])
async def health():
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
        workers = (
            (
                await conn.execute(
                    text(
                        "SELECT name,extract(epoch FROM clock_timestamp()-seen_at) AS age FROM worker_heartbeats"
                    )
                )
            )
            .mappings()
            .all()
        )
    return {
        "status": "ok",
        "database": "ok",
        "workers": {r["name"]: float(r["age"]) for r in workers},
    }


@app.post("/endpoints", status_code=201, tags=["Endpoints"])
async def endpoint(data: EndpointInput, user: User = Depends(current_user)):
    try:
        validate_url(data.url)
    except ValueError as error:
        raise HTTPException(422, str(error)) from None
    secret = secrets.token_urlsafe(32)
    async with engine.begin() as conn:
        # Блокируем аккаунт, чтобы два запроса не обошли лимит получателей.
        await conn.execute(text("SELECT id FROM users WHERE id=:id FOR UPDATE"), {"id": user.id})
        count = await conn.scalar(
            text("SELECT count(*) FROM endpoints WHERE user_id=:user"), {"user": user.id}
        )
        if count >= 50:
            raise HTTPException(409, "Endpoint quota exceeded (50)")
        row = (
            (
                await conn.execute(
                    text("""INSERT INTO endpoints(id,user_id,url,secret_ciphertext,event_types)
            VALUES (:id,:user,:url,:secret,CAST(:types AS jsonb))
            RETURNING id,url,event_types,enabled,created_at"""),
                    {
                        "id": uuid4(),
                        "user": user.id,
                        "url": data.url,
                        "secret": cipher().encrypt(secret.encode()).decode(),
                        "types": json.dumps(sorted(set(data.event_types))),
                    },
                )
            )
            .mappings()
            .one()
        )
        return {**dict(row), "signing_secret": secret}


@app.get("/endpoints", tags=["Endpoints"])
async def endpoints(user: User = Depends(current_user)):
    async with engine.connect() as conn:
        return [
            dict(r)
            for r in (
                await conn.execute(
                    text(
                        "SELECT id,url,event_types,enabled,created_at FROM endpoints WHERE user_id=:user ORDER BY created_at"
                    ),
                    {"user": user.id},
                )
            ).mappings()
        ]


@app.patch("/endpoints/{endpoint_id}", tags=["Endpoints"])
async def update_endpoint(
    endpoint_id: UUID, data: EndpointUpdate, user: User = Depends(current_user)
):
    async with engine.begin() as conn:
        row = (
            (
                await conn.execute(
                    text(
                        "UPDATE endpoints SET enabled=:enabled WHERE id=:id AND user_id=:user RETURNING id,url,event_types,enabled"
                    ),
                    {"enabled": data.enabled, "id": endpoint_id, "user": user.id},
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise HTTPException(404, "Endpoint not found")
        return dict(row)


@app.post("/events", status_code=202, tags=["Events"])
async def event(
    data: EventInput,
    response: Response,
    idempotency_key: Annotated[
        str, Header(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    ],
    user: User = Depends(current_user),
):
    result, replay = await publish_event(user.id, data.type, data.data, idempotency_key)
    response.status_code = 200 if replay else 202
    response.headers["Idempotency-Replayed"] = str(replay).lower()
    return result


@app.get("/deliveries", tags=["Deliveries"])
async def deliveries(
    user: User = Depends(current_user),
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
):
    async with engine.connect() as conn:
        return [
            dict(r)
            for r in (
                await conn.execute(
                    text("""SELECT d.id,d.event_id,d.endpoint_id,d.status,d.attempts,d.cycle_attempts,d.last_error,d.not_before,d.created_at
            FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.user_id=:user
            ORDER BY d.created_at DESC,d.id LIMIT :limit OFFSET :offset"""),
                    {"user": user.id, "limit": limit, "offset": offset},
                )
            ).mappings()
        ]


async def owned_delivery(conn, delivery_id, user_id, lock=False):
    suffix = " FOR UPDATE OF d" if lock else ""
    row = (
        (
            await conn.execute(
                text(
                    """SELECT d.* FROM deliveries d JOIN events e ON e.id=d.event_id
        WHERE d.id=:id AND e.user_id=:user"""
                    + suffix
                ),
                {"id": delivery_id, "user": user_id},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise HTTPException(404, "Delivery not found")
    return row


@app.get("/deliveries/{delivery_id}", tags=["Deliveries"])
async def delivery(delivery_id: UUID, user: User = Depends(current_user)):
    async with engine.connect() as conn:
        row = await owned_delivery(conn, delivery_id, user.id)
        attempts = [
            dict(r)
            for r in (
                await conn.execute(
                    text(
                        "SELECT attempt_no,result,status_code,error,started_at,finished_at FROM attempts WHERE delivery_id=:id ORDER BY attempt_no"
                    ),
                    {"id": delivery_id},
                )
            ).mappings()
        ]
        return {**dict(row), "history": attempts}


@app.post("/deliveries/{delivery_id}/replay", tags=["Deliveries"])
async def replay(delivery_id: UUID, user: User = Depends(current_user)):
    async with engine.begin() as conn:
        row = await owned_delivery(conn, delivery_id, user.id, lock=True)
        if row["status"] not in ("dead", "cancelled"):
            raise HTTPException(409, "Only dead or cancelled deliveries may be replayed")
        enabled = await conn.scalar(
            text("SELECT enabled FROM endpoints WHERE id=:id"), {"id": row["endpoint_id"]}
        )
        if not enabled:
            raise HTTPException(409, "Enable the endpoint before replaying")
        await conn.execute(
            text(
                "UPDATE deliveries SET status='pending',cycle_attempts=0,generation=NULL,lease_until=NULL,not_before=clock_timestamp(),last_error=NULL,updated_at=clock_timestamp() WHERE id=:id"
            ),
            {"id": delivery_id},
        )
    return {"id": delivery_id, "status": "pending"}
